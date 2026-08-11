package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// 编译器据以判断「这个 body 是不是子图」的判据必须是**值的形状**，不是节点类型、
// 也不是参数名。这条测试直接钉 declaresSubgraphBody 对本仓库里实际出现过的每一种
// body 取值的判定。
//
// 参数名不可用：xflow.http 也有一个叫 body 的参数，那是请求体。
// 节点类型不可用：见 TestSubgraphBodyCriterionIsSharedByCompileAndSnapshot——
// 类型白名单会与快照守卫的判据漂移，而那次漂移是真事故不是假想。
func TestDeclaresSubgraphBody_KeysOffTheValueNotTheName(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   bool
	}{
		{"no body at all", map[string]any{"url": "https://example.invalid"}, false},
		{"http object body", map[string]any{"body": map[string]any{"name": "a", "count": 3}}, false},
		{"http string body", map[string]any{"body": `{"a":1}`}, false},
		{"http array body", map[string]any{"body": []any{1, 2}}, false},
		{"nil body", map[string]any{"body": nil}, false},
		// 形似 NodeDef 但 type 不对：最接近的误判风险，必须仍然为 false。
		{"node-shaped body of another type", map[string]any{
			"body": map[string]any{"type": "xflow.http", "name": "x"},
		}, false},
		{"real subgraph body", map[string]any{"body": subgraphBody()}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := declaresSubgraphBody(tc.params); got != tc.want {
				t.Fatalf("declaresSubgraphBody(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// 编译期的投影判据与快照解码期的 fail-closed 判据必须是**同一个**，否则一侧认为
// 「这节点没 body、不投影」，另一侧认为「这节点声明了 body、却没带包」——图存下去
// 就再也读不回来。
//
// 这不是假想的漂移：修复前 snapshot.go 嗅 Parameters["body"] 是否存在，compile.go
// 按节点类型集判断，于是任何带 JSON 请求体的 xflow.http 节点都能编译、能持久化，
// 然后在 LoadGraph 处必然失败（rstate 那条路没有重编译回落，任务就地失败）。
//
// 覆盖 http 的 object / string 两种请求体：前者能解成 NodeDef（Type 为空），
// 后者根本解不动，两条分支不同。
func TestSubgraphBodyCriterionIsSharedByCompileAndSnapshot(t *testing.T) {
	bodies := map[string]any{
		"object": map[string]any{"name": "thing", "count": 3},
		"string": `{"name":"thing"}`,
	}
	for shape, payload := range bodies {
		t.Run(shape, func(t *testing.T) {
			def := &types.WorkflowDef{
				Name: "wf",
				Nodes: []types.NodeDef{
					{Name: "call", Type: "xflow.http", Parameters: map[string]any{
						"method": "POST",
						"url":    "https://example.invalid/v1/things",
						"body":   payload,
					}},
				},
			}
			g, err := Compile(def)
			if err != nil {
				t.Fatalf("an xflow.http node's request body must not be compiled as a "+
					"sub-graph, but compilation failed: %v", err)
			}
			idx, ok := g.NodeIndex("call")
			if !ok {
				t.Fatal(`compiled graph has no node "call"`)
			}
			if body := g.BodyAt(idx); body != nil {
				t.Fatalf("the http node came out with a projected body package (%+v): its "+
					`"body" parameter is a request payload and must stay an opaque parameter`, body)
			}
			// 参数本身必须原样留在节点上——若这里丢了，HTTP 节点运行期发不出请求体。
			if _, present := g.NodeAt(idx).Parameters["body"]; !present {
				t.Fatal(`the http node's "body" parameter was consumed during compilation: ` +
					"the request payload must reach the handler intact")
			}

			// 编译通过还不够：图要能存下去再读回来。这一步才是判据分裂的落点。
			data, err := json.Marshal(g)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back Graph
			if err := json.Unmarshal(data, &back); err != nil {
				t.Fatalf("snapshot round-trip rejected a compiled graph whose only special "+
					"feature is an xflow.http node with a request body: %v. The compile-time "+
					"projection criterion and the snapshot fail-closed criterion have drifted "+
					"apart; every execution reaching LoadGraph fails here", err)
			}
		})
	}
}

// map 的 body 这条正路必须仍然投影、仍然带着包往返——上面那条只证明了「不该投影的
// 不投影」，若判据退化成恒 false 它照样绿。
func TestSubgraphBodyIsProjectedAndSurvivesSnapshot(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.rows",
				"body":  subgraphBody(),
			}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal(`compiled graph has no node "m"`)
	}
	if g.BodyAt(idx) == nil {
		t.Fatal("a map node declaring a sub-graph body came out with no projected package: " +
			"the engine would reject every batch with ErrNoMapBody at run time")
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Graph
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("snapshot round-trip of a real body: %v", err)
	}
	if back.BodyAt(idx) == nil {
		t.Fatal("the projected body package did not survive the snapshot round trip")
	}
}

// 嵌套禁令必须按**成员自己的参数**判定，而不是只查一张类型表。类型表管不到
// 「将来某个节点类型长出了 body」——那个类型不在表里，于是它可以嵌进 body，
// 子执行树就无界了，而且没有任何编译错误。
//
// 这里的成员用一个与 map 无关的类型（xflow.noop）携带一个真子图 body：类型表
// 对它一无所知，只有值判据能拦住。
func TestBodyMemberDeclaringItsOwnSubgraphBodyIsRejected(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.rows",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name":       "nested",
								"type":       "xflow.noop",
								"parameters": map[string]any{"body": subgraphBody()},
							},
						},
					},
				},
			}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("a body member carrying a sub-graph body of its own must be rejected: " +
			"recursive fan-out makes the sub-execution tree unbounded, and the durable " +
			"layer and package hash are not sized for it")
	}
	if !strings.Contains(err.Error(), `"nested"`) {
		t.Errorf("rejection message %q does not name the offending member", err)
	}
}

// 三个保留类型不得作为 body 成员出现。它们各自的理由值判据看不见，所以这张表
// 必须留着——xflow.map 的 expression 形态根本没有 body，但它的 handler 无条件
// 发扩展标记，照样在 body 里 fan-out。
func TestReservedTypesAreRejectedAsBodyMembers(t *testing.T) {
	for _, memberType := range []string{"xflow.split", "xflow.map", subgraphNodeType} {
		t.Run(memberType, func(t *testing.T) {
			if !bannedBodyMemberTypes[memberType] {
				t.Fatalf("%q is not in bannedBodyMemberTypes", memberType)
			}
			def := &types.WorkflowDef{
				Name: "wf",
				Nodes: []types.NodeDef{
					{Name: "m", Type: "xflow.map", Parameters: map[string]any{
						"items": "$input.rows",
						"body": map[string]any{
							"type": "xflow.subgraph",
							"parameters": map[string]any{
								"nodes": []any{
									// 参数刻意留空：拦截必须来自类型本身，而不是
									// 顺带被某条参数规则挡下。
									map[string]any{"name": "inner", "type": memberType},
								},
							},
						},
					}},
				},
			}
			if _, err := Compile(def); err == nil {
				t.Fatalf("body member of type %q was accepted", memberType)
			}
		})
	}
}

// 「expression 与 body 二选一」是 transform 节点这个类的契约
// （types.TransformSpec 声明的就是这个形状），不是 xflow.map 的特例。
//
// 这条测试对 transformNodeTypes 里的**每一个**类型跑一遍，所以将来加进
// filter / reduce / sort 时，它们各自的「二选一」自动被覆盖，无需再写一遍测试。
// 若哪天有人给某个类型开后门（例如按类型跳过这条规则），这里会红。
func TestEveryTransformTypeEnforcesExpressionXorBody(t *testing.T) {
	if len(transformNodeTypes) == 0 {
		t.Fatal("transformNodeTypes is empty: no node type takes an expression-or-body " +
			"pair at all, which would make this contract dead")
	}
	for nodeType := range transformNodeTypes {
		t.Run(nodeType, func(t *testing.T) {
			cases := []struct {
				name    string
				params  map[string]any
				wantErr bool
			}{
				{"both", map[string]any{
					"items":      "$input.rows",
					"expression": "${{ $item.id }}",
					"body":       subgraphBody(),
				}, true},
				{"neither", map[string]any{"items": "$input.rows"}, true},
				// transform 节点的 body 是逐项计算本身，所以一个不是子图的 body
				// 是笔误而非载荷。不拦的话它会编译成一个没有包的节点，运行期每个
				// 批次都以 ErrNoMapBody 失败。
				{"body that is not a sub-graph", map[string]any{
					"items": "$input.rows",
					"body":  map[string]any{"name": "thing", "count": 3},
				}, true},
				{"body only", map[string]any{
					"items": "$input.rows",
					"body":  subgraphBody(),
				}, false},
				{"expression only", map[string]any{
					"items":      "$input.rows",
					"expression": "${{ $item.id }}",
				}, false},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
						{Name: "t", Type: nodeType, Parameters: tc.params},
					}}
					_, err := Compile(def)
					if tc.wantErr && err == nil {
						t.Fatalf("%q with %s must be rejected", nodeType, tc.name)
					}
					if !tc.wantErr && err != nil {
						t.Fatalf("%q with %s must compile: %v", nodeType, tc.name, err)
					}
				})
			}
		})
	}
}
