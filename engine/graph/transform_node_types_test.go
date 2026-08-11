package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// transformNodeTypes 与 bannedBodyMemberTypes 之间的派生关系必须成立：
// transform 节点按定义是逐项跑 body 的，把一个 transform 节点嵌进另一个的 body
// 正是这条禁令要防的递归。
//
// 这条钉的是 DERIVATION 而不是当下的成员表。把 bannedBodyMemberTypes 写回字面量
// 常量（看起来更直白）会让「新增一种 transform 节点」这个一行改动，静默地
// 把它变成可嵌套的——递归 fan-out 让子执行树无界，正是 v1 明确不做的事。
// 那种回退不会有任何编译错误，只有这条测试会红。
func TestEveryTransformTypeIsBannedFromBodies(t *testing.T) {
	if len(transformNodeTypes) == 0 {
		t.Fatal("transformNodeTypes is empty: no node type takes an expression-or-body " +
			"pair at all, which would make projectNodeBodies dead code")
	}
	for nodeType := range transformNodeTypes {
		if !bannedBodyMemberTypes[nodeType] {
			t.Errorf("transform node type %q is not in bannedBodyMemberTypes: it may "+
				"therefore be nested inside another node's body, and the sub-execution "+
				"tree the durable layer and package hash are sized for becomes unbounded",
				nodeType)
		}
	}
}

// xflow.http 也声明了一个叫 "body" 的参数（node/internal/action/http.go），
// 但它不是 transform 节点，那个 body 是 HTTP 请求体、不是子图。编译器据以判断
// 「要不要投影」的必须是显式的 transform 类型集，不能嗅探参数名——嗅参数名会把
// 每个 HTTP 节点的请求体送进子图编译。
//
// 这里给的 body 是一个合法的 HTTP 请求体、同时是一个非法的子图（没有 nodes、
// 也不是 xflow.subgraph）。若判据退化成嗅参数名，Compile 会在 validateNodeBody
// 里报错；判据正确时它干净通过，且节点不带投影出来的包。
func TestHTTPBodyParameterIsNotProjectedAsASubgraph(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "call", Type: "xflow.http", Parameters: map[string]any{
				"method": "POST",
				"url":    "https://example.invalid/v1/things",
				"body":   map[string]any{"name": "thing", "count": 3},
			}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("an xflow.http node's request body must not be compiled as a sub-graph, "+
			"but compilation failed: %v", err)
	}
	idx, ok := g.NodeIndex("call")
	if !ok {
		t.Fatal("compiled graph has no node \"call\"")
	}
	if body := g.BodyAt(idx); body != nil {
		t.Fatalf("the http node came out with a projected body package (%+v): its "+
			"\"body\" parameter is a request payload and must stay an opaque parameter", body)
	}
	// 参数本身必须原样留在节点上——投影会把 body 从 Parameters 里搬走，
	// 若这里丢了，HTTP 节点运行期就发不出请求体了。
	params := g.NodeAt(idx).Parameters
	if _, present := params["body"]; !present {
		t.Fatal("the http node's \"body\" parameter was consumed during compilation: " +
			"the request payload must reach the handler intact")
	}
}

// 「expression 与 body 二选一」是 transform 节点这个类的契约
// （types.TransformSpec 声明的就是这个形状），不是 xflow.map 的特例。
// validateNodeBody 对该集合里的每个类型一视同仁地执行它。
//
// 这条测试对 transformNodeTypes 里的**每一个**类型跑一遍三种形态，所以将来加进
// filter / reduce / sort 时，它们各自的「二选一」自动被覆盖，无需再写一遍测试。
// 若哪天有人给某个类型开后门（例如按类型跳过这条规则），这里会红。
func TestEveryTransformTypeEnforcesExpressionXorBody(t *testing.T) {
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
						t.Fatalf("%q with %s must be rejected: expression and body are "+
							"mutually exclusive for every transform node, not just xflow.map",
							nodeType, tc.name)
					}
					if !tc.wantErr && err != nil {
						t.Fatalf("%q with %s must compile: %v", nodeType, tc.name, err)
					}
				})
			}
		})
	}
}
