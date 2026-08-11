package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// 一个 fan-out 节点必须在编译期就带着已投影的 body，或者带着一个 expression。
// 两者都没有则拒绝。
//
// 这条规则的承重之处不是整洁，而是**运行期判据以它为前提**：引擎决定「这个输出
// 是 fan-out 描述符还是一个值」时读的是 g.BodyAt(idx) != nil，不再嗅输出里的
// 标记键。两种形态各自与这个判据配套：body 形态投影出包、handler 发描述符、
// 引擎扩展；expression 形态不投影、handler 就地算完 results/count、引擎原样提交。
// 而两者皆无的节点两头不靠——handler 会照 body 形态发出描述符，引擎却判定
// 「不扩展」，于是把 {items, batches, ...} 当成这个节点的普通成功输出提交下去，
// 下游读到 items/batches 而不是契约承诺的 results/count。静默的错答案。
func TestMapWithNeitherBodyNorExpressionIsRejectedAtCompileTime(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{
			// ErrNoMapBody 的 doc 曾把这个形状记作「compile-time-legal shape
			// with no runtime meaning」，并说 T11 的编译规则刻意豁免它。豁免
			// 到此为止：没有运行期含义的形状不该编译通过。
			name:   "parameterless",
			params: nil,
			want:   "body",
		},
		{
			name:   "items only, no body and no expression",
			params: map[string]any{"items": "$input.rows"},
			want:   "body",
		},
		{
			// 空字符串的 expression 不算声明了 expression：handler 对它走 body
			// 分支发描述符，正是上面描述的两头不靠。
			name:   "empty expression",
			params: map[string]any{"items": "$input.rows", "expression": ""},
			want:   "expression",
		},
		{
			// body 在，但不是子图（笔误而非载荷）。
			name:   "body that is not a sub-graph",
			params: map[string]any{"items": "$input.rows", "body": map[string]any{"n": 1}},
			want:   "body",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
				{Name: "m", Type: "xflow.map", Parameters: tc.params},
			}}
			_, err := Compile(def)
			if err == nil {
				t.Fatalf("an xflow.map node with neither a projected body nor an expression "+
					"compiled: at run time its handler still emits a fan-out descriptor, and an "+
					"engine that decides expansion from the compiled shape commits that "+
					"descriptor as this node's ordinary output — the body never runs and "+
					"downstream reads items/batches where the contract promises results/count "+
					"(params: %v)", tc.params)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("rejection message %q does not mention %q, so it does not tell the "+
					"author what to change", err, tc.want)
			}
		})
	}
}

// 正对照之二：合法的 body 形态必须仍然编译，且必须真的投影出包。没有它，一个
// 「拒绝一切 xflow.map」的实现能让上面每一条断言全绿。
//
// 另一半正对照——expression 形态编译通过且**不**投影 body——在
// node_body_package_test.go 的 TestCompileLeavesAnExpressionMapNodeWithoutAPackage，
// 与它「有 body 就有包」的成对邻居放在一起。
func TestMapWithASubgraphBodyStillCompilesAndProjects(t *testing.T) {
	def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
		{Name: "m", Type: "xflow.map", Parameters: map[string]any{
			"items": "$input.rows",
			"body":  subgraphBody(),
		}},
	}}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("the body form of xflow.map must compile: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal(`compiled graph has no node "m"`)
	}
	if g.BodyAt(idx) == nil {
		t.Fatal("compiled with no projected body package: the engine decides expansion " +
			"from exactly this field, so a nil here means the fan-out descriptor would be " +
			"committed as an ordinary output and the body would never run")
	}
}
