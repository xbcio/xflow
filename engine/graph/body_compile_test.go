package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// mapNode 造一个 xflow.map 节点，parameters 由调用方补。
func mapNode(params map[string]any) types.NodeDef {
	return types.NodeDef{Name: "m", Type: "xflow.map", Parameters: params}
}

// subgraphBody 造一个最小合法 body：单节点、入口唯一。
func subgraphBody() map[string]any {
	return map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{
				map[string]any{"name": "inner", "type": "xflow.noop"},
			},
		},
	}
}

// expression 与 body 恰好有一个。这条做在编译期而非 Execute 里：
// 运行期嗅探形状本仓库已经被坑过一次（ScriptNode 嗅 messages 键私搭平行
// fan-out 通路），不再重复。
func TestCompile_MapRequiresExactlyOneOfExpressionOrBody(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
	}{
		{"both", map[string]any{
			"items":      "$input.rows",
			"expression": "${{ $item.id }}",
			"body":       subgraphBody(),
		}},
		{"neither", map[string]any{
			"items": "$input.rows",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{mapNode(tc.params)}}
			if _, err := Compile(def); err == nil {
				t.Fatal("expression and body must be mutually exclusive at compile time")
			}
		})
	}

	t.Run("exactly one compiles", func(t *testing.T) {
		def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
			mapNode(map[string]any{"items": "$input.rows", "body": subgraphBody()}),
		}}
		if _, err := Compile(def); err != nil {
			t.Fatalf("a map with exactly one of the two must compile: %v", err)
		}
	})
}

// xflow.subgraph 是 body 专用类型，顶层 nodes: 里出现要拒绝 ——
// 它没有 unit 层语义，放顶层会成为入度 0 的根 unit 被自动调度。
// 形状照 compile.go:99-107 拒绝 ReservedNodeTypePrefix 的既有做法。
func TestCompile_SubgraphTypeRejectedAtTopLevel(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "loose", Type: "xflow.subgraph", Parameters: map[string]any{
				"nodes": []any{map[string]any{"name": "inner", "type": "xflow.noop"}},
			}},
		},
	}
	if _, err := Compile(def); err == nil {
		t.Fatal("xflow.subgraph at the top level must be rejected: it has no unit-layer " +
			"semantics and would be scheduled as an in-degree-0 root unit")
	}
}

// 第一版禁止嵌套：递归 fan-out 让子执行树无界，hash 与耐久层级都要重做。
func TestCompile_SubgraphBodyRejectsNestedExpansion(t *testing.T) {
	nested := map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{
				map[string]any{"name": "inner_map", "type": "xflow.map", "parameters": map[string]any{
					"items": "$item.rows",
					"body":  subgraphBody(),
				}},
			},
		},
	}
	def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
		mapNode(map[string]any{"items": "$input.rows", "body": nested}),
	}}
	if _, err := Compile(def); err == nil {
		t.Fatal("a map inside a body must be rejected in v1: recursive fan-out makes " +
			"the sub-execution tree unbounded")
	}
}

// body 入口唯一 + 入口支配全部成员 —— 复用 resolveGroupEntry /
// assertEntryDominates，不新写校验器。
func TestCompile_BodyEntryMustBeUniqueAndDominating(t *testing.T) {
	// 两个互不相连的节点 => 两个入口 => resolveGroupEntry 的 len(external) > 1 报错
	twoEntries := map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{
				map[string]any{"name": "a", "type": "xflow.noop"},
				map[string]any{"name": "b", "type": "xflow.noop"},
			},
		},
	}
	def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
		mapNode(map[string]any{"items": "$input.rows", "body": twoEntries}),
	}}
	if _, err := Compile(def); err == nil {
		t.Fatal("a body with two disconnected entries must be rejected, " +
			"the same way resolveGroupEntry rejects it for groups")
	}
}

// I2: a body member of a non-portable type (xflow.local/xflow.closure/
// xflow.inline) must be rejected exactly the way validateGroupPortability
// rejects it for a group member — the design's own fact-check listed this as
// one of three ready-made validators to reuse for bodies, and
// compileBodyMembers only reused two (resolveGroupEntry, assertEntryDominates).
// Before this fix a body containing an xflow.closure member compiled cleanly:
// it cannot actually run on a remote runner (execution/subgraph is engine-
// agnostic but the xflow.local/xflow.closure node handler genuinely is
// process-local), so an operator would only discover this gap the same way
// C1's supply gap surfaced -- at the first batch, not at deploy time.
func TestCompile_BodyRejectsNonPortableMemberType(t *testing.T) {
	cases := []string{"xflow.local", "xflow.closure", "xflow.inline"}
	for _, nonPortable := range cases {
		t.Run(nonPortable, func(t *testing.T) {
			body := map[string]any{
				"type": "xflow.subgraph",
				"parameters": map[string]any{
					"nodes": []any{
						map[string]any{"name": "inner", "type": nonPortable},
					},
				},
			}
			def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
				mapNode(map[string]any{"items": "$input.rows", "body": body}),
			}}
			_, err := Compile(def)
			if err == nil {
				t.Fatalf("a body member of type %q must be rejected: it cannot run "+
					"on a remote runner, the same reason validateGroupPortability "+
					"rejects it for a group member", nonPortable)
			}
			if !strings.Contains(err.Error(), "non-portable") {
				t.Errorf("error = %v, want mention of \"non-portable\"", err)
			}
			// The error must say "body", not "group": an operator debugging a
			// rejected deploy has no other way to tell which construct failed,
			// since compileBodyMembers reuses the SAME validator group
			// compilation uses.
			if !strings.Contains(err.Error(), "body") {
				t.Errorf("error = %v, want mention of \"body\" (not \"group\") so an "+
					"operator can tell which construct failed", err)
			}
		})
	}
}

