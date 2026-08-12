package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestInputNodes_ExecutedUpstream asserts that $nodes contains the output of an
// executed upstream node, keyed by name.
func TestInputNodes_ExecutedUpstream(t *testing.T) {
	// A -> B; B references $nodes['A']. A's output is {"value": 42}.
	def := &types.WorkflowDef{
		Name: "input-nodes",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop", Parameters: map[string]any{
				"data": "${{ $nodes['A'].value }}",
			}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	execID := types.ExecutionID("exec-1")
	state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  g,
	})
	// A has already executed and produced output.
	state.PutOutput(context.Background(), execID, "A", map[string]any{"value": 42})

	bIdx, _ := g.NodeIndex("B")
	task := &Task{
		ExecutionID: execID,
		NodeName:    "B",
		NodeIdx:     bIdx,
		Type:        TaskTypeNodeExec,
	}
	input, err := eng.buildInput(context.Background(), task, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}
	if input.Nodes == nil {
		t.Fatal("input.Nodes is nil, expected map with key \"A\"")
	}
	aData, ok := input.Nodes["A"]
	if !ok {
		t.Fatal("input.Nodes has no key \"A\"")
	}
	aMap, isMap := aData.(map[string]any)
	if !isMap {
		t.Fatalf("input.Nodes[\"A\"] type = %T, want map[string]any", aData)
	}
	if aMap["value"] != 42 {
		t.Fatalf("input.Nodes[\"A\"][\"value\"] = %v, want 42", aMap["value"])
	}
}

// TestInputNodes_UnexecutedIsTypedNilMap asserts that a node which has NOT
// executed (GetOutput returns nil, nil) appears in Input.Nodes as a key with a
// typed nil map value — NOT as an absent key and NOT as an untyped nil.
//
// This distinction is load-bearing: spec §4.2 recommends
//   ${{ $nodes['optional_step'].name ?? 'default' }}
// which requires member access on the value. The ?? operator rescues a typed
// nil map (.name returns nil → ?? fires) but CANNOT rescue:
//   - absent key: the $nodes['optional_step'] expression itself errors
//   - untyped nil: .name errors with "cannot fetch name from <nil>"
//
// See brief finding 2 for the full truth table.
func TestInputNodes_UnexecutedIsTypedNilMap(t *testing.T) {
	// A -> C; C references $nodes['B']; B is NOT upstream of C.
	// We craft a graph where B exists but is on a different branch that did
	// not execute. Since B has no output stored, GetOutput returns nil, nil.
	def := &types.WorkflowDef{
		Name: "typed-nil",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.noop"},
			{Name: "router", Type: "xflow.switch", Parameters: map[string]any{
				"mode": "rules",
				"rules": []any{
					map[string]any{"condition": "true", "output": "left"},
					map[string]any{"condition": "false", "output": "right"},
				},
				"default_output": "right",
			}},
			{Name: "B", Type: "test.noop"},
			{Name: "C", Type: "test.noop", Parameters: map[string]any{
				"data": "${{ $nodes['B'].result ?? 'default' }}",
			}},
		},
		Connections: types.Connections{
			"start":  {"main": {Targets: []types.Connection{{Node: "router", Input: "main"}}}},
			"router": {
				"left":  {Targets: []types.Connection{{Node: "B", Input: "main"}}},
				"right": {Targets: []types.Connection{{Node: "C", Input: "main"}}},
			},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	execID := types.ExecutionID("exec-2")
	state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  g,
	})
	// B has NOT executed — no PutOutput call.

	cIdx, _ := g.NodeIndex("C")
	task := &Task{
		ExecutionID: execID,
		NodeName:    "C",
		NodeIdx:     cIdx,
		Type:        TaskTypeNodeExec,
	}
	input, err := eng.buildInput(context.Background(), task, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}
	if input.Nodes == nil {
		t.Fatal("input.Nodes is nil, expected map with key \"B\"")
	}
	v, ok := input.Nodes["B"]
	if !ok {
		t.Fatal("未执行的节点必须以键存在的形式出现,否则 spec 推荐的 $nodes['B'].f ?? 'd' 会在运行期报错")
	}
	if _, isMap := v.(map[string]any); !isMap {
		t.Fatalf("未执行节点的值必须是 map[string]any(nil) 而非 untyped nil:"+
			"untyped nil 下 .f ?? 'd' 报 cannot fetch f from <nil>,实测见 brief 发现 2;got %T", v)
	}
}
