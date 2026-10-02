package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// fanInFixture builds A -> M and B -> M, with the destination port of each edge
// under the caller's control, and returns the assembled input for M.
func fanInFixture(t *testing.T, aPort, bPort string) *types.Input {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "input-ports",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "M", Type: "test.noop"},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M", Input: aPort}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "M", Input: bPort}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	state := newFakeState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-ports")
	state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  g,
	})
	state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"})
	state.PutOutput(context.Background(), execID, "B", map[string]any{"from": "b"})

	mIdx, ok := g.NodeIndex("M")
	if !ok {
		t.Fatal("node M is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "M",
		NodeIdx:     mIdx,
		Type:        TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}
	return input
}

// portOf returns the payload collected under one input port.
func portOf(t *testing.T, input *types.Input, port string) map[string]any {
	t.Helper()
	raw, ok := input.Inputs[port]
	if !ok {
		t.Fatalf("input.Inputs has no key %q; got keys %v", port, keysOf(input.Inputs))
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("input.Inputs[%q] type = %T, want map[string]any", port, raw)
	}
	return payload
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestInputPorts_FanInIsKeyedByPort is the contract the DSL documents: with
// several incoming edges, each upstream's data is read under the port its
// connection declares. Keying by upstream node name instead would make
// docs/dsl-samples' expressions -- and the spec's -- read nil at runtime.
func TestInputPorts_FanInIsKeyedByPort(t *testing.T) {
	input := fanInFixture(t, "inventory", "price")

	if got := portOf(t, input, "inventory")["from"]; got != "a" {
		t.Fatalf("$inputs.inventory.from = %v, want a", got)
	}
	if got := portOf(t, input, "price")["from"]; got != "b" {
		t.Fatalf("$inputs.price.from = %v, want b", got)
	}
	if len(input.Inputs) != 2 {
		t.Fatalf("len(input.Inputs) = %d, want 2 (one key per declared port): %v", len(input.Inputs), keysOf(input.Inputs))
	}
	// Neither edge targets main, so $input ($inputs.main) has nothing to read.
	if input.Data != nil {
		t.Fatalf("input.Data = %v, want nil: no edge targets the main port", input.Data)
	}
}

// TestInputPorts_UnlabelledEdgesShareTheMainPort pins the implicit-main rule
// and the consequence the compiler warns about: two upstreams that name no port
// both land on main, so only one payload survives.
func TestInputPorts_UnlabelledEdgesShareTheMainPort(t *testing.T) {
	input := fanInFixture(t, "", "")

	if len(input.Inputs) != 1 {
		t.Fatalf("len(input.Inputs) = %d, want 1 (both edges target the implicit main port): %v", len(input.Inputs), keysOf(input.Inputs))
	}
	// Last write wins among non-nil payloads; B's edge is compiled last.
	if got := portOf(t, input, types.DefaultInputPort)["from"]; got != "b" {
		t.Fatalf("$inputs.main.from = %v, want b (the last non-nil edge)", got)
	}
	// $input is the main port, so an unlabelled fan-in still feeds it.
	if got := input.Data["from"]; got != "b" {
		t.Fatalf("input.Data.from = %v, want b: $input reads the main port", got)
	}
}

// TestInputPorts_ASkippedUpstreamDoesNotEraseThePort is the reason the fan-in
// loop is not a plain last-write-wins: an upstream that never ran has no output,
// and if that nil were allowed to win, "one arm of the branch did not run" would
// read as "this port carries nothing".
func TestInputPorts_ASkippedUpstreamDoesNotEraseThePort(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "input-ports-skipped",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "M", Type: "test.noop"},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "M"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	state := newFakeState()
	eng := New(state, &fakeQueue{})
	execID := types.ExecutionID("exec-skipped")
	state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  g,
	})
	// A ran, B did not -- the shape of a branch whose other arm was skipped.
	state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"})

	mIdx, _ := g.NodeIndex("M")
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "M",
		NodeIdx:     mIdx,
		Type:        TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	if got := portOf(t, input, types.DefaultInputPort)["from"]; got != "a" {
		t.Fatalf("$inputs.main.from = %v, want a: the skipped upstream's nil overwrote the payload", got)
	}
	if got := input.Data["from"]; got != "a" {
		t.Fatalf("input.Data.from = %v, want a", got)
	}
}

// TestInputPorts_SingleEdgeKeepsItsPayloadAsData guards the shape every handler
// authored before ports existed depends on: one incoming edge means the upstream
// output IS the node's data, labels or not.
func TestInputPorts_SingleEdgeKeepsItsPayloadAsData(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "input-ports-single",
		Nodes: []types.NodeDef{{Name: "A", Type: "test.noop"}, {Name: "M", Type: "test.noop"}},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M", Input: "custom"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	execID := types.ExecutionID("exec-single")
	state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
		Graph:  g,
	})
	state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"})

	mIdx, _ := g.NodeIndex("M")
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "M",
		NodeIdx:     mIdx,
		Type:        TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	if got := input.Data["from"]; got != "a" {
		t.Fatalf("input.Data.from = %v, want a: a labelled single edge still delivers the payload", got)
	}
	if input.Inputs != nil {
		t.Fatalf("input.Inputs = %v, want nil: $inputs is the multi-edge lookup, not a second copy of $input", keysOf(input.Inputs))
	}
}
