package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// inspectTrackingState records the reads Inspect performs while retaining the
// ordinary fakeState behavior for the rest of StateStore.
type inspectTrackingState struct {
	*fakeState
	graphLoads  int
	outputReads map[string]int
}

func newInspectTrackingState() *inspectTrackingState {
	return &inspectTrackingState{
		fakeState:   newFakeState(),
		outputReads: make(map[string]int),
	}
}

func (s *inspectTrackingState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.graphLoads++
	return s.fakeState.LoadGraph(ctx, id)
}

func (s *inspectTrackingState) GetOutput(
	ctx context.Context,
	id types.ExecutionID,
	name string,
) (map[string]any, error) {
	s.outputReads[inspectOutputKey(id, name)]++
	return s.fakeState.GetOutput(ctx, id, name)
}

func (s *inspectTrackingState) outputReadCount(id types.ExecutionID, name string) int {
	return s.outputReads[inspectOutputKey(id, name)]
}

func inspectOutputKey(id types.ExecutionID, name string) string {
	return string(id) + "/" + name
}

func compileInspectGraph(t *testing.T, nodes ...types.NodeDef) *graph.Graph {
	t.Helper()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "inspect-private-output",
		Nodes: nodes,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func seedInspectExecution(t *testing.T, state *inspectTrackingState, id types.ExecutionID, g *graph.Graph) {
	t.Helper()

	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusSuccess,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
}

func seedInspectNode(
	t *testing.T,
	state *inspectTrackingState,
	id types.ExecutionID,
	name string,
	privateOutput bool,
	output map[string]any,
) {
	t.Helper()

	if err := state.UpsertNode(context.Background(), &NodeSnapshot{
		ExecutionID:   id,
		Name:          name,
		Status:        types.NodeStatusSuccess,
		PrivateOutput: privateOutput,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	if err := state.PutOutput(context.Background(), id, name, output); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}
}

func TestInspectExplicitPrivateNodeRedactsOutput(t *testing.T) {
	ctx := context.Background()
	state := newInspectTrackingState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("inspect-private-explicit")
	g := compileInspectGraph(t, types.NodeDef{
		Name:   "secret",
		Type:   "test.secret",
		Kind:   types.NodeKindAction,
		Output: &types.NodeOutputPolicy{Private: true},
	})
	seedInspectExecution(t, state, id, g)
	// The graph policy, not this marker, must decide while the graph is present.
	seedInspectNode(t, state, id, "secret", false, map[string]any{"token": "top-secret"})

	detail, err := eng.Inspect(ctx, id, "secret")
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if state.graphLoads != 1 {
		t.Fatalf("LoadGraph calls = %d, want 1 for explicit inspection", state.graphLoads)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}
	if detail.Nodes[0].Output != nil {
		t.Fatalf("private node output = %#v, want nil", detail.Nodes[0].Output)
	}
	if got := state.outputReadCount(id, "secret"); got != 0 {
		t.Fatalf("GetOutput(secret) calls = %d, want 0", got)
	}
}

func TestInspectAllNodesRedactsPrivateOutput(t *testing.T) {
	ctx := context.Background()
	state := newInspectTrackingState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("inspect-private-all-nodes")
	g := compileInspectGraph(t,
		types.NodeDef{
			Name:   "secret",
			Type:   "test.secret",
			Kind:   types.NodeKindAction,
			Output: &types.NodeOutputPolicy{Private: true},
		},
		types.NodeDef{
			Name: "public",
			Type: "test.public",
			Kind: types.NodeKindAction,
		},
	)
	seedInspectExecution(t, state, id, g)
	seedInspectNode(t, state, id, "secret", false, map[string]any{"token": "top-secret"})
	seedInspectNode(t, state, id, "public", false, map[string]any{"approved": true})

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(detail.Nodes) != 2 {
		t.Fatalf("Nodes len = %d, want 2", len(detail.Nodes))
	}
	if detail.Nodes[0].Name != "secret" || detail.Nodes[0].Output != nil {
		t.Fatalf("private detail = %#v, want secret with nil output", detail.Nodes[0])
	}
	if got := detail.Nodes[1].Output["approved"]; got != true {
		t.Fatalf("public node output approved = %#v, want true", got)
	}
	if got := state.outputReadCount(id, "secret"); got != 0 {
		t.Fatalf("GetOutput(secret) calls = %d, want 0", got)
	}
	if got := state.outputReadCount(id, "public"); got != 1 {
		t.Fatalf("GetOutput(public) calls = %d, want 1", got)
	}
}

func TestInspectGraphPolicyIsAuthoritativeForPublicOutput(t *testing.T) {
	ctx := context.Background()
	state := newInspectTrackingState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("inspect-public-authoritative")
	g := compileInspectGraph(t, types.NodeDef{
		Name: "public",
		Type: "test.public",
		Kind: types.NodeKindAction,
	})
	seedInspectExecution(t, state, id, g)
	// A stale marker cannot override a resolved public graph policy.
	seedInspectNode(t, state, id, "public", true, map[string]any{"approved": true})

	detail, err := eng.Inspect(ctx, id, "public")
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}
	if got := detail.Nodes[0].Output["approved"]; got != true {
		t.Fatalf("public node output approved = %#v, want true", got)
	}
	if got := state.outputReadCount(id, "public"); got != 1 {
		t.Fatalf("GetOutput(public) calls = %d, want 1", got)
	}
}

func TestInspectUnresolvedGraphPolicyFailsClosed(t *testing.T) {
	knownGraph := compileInspectGraph(t, types.NodeDef{
		Name: "known",
		Type: "test.known",
		Kind: types.NodeKindAction,
	})

	for _, tc := range []struct {
		name  string
		graph *graph.Graph
	}{
		{name: "graph unavailable"},
		{name: "unknown graph node", graph: knownGraph},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			state := newInspectTrackingState()
			eng := New(state, &fakeQueue{})
			id := types.ExecutionID("inspect-private-unresolved-" + tc.name)
			seedInspectExecution(t, state, id, tc.graph)
			// A missing or stale public marker must not make an unresolved node's
			// trusted runtime value available through Inspect.
			seedInspectNode(t, state, id, "secret", false, map[string]any{"token": "top-secret"})

			detail, err := eng.Inspect(ctx, id, "secret")
			if err != nil {
				t.Fatalf("Inspect() error = %v", err)
			}
			if len(detail.Nodes) != 1 {
				t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
			}
			if detail.Nodes[0].Output != nil {
				t.Fatalf("unresolved private node output = %#v, want nil", detail.Nodes[0].Output)
			}
			if got := state.outputReadCount(id, "secret"); got != 0 {
				t.Fatalf("GetOutput(secret) calls = %d, want 0", got)
			}
		})
	}
}

func TestInspectWithoutGraphAndNamesReturnsNoNodes(t *testing.T) {
	ctx := context.Background()
	state := newInspectTrackingState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("inspect-no-graph-no-names")
	seedInspectExecution(t, state, id, nil)
	seedInspectNode(t, state, id, "secret", true, map[string]any{"token": "top-secret"})

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if state.graphLoads != 1 {
		t.Fatalf("LoadGraph calls = %d, want 1", state.graphLoads)
	}
	if detail.Nodes != nil {
		t.Fatalf("Nodes = %#v, want nil when graph is unavailable", detail.Nodes)
	}
	if got := state.outputReadCount(id, "secret"); got != 0 {
		t.Fatalf("GetOutput(secret) calls = %d, want 0", got)
	}
}
