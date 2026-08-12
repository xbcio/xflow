package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// cyclicStartRefHandler records the evaluated value of its "v" parameter, which
// carries a $nodes reference guarded by ??. The recorded value is the evidence:
// "MISS" means the guard fired, and no recorded value at all means the node
// never ran because parameter evaluation failed first.
type cyclicStartRefHandler struct {
	mu   sync.Mutex
	seen []any
}

func (h *cyclicStartRefHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.start"}
}

func (h *cyclicStartRefHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen = append(h.seen, in.Params["v"])
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *cyclicStartRefHandler) values() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.seen...)
}

type cyclicRefEchoHandler struct{}

func (cyclicRefEchoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cyclic_ref_echo"}
}

func (cyclicRefEchoHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"x": 1}}, nil
}

// TestCyclicStartNodeGetsNodesPrefetch covers the one path in buildInput that
// used to return before prefetchNodesRefs: a cyclic workflow's start node on
// its first activation (engine/input.go).
//
// The start node of a cyclic workflow CAN carry a compiling $nodes reference —
// buildNodesRefs only rejects a reference that is reachable FROM the start
// node, so a reference to a peer root like "r" below passes compilation and
// lands in g.NodesRefsFor(startIdx). Skipping the prefetch left input.Nodes nil,
// so $nodes['r'] evaluated to an UNTYPED nil rather than the typed nil map the
// Input.Nodes contract requires. That is the "key absent" row of that contract's
// table: ?? cannot fire through a field access on it, so the whole node failed
// with "cannot fetch x from <nil>" — the guard the DSL spec recommends for
// exactly this case was defeated by the value's form.
//
// The assertion is on the handler's observed parameter, not on the error string:
// before the fix the handler was never invoked at all, so an error-only
// assertion would not have distinguished "guard fired" from "node never ran".
func TestCyclicStartNodeGetsNodesPrefetch(t *testing.T) {
	reg := execution.NewRegistry()
	start := &cyclicStartRefHandler{}
	reg.RegisterGlobal("xflow.start", start)
	reg.RegisterGlobal("test.cyclic_ref_echo", cyclicRefEchoHandler{})

	def := &types.WorkflowDef{
		Name: "cyclic-start-nodes-ref",
		Nodes: []types.NodeDef{
			{Name: "s", Type: "xflow.start", Parameters: map[string]any{
				"v": "${{ $nodes['r'].x ?? 'MISS' }}",
			}},
			{Name: "b", Type: "test.cyclic_ref_echo"},
			// A peer root. In cyclic mode only the start node is enqueued, so "r"
			// never executes and its output is a genuine miss — which is the case
			// the ?? guard exists to survive.
			{Name: "r", Type: "test.cyclic_ref_echo"},
		},
		Connections: types.Connections{
			"s": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"r": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 5},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := b.WaitDone(ctx, id); err != nil {
		t.Fatalf("wait: %v", err)
	}

	node, err := b.State().GetNode(ctx, id, "s")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if node == nil {
		t.Fatal("start node has no state")
	}
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("start node status = %v, want success (error: %s)", node.Status, node.Error)
	}

	got := start.values()
	if len(got) == 0 {
		t.Fatal("start handler was never invoked; its $nodes reference failed to evaluate")
	}
	if got[0] != "MISS" {
		t.Errorf("start node's v = %#v, want \"MISS\" (the ?? guard should fire on an unexecuted $nodes reference)", got[0])
	}
}
