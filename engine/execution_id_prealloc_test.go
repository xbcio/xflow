package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestSubmitUsesPreallocatedExecutionID asserts that when the submission
// context carries a pre-allocated execution id (engine.WithExecutionID, set by
// the apiserver authz wrapper at admission time — R3.1), Submit persists and
// returns THAT id rather than minting a fresh one. This is what lets the
// admission audit row and the persisted execution share one id so the
// reconcile worker's Probe can correlate them.
func TestSubmitUsesPreallocatedExecutionID(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	pre := types.ExecutionID("exec-preallocated-fixed")
	id, err := eng.Submit(WithExecutionID(ctx, pre), compileAtomicOutboxGraph(t, nil), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v, want success", err)
	}
	if id != pre {
		t.Fatalf("Submit() id = %q, want pre-allocated %q", id, pre)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) snap=%v err=%v, want persisted snapshot", id, snap, err)
	}
	if snap.ID != pre {
		t.Fatalf("persisted snapshot id = %q, want %q", snap.ID, pre)
	}
}

// TestInvokeUsesPreallocatedExecutionID mirrors the above for the explicit
// entry-node path.
func TestInvokeUsesPreallocatedExecutionID(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	pre := types.ExecutionID("exec-preallocated-invoke")
	id, err := eng.Invoke(WithExecutionID(ctx, pre), compileTriggerEntryGraph(t), "trigger", nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v, want success", err)
	}
	if id != pre {
		t.Fatalf("Invoke() id = %q, want pre-allocated %q", id, pre)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) snap=%v err=%v, want persisted snapshot", id, snap, err)
	}
	if snap.ID != pre {
		t.Fatalf("persisted snapshot id = %q, want %q", snap.ID, pre)
	}
}

// TestSubmitMintsIDWhenContextHasNone asserts backward compatibility: callers
// that do not pre-allocate (e.g. SDK direct NewLocal/NewCluster usage) still get
// a freshly minted exec- id.
func TestSubmitMintsIDWhenContextHasNone(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &toggleOutboxQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !strings.HasPrefix(string(id), "exec-") {
		t.Fatalf("minted id = %q, want exec- prefix", id)
	}
}

// TestWithExecutionIDIgnoresEmpty asserts the ctx helper does not inject an
// empty id (so an unset context never overrides a later minted id).
func TestWithExecutionIDIgnoresEmpty(t *testing.T) {
	ctx := context.Background()
	if _, ok := ExecutionIDFromContext(WithExecutionID(ctx, "")); ok {
		t.Fatal("WithExecutionID(ctx,\"\") should not inject an id")
	}
}

// compileTriggerEntryGraph builds a single-node graph whose node is a trigger
// entry (Kind: NodeKindTrigger) so Invoke has a resolvable entry node.
func compileTriggerEntryGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "trigger-entry",
		Nodes: []types.NodeDef{{Name: "trigger", Type: "xflow.trigger.kafka", Kind: types.NodeKindTrigger}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}
