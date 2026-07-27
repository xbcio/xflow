package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestMemoryCancelSuspendedNode covers M6: CancelSuspendedNode atomically flips
// a Suspended node to Canceled and reports canceled=true; for any non-Suspended
// (or missing) node it is a no-op that returns canceled=false without mutating
// state, so a concurrent resume's live lease is never clobbered.
func TestMemoryCancelSuspendedNode(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()

	// memoryState must satisfy the optional engine.SuspendedNodeCanceler.
	var _ engine.SuspendedNodeCanceler = state

	id := types.ExecutionID("exec-cancel-suspended")

	// (1) A Suspended node flips to Canceled and returns true.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:  id,
		Name:         "waiter",
		NodeIdx:      0,
		Status:       types.NodeStatusSuspended,
		ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(suspended) error = %v", err)
	}
	canceled, err := state.CancelSuspendedNode(ctx, id, "waiter")
	if err != nil {
		t.Fatalf("CancelSuspendedNode(suspended) error = %v", err)
	}
	if !canceled {
		t.Fatal("CancelSuspendedNode(suspended) = false, want true")
	}
	node, _ := state.GetNode(ctx, id, "waiter")
	if node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status = %+v, want canceled", node)
	}

	// (2) A non-Suspended node (e.g. concurrently resumed to Running) is a
	// no-op that returns false and leaves the live status untouched.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:  id,
		Name:         "running",
		NodeIdx:      1,
		Status:       types.NodeStatusRunning,
		LeaseToken:   "live-token",
		ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(running) error = %v", err)
	}
	canceled, err = state.CancelSuspendedNode(ctx, id, "running")
	if err != nil {
		t.Fatalf("CancelSuspendedNode(running) error = %v", err)
	}
	if canceled {
		t.Fatal("CancelSuspendedNode(running) = true, want false (must not clobber a running node)")
	}
	node, _ = state.GetNode(ctx, id, "running")
	if node == nil || node.Status != types.NodeStatusRunning || node.LeaseToken != "live-token" {
		t.Fatalf("running node = %+v, want status=running with live lease preserved", node)
	}

	// (3) A missing node is a no-op returning false.
	canceled, err = state.CancelSuspendedNode(ctx, id, "absent")
	if err != nil {
		t.Fatalf("CancelSuspendedNode(absent) error = %v", err)
	}
	if canceled {
		t.Fatal("CancelSuspendedNode(absent) = true, want false")
	}
}
