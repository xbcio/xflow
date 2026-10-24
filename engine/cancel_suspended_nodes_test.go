package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestCancelSuspendedNodesCancelsNodesAndLeavesTheExecutionStatus pins the
// contract the execution-level timeout relies on: the cascade moves the
// suspended node to canceled but never writes the execution's own status, so
// the caller decides which terminal status the execution reaches.
func TestCancelSuspendedNodesCancelsNodesAndLeavesTheExecutionStatus(t *testing.T) {
	ctx := context.Background()
	state := &cancelResumeState{fakeState: newFakeState()}
	eng, id := newSuspendedCancelExecution(t, state)

	before, err := state.GetExecution(ctx, id)
	if err != nil || before == nil {
		t.Fatalf("GetExecution() before = (%v, %v)", before, err)
	}
	g, active, err := eng.loadActiveGraph(ctx, id)
	if err != nil || !active {
		t.Fatalf("loadActiveGraph() = (active=%v, err=%v)", active, err)
	}

	if err := eng.cancelSuspendedNodes(ctx, id, g); err != nil {
		t.Fatalf("cancelSuspendedNodes() error = %v", err)
	}

	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status = %q, want canceled", node.Status)
	}
	after, err := state.GetExecution(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("GetExecution() after = (%v, %v)", after, err)
	}
	if after.Status != before.Status {
		t.Fatalf("execution status = %q, want %q unchanged: the cascade must not drive execution status", after.Status, before.Status)
	}

	// Idempotent: the node is no longer suspended, so a second pass is a no-op.
	if err := eng.cancelSuspendedNodes(ctx, id, g); err != nil {
		t.Fatalf("repeated cancelSuspendedNodes() error = %v", err)
	}
	node, err = state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() after repeat = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status after repeat = %q, want canceled", node.Status)
	}
}

// TestCancelSuspendedNodesWithNothingSuspendedHasNoSideEffects covers the
// common timeout case: the execution has no parked node, so the cascade must
// neither fail nor touch the execution or its nodes.
func TestCancelSuspendedNodesWithNothingSuspendedHasNoSideEffects(t *testing.T) {
	ctx := context.Background()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "cancel-nothing-suspended",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "wait", Type: "test.suspend"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := &cancelResumeState{fakeState: newFakeState()}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.suspend": &fakeSuspendHandler{},
	}}
	// The queued task is deliberately never executed, so no node ever suspends.
	eng := newTestEngine(t, state, &fakeQueue{}, reg)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	state.InitInDegrees(id, g)

	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 0 {
		t.Fatalf("suspended nodes = %v, want none", suspended)
	}
	before, err := state.GetExecution(ctx, id)
	if err != nil || before == nil {
		t.Fatalf("GetExecution() before = (%v, %v)", before, err)
	}
	nodeBefore, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatalf("GetNode() before error = %v", err)
	}

	if err := eng.cancelSuspendedNodes(ctx, id, g); err != nil {
		t.Fatalf("cancelSuspendedNodes() error = %v", err)
	}

	after, err := state.GetExecution(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("GetExecution() after = (%v, %v)", after, err)
	}
	if after.Status != before.Status {
		t.Fatalf("execution status = %q, want %q unchanged", after.Status, before.Status)
	}
	nodeAfter, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatalf("GetNode() after error = %v", err)
	}
	if (nodeBefore == nil) != (nodeAfter == nil) {
		t.Fatalf("node presence changed: before=%v after=%v", nodeBefore, nodeAfter)
	}
	if nodeBefore != nil && nodeAfter.Status != nodeBefore.Status {
		t.Fatalf("node status = %q, want %q unchanged", nodeAfter.Status, nodeBefore.Status)
	}
}

// TestCancelSuspendedNodesRejectsASuspendedNodeOutsideTheGraph pins the guard
// against a suspended node the supplied graph does not know: the cascade must
// fail loudly rather than skip it, and leave the node as it found it.
func TestCancelSuspendedNodesRejectsASuspendedNodeOutsideTheGraph(t *testing.T) {
	ctx := context.Background()
	state := &cancelResumeState{fakeState: newFakeState()}
	eng, id := newSuspendedCancelExecution(t, state)

	other, err := graph.Compile(&types.WorkflowDef{
		Name:    "cancel-other-graph",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "other", Type: "test.suspend"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	err = eng.cancelSuspendedNodes(ctx, id, other)
	if err == nil {
		t.Fatal("cancelSuspendedNodes() error = nil, want a not-in-graph error")
	}
	if !strings.Contains(err.Error(), "is not in execution graph") {
		t.Fatalf("cancelSuspendedNodes() error = %v, want it to contain %q", err, "is not in execution graph")
	}

	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil {
		t.Fatalf("GetNode() = (%v, %v)", node, err)
	}
	if node.Status != types.NodeStatusSuspended {
		t.Fatalf("node status = %q, want suspended: the guard must fire before any write", node.Status)
	}
}
