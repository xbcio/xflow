package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestInspectReportsExecutionError is the online-readback probe for
// COMMIT-PATH-TODO's "失败原因的读回面" gap.
//
// A failed execution stores its reason, but until this test's fix nothing
// could read it back online: ExecutionSnapshot had no Error field, so
// GetExecution had nowhere to load the stored reason into, and Inspect never
// assigned ExecutionDetail.Error even though the field existed. The only
// readback was the SQL audit row (executions.error_msg), which callers of the
// HTTP inspect API cannot see.
//
// The cyclic depth-limit case is the sharpest one: the node that trips the
// limit COMMITS SUCCESSFULLY (scheduler.go rejects the downstream activation
// instead of failing the node), so no node carries the reason and the
// execution-level error is the ONLY carrier. A caller sees a failed execution
// whose every node is success and whose error is empty.
func TestInspectReportsExecutionError(t *testing.T) {
	ctx := context.Background()
	eng, state := newErrorReadbackEngine(t)

	const reason = "max auto execution depth exceeded"
	id := types.ExecutionID("exec-readback-1")
	seedFailedExecution(ctx, t, state, id, reason)

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if detail.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %q, want %q", detail.Status, types.ExecutionStatusFailed)
	}
	if detail.Error != reason {
		t.Errorf("ExecutionDetail.Error = %q, want %q — the stored failure reason is not readable online", detail.Error, reason)
	}
}

// TestGetExecutionLoadsError pins the layer below Inspect: the state store must
// surface the reason on the snapshot, or Inspect has nothing to copy.
func TestGetExecutionLoadsError(t *testing.T) {
	ctx := context.Background()
	_, state := newErrorReadbackEngine(t)

	const reason = "node boom failed: upstream timeout"
	id := types.ExecutionID("exec-readback-2")
	seedFailedExecution(ctx, t, state, id, reason)

	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if snap == nil {
		t.Fatal("GetExecution returned nil snapshot")
	}
	if snap.Error != reason {
		t.Errorf("ExecutionSnapshot.Error = %q, want %q", snap.Error, reason)
	}
}

// TestInspectLeavesErrorEmptyOnSuccess is the negative half: a successful
// execution must not carry a reason. Without it, a fix that unconditionally
// stuffed some string into detail.Error would pass the two tests above.
func TestInspectLeavesErrorEmptyOnSuccess(t *testing.T) {
	ctx := context.Background()
	eng, state := newErrorReadbackEngine(t)

	id := types.ExecutionID("exec-readback-3")
	g := singleNodeReadbackGraph(t)
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus: %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if detail.Error != "" {
		t.Errorf("ExecutionDetail.Error = %q on a successful execution, want empty", detail.Error)
	}
}

// seedFailedExecution creates a running execution and drives it to failed with
// the given reason through the normal UpdateExecutionStatus path — the same
// call completeExecution and failInitialExecution use — rather than writing
// store internals, so the probe exercises real wiring.
func seedFailedExecution(ctx context.Context, t *testing.T, state StateStore, id types.ExecutionID, reason string) {
	t.Helper()
	g := singleNodeReadbackGraph(t)
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, reason); err != nil {
		t.Fatalf("UpdateExecutionStatus: %v", err)
	}
}

func singleNodeReadbackGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "readback",
		Nodes: []types.NodeDef{{Name: "start", Type: "xflow.function", Parameters: map[string]any{"code": "1"}}},
	})
	if err != nil {
		t.Fatalf("compile graph: %v", err)
	}
	return g
}

func newErrorReadbackEngine(t *testing.T) (*Engine, StateStore) {
	t.Helper()
	state := newFakeState()
	return New(state, &fakeQueue{}), state
}
