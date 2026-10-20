package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestMemoryCancelingExecutionContract runs the shared canceling-index contract
// against the in-memory backend, keeping it semantically aligned with the
// Redis implementation the distributed deployments rely on.
func TestMemoryCancelingExecutionContract(t *testing.T) {
	statestoretest.RunCancelingExecutionContract(t, func(t *testing.T) engine.StateStore {
		return New().State()
	})
}

// TestMemoryCancelingIndexRepairsStaleEntries covers the registration whose
// terminal removal was skipped: the status moved on but the map entry is still
// there. List must not report it and must drop it.
func TestMemoryCancelingIndexRepairsStaleEntries(t *testing.T) {
	state := New().State().(*memoryState)
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: "exec-canceling-stale", Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-stale", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	// Simulate a lost terminal removal by rewriting the status directly.
	state.mu.Lock()
	state.executions["exec-canceling-stale"].snap.Status = types.ExecutionStatusCanceled
	state.mu.Unlock()

	candidates, err := state.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("ListCancelingExecutions() = %+v, want the stale entry excluded", candidates)
	}
	state.mu.Lock()
	_, tracked := state.cancelingSince["exec-canceling-stale"]
	state.mu.Unlock()
	if tracked {
		t.Fatal("stale canceling registration was not repaired by the list")
	}
}
