package transient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestNewLocalTransientBackendWaitDoneReturnsOutputsAndExpiresState(t *testing.T) {
	backend := New(WithActiveTTL(time.Second), WithCompletionTTL(25*time.Millisecond))
	state := backend.State()
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-backend")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  testTransientGraph(t),
		Status: types.ExecutionStatusRunning,
		Params: map[string]any{"value": "ok"},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.PutOutput(ctx, id, "start", map[string]any{"seen": "ok"}); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	result, err := backend.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("result status = %s, want success", result.Status)
	}
	if got := result.Output["start"].(map[string]any)["seen"]; got != "ok" {
		t.Fatalf("output seen = %v, want ok", got)
	}

	waitForTransientCondition(t, time.Second, func() bool {
		snap, err := state.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		return snap == nil
	})
}

func TestTransientStateRefreshesActiveTTLOnMutation(t *testing.T) {
	backend := New(
		WithActiveTTL(120*time.Millisecond),
		WithCompletionTTL(time.Second),
	)
	state := backend.State()
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-refresh")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  testTransientGraph(t),
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	time.Sleep(70 * time.Millisecond)
	if err := state.PutOutput(ctx, id, "start", map[string]any{"seen": "touch"}); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}

	time.Sleep(70 * time.Millisecond)
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap == nil {
		t.Fatal("execution expired before refreshed TTL elapsed")
	}

	waitForTransientCondition(t, 400*time.Millisecond, func() bool {
		snap, err := state.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		return snap == nil
	})
}

func TestTransientStateRejectsNodeMutationAfterCleanup(t *testing.T) {
	backend := New(WithActiveTTL(time.Second), WithCompletionTTL(time.Second))
	state := backend.State()
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-expired")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  testTransientGraph(t),
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	backend.state.cleanupExecution(id)

	err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusRunning,
	})
	if !errors.Is(err, engine.ErrExecutionInactive) {
		t.Fatalf("UpsertNode() error = %v, want ErrExecutionInactive", err)
	}
}

func testTransientGraph(t *testing.T) *graph.Graph {
	t.Helper()

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "transient-backend",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func waitForTransientCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before timeout")
		}
	}
}
