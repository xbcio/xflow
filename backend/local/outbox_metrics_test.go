package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestMemoryOutboxFailureTracksAttemptsAndDeadLetters(t *testing.T) {
	state := newMemoryState()
	ctx := context.Background()
	id := types.ExecutionID("memory-outbox-dead-letter")
	entry := engine.OutboxEntry{
		ID:   "root/memory-outbox-dead-letter/start/0",
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	first, err := state.RecordOutboxFailure(ctx, id, entry.ID, 2)
	if err != nil {
		t.Fatalf("first RecordOutboxFailure() error = %v", err)
	}
	if first.Attempts != 1 || first.DeadLettered {
		t.Fatalf("first RecordOutboxFailure() = %+v, want retained first attempt", first)
	}
	second, err := state.RecordOutboxFailure(ctx, id, entry.ID, 2)
	if err != nil {
		t.Fatalf("second RecordOutboxFailure() error = %v", err)
	}
	if second.Attempts != 2 || !second.DeadLettered {
		t.Fatalf("second RecordOutboxFailure() = %+v, want dead letter", second)
	}

	snapshot, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() error = %v", err)
	}
	if snapshot.Pending != 0 || snapshot.DeadLettered != 1 {
		t.Fatalf("OutboxMetrics() = %+v, want zero pending and one dead letter", snapshot)
	}
}
