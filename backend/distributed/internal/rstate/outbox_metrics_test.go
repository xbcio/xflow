package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/types"
)

func TestRedisOutboxFailureTracksAttemptsAndDeadLetters(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("outbox-dead-letter")
	entry := engine.OutboxEntry{
		ID:   "root/outbox-dead-letter/start/0",
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	initial, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() initial error = %v", err)
	}
	if initial.Pending != 1 || initial.DeadLettered != 0 || initial.OldestPendingAt.IsZero() {
		t.Fatalf("initial OutboxMetrics() = %+v, want one timestamped pending entry", initial)
	}

	first, err := state.RecordOutboxFailure(ctx, id, entry, 2)
	if err != nil {
		t.Fatalf("first RecordOutboxFailure() error = %v", err)
	}
	if first.Attempts != 1 || first.DeadLettered {
		t.Fatalf("first RecordOutboxFailure() = %+v, want attempt 1 without dead letter", first)
	}
	if entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 2); err != nil || len(entries) != 1 {
		t.Fatalf("outbox after first failure entries=%+v err=%v, want retained entry", entries, err)
	}

	second, err := state.RecordOutboxFailure(ctx, id, entry, 2)
	if err != nil {
		t.Fatalf("second RecordOutboxFailure() error = %v", err)
	}
	if second.Attempts != 2 || !second.DeadLettered {
		t.Fatalf("second RecordOutboxFailure() = %+v, want dead letter on threshold", second)
	}
	if entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 2); err != nil || len(entries) != 0 {
		t.Fatalf("outbox after dead letter entries=%+v err=%v, want empty", entries, err)
	}
	if _, err := rdb.HGet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), entry.ID).Result(); err != nil {
		t.Fatalf("dead-letter body missing: %v", err)
	}
	final, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() final error = %v", err)
	}
	if final.Pending != 0 || final.DeadLettered != 1 {
		t.Fatalf("final OutboxMetrics() = %+v, want one dead-letter entry", final)
	}
}
