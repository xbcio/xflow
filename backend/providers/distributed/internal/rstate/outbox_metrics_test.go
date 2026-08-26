package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestRedisOutboxFailureTracksAttemptsAndDeadLetters(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("outbox-dead-letter")
	// Every Task field below is deliberately non-zero: the dead-letter body is
	// read back and compared field by field at the end of this test, and a zero
	// value cannot tell "carried through" from "never written".
	entry := engine.OutboxEntry{
		ID: "root/outbox-dead-letter/start/0",
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "start",
			NodeIdx:      2,
			Type:         engine.TaskTypeNodeResume,
			ActivationID: 3,
		},
	}
	before := time.Now().UTC().Add(-time.Second)
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	initial, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() initial error = %v", err)
	}
	if initial.Pending != 1 || initial.DeadLettered != 0 {
		t.Fatalf("initial OutboxMetrics() = %+v, want one pending entry", initial)
	}
	// IsZero() alone would not settle this: time.UnixMilli(0) is 1970 and Go's
	// zero Time is year 1, so a scanner reporting the epoch passes an IsZero
	// check while reporting an age of half a century. Bound it on both sides
	// against the instant the entry was actually created.
	if initial.OldestPendingAt.Before(before) || initial.OldestPendingAt.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("initial OldestPendingAt = %s, want an instant near the seed at %s",
			initial.OldestPendingAt.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano))
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
	// The body, not just its presence. Nothing in this repo reads a dead-letter
	// entry's Task back: every dead-letter assertion anywhere (here,
	// deadletter_replay_test.go:71, deadletter_re_replay_test.go:66,
	// local/dead_letter_replay_test.go:42) stops at .ID -- and .ID is passed to
	// marshalRedisOutboxEntry separately from the Task, so it survives a body
	// that has lost everything else. recordOutboxFailureLua could HSET a
	// stub carrying only the ID and the whole suite stays green.
	//
	// The Task is what replay is for. ReplayDeadLetter moves this exact body
	// back onto the ready set (state_deadletter.go:191) and the dispatcher
	// routes it by NodeName/NodeIdx/Type, with ActivationID gating the
	// activation guard. A body that decodes to a zero Task replays into a task
	// addressed to node "" at index 0 on activation 0.
	raw, err := rdb.HGet(ctx, outboxDeadBodyKey(namespace.Default, id), entry.ID).Result()
	if err != nil {
		t.Fatalf("dead-letter body missing: %v", err)
	}
	got, err := unmarshalRedisOutboxEntry(raw)
	if err != nil {
		t.Fatalf("dead-letter body does not decode as an outbox entry: %v", err)
	}
	if got.ID != entry.ID {
		t.Errorf("dead-letter ID = %q, want %q", got.ID, entry.ID)
	}
	if got.Task.ExecutionID != id || got.Task.NodeName != "start" ||
		got.Task.NodeIdx != 2 || got.Task.Type != engine.TaskTypeNodeResume ||
		got.Task.ActivationID != 3 {
		t.Errorf("dead-letter Task = %+v, want the task that was dead-lettered "+
			"(exec %q, node \"start\", idx 2, type %v, activation 3): replay "+
			"re-dispatches this body verbatim", got.Task, id, engine.TaskTypeNodeResume)
	}
	final, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics() final error = %v", err)
	}
	if final.Pending != 0 || final.DeadLettered != 1 {
		t.Fatalf("final OutboxMetrics() = %+v, want one dead-letter entry", final)
	}
}
