package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestReplayDeadLetterSecondCycleNotBlocked verifies the fix for the
// replay:entryidx stale-index bug: after an entry is replayed (dead→ready),
// re-delivered, and re-dead-lettered (ready→dead again), a new ReplayDeadLetter
// with a different RequestID must NOT be rejected as already_replayed. The
// recordOutboxFailureLua must clear the stale entryidx mapping when the entry
// re-enters dead-letter storage.
func TestReplayDeadLetterSecondCycleNotBlocked(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-re-replay-1")
	entryID := "execute/dl-re-replay-1/review/1"
	seedDeadLetter(t, state, id, entryID)

	// First replay: dead→ready.
	res1, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-cycle1"))
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if res1.Outcome != engine.ReplayReplayed {
		t.Fatalf("first outcome = %q, want replayed", res1.Outcome)
	}

	// Verify the entry is back in ready.
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox after first replay: %v", err)
	}
	if len(ready) != 1 || ready[0].ID != entryID {
		t.Fatalf("ready after first replay = %+v, want 1 entry %q", ready, entryID)
	}

	// Re-dead-letter via repeated delivery failure (simulating queue re-failure).
	entry := engine.OutboxEntry{
		ID: entryID,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "review",
			NodeIdx:      1,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d] second cycle: %v", i, err)
		}
	}

	// Verify entry is back in dead-letter.
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters after second dead-letter: %v", err)
	}
	if len(dead.Entries) != 1 || dead.Entries[0].ID != entryID {
		t.Fatalf("dead after second cycle = %+v, want 1 entry %q", dead, entryID)
	}

	// Second replay with a NEW RequestID: must NOT be blocked as already_replayed.
	res2, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-cycle2"))
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if res2.Outcome != engine.ReplayReplayed {
		t.Fatalf("second outcome = %q, want replayed (not already_replayed)", res2.Outcome)
	}
	if res2.AuditID == "" {
		t.Fatalf("second replay must have an audit_id")
	}
	if res2.AuditID == res1.AuditID {
		t.Fatalf("second replay audit_id must differ from first: both %q", res1.AuditID)
	}

	// Verify dead is empty and ready has the entry again.
	dead2, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters after second replay: %v", err)
	}
	if len(dead2.Entries) != 0 {
		t.Fatalf("dead after second replay = %d, want 0", len(dead2.Entries))
	}
	ready2, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox after second replay: %v", err)
	}
	if len(ready2) != 1 || ready2[0].ID != entryID {
		t.Fatalf("ready after second replay = %+v, want 1 entry %q", ready2, entryID)
	}
}

// TestReplayEntryIdxClearedOnReDeadLetter directly verifies that the
// replay:entryidx hash field for an entry is removed when
// recordOutboxFailureLua moves it back to dead-letter storage.
func TestReplayEntryIdxClearedOnReDeadLetter(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-entryidx-clear")
	entryID := "execute/dl-entryidx-clear/review/1"
	seedDeadLetter(t, state, id, entryID)

	// Replay: sets replay:entryidx[entryID] = requestID.
	_, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-idx"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	// Verify the entryidx mapping exists.
	val, err := rdb.HGet(ctx, outboxReplayEntryIdxKey(namespace.Default, id), entryID).Result()
	if err != nil {
		t.Fatalf("HGet entryidx after replay: %v", err)
	}
	if val != "req-idx" {
		t.Fatalf("entryidx = %q, want req-idx", val)
	}

	// Re-dead-letter.
	entry := engine.OutboxEntry{
		ID: entryID,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     "review",
			NodeIdx:      1,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}

	// Verify the entryidx mapping is cleared.
	exists, err := rdb.HExists(ctx, outboxReplayEntryIdxKey(namespace.Default, id), entryID).Result()
	if err != nil {
		t.Fatalf("HExists entryidx after re-dead-letter: %v", err)
	}
	if exists {
		t.Fatalf("replay:entryidx[%q] still exists after re-dead-letter; want cleared", entryID)
	}
}
