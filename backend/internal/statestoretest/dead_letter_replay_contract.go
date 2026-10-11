package statestoretest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// DeadLetterReplayStore is a backend that can dead-letter durable outbox
// entries and replay them.
type DeadLetterReplayStore interface {
	engine.StateStore
	engine.AtomicStateStore
	engine.OutboxFailureRecorder
	engine.DeadLetterStore
}

// DeadLetterReplayBackend is one fresh backend under test. ExpireExecution
// makes an execution inactive the way that backend loses it (Redis: the
// status key expires), which no public StateStore method does.
type DeadLetterReplayBackend struct {
	Store           DeadLetterReplayStore
	ExpireExecution func(t *testing.T, id types.ExecutionID)
}

// RunDeadLetterReplayContract pins the engine.DeadLetterStore replay contract
// every backend must share, so a dead-letter replayed through the local
// backend gets the same verdict it would get from Redis.
//
// Evaluation order is part of the contract: a receipt under the same
// RequestID recovers its stored outcome; a missing entry is not_found with no
// receipt; then metadata, execution status (only running replays), node, and
// activation guards run in that order. Every decidable rejection writes a
// receipt that a retry under the same RequestID recovers unchanged.
func RunDeadLetterReplayContract(t *testing.T, newBackend func(*testing.T) DeadLetterReplayBackend) {
	ctx := context.Background()

	// seed creates a running execution whose single outbox entry targets
	// task.NodeName, optionally sets that node's state, and fails the entry's
	// delivery until it is dead-lettered.
	seed := func(t *testing.T, s DeadLetterReplayStore, entryID string, task engine.Task, node *engine.NodeSnapshot) (types.ExecutionID, engine.OutboxEntry) {
		t.Helper()
		id := types.ExecutionID("dlr-" + t.Name())
		task.ExecutionID = id
		entry := engine.OutboxEntry{ID: fmt.Sprintf(entryID, id), Task: task}
		if err := s.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{entry}); err != nil {
			t.Fatalf("CreateExecutionWithOutbox: %v", err)
		}
		if node != nil {
			node.ExecutionID = id
			if err := s.UpsertNode(ctx, node); err != nil {
				t.Fatalf("UpsertNode: %v", err)
			}
		}
		for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
			if _, err := s.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
				t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
			}
		}
		requireDeadLetters(t, s, id, 1)
		return id, entry
	}
	execTask := func(activation int) engine.Task {
		return engine.Task{NodeName: "finish", NodeIdx: 1, Type: engine.TaskTypeNodeExec, ActivationID: activation}
	}
	runningNode := func(activation int) *engine.NodeSnapshot {
		return &engine.NodeSnapshot{Name: "finish", NodeIdx: 1, Status: types.NodeStatusRunning, ActivationID: activation}
	}
	replay := func(t *testing.T, s DeadLetterReplayStore, id types.ExecutionID, entryID, requestID string) engine.ReplayDeadLetterResult {
		t.Helper()
		res, err := s.ReplayDeadLetter(ctx, engine.ReplayDeadLetterRequest{
			ExecutionID: id, EntryID: entryID, RequestID: requestID,
			Operator: "contract:tester", Reason: "contract replay",
		})
		if err != nil {
			t.Fatalf("ReplayDeadLetter(%s): %v", requestID, err)
		}
		return res
	}
	// requireRejected asserts a rejection that wrote a receipt and left the
	// entry in dead-letter storage with nothing made ready.
	requireRejected := func(t *testing.T, s DeadLetterReplayStore, id types.ExecutionID, res engine.ReplayDeadLetterResult, want engine.DeadLetterReplayOutcome, node, activation string) {
		t.Helper()
		if res.Outcome != want {
			t.Fatalf("outcome = %q, want %q", res.Outcome, want)
		}
		if res.AuditID == "" {
			t.Fatalf("%s wrote no receipt: result %+v has no AuditID", want, res)
		}
		if res.ExecutionID != id || res.NodeID != node || res.ActivationID != activation {
			t.Fatalf("result = %+v, want execution %q node %q activation %q", res, id, node, activation)
		}
		requireDeadLetters(t, s, id, 1)
		requireReady(t, s, id, 0)
	}

	t.Run("ReplayMovesEntryToReady", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		res := replay(t, b.Store, id, entry.ID, "req-1")
		if res.Outcome != engine.ReplayReplayed || res.AuditID == "" {
			t.Fatalf("result = %+v, want replayed with an AuditID", res)
		}
		if res.ExecutionID != id || res.NodeID != "finish" || res.ActivationID != "1" {
			t.Fatalf("result = %+v, want node finish activation 1", res)
		}
		requireDeadLetters(t, b.Store, id, 0)
		ready := requireReady(t, b.Store, id, 1)
		if ready[0].ID != entry.ID || ready[0].Attempts != 0 {
			t.Fatalf("ready = %+v, want %q with attempts reset to 0", ready, entry.ID)
		}
	})

	// A scheduling-stage intent whose node has no state yet has no current
	// activation to be stale against.
	t.Run("ReplaySchedulingIntentWithoutNodeState", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), nil)
		if res := replay(t, b.Store, id, entry.ID, "req-1"); res.Outcome != engine.ReplayReplayed {
			t.Fatalf("outcome = %q, want replayed", res.Outcome)
		}
	})

	// not_found writes no receipt: the same RequestID is still free to replay
	// the entry once it actually exists. It also wins over an inactive
	// execution, because the entry is read before any status guard.
	t.Run("NotFoundWritesNoReceipt", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		missing := replay(t, b.Store, id, entry.ID+"-missing", "req-1")
		if missing.Outcome != engine.ReplayNotFound || missing.AuditID != "" {
			t.Fatalf("result = %+v, want not_found without an AuditID", missing)
		}
		if res := replay(t, b.Store, id, entry.ID, "req-1"); res.Outcome != engine.ReplayReplayed {
			t.Fatalf("outcome = %q, want replayed: not_found must not have stored a receipt", res.Outcome)
		}
		b.ExpireExecution(t, id)
		if res := replay(t, b.Store, id, entry.ID+"-missing", "req-2"); res.Outcome != engine.ReplayNotFound {
			t.Fatalf("outcome on an inactive execution = %q, want not_found", res.Outcome)
		}
	})

	// Without node metadata the activation guards cannot run, so replay fails
	// closed — ahead of the execution status guard.
	t.Run("MetadataMissingFailsClosed", func(t *testing.T) {
		b := newBackend(t)
		task := execTask(1)
		task.NodeName = ""
		id, entry := seed(t, b.Store, "execute/%s/finish/1", task, nil)
		if err := b.Store.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedMetadataMissing, "", "1")
	})

	// An entry ID with no intent prefix is legacy metadata.
	t.Run("UnknownIntentFailsClosed", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "legacy-%s", execTask(1), runningNode(1))
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedMetadataMissing, "finish", "1")
	})

	t.Run("InactiveExecutionRejected", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		b.ExpireExecution(t, id)
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedInactive, "finish", "1")
	})

	t.Run("TerminalExecutionRejected", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		if err := b.Store.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, "boom"); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedTerminal, "finish", "1")
	})

	// Only a running execution replays: a cancel in flight must not be
	// bypassed by resurrecting work.
	t.Run("CancelingExecutionRejected", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		if err := b.Store.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedTerminal, "finish", "1")
	})

	t.Run("TerminalNodeRejected", func(t *testing.T) {
		b := newBackend(t)
		node := runningNode(1)
		node.Status = types.NodeStatusSuccess
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), node)
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedNodeTerminal, "finish", "1")
	})

	t.Run("ActivationMismatchRejected", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(2))
		requireRejected(t, b.Store, id, replay(t, b.Store, id, entry.ID, "req-1"), engine.ReplayRejectedActivationMismatch, "finish", "1")
	})

	// A retry under the same RequestID recovers the stored rejection even once
	// the state would now allow the replay; a new RequestID re-evaluates.
	t.Run("SameRequestIDRecoversRejection", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(2))
		first := replay(t, b.Store, id, entry.ID, "req-1")
		requireRejected(t, b.Store, id, first, engine.ReplayRejectedActivationMismatch, "finish", "1")

		fixed := runningNode(1)
		fixed.ExecutionID = id
		if err := b.Store.UpsertNode(ctx, fixed); err != nil {
			t.Fatalf("UpsertNode: %v", err)
		}
		if again := replay(t, b.Store, id, entry.ID, "req-1"); again != first {
			t.Fatalf("retry = %+v, want the original rejection %+v", again, first)
		}
		if fresh := replay(t, b.Store, id, entry.ID, "req-2"); fresh.Outcome != engine.ReplayReplayed {
			t.Fatalf("new RequestID outcome = %q, want replayed", fresh.Outcome)
		}
	})

	// After a replay, the same RequestID and any other RequestID both collapse
	// to already_replayed carrying the original AuditID.
	t.Run("ReplayedRequestIsIdempotent", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		first := replay(t, b.Store, id, entry.ID, "req-1")
		if first.Outcome != engine.ReplayReplayed {
			t.Fatalf("outcome = %q, want replayed", first.Outcome)
		}
		for _, requestID := range []string{"req-1", "req-2"} {
			res := replay(t, b.Store, id, entry.ID, requestID)
			if res.Outcome != engine.ReplayAlreadyReplayed || res.AuditID != first.AuditID {
				t.Fatalf("%s: result = %+v, want already_replayed with AuditID %q", requestID, res, first.AuditID)
			}
			if res.NodeID != first.NodeID || res.ActivationID != first.ActivationID {
				t.Fatalf("%s: result = %+v, want node/activation of %+v", requestID, res, first)
			}
		}
		requireReady(t, b.Store, id, 1)
	})

	// An entry that fails again after a replay is a new dead-letter: a new
	// RequestID must replay it rather than collapse to the first cycle.
	t.Run("SecondDeadLetterCycleReplays", func(t *testing.T) {
		b := newBackend(t)
		id, entry := seed(t, b.Store, "execute/%s/finish/1", execTask(1), runningNode(1))
		first := replay(t, b.Store, id, entry.ID, "req-1")
		if first.Outcome != engine.ReplayReplayed {
			t.Fatalf("first outcome = %q, want replayed", first.Outcome)
		}
		for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
			if _, err := b.Store.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
				t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
			}
		}
		requireDeadLetters(t, b.Store, id, 1)
		second := replay(t, b.Store, id, entry.ID, "req-2")
		if second.Outcome != engine.ReplayReplayed || second.AuditID == first.AuditID {
			t.Fatalf("second cycle = %+v, want a fresh replay (first %+v)", second, first)
		}
		requireReady(t, b.Store, id, 1)
	})
}

func requireDeadLetters(t *testing.T, s DeadLetterReplayStore, id types.ExecutionID, want int) []engine.OutboxEntry {
	t.Helper()
	dead, err := s.ListDeadLetters(context.Background(), id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead.Entries) != want {
		t.Fatalf("dead-letter entries = %+v, want %d", dead.Entries, want)
	}
	return dead.Entries
}

func requireReady(t *testing.T, s DeadLetterReplayStore, id types.ExecutionID, want int) []engine.OutboxEntry {
	t.Helper()
	ready, err := s.ListOutbox(context.Background(), id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != want {
		t.Fatalf("ready entries = %+v, want %d", ready, want)
	}
	return ready
}
