package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// seedDeadLetterIntent produces a dead-letter entry whose dead-meta carries
// the intent derived from entryID (the production path: RecordOutboxFailure
// extracts the prefix and writes it). nodeStatus controls the live node:status
// key — "" leaves it absent (the scheduling-stage case); otherwise the key is
// set to the given value. nodeMetaActivation is written to node:meta so the
// activation guard has a current value when node:status is present.
func seedDeadLetterIntent(t *testing.T, state *Store, id types.ExecutionID, entryID, nodeStatus string, nodeMetaActivation int) {
	t.Helper()
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
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
	body, err := marshalRedisOutboxEntry(entryID, entry.Task, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := state.rdb.HSet(ctx, outboxBodyKey(tenant.DefaultTenant, id), entryID, body).Err(); err != nil {
		t.Fatalf("HSet outbox body: %v", err)
	}
	if err := state.rdb.ZAdd(ctx, outboxReadyKey(tenant.DefaultTenant, id), redis.Z{Score: float64(time.Now().UTC().UnixMilli()), Member: entryID}).Err(); err != nil {
		t.Fatalf("ZAdd outbox ready: %v", err)
	}
	if nodeStatus != "" {
		if err := state.rdb.Set(ctx, nodeStatusKey(tenant.DefaultTenant, id, "review"), nodeStatus, time.Minute).Err(); err != nil {
			t.Fatalf("set node status: %v", err)
		}
	}
	if nodeMetaActivation > 0 {
		if err := state.rdb.HSet(ctx, nodeMetaKey(tenant.DefaultTenant, id, "review"), "activation_id", nodeMetaActivation).Err(); err != nil {
			t.Fatalf("set node meta: %v", err)
		}
	}
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	// The dead-meta hash must now carry intent (proof the production path wrote it).
	intent, err := state.rdb.HGet(ctx, outboxDeadMetaKey(tenant.DefaultTenant, id, entryID), "intent").Result()
	if err != nil {
		t.Fatalf("HGet dead meta intent: %v", err)
	}
	if intent == "" {
		t.Fatalf("dead-meta intent empty; RecordOutboxFailure did not write intent for %q", entryID)
	}
}

// TestReplayDeadLetterIntentAllowlist proves the intent-branched node guard:
// each intent has its own "safe to replay" precondition. The pre-fix single
// allowlist (running/committing/waiting only) wrongly rejected the typical
// dead-letter — root/advance/execute/skip at scheduling time have no node:status
// yet, retry/requeue land on pending, resume lands on suspended. Each case here
// must replay. Uses miniredis so it runs without a real Redis.
func TestReplayDeadLetterIntentAllowlist(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	cases := []struct {
		name       string
		execID     string
		entryID    string
		nodeStatus string
		activation int
	}{
		{"root_no_node_status", "dl-intent-root", "root/dl-intent-root/review/1", "", 0},
		{"retry_pending", "dl-intent-retry", "retry/dl-intent-retry/review/1", "pending", 1},
		{"retry_running", "dl-intent-retry-running", "retry/dl-intent-retry-running/review/1", "running", 1},
		{"requeue_pending", "dl-intent-requeue", "requeue/dl-intent-requeue/review/1", "pending", 1},
		{"resume_suspended", "dl-intent-resume", "resume/dl-intent-resume/review/1", "suspended", 1},
		{"resume_pending", "dl-intent-resume-pending", "resume/dl-intent-resume-pending/review/1", "pending", 1},
		// A real advance entry's source is ALWAYS terminal (commitNodeLua writes
		// source terminal + advance outbox atomically). Model that lifecycle here.
		{"advance_source_terminal", "dl-intent-advance", "advance/dl-intent-advance/review/1", "success", 1},
		{"execute_running", "dl-intent-execute", "execute/dl-intent-execute/review/1", "running", 1},
		{"execute_no_node_status", "dl-intent-execute-missing", "execute/dl-intent-execute-missing/review/1", "", 0},
		{"skip_no_node_status", "dl-intent-skip", "skip/dl-intent-skip/review/1", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := types.ExecutionID(tc.execID)
			seedDeadLetterIntent(t, state, id, tc.entryID, tc.nodeStatus, tc.activation)

			res, err := state.ReplayDeadLetter(ctx, replayReq(id, tc.entryID, "req-"+tc.name))
			if err != nil {
				t.Fatalf("ReplayDeadLetter: %v", err)
			}
			if res.Outcome != engine.ReplayReplayed {
				t.Fatalf("outcome = %q, want replayed (intent-branched guard must allow this precondition)", res.Outcome)
			}
			// dead→ready move must have happened.
			dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
			if err != nil {
				t.Fatalf("ListDeadLetters: %v", err)
			}
			if len(dead.Entries) != 0 {
				t.Fatalf("dead = %d entries, want 0 after replay", len(dead.Entries))
			}
			ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
			if err != nil {
				t.Fatalf("ListOutbox: %v", err)
			}
			if len(ready) != 1 || ready[0].ID != tc.entryID {
				t.Fatalf("ready = %+v, want 1 entry %q", ready, tc.entryID)
			}
		})
	}
}

// TestReplayDeadLetterIntentRejects proves the fail-closed side of the
// intent-branched guard: a node status that is corrupt for the entry's intent
// (or a terminal node, or an activation mismatch) still rejects — the widened
// allowlist does not let guard-state corruption through.
func TestReplayDeadLetterIntentRejects(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	cases := []struct {
		name        string
		execID      string
		entryID     string
		nodeStatus  string
		activation  int
		wantOutcome engine.DeadLetterReplayOutcome
	}{
		// retry/requeue on a suspended node is corrupt guard state (reset/revoke
		// would have left pending; suspended means the node was paused out from
		// under the retry) → outcome 7.
		{"retry_suspended_corrupt", "dl-rej-retry-susp", "retry/dl-rej-retry-susp/review/1", "suspended", 1, engine.ReplayRejectedMetadataMissing},
		// resume targets a suspended/pending node; running is not a valid target
		// → outcome 7.
		{"resume_running_invalid", "dl-rej-resume-running", "resume/dl-rej-resume-running/review/1", "running", 1, engine.ReplayRejectedMetadataMissing},
		// advance on a suspended node is not a valid fresh schedule → outcome 7.
		{"advance_suspended_invalid", "dl-rej-advance-susp", "advance/dl-rej-advance-susp/review/1", "suspended", 1, engine.ReplayRejectedMetadataMissing},
		// advance whose source is NOT terminal is inconsistent with the advance
		// lifecycle (commitNodeLua writes source terminal + advance outbox
		// atomically; the entry cannot exist with a non-terminal source) → 7.
		{"advance_source_running_invalid", "dl-rej-advance-running", "advance/dl-rej-advance-running/review/1", "running", 1, engine.ReplayRejectedMetadataMissing},
		// advance with an absent source is likewise not a real advance dead-letter → 7.
		{"advance_source_absent_invalid", "dl-rej-advance-absent", "advance/dl-rej-advance-absent/review/1", "", 0, engine.ReplayRejectedMetadataMissing},
		// terminal node rejects regardless of intent → outcome 4.
		{"execute_node_success_terminal", "dl-rej-execute-succ", "execute/dl-rej-execute-succ/review/1", "success", 1, engine.ReplayRejectedNodeTerminal},
		// retry with a current activation that does not match the entry's stale
		// activation → outcome 5 (activation mismatch, cyclic re-entry guard).
		{"retry_activation_mismatch", "dl-rej-retry-act", "retry/dl-rej-retry-act/review/1", "pending", 2, engine.ReplayRejectedActivationMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := types.ExecutionID(tc.execID)
			seedDeadLetterIntent(t, state, id, tc.entryID, tc.nodeStatus, tc.activation)

			res, err := state.ReplayDeadLetter(ctx, replayReq(id, tc.entryID, "req-"+tc.name))
			if err != nil {
				t.Fatalf("ReplayDeadLetter: %v", err)
			}
			if res.Outcome != tc.wantOutcome {
				t.Fatalf("outcome = %q, want %q", res.Outcome, tc.wantOutcome)
			}
			// Rejection must not move the entry.
			dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
			if err != nil {
				t.Fatalf("ListDeadLetters: %v", err)
			}
			if len(dead.Entries) != 1 {
				t.Fatalf("dead = %d entries, want 1 (rejection must not move)", len(dead.Entries))
			}
		})
	}
}

// TestReplayDeadLetterLegacyIntentFailClosed proves the migration safety: an
// entry written before the intent field existed (legacy dead-meta with no
// intent) stays fail-closed outcome 7 — operators must re-create such entries
// rather than have them silently replay under the widened, intent-branched rules.
func TestReplayDeadLetterLegacyIntentFailClosed(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-legacy-no-intent")
	entryID := "execute/dl-legacy-no-intent/review/1"
	// seedDeadLetterFullGuard now writes intent/task_type; delete them to
	// simulate legacy shape (no intent) for the migration safety contract.
	seedDeadLetterFullGuard(t, state, id, entryID)
	if err := state.rdb.HDel(ctx, outboxDeadMetaKey(tenant.DefaultTenant, id, entryID), "intent", "task_type").Err(); err != nil {
		t.Fatalf("HDel intent/task_type: %v", err)
	}

	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-legacy"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedMetadataMissing {
		t.Fatalf("outcome = %q, want rejected_metadata_missing (legacy no-intent must stay fail-closed)", res.Outcome)
	}
}
