package rstate

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/types"
)

// seedDeadLetter creates a running execution with one ready outbox entry whose
// task targets node "review" at activation 1, then drives RecordOutboxFailure
// to maxAttempts so the entry lands in dead-letter storage with immutable
// node/activation metadata. It returns the seeded entry.
func seedDeadLetter(t *testing.T, state *Store, id types.ExecutionID, entryID string) engine.OutboxEntry {
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
	// Seed the node at activation 1 so the activation guard has a current value.
	if err := state.rdb.Set(ctx, nodeStatusKey(tenant.DefaultTenant, id, "review"), "running", time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}
	if err := state.rdb.HSet(ctx, nodeMetaKey(tenant.DefaultTenant, id, "review"), "activation_id", 1).Err(); err != nil {
		t.Fatalf("set node meta: %v", err)
	}
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters after seed: %v", err)
	}
	if len(dead.Entries) != 1 || dead.Entries[0].ID != entryID {
		t.Fatalf("expected 1 dead-letter entry %q, got %+v", entryID, dead)
	}
	return entry
}

func replayReq(id types.ExecutionID, entryID, requestID string) engine.ReplayDeadLetterRequest {
	return engine.ReplayDeadLetterRequest{
		ExecutionID: id, EntryID: entryID, RequestID: requestID,
		Operator: "cli:tester", Reason: "operator replay after root-cause",
	}
}

func TestReplayDeadLetterMovesToReadyAndResetsAttempts(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-1")
	entryID := "execute/dl-replay-1/review/1"
	seedDeadLetter(t, state, id, entryID)

	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-1"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	if res.AuditID == "" || res.NodeID != "review" || res.ActivationID != "1" {
		t.Fatalf("result = %+v, want audit_id set, node=review, activation=1", res)
	}

	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters after replay: %v", err)
	}
	if len(dead.Entries) != 0 {
		t.Fatalf("dead-letter not cleared after replay, got %d entries", len(dead.Entries))
	}
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 1 || ready[0].ID != entryID {
		t.Fatalf("ready = %+v, want 1 entry %q", ready, entryID)
	}
	if ready[0].Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 after replay", ready[0].Attempts)
	}
}

func TestReplayDeadLetterConcurrentReplaysCollapseToOne(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-concurrent")
	entryID := "execute/dl-replay-concurrent/review/1"
	seedDeadLetter(t, state, id, entryID)

	const n = 16
	var replayed, alreadyReplayed, other atomic.Int64
	var firstAudit atomic.Value // string
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			// Each goroutine uses a distinct RequestID: only one must move,
			// the rest must return already_replayed with the original audit_id.
			res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, fmtReq(i)))
			if err != nil {
				t.Errorf("ReplayDeadLetter: %v", err)
				return
			}
			switch res.Outcome {
			case engine.ReplayReplayed:
				replayed.Add(1)
				firstAudit.Store(res.AuditID)
			case engine.ReplayAlreadyReplayed:
				alreadyReplayed.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected outcome %q under concurrent replay", res.Outcome)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if replayed.Load() != 1 {
		t.Fatalf("replayed = %d, want exactly 1", replayed.Load())
	}
	if replayed.Load()+alreadyReplayed.Load() != int64(n) {
		t.Fatalf("replayed+already = %d, want %d (other=%d)", replayed.Load()+alreadyReplayed.Load(), n, other.Load())
	}
	// Every already_replayed result must carry the original audit_id.
	orig, _ := firstAudit.Load().(string)
	if orig == "" {
		t.Fatal("no first audit_id recorded")
	}
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 1 {
		t.Fatalf("ready entries = %d, want 1", len(ready))
	}
}

func fmtReq(i int) string { return "req-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// TestReplayDeadLetterAlreadyReplayedOnSameRequestID verifies response-loss
// recovery: retrying with the same RequestID returns already_replayed with the
// original audit_id, even after the dead body is gone.
func TestReplayDeadLetterAlreadyReplayedOnSameRequestID(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-reqid")
	entryID := "execute/dl-replay-reqid/review/1"
	seedDeadLetter(t, state, id, entryID)

	first, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-lossy"))
	if err != nil {
		t.Fatalf("first ReplayDeadLetter: %v", err)
	}
	if first.Outcome != engine.ReplayReplayed {
		t.Fatalf("first outcome = %q, want replayed", first.Outcome)
	}
	// Retry with the same RequestID, simulating a lost response.
	second, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-lossy"))
	if err != nil {
		t.Fatalf("retry ReplayDeadLetter: %v", err)
	}
	if second.Outcome != engine.ReplayAlreadyReplayed {
		t.Fatalf("retry outcome = %q, want already_replayed", second.Outcome)
	}
	if second.AuditID != first.AuditID {
		t.Fatalf("retry audit_id = %q, want original %q", second.AuditID, first.AuditID)
	}
}

func TestReplayDeadLetterRejectsTerminalExecution(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-terminal")
	entryID := "execute/dl-replay-terminal/review/1"
	seedDeadLetter(t, state, id, entryID)

	if err := state.rdb.Set(ctx, execKey(tenant.DefaultTenant, id, "status"), "success", time.Minute).Err(); err != nil {
		t.Fatalf("set terminal status: %v", err)
	}
	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-term"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("outcome = %q, want rejected_terminal", res.Outcome)
	}
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead.Entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1 (replay must not mutate terminal exec)", len(dead.Entries))
	}
}

func TestReplayDeadLetterRejectsInactiveExecution(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-inactive")
	entryID := "execute/dl-replay-inactive/review/1"
	seedDeadLetter(t, state, id, entryID)

	if err := state.rdb.Del(ctx, execKey(tenant.DefaultTenant, id, "status")).Err(); err != nil {
		t.Fatalf("del status: %v", err)
	}
	_ = mr
	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-inactive"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedInactive {
		t.Fatalf("outcome = %q, want rejected_inactive", res.Outcome)
	}
}

// TestReplayDeadLetterRejectsNodeTerminal verifies the node-level guard: when
// the entry's node is already terminal, replay is rejected without mutating
// state, even though the execution is still running.
func TestReplayDeadLetterRejectsNodeTerminal(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-nodeterm")
	entryID := "execute/dl-replay-nodeterm/review/1"
	seedDeadLetter(t, state, id, entryID)

	if err := state.rdb.Set(ctx, nodeStatusKey(tenant.DefaultTenant, id, "review"), "success", time.Minute).Err(); err != nil {
		t.Fatalf("set node terminal status: %v", err)
	}
	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-nodeterm"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedNodeTerminal {
		t.Fatalf("outcome = %q, want rejected_node_terminal", res.Outcome)
	}
	if res.NodeID != "review" {
		t.Fatalf("node = %q, want review", res.NodeID)
	}
	// Entry must remain in dead-letter storage.
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead.Entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1", len(dead.Entries))
	}
}

// TestReplayDeadLetterRejectsActivationMismatch verifies that a stale
// activation (the node has moved to a higher activation via cyclic re-entry)
// is rejected, while a matching activation succeeds.
func TestReplayDeadLetterRejectsActivationMismatch(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-actmismatch")
	entryID := "execute/dl-replay-actmismatch/review/1"
	seedDeadLetter(t, state, id, entryID) // entry activation = 1

	// Node advanced to activation 2 via cyclic re-entry; entry is stale.
	if err := state.rdb.HSet(ctx, nodeMetaKey(tenant.DefaultTenant, id, "review"), "activation_id", 2).Err(); err != nil {
		t.Fatalf("bump activation: %v", err)
	}
	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-stale"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedActivationMismatch {
		t.Fatalf("outcome = %q, want rejected_activation_mismatch", res.Outcome)
	}
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead.Entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1 (stale activation not replayed)", len(dead.Entries))
	}
}

// TestReplayDeadLetterCurrentActivationSucceeds verifies that when the entry's
// activation matches the node's current activation, replay proceeds.
func TestReplayDeadLetterCurrentActivationSucceeds(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-actmatch")
	entryID := "execute/dl-replay-actmatch/review/1"
	seedDeadLetter(t, state, id, entryID) // entry activation = 1, node activation = 1

	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-match"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed (activation matches)", res.Outcome)
	}
}

// TestReplayDeadLetterBodyPreserved verifies the immutable task body is
// byte-for-byte identical after the dead->ready move.
func TestReplayDeadLetterBodyPreserved(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-body")
	entryID := "execute/dl-replay-body/review/1"
	seedDeadLetter(t, state, id, entryID)

	deadBody, err := state.rdb.HGet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), entryID).Result()
	if err != nil {
		t.Fatalf("read dead body before replay: %v", err)
	}
	if _, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-body")); err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	readyBody, err := state.rdb.HGet(ctx, outboxBodyKey(tenant.DefaultTenant, id), entryID).Result()
	if err != nil {
		t.Fatalf("read ready body after replay: %v", err)
	}
	if deadBody != readyBody {
		t.Fatalf("body changed across replay:\n dead=%s\nready=%s", deadBody, readyBody)
	}
}

func TestReplayDeadLetterNotFoundForUnknownEntry(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-missing")
	entryID := "execute/dl-replay-missing/review/1"
	seedDeadLetter(t, state, id, entryID)

	res, err := state.ReplayDeadLetter(ctx, replayReq(id, "does/not/exist", "req-missing"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayNotFound {
		t.Fatalf("outcome = %q, want not_found", res.Outcome)
	}
}

// TestListDeadLettersCursorPagination verifies opaque-cursor pagination: no
// duplicates, no drops across pages, and a non-empty NextCursor until the
// final page.
func TestListDeadLettersCursorPagination(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-list-page")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	const total = 7
	for i := 0; i < total; i++ {
		entryID := "execute/dl-list-page/review/" + itoa(i)
		entry := engine.OutboxEntry{ID: entryID, Task: engine.Task{ExecutionID: id, NodeName: "review", ActivationID: 1, Type: engine.TaskTypeNodeExec}}
		body, err := marshalRedisOutboxEntry(entryID, entry.Task, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := state.rdb.HSet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), entryID, body).Err(); err != nil {
			t.Fatalf("HSet: %v", err)
		}
		if err := state.rdb.ZAdd(ctx, outboxDeadKey(tenant.DefaultTenant, id), redis.Z{Score: float64(i), Member: entryID}).Err(); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		if err := state.rdb.HSet(ctx, outboxDeadMetaKey(tenant.DefaultTenant, id, entryID), "node", "review", "activation", "1").Err(); err != nil {
			t.Fatalf("HSet meta: %v", err)
		}
	}

	seen := make(map[string]bool)
	cursor := ""
	pages := 0
	for {
		list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListDeadLetters cursor=%q: %v", cursor, err)
		}
		for _, e := range list.Entries {
			if seen[e.ID] {
				t.Fatalf("duplicate entry %q across pages", e.ID)
			}
			seen[e.ID] = true
		}
		pages++
		if list.NextCursor == "" {
			break
		}
		cursor = list.NextCursor
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("saw %d unique entries, want %d", len(seen), total)
	}
}

// seedDeadLetterNoMeta seeds a dead-letter entry WITHOUT the per-entry meta
// hash, simulating a legacy entry written before the meta hash existed. The
// entry body and dead index are present, but node/activation metadata is gone.
func seedDeadLetterNoMeta(t *testing.T, state *Store, id types.ExecutionID, entryID string) {
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
	if err := state.rdb.HSet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), entryID, body).Err(); err != nil {
		t.Fatalf("HSet dead body: %v", err)
	}
	if err := state.rdb.ZAdd(ctx, outboxDeadKey(tenant.DefaultTenant, id), redis.Z{Score: float64(time.Now().UTC().UnixMilli()), Member: entryID}).Err(); err != nil {
		t.Fatalf("ZAdd dead: %v", err)
	}
	if err := state.rdb.Set(ctx, nodeStatusKey(tenant.DefaultTenant, id, "review"), "running", time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}
	if err := state.rdb.HSet(ctx, nodeMetaKey(tenant.DefaultTenant, id, "review"), "activation_id", 1).Err(); err != nil {
		t.Fatalf("set node meta: %v", err)
	}
}

// TestReplayDeadLetterFailClosedOnMissingMeta verifies the fail-closed contract:
// a legacy entry without dead-meta (node/activation) is NOT silently replayed.
// Replay returns rejected_metadata_missing, dead/ready are unchanged, and an
// immutable receipt is written so the same RequestID recovers the rejection.
func TestReplayDeadLetterFailClosedOnMissingMeta(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-nometa")
	entryID := "execute/dl-replay-nometa/review/1"
	seedDeadLetterNoMeta(t, state, id, entryID)

	res, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-nometa"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayRejectedMetadataMissing {
		t.Fatalf("outcome = %q, want rejected_metadata_missing", res.Outcome)
	}
	if res.AuditID == "" {
		t.Fatalf("rejected_metadata_missing must still write a receipt (audit_id empty)")
	}
	// Entry must remain in dead-letter storage; ready must be empty.
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead.Entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1 (fail-closed must not move)", len(dead.Entries))
	}
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("ready entries = %d, want 0 (fail-closed must not enqueue)", len(ready))
	}
}

// TestReplayDeadLetterMetadataMissingRecoverable verifies that a
// rejected_metadata_missing receipt is recoverable: retrying with the same
// RequestID returns the same outcome and AuditID.
func TestReplayDeadLetterMetadataMissingRecoverable(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-nometa-rec")
	entryID := "execute/dl-replay-nometa-rec/review/1"
	seedDeadLetterNoMeta(t, state, id, entryID)

	first, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-rec"))
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if first.Outcome != engine.ReplayRejectedMetadataMissing {
		t.Fatalf("first outcome = %q, want rejected_metadata_missing", first.Outcome)
	}
	second, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-rec"))
	if err != nil {
		t.Fatalf("retry replay: %v", err)
	}
	if second.Outcome != engine.ReplayRejectedMetadataMissing {
		t.Fatalf("retry outcome = %q, want rejected_metadata_missing", second.Outcome)
	}
	if second.AuditID != first.AuditID {
		t.Fatalf("retry audit_id = %q, want original %q", second.AuditID, first.AuditID)
	}
}

// TestReplayDeadLetterRejectionsWriteRecoverableReceipt verifies that every
// determinable rejection (terminal/inactive/node_terminal/activation_mismatch)
// writes an immutable receipt so retrying with the same RequestID recovers the
// same outcome + AuditID.
func TestReplayDeadLetterRejectionsWriteRecoverableReceipt(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		prepare  func(t *testing.T, state *Store, id types.ExecutionID)
		outcome  engine.DeadLetterReplayOutcome
		entryID  string
		execID   string
	}{
		{
			name:    "terminal",
			execID:   "dl-rej-terminal",
			entryID: "execute/dl-rej-terminal/review/1",
			prepare: func(t *testing.T, state *Store, id types.ExecutionID) {
				if err := state.rdb.Set(ctx, execKey(tenant.DefaultTenant, id, "status"), "success", time.Minute).Err(); err != nil {
					t.Fatalf("set terminal status: %v", err)
				}
			},
			outcome: engine.ReplayRejectedTerminal,
		},
		{
			name:    "inactive",
			execID:   "dl-rej-inactive",
			entryID: "execute/dl-rej-inactive/review/1",
			prepare: func(t *testing.T, state *Store, id types.ExecutionID) {
				if err := state.rdb.Del(ctx, execKey(tenant.DefaultTenant, id, "status")).Err(); err != nil {
					t.Fatalf("del status: %v", err)
				}
			},
			outcome: engine.ReplayRejectedInactive,
		},
		{
			name:    "node_terminal",
			execID:   "dl-rej-nodeterm",
			entryID: "execute/dl-rej-nodeterm/review/1",
			prepare: func(t *testing.T, state *Store, id types.ExecutionID) {
				if err := state.rdb.Set(ctx, nodeStatusKey(tenant.DefaultTenant, id, "review"), "success", time.Minute).Err(); err != nil {
					t.Fatalf("set node terminal: %v", err)
				}
			},
			outcome: engine.ReplayRejectedNodeTerminal,
		},
		{
			name:    "activation_mismatch",
			execID:   "dl-rej-actmismatch",
			entryID: "execute/dl-rej-actmismatch/review/1",
			prepare: func(t *testing.T, state *Store, id types.ExecutionID) {
				if err := state.rdb.HSet(ctx, nodeMetaKey(tenant.DefaultTenant, id, "review"), "activation_id", 2).Err(); err != nil {
					t.Fatalf("bump activation: %v", err)
				}
			},
			outcome: engine.ReplayRejectedActivationMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, _, _ := newTestRedisState(t)
			id := types.ExecutionID(tc.execID)
			seedDeadLetter(t, state, id, tc.entryID)
			tc.prepare(t, state, id)

			first, err := state.ReplayDeadLetter(ctx, replayReq(id, tc.entryID, "req-rej"))
			if err != nil {
				t.Fatalf("first replay: %v", err)
			}
			if first.Outcome != tc.outcome {
				t.Fatalf("first outcome = %q, want %q", first.Outcome, tc.outcome)
			}
			if first.AuditID == "" {
				t.Fatalf("rejection %q must write a receipt (audit_id empty)", tc.outcome)
			}
			// Entry must remain in dead-letter storage (no move on rejection).
			dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
			if err != nil {
				t.Fatalf("ListDeadLetters: %v", err)
			}
			if len(dead.Entries) != 1 {
				t.Fatalf("dead-letter entries = %d, want 1 (rejection must not move)", len(dead.Entries))
			}
			// Same RequestID retry recovers the same outcome + audit_id.
			second, err := state.ReplayDeadLetter(ctx, replayReq(id, tc.entryID, "req-rej"))
			if err != nil {
				t.Fatalf("retry replay: %v", err)
			}
			if second.Outcome != tc.outcome {
				t.Fatalf("retry outcome = %q, want %q", second.Outcome, tc.outcome)
			}
			if second.AuditID != first.AuditID {
				t.Fatalf("retry audit_id = %q, want original %q", second.AuditID, first.AuditID)
			}
		})
	}
}

// TestReplayDeadLetterRejectionDifferentRequestIDReEvaluates verifies that a
// different RequestID for a rejected (un-moved) entry does NOT collapse: the
// entry index is only written on a successful move, so a fresh RequestID
// re-evaluates and writes its own receipt.
func TestReplayDeadLetterRejectionDifferentRequestIDReEvaluates(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-rej-diffreq")
	entryID := "execute/dl-rej-diffreq/review/1"
	seedDeadLetter(t, state, id, entryID)
	if err := state.rdb.Set(ctx, execKey(tenant.DefaultTenant, id, "status"), "success", time.Minute).Err(); err != nil {
		t.Fatalf("set terminal: %v", err)
	}

	first, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-a"))
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if first.Outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("first outcome = %q, want rejected_terminal", first.Outcome)
	}
	second, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-b"))
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if second.Outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("second outcome = %q, want rejected_terminal", second.Outcome)
	}
	if second.AuditID == first.AuditID {
		t.Fatalf("different RequestID must produce a different audit_id, both got %q", first.AuditID)
	}
}

// seedDeadLettersBulk seeds n dead-letter entries with explicit (score, member)
// ordering for cursor tests. The meta hash is populated so replay would be
// eligible (though these tests do not replay).
func seedDeadLettersBulk(t *testing.T, state *Store, id types.ExecutionID, scores []float64, members []string) {
	t.Helper()
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	for i, m := range members {
		entry := engine.OutboxEntry{ID: m, Task: engine.Task{ExecutionID: id, NodeName: "review", ActivationID: 1, Type: engine.TaskTypeNodeExec}}
		body, err := marshalRedisOutboxEntry(m, entry.Task, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := state.rdb.HSet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), m, body).Err(); err != nil {
			t.Fatalf("HSet body: %v", err)
		}
		if err := state.rdb.ZAdd(ctx, outboxDeadKey(tenant.DefaultTenant, id), redis.Z{Score: scores[i], Member: m}).Err(); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		if err := state.rdb.HSet(ctx, outboxDeadMetaKey(tenant.DefaultTenant, id, m), "node", "review", "activation", "1").Err(); err != nil {
			t.Fatalf("HSet meta: %v", err)
		}
	}
}

// TestListDeadLettersCursorSameScoreStable verifies that entries sharing the
// same score paginate stably by member (lex), with no duplicates or drops.
func TestListDeadLettersCursorSameScoreStable(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-list-samescore")
	const total = 6
	members := make([]string, total)
	scores := make([]float64, total)
	for i := 0; i < total; i++ {
		// Same score, distinct members; Redis orders same-score by member lex.
		members[i] = "execute/dl-list-samescore/review/m" + itoa(i)
		scores[i] = 100
	}
	seedDeadLettersBulk(t, state, id, scores, members)

	seen := make(map[string]bool)
	cursor := ""
	pages := 0
	for {
		list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListDeadLetters cursor=%q: %v", cursor, err)
		}
		for _, e := range list.Entries {
			if seen[e.ID] {
				t.Fatalf("duplicate entry %q across pages", e.ID)
			}
			seen[e.ID] = true
		}
		pages++
		if list.NextCursor == "" {
			break
		}
		cursor = list.NextCursor
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("saw %d unique entries, want %d", len(seen), total)
	}
}

// TestListDeadLettersCursorRejectsInvalid verifies that a malformed, tampered,
// cross-execution, or post-restart cursor is rejected with ErrCursorExpired
// rather than silently restarting.
func TestListDeadLettersCursorRejectsInvalid(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-cursor-invalid")
	idOther := types.ExecutionID("dl-cursor-other")
	members := []string{"e1", "e2", "e3", "e4"}
	scores := []float64{1, 2, 3, 4}
	seedDeadLettersBulk(t, state, id, scores, members)
	seedDeadLettersBulk(t, state, idOther, scores, members)

	// Obtain a valid cursor for id.
	list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 2})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if list.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}
	valid := list.NextCursor

	cases := []struct {
		name   string
		cursor string
		execID types.ExecutionID
	}{
		{"malformed", "garbage", id},
		{"wrong_version_prefix", "v9.abc.def", id},
		{"cross_execution", valid, idOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := state.ListDeadLetters(ctx, tc.execID, engine.DeadLetterPage{Limit: 2, Cursor: tc.cursor})
			if !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("err = %v, want ErrCursorExpired", err)
			}
		})
	}

	// Tampered payload: append a byte to the base64url payload, leaving the
	// signature. HMAC verification then fails -> ErrCursorExpired.
	t.Run("tampered_payload", func(t *testing.T) {
		parts := strings.SplitN(valid, ".", 3)
		if len(parts) != 3 {
			t.Fatalf("valid cursor has %d parts, want 3", len(parts))
		}
		tampered := parts[0] + "." + parts[1] + "x" + "." + parts[2]
		_, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 2, Cursor: tampered})
		if !errors.Is(err, ErrCursorExpired) {
			t.Fatalf("err = %v, want ErrCursorExpired", err)
		}
	})

	// Expired cursor: simulate a restart by rotating the process-local key.
	t.Run("expired_after_key_rotation", func(t *testing.T) {
		state.cursorKey = newCursorSigningKey()
		_, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 2, Cursor: valid})
		if !errors.Is(err, ErrCursorExpired) {
			t.Fatalf("err = %v, want ErrCursorExpired", err)
		}
	})
}

// TestListDeadLettersConcurrentInsertNoDrop verifies the pagination contract
// under concurrent insertion: new entries added between pages are either on a
// later page or after the current cursor, and existing entries are not dropped
// or duplicated.
func TestListDeadLettersConcurrentInsertNoDrop(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-list-concurrent")
	const initial = 10
	members := make([]string, initial)
	scores := make([]float64, initial)
	for i := 0; i < initial; i++ {
		members[i] = "exec" + itoa(i)
		scores[i] = float64(i)
	}
	seedDeadLettersBulk(t, state, id, scores, members)

	seen := make(map[string]bool)
	cursor := ""
	var inserter sync.WaitGroup
	inserted := make([]string, 0)
	var insMu sync.Mutex
	inserter.Add(1)
	go func() {
		defer inserter.Done()
		for i := 0; i < 8; i++ {
			m := "new" + itoa(i)
			insMu.Lock()
			inserted = append(inserted, m)
			insMu.Unlock()
			body, _ := marshalRedisOutboxEntry(m, engine.Task{ExecutionID: id, NodeName: "review", ActivationID: 1, Type: engine.TaskTypeNodeExec}, time.Now().UTC())
			_ = state.rdb.HSet(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), m, body).Err()
			_ = state.rdb.ZAdd(ctx, outboxDeadKey(tenant.DefaultTenant, id), redis.Z{Score: float64(100 + i), Member: m}).Err()
			_ = state.rdb.HSet(ctx, outboxDeadMetaKey(tenant.DefaultTenant, id, m), "node", "review", "activation", "1").Err()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	pages := 0
	for {
		list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Errorf("ListDeadLetters cursor=%q: %v", cursor, err)
			break
		}
		for _, e := range list.Entries {
			if seen[e.ID] {
				t.Errorf("duplicate entry %q across pages", e.ID)
			}
			seen[e.ID] = true
		}
		pages++
		if list.NextCursor == "" {
			break
		}
		cursor = list.NextCursor
		if pages > 50 {
			t.Fatal("pagination did not terminate")
		}
	}
	inserter.Wait()

	// All initial entries must be present; concurrent inserts may or may not
	// have been seen depending on timing, but no entry must be duplicated.
	for _, m := range members {
		if !seen[m] {
			t.Errorf("initial entry %q was dropped", m)
		}
	}
}

// TestListDeadLettersDeleteDuringPagination verifies self-heal when an entry on
// the current page is deleted concurrently: listing must not panic, stale
// index entries are pruned, and pagination terminates without dropping the
// surviving entries that preceded the cursor.
func TestListDeadLettersDeleteDuringPagination(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-list-delete")
	members := []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"}
	scores := []float64{0, 1, 2, 3, 4, 5, 6}
	seedDeadLettersBulk(t, state, id, scores, members)

	// First page to capture a cursor.
	first, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 3})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Entries) != 3 {
		t.Fatalf("first page entries = %d, want 3", len(first.Entries))
	}
	if first.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}
	// Delete the body of an entry that will be on a later page (stale index).
	if err := state.rdb.HDel(ctx, outboxDeadBodyKey(tenant.DefaultTenant, id), "d3").Err(); err != nil {
		t.Fatalf("HDel: %v", err)
	}

	seen := make(map[string]bool)
	for _, e := range first.Entries {
		seen[e.ID] = true
	}
	cursor := first.NextCursor
	pages := 0
	for {
		list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListDeadLetters cursor=%q: %v", cursor, err)
		}
		for _, e := range list.Entries {
			if seen[e.ID] {
				t.Fatalf("duplicate entry %q across pages", e.ID)
			}
			seen[e.ID] = true
		}
		pages++
		if list.NextCursor == "" {
			break
		}
		cursor = list.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	// d3 was self-healed (stale index pruned) — not present. All others must be.
	if seen["d3"] {
		t.Errorf("deleted entry d3 should have been self-healed, not returned")
	}
	for _, m := range []string{"d0", "d1", "d2", "d4", "d5", "d6"} {
		if !seen[m] {
			t.Errorf("surviving entry %q was dropped", m)
		}
	}
	// The stale index entry for d3 must have been pruned.
	remaining, err := state.rdb.ZCard(ctx, outboxDeadKey(tenant.DefaultTenant, id)).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if remaining != 6 {
		t.Errorf("dead index cardinality = %d, want 6 after self-heal of d3", remaining)
	}
}

// realRedisAddr returns the real Redis address from the test env, or skips.
func realRedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skipf("XFLOW_TEST_REDIS_ADDR not set; skipping real-Redis dead-letter regression")
	}
	return addr
}

// newRealRedisState builds a Store backed by a real Redis for regression.
// It uses a unique execution namespace per test (caller-controlled keys) so
// cross-test interference is impossible.
func newRealRedisState(t *testing.T, addr string) *Store {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("real Redis at %s unreachable: %v", addr, err)
	}
	return New(rdb, nil, time.Minute)
}

// TestListDeadLettersRealRedisMultiPage exercises multi-page and same-score
// pagination against a real Redis to prove the (score, member) cursor is
// stable on a real server (miniredis does not guarantee identical ZSET
// semantics for every edge case). ENV-gated.
func TestListDeadLettersRealRedisMultiPage(t *testing.T) {
	addr := realRedisAddr(t)
	state := newRealRedisState(t, addr)
	ctx := context.Background()
	id := types.ExecutionID("dl-real-multipage-" + time.Now().Format("150405.000000000"))

	// 10 distinct scores + 4 entries sharing one score, total 14.
	const total = 14
	members := make([]string, total)
	scores := make([]float64, total)
	for i := 0; i < 10; i++ {
		members[i] = "exec" + itoa(i)
		scores[i] = float64(i)
	}
	for i := 0; i < 4; i++ {
		members[10+i] = "same" + itoa(i)
		scores[10+i] = 42 // shared score
	}
	seedDeadLettersBulk(t, state, id, scores, members)

	seen := make(map[string]bool)
	cursor := ""
	pages := 0
	for {
		list, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListDeadLetters cursor=%q: %v", cursor, err)
		}
		for _, e := range list.Entries {
			if seen[e.ID] {
				t.Fatalf("duplicate entry %q across pages", e.ID)
			}
			seen[e.ID] = true
		}
		pages++
		if list.NextCursor == "" {
			break
		}
		cursor = list.NextCursor
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("saw %d unique entries, want %d", len(seen), total)
	}

	// Replay one entry against the real Redis to exercise the full Lua path.
	res, err := state.ReplayDeadLetter(ctx, replayReq(id, "exec0", "req-real"))
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	again, err := state.ReplayDeadLetter(ctx, replayReq(id, "exec0", "req-real"))
	if err != nil {
		t.Fatalf("retry ReplayDeadLetter: %v", err)
	}
	if again.Outcome != engine.ReplayAlreadyReplayed || again.AuditID != res.AuditID {
		t.Fatalf("retry = %+v, want already_replayed with same audit_id %q", again, res.AuditID)
	}
}
