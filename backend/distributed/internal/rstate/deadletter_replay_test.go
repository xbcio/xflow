package rstate

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
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
	if err := state.rdb.HSet(ctx, outboxBodyKey(id), entryID, body).Err(); err != nil {
		t.Fatalf("HSet outbox body: %v", err)
	}
	if err := state.rdb.ZAdd(ctx, outboxReadyKey(id), redis.Z{Score: float64(time.Now().UTC().UnixMilli()), Member: entryID}).Err(); err != nil {
		t.Fatalf("ZAdd outbox ready: %v", err)
	}
	// Seed the node at activation 1 so the activation guard has a current value.
	if err := state.rdb.Set(ctx, nodeStatusKey(id, "review"), "running", time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}
	if err := state.rdb.HSet(ctx, nodeMetaKey(id, "review"), "activation_id", 1).Err(); err != nil {
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

	if err := state.rdb.Set(ctx, execKey(id, "status"), "success", time.Minute).Err(); err != nil {
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

	if err := state.rdb.Del(ctx, execKey(id, "status")).Err(); err != nil {
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

	if err := state.rdb.Set(ctx, nodeStatusKey(id, "review"), "success", time.Minute).Err(); err != nil {
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
	if err := state.rdb.HSet(ctx, nodeMetaKey(id, "review"), "activation_id", 2).Err(); err != nil {
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

	deadBody, err := state.rdb.HGet(ctx, outboxDeadBodyKey(id), entryID).Result()
	if err != nil {
		t.Fatalf("read dead body before replay: %v", err)
	}
	if _, err := state.ReplayDeadLetter(ctx, replayReq(id, entryID, "req-body")); err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	readyBody, err := state.rdb.HGet(ctx, outboxBodyKey(id), entryID).Result()
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
		if err := state.rdb.HSet(ctx, outboxDeadBodyKey(id), entryID, body).Err(); err != nil {
			t.Fatalf("HSet: %v", err)
		}
		if err := state.rdb.ZAdd(ctx, outboxDeadKey(id), redis.Z{Score: float64(i), Member: entryID}).Err(); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		if err := state.rdb.HSet(ctx, outboxDeadMetaKey(id, entryID), "node", "review", "activation", "1").Err(); err != nil {
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
