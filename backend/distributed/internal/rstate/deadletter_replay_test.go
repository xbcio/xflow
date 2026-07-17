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

// seedDeadLetter creates a running execution with one ready outbox entry,
// then drives RecordOutboxFailure to maxAttempts so the entry lands in
// dead-letter storage. It returns the entry ID.
func seedDeadLetter(t *testing.T, state *Store, id types.ExecutionID, entryID string) {
	t.Helper()
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	body, err := marshalRedisOutboxEntry(entryID, engine.Task{
		ExecutionID:  id,
		NodeName:     "review",
		NodeIdx:      1,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: 1,
	}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := state.rdb.HSet(ctx, outboxBodyKey(id), entryID, body).Err(); err != nil {
		t.Fatalf("HSet outbox body: %v", err)
	}
	if err := state.rdb.ZAdd(ctx, outboxReadyKey(id), redis.Z{Score: float64(time.Now().UTC().UnixMilli()), Member: entryID}).Err(); err != nil {
		t.Fatalf("ZAdd outbox ready: %v", err)
	}
	// Drive to dead-letter via the production failure path.
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entryID, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	dead, err := state.ListDeadLetters(ctx, id, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters after seed: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != entryID {
		t.Fatalf("expected 1 dead-letter entry %q, got %+v", entryID, dead)
	}
}

func TestReplayDeadLetterMovesToReadyAndResetsAttempts(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-1")
	entryID := "execute/dl-replay-1/review/1"
	seedDeadLetter(t, state, id, entryID)

	outcome, err := state.ReplayDeadLetter(ctx, id, entryID)
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", outcome)
	}

	// Dead-letter storage is empty; ready set now holds the entry.
	dead, err := state.ListDeadLetters(ctx, id, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters after replay: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead-letter not cleared after replay, got %d entries", len(dead))
	}
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 1 || ready[0].ID != entryID {
		t.Fatalf("ready = %+v, want 1 entry %q", ready, entryID)
	}
	// Attempts must be reset so the entry is not immediately re-dead-lettered.
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
	var replayed, notFound atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			outcome, err := state.ReplayDeadLetter(ctx, id, entryID)
			if err != nil {
				t.Errorf("ReplayDeadLetter: %v", err)
				return
			}
			switch outcome {
			case engine.ReplayReplayed:
				replayed.Add(1)
			case engine.ReplayNotFound:
				notFound.Add(1)
			default:
				t.Errorf("unexpected outcome %q under concurrent replay", outcome)
			}
		}()
	}
	close(start)
	wg.Wait()

	if replayed.Load() != 1 {
		t.Fatalf("replayed = %d, want exactly 1", replayed.Load())
	}
	if replayed.Load()+notFound.Load() != int64(n) {
		t.Fatalf("replayed+notfound = %d, want %d", replayed.Load()+notFound.Load(), n)
	}
	// Exactly one entry in ready, none in dead-letter.
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 1 {
		t.Fatalf("ready entries = %d, want 1", len(ready))
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
	outcome, err := state.ReplayDeadLetter(ctx, id, entryID)
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("outcome = %q, want rejected_terminal", outcome)
	}
	// Dead-letter entry must remain intact.
	dead, err := state.ListDeadLetters(ctx, id, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1 (replay must not mutate terminal exec)", len(dead))
	}
}

func TestReplayDeadLetterRejectsInactiveExecution(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-inactive")
	entryID := "execute/dl-replay-inactive/review/1"
	seedDeadLetter(t, state, id, entryID)

	// Simulate execution expiry: drop the status key.
	if err := state.rdb.Del(ctx, execKey(id, "status")).Err(); err != nil {
		t.Fatalf("del status: %v", err)
	}
	_ = mr
	outcome, err := state.ReplayDeadLetter(ctx, id, entryID)
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if outcome != engine.ReplayRejectedInactive {
		t.Fatalf("outcome = %q, want rejected_inactive", outcome)
	}
}

func TestReplayDeadLetterNotFoundForUnknownEntry(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("dl-replay-missing")
	entryID := "execute/dl-replay-missing/review/1"
	seedDeadLetter(t, state, id, entryID)

	outcome, err := state.ReplayDeadLetter(ctx, id, "does/not/exist")
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if outcome != engine.ReplayNotFound {
		t.Fatalf("outcome = %q, want not_found", outcome)
	}
}
