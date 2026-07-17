package local

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func newDeadLetterState(t *testing.T) (types.ExecutionID, *memoryState, string) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "memory-dead-letter",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("memory-dead-letter")
	entryID := "root/memory-dead-letter/start/0"
	root := engine.OutboxEntry{
		ID:   entryID,
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}, []engine.OutboxEntry{root}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox: %v", err)
	}
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, id, entryID, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	dead, err := state.ListDeadLetters(ctx, id, 10)
	if err != nil || len(dead) != 1 || dead[0].ID != entryID {
		t.Fatalf("seed dead-letter: entries=%+v err=%v", dead, err)
	}
	return id, state, entryID
}

func TestMemoryReplayDeadLetterMovesToReadyAndResetsAttempts(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	outcome, err := state.ReplayDeadLetter(ctx, id, entryID)
	if err != nil || outcome != engine.ReplayReplayed {
		t.Fatalf("ReplayDeadLetter outcome=%q err=%v, want replayed", outcome, err)
	}
	dead, _ := state.ListDeadLetters(ctx, id, 10)
	if len(dead) != 0 {
		t.Fatalf("dead-letter not cleared, got %d", len(dead))
	}
	ready, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(ready) != 1 || ready[0].ID != entryID || ready[0].Attempts != 0 {
		t.Fatalf("ready=%+v, want 1 entry %q with attempts 0", ready, entryID)
	}
}

func TestMemoryReplayDeadLetterConcurrentCollapseToOne(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	const n = 32
	var replayed, notFound atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			outcome, _ := state.ReplayDeadLetter(ctx, id, entryID)
			switch outcome {
			case engine.ReplayReplayed:
				replayed.Add(1)
			case engine.ReplayNotFound:
				notFound.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if replayed.Load() != 1 {
		t.Fatalf("replayed = %d, want 1", replayed.Load())
	}
	if replayed.Load()+notFound.Load() != int64(n) {
		t.Fatalf("replayed+notfound = %d, want %d", replayed.Load()+notFound.Load(), n)
	}
}

func TestMemoryReplayDeadLetterRejectsTerminal(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	state.mu.Lock()
	state.executions[id].snap.Status = types.ExecutionStatusSuccess
	state.mu.Unlock()

	outcome, _ := state.ReplayDeadLetter(ctx, id, entryID)
	if outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("outcome = %q, want rejected_terminal", outcome)
	}
	dead, _ := state.ListDeadLetters(ctx, id, 10)
	if len(dead) != 1 {
		t.Fatalf("dead-letter = %d, want 1 (terminal replay must not mutate)", len(dead))
	}
}

func TestMemoryReplayDeadLetterRejectsInactive(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	state.mu.Lock()
	delete(state.executions, id)
	state.mu.Unlock()

	outcome, _ := state.ReplayDeadLetter(ctx, id, entryID)
	if outcome != engine.ReplayRejectedInactive {
		t.Fatalf("outcome = %q, want rejected_inactive", outcome)
	}
}
