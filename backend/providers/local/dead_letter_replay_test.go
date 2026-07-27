package local

import (
	"context"
	"fmt"
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
		if _, err := state.RecordOutboxFailure(ctx, id, root, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	dead, err := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if err != nil || len(dead.Entries) != 1 || dead.Entries[0].ID != entryID {
		t.Fatalf("seed dead-letter: entries=%+v err=%v", dead, err)
	}
	return id, state, entryID
}

func memReplayReq(id types.ExecutionID, entryID, requestID string) engine.ReplayDeadLetterRequest {
	return engine.ReplayDeadLetterRequest{
		ExecutionID: id, EntryID: entryID, RequestID: requestID,
		Operator: "cli:tester", Reason: "operator replay after root-cause",
	}
}

func TestMemoryReplayDeadLetterMovesToReadyAndResetsAttempts(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	res, err := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, "req-1"))
	if err != nil || res.Outcome != engine.ReplayReplayed {
		t.Fatalf("ReplayDeadLetter outcome=%q err=%v, want replayed", res.Outcome, err)
	}
	if res.AuditID == "" {
		t.Fatalf("result = %+v, want audit_id set", res)
	}
	dead, _ := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if len(dead.Entries) != 0 {
		t.Fatalf("dead-letter not cleared, got %d", len(dead.Entries))
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
	var replayed, alreadyReplayed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			res, _ := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, fmt.Sprintf("req-%d", i)))
			switch res.Outcome {
			case engine.ReplayReplayed:
				replayed.Add(1)
			case engine.ReplayAlreadyReplayed:
				alreadyReplayed.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if replayed.Load() != 1 {
		t.Fatalf("replayed = %d, want 1", replayed.Load())
	}
	if replayed.Load()+alreadyReplayed.Load() != int64(n) {
		t.Fatalf("replayed+already = %d, want %d", replayed.Load()+alreadyReplayed.Load(), n)
	}
}

func TestMemoryReplayDeadLetterAlreadyReplayedOnSameRequestID(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	first, _ := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, "req-lossy"))
	if first.Outcome != engine.ReplayReplayed {
		t.Fatalf("first outcome = %q, want replayed", first.Outcome)
	}
	second, _ := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, "req-lossy"))
	if second.Outcome != engine.ReplayAlreadyReplayed {
		t.Fatalf("retry outcome = %q, want already_replayed", second.Outcome)
	}
	if second.AuditID != first.AuditID {
		t.Fatalf("retry audit_id = %q, want original %q", second.AuditID, first.AuditID)
	}
}

func TestMemoryReplayDeadLetterRejectsTerminal(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	state.mu.Lock()
	state.executions[id].snap.Status = types.ExecutionStatusSuccess
	state.mu.Unlock()

	res, _ := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, "req-term"))
	if res.Outcome != engine.ReplayRejectedTerminal {
		t.Fatalf("outcome = %q, want rejected_terminal", res.Outcome)
	}
	dead, _ := state.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
	if len(dead.Entries) != 1 {
		t.Fatalf("dead-letter = %d, want 1 (terminal replay must not mutate)", len(dead.Entries))
	}
}

func TestMemoryReplayDeadLetterRejectsInactive(t *testing.T) {
	ctx := context.Background()
	id, state, entryID := newDeadLetterState(t)

	state.mu.Lock()
	delete(state.executions, id)
	state.mu.Unlock()

	res, _ := state.ReplayDeadLetter(ctx, memReplayReq(id, entryID, "req-inactive"))
	if res.Outcome != engine.ReplayRejectedInactive {
		t.Fatalf("outcome = %q, want rejected_inactive", res.Outcome)
	}
}
