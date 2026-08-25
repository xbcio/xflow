package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// countingClaimState counts how many times FlushOutbox asks the store for a
// batch. claimOutbox prefers OutboxLeaser and falls back to ListOutbox;
// fakeState is not a leaser, so ListOutbox is the claim.
type countingClaimState struct {
	*fakeState
	claims int
}

func (s *countingClaimState) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error) {
	s.claims++
	return s.fakeState.ListOutbox(ctx, id, before, limit)
}

// TestFlushOutboxDoesNotPollAnEmptiedOutbox pins the round-trip count of the
// drain loop, which is the part of a node commit that no correctness test can
// see: every claim, delivery and ack below happens either way, so the loop that
// polls once more to be told "empty" and the loop that stops on a short batch
// produce byte-identical state.
//
// Measured against real Redis, a two-node commit was ten round trips and three
// of them were outbox claims for two batches of one entry each. The third claim
// is this test's subject.
//
// Two claims, not one: the first batch's advance intent is what appends the
// downstream node's exec intent, so the loop genuinely has to come back for it.
// Asserting 1 here would be asserting a bug.
func TestFlushOutboxDoesNotPollAnEmptiedOutbox(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	state := &countingClaimState{fakeState: inner}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "flush-poll",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := eng.Submit(ctx, g, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after submit error = %v", err)
	}
	queued := queue.Drain()
	if len(queued) != 1 || queued[0].NodeName != "start" {
		t.Fatalf("queued after submit = %+v, want the start task", queued)
	}
	lease, err := eng.BuildTaskLease(ctx, queued[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	state.claims = 0
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult() error = %v", err)
	}

	// The claim count is the whole point, but on its own it would also pass if
	// the loop exited early and dropped the downstream task on the floor. The
	// delivery assertion below is what makes the number mean "no wasted poll"
	// rather than "stopped too soon".
	if state.claims != 2 {
		t.Errorf("outbox claims during commit = %d, want 2 (one batch per appended "+
			"intent, and no trailing poll to be told the outbox is empty)", state.claims)
	}
	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "next" || delivered[0].Type != TaskTypeNodeExec {
		t.Fatalf("delivered after commit = %+v, want the downstream exec task", delivered)
	}
}

// TestFlushOutboxKeepsDrainingAFullBatch is the other half of the exit
// condition. The loop stops on a batch that is BOTH short and free of system
// tasks; a batch that fills the limit says nothing about what is behind it, so
// the loop must come back even though it handled no advance.
//
// Without this, the early exit would silently cap delivery at one batch and a
// wide fan-out would strand everything past the limit until the dispatcher's
// next tick.
func TestFlushOutboxKeepsDrainingAFullBatch(t *testing.T) {
	ctx := context.Background()
	inner := newFakeState()
	state := &countingClaimState{fakeState: inner}
	queue := &fakeQueue{}
	eng := New(state, queue)

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "flush-full-batch",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// 600 plain exec intents: more than two full batches of 256, none of them a
	// system task, so nothing inside the loop appends and only the "was the
	// batch full?" half of the exit condition can keep the drain going.
	const total = 600
	for i := range total {
		entry := OutboxEntry{
			ID: "bulk-" + itoa(i),
			Task: Task{
				ExecutionID: id,
				NodeName:    "start",
				Type:        TaskTypeNodeExec,
			},
		}
		inner.mu.Lock()
		if inner.atomicOutbox[id] == nil {
			inner.atomicOutbox[id] = map[string]OutboxEntry{}
		}
		inner.atomicOutbox[id][entry.ID] = entry
		inner.mu.Unlock()
	}

	state.claims = 0
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	if got := len(queue.Drain()); got < total {
		t.Fatalf("delivered %d of %d intents: the drain stopped on a full batch", got, total)
	}
	if state.claims < 3 {
		t.Errorf("outbox claims = %d, want at least 3 for %d entries at 256 per batch",
			state.claims, total)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
