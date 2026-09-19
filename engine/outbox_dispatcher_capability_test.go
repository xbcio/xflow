package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestOutboxDispatcherDrainsAStoreThatOffersNoReadinessIndex is the dispatcher
// half of the readiness index's contract: the accelerator lives entirely inside
// the distributed state store, behind AtomicStateStore, and the engine must keep
// working — without discovering less — on a backend that has none.
//
// The store here is the bare fake: it satisfies StateStore and
// AtomicStateStore and nothing else. It is asserted to have none of the optional
// capabilities the drain probes for, which is what makes "no index" concrete
// rather than assumed: there is no interface for the index to be missing from,
// so a backend without one is not a degraded case, it is the contract.
func TestOutboxDispatcherDrainsAStoreThatOffersNoReadinessIndex(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()

	if _, ok := any(state).(OutboxLeaser); ok {
		t.Fatal("the fixture gained an optional capability; this test is supposed to run " +
			"against a store with none, so it is no longer testing the fallback")
	}
	if _, ok := any(state).(OutboxMetricsReader); ok {
		t.Fatal("the fixture gained an optional capability; this test is supposed to run " +
			"against a store with none, so it is no longer testing the fallback")
	}

	ids := []types.ExecutionID{"exec-no-index-0", "exec-no-index-1"}
	state.mu.Lock()
	for _, id := range ids {
		state.putAtomicOutboxLocked(id, string(id)+"/start/0",
			Task{ExecutionID: id, NodeName: "start", Type: TaskTypeNodeExec}, time.Time{})
	}
	state.mu.Unlock()

	queue := &toggleOutboxQueue{}
	eng := New(state, queue)
	NewOutboxDispatcher(eng, time.Hour).drain(ctx)

	delivered := map[types.ExecutionID]int{}
	for _, task := range queue.Drain() {
		delivered[task.ExecutionID]++
	}
	for _, id := range ids {
		if delivered[id] != 1 {
			t.Fatalf("delivered %d tasks for %q, want 1 — discovery must hand the "+
				"dispatcher every execution with ready work whether or not the backend "+
				"keeps a readiness index", delivered[id], id)
		}
	}

	remaining := listAtomicOutbox(t, state, ids[0], time.Now().Add(time.Hour))
	if len(remaining) != 0 {
		t.Fatalf("outbox entries left for %q = %v, want none", ids[0], outboxEntryIDsOf(remaining))
	}
}

func outboxEntryIDsOf(entries []OutboxEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.ID)
	}
	return out
}
