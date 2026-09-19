package engine

import (
	"context"
	"testing"
	"time"
)

// TestOutboxDispatcherDoesNotIdleAfterAnOverrunningDrain pins the loop's
// no-idle property. Discovery on a large keyspace routinely takes longer than
// the tick, and a loop that blocked on the ticker afterwards would run one
// drain per (drain + interval) instead of one per drain -- halving or worse the
// dispatch ceiling for no reason, because the wait it would be taking has
// already elapsed inside the drain.
func TestOutboxDispatcherDoesNotIdleAfterAnOverrunningDrain(t *testing.T) {
	const interval = 20 * time.Millisecond
	const drainCost = 60 * time.Millisecond
	state := newPageRecordingState()
	state.delay = drainCost
	eng := New(state, &toggleOutboxQueue{})
	dispatcher := NewOutboxDispatcher(eng, interval)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatcher.Run(ctx)
	}()

	const window = 300 * time.Millisecond
	time.Sleep(window)
	cancel()
	<-done

	// Every drain costs drainCost on its own, so a loop that idled for another
	// interval per iteration could fit at most window/(drainCost+interval) = 3
	// drains in this window. A loop that does not idle fits at least 4.
	calls := state.calls()
	if calls < 4 {
		t.Fatalf("drains in %v = %d, want at least 4 -- with a %v drain and a %v tick, "+
			"idling on the ticker after an overrun caps the loop at %d drains, so the "+
			"wait the drain already paid for is being charged twice",
			window, calls, drainCost, interval, window/(drainCost+interval))
	}
}
