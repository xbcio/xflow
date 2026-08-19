package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// silentConsumer holds its message channel open and never delivers anything. It
// exists so a hand-built aggregateRuntime has a usable Consumer on the commit
// path; the messages under test are injected directly.
type silentConsumer struct {
	ch     chan Message
	closed sync.Once

	mu      sync.Mutex
	commits []Message
}

func newSilentConsumer() *silentConsumer {
	return &silentConsumer{ch: make(chan Message)}
}

func (c *silentConsumer) Messages() <-chan Message { return c.ch }

func (c *silentConsumer) Close() error {
	c.closed.Do(func() { close(c.ch) })
	return nil
}

func (c *silentConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

// TestKafkaAggregateSubmitTakesReplacementAfterReap is the regression test for
// the freeze that stopped scenario A's pipeline at t+50s with no recovery.
//
// Mechanism: the read loop is a SINGLE goroutine serving every partition
// (activateAggregate). Its submit blocks whenever the target aggregator is
// inside a flush, because flush runs on the aggregator's own goroutine and does
// not drain agg.ch meanwhile — at the live topic's measured 329 msg/s per
// partition the 100-slot channel fills in 0.3s, while one flush can take the
// group execution deadline plus the admission timeout. If that aggregator then
// reaps itself on its idle timer, closing done and removing itself from the map,
// the blocked send is parked on a channel with no reader for the process
// lifetime and never returns to aggregator() to obtain the replacement.
// Consumption stops for ALL partitions, not just the one whose aggregator died.
//
// Live signature: consumed stuck at 0.3% of produced for 4m10s with lag rising
// linearly, and with goroutines (539→149) and heap (553→348MiB) both SETTLING
// rather than growing — work having stopped, not resources being exhausted.
//
// The reap is driven directly rather than waited for on the idle timer. An
// earlier version of this test drove a real burst through a real activation and
// hoped the collision would land: it passed against the unfixed code, because
// the idle window's 5s floor (aggregatorIdleTimeout) outlasts any burst the test
// can afford to queue. A probe that cannot fail is worse than no probe, since it
// reads as coverage.
func TestKafkaAggregateSubmitTakesReplacementAfterReap(t *testing.T) {
	// The replacement aggregator's run() will really flush, so the runtime needs
	// a usable Runtime and consumer or it panics on the emit path.
	consumer := newSilentConsumer()
	t.Cleanup(func() { _ = consumer.Close() })
	rt := &aggregateRuntime{
		in: &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "kafka",
			Runtime:    triggertest.NewFakeRuntime(),
		},
		cfg:         AggregateConfig{MaxSize: 1, FlushInterval: time.Second},
		consumer:    consumer,
		emitSem:     make(chan struct{}, 1),
		aggregators: make(map[partitionKey]*partitionAggregator),
	}
	key := partitionKey{topic: "t", partition: 0}

	// Build the aggregator by hand, WITHOUT starting run(): nothing drains
	// agg.ch, which is what makes the send block.
	victim := &partitionAggregator{
		key:         key,
		rt:          rt,
		ch:          make(chan Message, 1),
		done:        make(chan struct{}),
		idleTimeout: time.Hour,
	}
	rt.aggregators[key] = victim
	victim.ch <- Message{Topic: "t", Partition: 0, Offset: 0} // fill it

	submitted := make(chan bool, 1)
	go func() {
		submitted <- rt.submit(context.Background(), Message{Topic: "t", Partition: 0, Offset: 1})
	}()

	// Let the send block on the full channel before reaping.
	select {
	case got := <-submitted:
		t.Fatalf("submit returned %v immediately; the channel was full, so it should "+
			"have blocked and this test would not be exercising the reap path", got)
	case <-time.After(50 * time.Millisecond):
	}

	// Reap in run()'s order: evict from the map, then close done (run's defers
	// are LIFO). A submit that wakes on done and re-reads the map must therefore
	// find the entry gone and spawn a replacement, never the corpse.
	rt.evictAggregator(key, victim)
	close(victim.done)

	select {
	case ok := <-submitted:
		if !ok {
			t.Fatal("submit reported failure after the aggregator was reaped; a reap is " +
				"not a shutdown, and returning false here stops the read loop for every " +
				"partition")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit never returned after the aggregator was reaped: it is parked on " +
			"a channel with no reader. Because the read loop is a single goroutine " +
			"serving all partitions, this stops consumption process-wide.")
	}

	// The replacement must be a DIFFERENT aggregator. One that somehow reused the
	// corpse would satisfy the return value above while leaving the message
	// unreadable, since the corpse's run() has already exited.
	rt.mu.Lock()
	replacement, ok := rt.aggregators[key]
	rt.mu.Unlock()
	if !ok {
		t.Fatal("no aggregator registered for the partition after submit returned")
	}
	if replacement == victim {
		t.Fatal("submit reused the reaped aggregator; its run() has exited, so the " +
			"message will never be flushed")
	}
	t.Cleanup(func() {
		close(replacement.ch)
		<-replacement.done
	})
}

// TestKafkaAggregateSubmitFailsClosedDuringShutdown guards the fix's own hazard.
//
// submit now loops, so a woken done case re-enters aggregator() — which SPAWNS
// an aggregator when the map has no entry. rt.close takes a snapshot of the set
// and closes those channels; an aggregator spawned after that snapshot is one
// nobody closes and nobody waits for. The ctx check at the loop head is what
// prevents it, and it cannot live in the select instead: with both done and
// ctx.Done() ready, select picks either.
//
// The test enters the loop head in the state a reap-during-shutdown produces —
// cancelled ctx, no map entry — rather than trying to race a real reap against a
// real cancel. That race cannot be driven deterministically from a test (whichever
// of the two lands first decides which branch submit takes, and the loser is never
// observed), and an attempt at it passed against the unguarded code every time.
func TestKafkaAggregateSubmitFailsClosedDuringShutdown(t *testing.T) {
	consumer := newSilentConsumer()
	t.Cleanup(func() { _ = consumer.Close() })
	rt := &aggregateRuntime{
		in: &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "kafka",
			Runtime:    triggertest.NewFakeRuntime(),
		},
		cfg:         AggregateConfig{MaxSize: 1, FlushInterval: time.Second},
		consumer:    consumer,
		emitSem:     make(chan struct{}, 1),
		aggregators: make(map[partitionKey]*partitionAggregator),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if rt.submit(ctx, Message{Topic: "t", Partition: 0, Offset: 1}) {
		t.Error("submit reported success with a cancelled context; the message was " +
			"handed to an aggregator spawned after rt.close took its snapshot, so " +
			"nobody closes its channel and nobody waits for its run()")
	}
	rt.mu.Lock()
	spawned := len(rt.aggregators)
	for _, agg := range rt.aggregators {
		close(agg.ch)
	}
	rt.mu.Unlock()
	if spawned != 0 {
		t.Errorf("submit spawned %d aggregator(s) with a cancelled context; these "+
			"outlive shutdown", spawned)
	}
}
