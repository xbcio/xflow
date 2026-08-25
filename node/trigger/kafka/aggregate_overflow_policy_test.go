package kafka

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// unboundedProducerConsumer hands messages over an UNBUFFERED channel and
// counts how many it managed to hand over.
//
// The buffering matters more than anything else in this file. Every other fake
// in this package either pre-fills a large channel (replayableKafkaConsumer) or
// drops on producer pressure (shedTrackingConsumer), and both of those make
// backpressure invisible: the producer never notices that the consumer stopped
// receiving. A real group Reader is not buffered like that — it fetches when the
// caller asks for a message and not otherwise — so an unbuffered send with a
// counter is the closest available model of "did the reader stop fetching".
type unboundedProducerConsumer struct {
	ch     chan Message
	stop   chan struct{}
	closed sync.Once
	sent   atomic.Int64

	mu      sync.Mutex
	commits []Message
}

func newUnboundedProducerConsumer(topic string, partition int) *unboundedProducerConsumer {
	c := &unboundedProducerConsumer{ch: make(chan Message), stop: make(chan struct{})}
	go func() {
		defer close(c.ch)
		for offset := int64(0); ; offset++ {
			msg := Message{Topic: topic, Partition: partition, Offset: offset, Value: []byte("v")}
			select {
			case <-c.stop:
				return
			case c.ch <- msg:
				c.sent.Add(1)
			}
		}
	}()
	return c
}

func (c *unboundedProducerConsumer) Messages() <-chan Message { return c.ch }

func (c *unboundedProducerConsumer) Close() error {
	c.closed.Do(func() { close(c.stop) })
	return nil
}

func (c *unboundedProducerConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

// blockedEmitTrigger builds an activated trigger whose downstream never returns
// until release is closed, so the partition reaches its retained bound and stays
// there. onOverflow selects the policy under test.
//
// The returned close func releases the downstream BEFORE closing the
// subscription, because a caller that does it the other way round is testing the
// shutdown path rather than the policy.
func blockedEmitTrigger(t *testing.T, consumer Consumer, onOverflow string, maxSize int) (*triggertest.FakeRuntime, func()) {
	t.Helper()

	orig := newConsumer
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	release := make(chan struct{})
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
		select {
		case <-release:
			return "exec-1", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(maxSize, 200*time.Millisecond)
	if onOverflow == onOverflowBlock {
		tr = tr.BlockOnOverflow()
	}
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(release)
			_ = sub.Close(context.Background())
		})
	}
	t.Cleanup(stop)
	return rt, stop
}

// TestKafkaAggregateOverflowPolicyIsAChoice runs ONE fixture through both
// policies and asserts they diverge.
//
// The two arms are what make each other meaningful. "block discards nothing" is
// satisfied by any run that never reached the cap, so on its own it would pass
// against a fixture too small to overflow, against a broken producer, and
// against the policy having no effect at all. The discard arm is the positive
// control: same fixture, same downstream, and it must lose records. Only the
// difference between them is evidence.
func TestKafkaAggregateOverflowPolicyIsAChoice(t *testing.T) {
	const (
		maxSize      = 10
		retainedWant = maxSize * (maxPartitionPendingBatches + maxBufferedBatches)
		// Comfortably past the retained bound, so the cap is reached regardless of
		// how the in-flight window happens to be filled when the run starts.
		deliveredCount = retainedWant + 60
	)

	run := func(t *testing.T, policy string) (discards int, blocked []string) {
		t.Helper()
		messages := make([]Message, deliveredCount)
		for i := range messages {
			messages[i] = Message{Topic: "t", Partition: 0, Offset: int64(i), Value: []byte("v")}
		}
		consumer := newReplayableKafkaConsumer(messages, nil)
		observer := installRecordingObserver(t)
		blockedEmitTrigger(t, consumer, policy, maxSize)

		// Wait for the partition to be AT its bound, established differently per
		// policy because the two have different observable evidence — that is the
		// whole point of the change. Waiting on the same signal for both would
		// mean one arm was waiting for something it can never produce.
		switch policy {
		case onOverflowDiscard:
			observer.waitFor(3*time.Second, func(d, _ []string) bool { return len(d) >= 1 })
		case onOverflowBlock:
			observer.waitForBlocked(3*time.Second, func(b []string) bool { return len(b) >= 1 })
		}
		// Then let it sit, so a policy that merely DELAYS the loss is not mistaken
		// for one that prevents it.
		time.Sleep(300 * time.Millisecond)

		d, _ := observer.snapshot()
		return len(d), observer.blockedSnapshot()
	}

	var discardArm, blockArm int
	var discardBlocked, blockBlocked []string
	t.Run("discard", func(t *testing.T) { discardArm, discardBlocked = run(t, onOverflowDiscard) })
	t.Run("block", func(t *testing.T) { blockArm, blockBlocked = run(t, onOverflowBlock) })

	if discardArm == 0 {
		t.Fatalf("the discard arm lost nothing, so this fixture never reached the "+
			"retained bound of %d records and the block arm's zero proves nothing",
			retainedWant)
	}
	if len(discardBlocked) != 0 {
		t.Errorf("the discard arm reported %v; a partition that keeps draining must "+
			"never report itself blocked", discardBlocked)
	}
	if blockArm != 0 {
		t.Errorf("the block arm discarded %d records. The policy exists for exactly "+
			"one reason and it is that this number is zero.", blockArm)
	}
	if len(blockBlocked) == 0 {
		t.Fatal("the block arm discarded nothing but never reported OnConsumptionBlocked. " +
			"That is the wrong kind of green: a partition that quietly stops consuming " +
			"with no signal reads as a healthy one, which is the failure the whole " +
			"observability half of this change exists to prevent.")
	}
	if got := blockBlocked[0]; got != "t/0/blocked" {
		t.Errorf("first transition = %q, want t/0/blocked", got)
	}
	t.Logf("same %d-record fixture: discard lost %d, block lost %d and reported %v",
		deliveredCount, discardArm, blockArm, blockBlocked)
}

// TestKafkaAggregateBlockStopsConsuming pins the COST, not the benefit.
//
// It exists because a test suite that only proves "block loses nothing" would
// describe the policy as strictly better than the default, which it is not.
// Blocking works by ceasing to receive, and one goroutine reads every
// partition's messages, so a partition that stops consuming stops the reader
// that the whole assignment shares. That is the thing an operator is choosing,
// and it should be as pinned as the benefit.
//
// The measurement is a direction, not a rate: sample the producer's handover
// count, wait, sample again. Under block it must not move at all. The discard
// arm over the same interval is the control that proves the producer had more to
// give — without it, "the count did not move" is equally consistent with a
// finished fixture or a deadlocked harness.
func TestKafkaAggregateBlockStopsConsuming(t *testing.T) {
	const maxSize = 10

	run := func(t *testing.T, policy string) (before, after int64) {
		t.Helper()
		consumer := newUnboundedProducerConsumer("t", 0)
		observer := installRecordingObserver(t)
		blockedEmitTrigger(t, consumer, policy, maxSize)

		switch policy {
		case onOverflowDiscard:
			observer.waitFor(3*time.Second, func(d, _ []string) bool { return len(d) >= 1 })
		case onOverflowBlock:
			observer.waitForBlocked(3*time.Second, func(b []string) bool { return len(b) >= 1 })
			// Settle before sampling. The first blocked transition fires when the
			// RETAINED count hits its bound, and at that instant the aggregator's
			// own channel and the handover in flight are still filling — about ten
			// more messages, which arrive over the next microseconds and have
			// nothing to do with the policy leaking. Sampling `before` in the
			// middle of that made this test read one message of settling as
			// ongoing consumption, and it failed under -race, where the window is
			// wider. Waiting for a quiet interval is what makes the later
			// comparison a statement about the steady state.
			settle(t, &consumer.sent)
		}
		before = consumer.sent.Load()
		time.Sleep(300 * time.Millisecond)
		return before, consumer.sent.Load()
	}

	var dBefore, dAfter, bBefore, bAfter int64
	t.Run("discard", func(t *testing.T) { dBefore, dAfter = run(t, onOverflowDiscard) })
	t.Run("block", func(t *testing.T) { bBefore, bAfter = run(t, onOverflowBlock) })

	if dAfter <= dBefore {
		t.Fatalf("the discard arm consumed nothing further (%d -> %d), so this harness "+
			"cannot tell a stalled consumer from an exhausted producer and the block "+
			"arm's flat line means nothing", dBefore, dAfter)
	}
	if bAfter != bBefore {
		t.Errorf("the block arm consumed %d more messages while at its cap (%d -> %d); "+
			"it is supposed to have stopped fetching entirely", bAfter-bBefore, bBefore, bAfter)
	}
	// The absolute figure, not just the flat line: a build that stopped fetching
	// after ten thousand messages would also be flat over one interval. The
	// steady state is derivable — maxRetained records held by the coordinator,
	// maxSize sitting in its channel, and one in the handover — so pin it, with
	// room for the settling above to land differently.
	const ceiling = int64(maxSize*(maxPartitionPendingBatches+maxBufferedBatches)) + maxSize + 4
	if bBefore > ceiling {
		t.Errorf("the block arm consumed %d messages before stopping, over the %d the "+
			"retained bound and channel depth account for; it is bounded by something "+
			"else than the cap", bBefore, ceiling)
	}
	t.Logf("over the same 300ms at cap: discard consumed %d more, block consumed %d more",
		dAfter-dBefore, bAfter-bBefore)
}

// TestKafkaAggregateBlockedPartitionStillCloses guards a defect that the two
// tests above cannot see, because both of them release the downstream before
// closing.
//
// A blocked coordinator has removed the input-channel receive from its select.
// That receive is how every other shutdown path learns the channel closed, so
// without a separate signal the coordinator never returns and the goroutine
// survives for the life of the process — one leak per partition per activation.
// Deleting the `case <-stopC` arm, or the `close(agg.stop)` that feeds it, must
// make this test red.
//
// It asserts the goroutine is GONE, not that Close returned. Close returning is
// not evidence of anything here: rt.close waits on a timeout and then returns
// regardless, so the leaking build and the fixed build both come back in about
// the drain interval. An earlier version of this test asserted the return and
// passed against the leak it was written to catch.
func TestKafkaAggregateBlockedPartitionStillCloses(t *testing.T) {
	const maxSize = 10

	orig := newConsumer
	consumer := newUnboundedProducerConsumer("t", 0)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	// Never released. This is the wedged downstream that produced the
	// backpressure in the first place, and closing must not depend on it
	// recovering.
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	observer := installRecordingObserver(t)

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(maxSize, 200*time.Millisecond).
		BlockOnOverflow()
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !observer.waitForBlocked(5*time.Second, func(b []string) bool { return len(b) >= 1 }) {
		t.Fatal("the partition never reached its cap, so this run never entered the " +
			"blocked state whose shutdown it is meant to test")
	}
	if coordinatorGoroutines() == 0 {
		t.Fatal("no coordinator goroutine was running before Close, so this test cannot " +
			"observe whether one survives it — check countGoroutines' frame name")
	}

	// Close on its own goroutine: a hang here would otherwise be reported as a
	// whole-package timeout rather than as this test failing.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.Close(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return within 10s while a partition was blocked")
	}

	// Poll: the coordinator returns on its own goroutine, and Close is bounded by
	// a timeout rather than by that return, so the two are not ordered.
	deadline := time.Now().Add(5 * time.Second)
	for coordinatorGoroutines() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := coordinatorGoroutines(); n > 0 {
		t.Fatalf("%d partition coordinator goroutine(s) still running 5s after Close, "+
			"with the downstream still wedged. The blocked coordinator has no shutdown "+
			"signal that survives backpressure, so it leaks — one per partition, per "+
			"activation, for the life of the process.", n)
	}
}

// settle waits until a counter stops moving, so a later measurement describes a
// steady state rather than the tail of one being reached. It gives up quietly:
// the assertions that follow are what report a counter that never settled, and
// they say more about why than a timeout here could.
func settle(t *testing.T, counter *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	last := int64(-1)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		if now := counter.Load(); now == last {
			return
		} else {
			last = now
		}
	}
}

// coordinatorGoroutines counts live partitionAggregator.run goroutines.
//
// The frame name is the load-bearing part: if run is ever renamed or inlined
// into something else, this returns 0 and the leak test above goes quietly
// green. The pre-Close check in that test is what makes that failure loud.
func coordinatorGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "kafka.(*partitionAggregator).run(")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// TestKafkaAggregateOnOverflowRoundTripsThroughParams is the wiring test, and
// nothing above can stand in for it.
//
// AggregateConfig round-trips through RawParams, which rebuilds the aggregate
// map one key at a time. A key that is not listed there does not survive the
// trip: the setter appears to work, the config normalizes cleanly, and the
// runtime on the other side parses OnOverflow as "" and defaults it back to
// discard. The result is a deployment that asked not to lose records, was told
// nothing, and loses them.
//
// That is not hypothetical for this file — the same map dropped a field this way
// before. So this asserts the value AFTER a full serialize-and-reparse, not on
// the builder's struct.
func TestKafkaAggregateOnOverflowRoundTripsThroughParams(t *testing.T) {
	base := func() *Node {
		return New().Brokers("b").Topic("t").Group("g").
			AggregateByPartition(10, 100*time.Millisecond)
	}

	t.Run("block survives", func(t *testing.T) {
		params := base().BlockOnOverflow().RawParams().(map[string]any)
		cfg, err := aggregateConfigFromParamForMode(params["aggregate"], false)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OnOverflow != onOverflowBlock {
			t.Fatalf("OnOverflow = %q after a round trip through RawParams, want %q. "+
				"BlockOnOverflow set it and the serializer dropped it, so the runtime "+
				"is discarding records for a deployment that opted out of that.",
				cfg.OnOverflow, onOverflowBlock)
		}
	})

	t.Run("default is discard and is not serialized", func(t *testing.T) {
		params := base().RawParams().(map[string]any)
		agg := params["aggregate"].(map[string]any)
		// Absent rather than "discard", matching value_json and tuning: normalize
		// fills this in for everyone, so writing it unconditionally would move the
		// stored definition hash of every workflow that already aggregates.
		if _, present := agg["on_overflow"]; present {
			t.Errorf("on_overflow is serialized for a node that never set it, which "+
				"moves the definition hash of every existing aggregating workflow: %v", agg)
		}
		cfg, err := aggregateConfigFromParamForMode(agg, false)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OnOverflow != onOverflowDiscard {
			t.Fatalf("OnOverflow = %q for an unset node, want %q — upgrading must not "+
				"move anyone onto the other trade", cfg.OnOverflow, onOverflowDiscard)
		}
	})
}

// TestKafkaAggregateOnOverflowRejectsUnknown: a typo must not be read as the
// lossy default. This mirrors on_invalid, and for the same reason — an operator
// who typed the field at all had a reason, and silently giving them the
// behaviour they were trying to leave is the one outcome they cannot have
// wanted.
func TestKafkaAggregateOnOverflowRejectsUnknown(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    string
		wantErr bool
	}{
		{name: "absent", value: nil, want: onOverflowDiscard},
		{name: "explicit discard", value: "discard", want: onOverflowDiscard},
		{name: "explicit block", value: "block", want: onOverflowBlock},
		{name: "case and space tolerated", value: "  BLOCK ", want: onOverflowBlock},
		{name: "typo", value: "blcok", wantErr: true},
		// Rejected rather than treated as absent: "" cannot be typed into YAML by
		// accident in a way that means anything else, but a key present with an
		// empty value is a half-finished edit, not a choice.
		{name: "misspelled neighbour", value: "drop", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := map[string]any{
				"enabled":  true,
				"by":       "partition",
				"max_size": 10,
			}
			if tc.value != nil {
				raw["on_overflow"] = tc.value
			}
			cfg, err := aggregateConfigFromParamForMode(raw, false)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("on_overflow=%v was accepted as %q; a typo must not silently "+
						"select the lossy default", tc.value, cfg.OnOverflow)
				}
				if !strings.Contains(err.Error(), "on_overflow") {
					t.Errorf("error %q does not name the field the operator has to fix", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.OnOverflow != tc.want {
				t.Fatalf("OnOverflow = %q, want %q", cfg.OnOverflow, tc.want)
			}
		})
	}
}
