package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// shedTrackingConsumer records every offset it handed over and every offset
// that was committed, so a test can ask the one question that matters about a
// shed message: was it skipped?
type shedTrackingConsumer struct {
	ch     chan Message
	stop   chan struct{}
	closed sync.Once

	mu        sync.Mutex
	delivered map[int64]struct{}
	commits   []Message
}

func newShedTrackingConsumer(topic string, capacity int, interval time.Duration) *shedTrackingConsumer {
	c := &shedTrackingConsumer{
		ch:        make(chan Message, capacity),
		stop:      make(chan struct{}),
		delivered: make(map[int64]struct{}),
	}
	go func() {
		defer close(c.ch)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for offset := int64(0); ; offset++ {
			select {
			case <-c.stop:
				return
			case <-tick.C:
			}
			msg := Message{Topic: topic, Partition: 0, Offset: offset, Value: []byte("v")}
			// CommitMessages uses this same lock, so a commit snapshot cannot
			// observe a sent offset before the harness records it as delivered.
			c.mu.Lock()
			stopped := false
			select {
			case <-c.stop:
				stopped = true
			case c.ch <- msg:
				c.delivered[offset] = struct{}{}
			default:
				// Producer-side channel pressure is not aggregator shedding.
			}
			c.mu.Unlock()
			if stopped {
				return
			}
		}
	}()
	return c
}

func (c *shedTrackingConsumer) Messages() <-chan Message { return c.ch }

func (c *shedTrackingConsumer) Close() error {
	c.closed.Do(func() { close(c.stop) })
	return nil
}

func (c *shedTrackingConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

// commitSnapshot reports whether an offset that really entered the consumer
// channel now sits below Kafka's effective committed position without ever
// having been explicitly committed by the aggregator.
func (c *shedTrackingConsumer) commitSnapshot() (highest int64, delivered, committed, sweptPast int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	highest = -1
	committedOffsets := make(map[int64]struct{}, len(c.commits))
	for _, msg := range c.commits {
		committedOffsets[msg.Offset] = struct{}{}
		if msg.Offset > highest {
			highest = msg.Offset
		}
	}
	for offset := range c.delivered {
		if _, explicitlyCommitted := committedOffsets[offset]; !explicitlyCommitted && offset < highest {
			sweptPast++
		}
	}
	return highest, len(c.delivered), len(committedOffsets), sweptPast
}

// TestKafkaAggregateShedMessagesAreSilentlySkipped measures what happens to a
// message the aggregator sheds when its buffer is at cap.
//
// The code shedding those messages described them as safe: "its offset is
// uncommitted, so Kafka redelivers it once the retained head commits". Both
// halves of that are wrong, and the second is backwards.
//
//  1. Within a live session nothing is redelivered at all. kafka-go advances
//     r.offset to msg.Offset+1 the moment a message is handed to the caller
//     (reader.go:846) and CommitMessages never writes r.offset back; the only
//     rewind is subscribe()->start() on a NEW generation. So a shed message
//     returns after a rebalance or a restart, not "once the head commits".
//
//  2. Far from being redelivered by a later commit, a shed message is ERASED by
//     one. Kafka tracks a single offset per partition, so committing offset N
//     commits everything below N (kafka-go reader.go CommitMessages doc,
//     verbatim: "the highest message offset passed to CommitMessages will cause
//     all previous messages to be committed"). The aggregator sheds the
//     ARRIVING message — the highest offset it holds — and keeps the lower ones.
//     Every message that arrives afterwards has a higher offset still, so the
//     next successful flush commits straight past the shed offset and the
//     group's position moves beyond it forever.
//
// This test does not assert that shedding is wrong: at cap, something must give.
// It asserts that the loss is REAL, so the code cannot go on describing it as a
// deferred redelivery.
//
// What it pins is now the DEFAULT policy rather than the only one. A deployment
// that cannot afford this sets on_overflow: block, which halts consumption
// instead of dropping — see TestKafkaAggregateOverflowPolicyIsAChoice, which
// runs one fixture through both and is the reason this file's numbers can be
// read as a choice rather than as a limitation. The default is unchanged, so
// everything below still describes what an unconfigured topic does.
func TestKafkaAggregateShedMessagesAreSilentlySkipped(t *testing.T) {
	const (
		maxSize    = 4
		flushEvery = 50 * time.Millisecond
		// The downstream fails, which is what puts the aggregator into the
		// retry state where shedding happens.
		produceGap = 1 * time.Millisecond
	)

	orig := newConsumer
	consumer := newShedTrackingConsumer("t", maxSize, produceGap)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	// The downstream fails for a while, so the buffer fills to its cap and the
	// aggregator starts shedding, then recovers so a later flush commits and
	// moves the group position past the shed offsets.
	var failing atomic.Bool
	failing.Store(true)
	var emits atomic.Int32
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		emits.Add(1)
		if failing.Load() {
			return "", errShedProbeDownstream
		}
		return "", nil
	})

	observer := installRecordingObserver(t)

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(maxSize, flushEvery)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	// Recover only once shedding has been OBSERVED. The previous version of this
	// probe recovered as soon as `delivered` exceeded the aggregator's retained
	// capacity, on the theory that the excess must have overflowed. It had not:
	// the consumer channel and the aggregator's input channel hold messages that
	// maxRetainedUpperBound does not count, so the condition went true about 48ms
	// in, before the first batch had even finished failing. Every run then
	// reported swept_past=0 and the test failed on its own guard clause -- which
	// is the guard working, but it had stopped measuring shedding at all.
	//
	// buffer_overflow on the observer is the event itself, so there is nothing
	// left to infer.
	if !observer.waitFor(5*time.Second, func(discarded, _ []string) bool {
		for _, d := range discarded {
			if d == "t/buffer_overflow" {
				return true
			}
		}
		return false
	}) {
		discarded, _ := observer.snapshot()
		_, delivered, committed, sweptPast := consumer.commitSnapshot()
		t.Fatalf("no buffer_overflow was reported in 5s (delivered=%d "+
			"committed=%d swept_past=%d discards=%v); the buffer never reached "+
			"its cap, so this run cannot say anything about what happens to a "+
			"shed message", delivered, committed, sweptPast, discarded)
	}

	// Let the downstream recover so commits resume and sweep past the shed
	// offsets. Poll for the sweep itself rather than for the first commit of any
	// kind: the head batch commits offsets 0..3, which are below every shed
	// offset, so a run that stopped there would report swept_past=0 while the
	// effect was still seconds away.
	failing.Store(false)
	var highest int64
	var delivered, committed, skipped int
	sweepDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(sweepDeadline) {
		highest, delivered, committed, skipped = consumer.commitSnapshot()
		if skipped > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if highest < 0 {
		t.Fatal("nothing was ever committed, so the sweep-past effect this test " +
			"measures never had a chance to happen — the downstream never recovered")
	}

	t.Logf("delivered=%d highest_committed=%d explicitly_committed=%d swept_past=%d",
		delivered, highest, committed, skipped)

	if skipped == 0 {
		t.Fatal("buffer_overflow was reported, so messages WERE shed, but no delivered " +
			"offset ended up below the committed position. Either the commit never " +
			"advanced past the shed region within the window, or shed offsets are no " +
			"longer swept past — the second would be a real behaviour change and this " +
			"test should be rewritten to pin it.")
	}

	// The finding, pinned: those messages are gone. Not deferred, not
	// redelivered on the next commit — gone until someone replays the topic.
	t.Logf("CONFIRMED: %d delivered messages sit below the committed position "+
		"without ever having been emitted. They are lost for this consumer group, "+
		"not queued for redelivery.", skipped)
}

// errShedProbeDownstream is the failure the probe's downstream returns while it
// is meant to be failing.
var errShedProbeDownstream = errors.New("downstream unavailable (shed probe)")
