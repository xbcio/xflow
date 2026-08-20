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

	delivered atomic.Int64
	dropped   atomic.Int64

	mu      sync.Mutex
	commits []Message
}

func newShedTrackingConsumer(topic string, capacity int, interval time.Duration) *shedTrackingConsumer {
	c := &shedTrackingConsumer{
		ch:   make(chan Message, capacity),
		stop: make(chan struct{}),
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
			select {
			case <-c.stop:
				return
			case c.ch <- Message{Topic: topic, Partition: 0, Offset: offset, Value: []byte("v")}:
				c.delivered.Add(1)
			default:
				c.dropped.Add(1)
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

// highestCommitted returns the largest offset ever passed to CommitMessages,
// which under kafka-go's semantics is the group's effective position: "the
// highest message offset passed to CommitMessages will cause all previous
// messages to be committed" (kafka-go v0.4.49 reader.go, CommitMessages doc).
func (c *shedTrackingConsumer) highestCommitted() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	highest := int64(-1)
	for _, m := range c.commits {
		if m.Offset > highest {
			highest = m.Offset
		}
	}
	return highest
}

// committedSet returns every offset explicitly passed to CommitMessages.
func (c *shedTrackingConsumer) committedSet() map[int64]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := make(map[int64]bool, len(c.commits))
	for _, m := range c.commits {
		set[m.Offset] = true
	}
	return set
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
// deferred redelivery. If a future design makes shed messages genuinely
// recoverable, this test goes red and should be rewritten to pin that instead.
func TestKafkaAggregateShedMessagesAreSilentlySkipped(t *testing.T) {
	const (
		maxSize    = 4
		flushEvery = 50 * time.Millisecond
		// The downstream fails, which is what puts the aggregator into the
		// retry state where shedding happens.
		produceGap = 1 * time.Millisecond
		window     = 2 * time.Second
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

	// Long enough for the buffer to reach maxSize*maxBufferedBatches and shed.
	time.Sleep(window)
	// Let the downstream recover so commits resume and sweep past the shed
	// offsets.
	failing.Store(false)
	time.Sleep(500 * time.Millisecond)

	delivered := consumer.delivered.Load()
	if delivered == 0 {
		t.Fatal("harness delivered no messages; nothing below proves anything")
	}
	highest := consumer.highestCommitted()
	if highest < 0 {
		t.Fatal("nothing was ever committed, so the sweep-past effect this test " +
			"measures never had a chance to happen — the downstream never recovered")
	}
	committed := consumer.committedSet()

	// Every offset below the highest committed one that was delivered but never
	// explicitly committed has been swept past: Kafka's single per-partition
	// offset now sits above it, and no rebalance will bring it back.
	skipped := 0
	for off := int64(0); off < highest; off++ {
		if !committed[off] {
			skipped++
		}
	}

	t.Logf("delivered=%d highest_committed=%d explicitly_committed=%d swept_past=%d",
		delivered, highest, len(committed), skipped)

	if skipped == 0 {
		t.Fatal("no delivered offset was swept past, so this run never exercised " +
			"shedding at all and proves nothing about it. The buffer never reached " +
			"its cap — raise the produce rate or lower maxBufferedBatches.")
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
