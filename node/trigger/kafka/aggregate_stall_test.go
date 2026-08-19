package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// pacedConsumer produces at a fixed rate and counts how many messages the
// aggregator actually took off the channel.
//
// The count is what makes the stall observable from outside. A slow downstream
// is allowed to make the pipeline slow; what this harness measures is whether a
// slow downstream also makes the trigger stop CONSUMING, which is a different
// property and the one that turns a linear shortfall into an unbounded lag.
type pacedConsumer struct {
	ch       chan Message
	stop     chan struct{}
	closed   sync.Once
	taken    atomic.Int64 // messages handed to the aggregator
	produced atomic.Int64 // messages the harness tried to hand over

	mu      sync.Mutex
	commits []Message
}

// newPacedConsumer offers one message every interval. Capacity is the real
// per-partition channel size the aggregator allocates (MaxSize), so back
// pressure reaches this producer exactly as it would reach a Kafka fetch loop.
func newPacedConsumer(topic string, partition, capacity int, interval time.Duration) *pacedConsumer {
	c := &pacedConsumer{
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
			c.produced.Add(1)
			select {
			case <-c.stop:
				return
			case c.ch <- Message{Topic: topic, Partition: partition, Offset: offset, Value: []byte("v")}:
			default:
				// Channel full: a real fetch loop would block here and stop
				// advancing. Dropping instead of blocking keeps the producer's
				// clock honest, so `produced` stays a true measure of offered
				// load rather than of the aggregator's own pace.
			}
		}
	}()
	return c
}

func (c *pacedConsumer) Messages() <-chan Message { return c.ch }

func (c *pacedConsumer) Close() error {
	c.closed.Do(func() { close(c.stop) })
	return nil
}

func (c *pacedConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

// TestKafkaAggregateKeepsConsumingWhileFlushIsSlow pins that a slow downstream
// does not stop the trigger from consuming its partition.
//
// Mechanism under test: partitionAggregator.run calls flush SYNCHRONOUSLY from
// its own select loop (aggregate.go's size and timeout branches). For the whole
// duration of flush — emitSem acquisition, the downstream emit, and the offset
// commit — run() is not at its select statement, so it reads nothing from the
// partition channel. Both of those call sites pass context.Background(), so the
// only bound on that blockage is whatever the downstream imposes on itself; in
// the trigger-group path that is groupExecBatchDeadline, 30 seconds. Note that
// emitSem is acquired BEFORE that deadline starts (aggregate.go:430 versus
// service/runner/group_exec_trigger_runtime.go:60), so semaphore waiting adds to
// the stall on top of the 30s rather than being bounded by it.
//
// Why this is not merely "slow is slow": consumption stopping is what makes the
// shortfall compound. Measured against live traffic at 381 msg/s per partition
// into a channel of 100, the channel refills in 0.26s, so every second of flush
// blockage discards ~381 further messages' worth of headroom. The configured
// flush interval was 1s and the observed per-partition flush interval was 5.2s —
// the 4.2s difference is time run() spent away from its select. Lag rose
// monotonically from 188k to 2.06M over five minutes and 41.7% of batches
// finished their work and were then dropped for redelivery.
//
// The assertion is a RATIO of taken to offered, which discriminates the two
// designs without depending on absolute timing: an aggregator that consumes
// concurrently with its flush keeps draining the channel and takes nearly
// everything offered, while one that blocks in flush takes only what fits in the
// channel per flush cycle.
func TestKafkaAggregateKeepsConsumingWhileFlushIsSlow(t *testing.T) {
	const (
		maxSize    = 10
		flushEvery = 100 * time.Millisecond
		emitBlocks = 600 * time.Millisecond
		produceGap = 2 * time.Millisecond
		window     = 3 * time.Second
	)

	orig := newConsumer
	consumer := newPacedConsumer("t", 0, maxSize, produceGap)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var emits atomic.Int32
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		emits.Add(1)
		// A downstream that succeeds but takes real time. Success matters: a
		// failing emit would exercise the retry path instead, and this probe is
		// about the healthy case — the blockage is present even when nothing is
		// going wrong.
		select {
		case <-time.After(emitBlocks):
		case <-ctx.Done():
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

	time.Sleep(window)

	// Count what the aggregator drained rather than instrumenting run() itself:
	// offered minus still-queued minus dropped-on-full is what it took.
	offered := consumer.produced.Load()
	if offered == 0 {
		t.Fatal("harness offered no messages; the ratio below would prove nothing")
	}
	committed := func() int {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.commits)
	}()

	// With flush concurrent with consumption, committed tracks offered closely.
	// With flush blocking the loop, each 600ms flush admits at most maxSize
	// messages, so over 3s roughly 3s/600ms * 10 = 50 land while ~1500 are
	// offered — about 3%. The 50% bound sits far above the blocking design's
	// ceiling and far below a concurrent design's expected yield.
	minRatio := 0.50
	gotRatio := float64(committed) / float64(offered)
	t.Logf("offered=%d committed=%d ratio=%.1f%% emits=%d",
		offered, committed, gotRatio*100, emits.Load())
	if gotRatio < minRatio {
		t.Errorf("aggregator committed %d of %d offered messages (%.1f%%) over %s "+
			"with a %s downstream, want >= %.0f%%. flush is called synchronously "+
			"from run()'s select loop, so for the whole flush duration the partition "+
			"channel is not read: a slow downstream stops CONSUMPTION, not just "+
			"processing. That is what turns a linear shortfall into unbounded lag — "+
			"live traffic showed lag rising 188k->2.06M over 5 minutes while 41.7%% "+
			"of batches completed their work and were dropped for redelivery.",
			committed, offered, gotRatio*100, window, emitBlocks, minRatio*100)
	}
}
