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
	produced atomic.Int64 // messages the harness tried to hand over
	dropped  atomic.Int64 // messages the harness could not hand over: channel full

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
				c.dropped.Add(1)
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
// Mechanism under test: partitionAggregator.run hands each batch to a flusher
// goroutine and stays at its select, so the partition channel keeps draining
// while the downstream works. Before that split, flush ran inline from the
// select loop: for the whole duration of flush — emitSem acquisition, the
// downstream emit, and the offset commit — run() was not at its select and read
// nothing. Both call sites pass context.Background(), so the only bound on that
// blockage was whatever the downstream imposed on itself; in the trigger-group
// path that is groupExecBatchDeadline, 30 seconds. emitSem is acquired BEFORE
// that deadline starts (flush versus
// service/runner/group_exec_trigger_runtime.go:60), so semaphore waiting added
// to the stall on top of the 30s rather than being bounded by it.
//
// WHAT THIS ASSERTS, AND WHAT IT DELIBERATELY DOES NOT.
//
// It asserts CONSUMPTION, not throughput. A downstream taking 600ms per batch of
// 10 cannot match 500 msg/s offered even with the bounded four-batch reorder
// window. When downstream cannot keep up, lag (and, in the current bounded-loss
// policy, buffer_overflow) is the honest signal. An earlier revision demanded a
// 50% committed/offered ratio, which would require far more per-partition
// concurrency than the configured reorder window. That measured downstream
// capacity rather than whether the coordinator itself blocked consumption.
//
// The defect worth pinning is that a stalled flush stopped the SINGLE goroutine
// reading consumer.Messages() for every partition, taking the whole assignment
// down with it (see TestKafkaAggregateHeadOfLine, which measures that directly
// across two partitions). Its local signature is this one: the channel stops
// being drained. So the assertion is that the aggregator keeps TAKING messages
// throughout the window at a rate far above what one blocking flush cycle
// admits.
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
	// offered minus dropped-on-full minus still-queued is what it took off the
	// channel. This is CONSUMPTION, which is the property under test — not
	// commits, which additionally require the 600ms downstream to have returned
	// and so measure throughput the aggregator cannot control.
	offered := consumer.produced.Load()
	if offered == 0 {
		t.Fatal("harness offered no messages; the ratio below would prove nothing")
	}
	taken := offered - consumer.dropped.Load() - int64(len(consumer.ch))
	committed := func() int {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return len(consumer.commits)
	}()

	// A blocking flush takes at most one channel-full (maxSize) per flush cycle:
	// over 3s with a 600ms downstream that is 5 cycles x 10 = ~50 of ~1500
	// offered, about 3%. A flush that runs concurrently with consumption keeps
	// draining continuously and takes nearly everything. The 50% bound sits an
	// order of magnitude above the blocking ceiling and well below the
	// concurrent design's yield, so it discriminates the two without depending
	// on absolute timing.
	//
	// Note this is NOT a throughput assertion: committed is logged but not
	// asserted on, because a downstream 30x slower than the offered rate is
	// entitled to fall behind. See the doc comment.
	minRatio := 0.50
	gotRatio := float64(taken) / float64(offered)
	t.Logf("offered=%d taken=%d ratio=%.1f%% committed=%d emits=%d",
		offered, taken, gotRatio*100, committed, emits.Load())
	if gotRatio < minRatio {
		t.Errorf("aggregator took only %d of %d offered messages (%.1f%%) over %s "+
			"with a %s downstream, want >= %.0f%%. That means flush is blocking "+
			"run()'s select loop, so for the flush duration the partition channel "+
			"is not read: a slow downstream stops CONSUMPTION, not just processing. "+
			"Because ONE goroutine reads consumer.Messages() for every partition, "+
			"that back-pressure reaches the shared reader and stalls the whole "+
			"assignment — one slow partition stops all of them.",
			taken, offered, gotRatio*100, window, emitBlocks, minRatio*100)
	}
}
