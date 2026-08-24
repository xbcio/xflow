package kafka

// TestKafkaAggregateHeadOfLine is a regression test for cross-partition
// head-of-line blocking in the shared consumer read loop.
//
// The reader still submits every partition through one goroutine. The partition
// coordinator must therefore keep draining its input while downstream work runs;
// once its bounded retention window fills, it explicitly sheds new arrivals
// rather than blocking the shared reader. That loss is separately pinned by
// TestKafkaAggregateShedMessagesAreSilentlySkipped.
//
// This test blocks partition 0 downstream for the whole observation window and
// verifies that partition 1 continues to flush. Before downstream execution was
// handed off from the coordinator, partition 0 stopped draining its channel and
// submit blocked the shared reader, starving partition 1 as collateral damage.
import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// twoPartitionConsumer delivers messages interleaved between partition 0 and
// partition 1 at a fixed pace. Both partitions share a single Messages() channel,
// exactly as a real kafka-go consumer presents a multi-partition assignment.
type twoPartitionConsumer struct {
	ch     chan Message
	stop   chan struct{}
	closed sync.Once

	mu      sync.Mutex
	commits []Message
}

func newTwoPartitionConsumer(topic string, capacity int, interval time.Duration) *twoPartitionConsumer {
	c := &twoPartitionConsumer{
		ch:   make(chan Message, capacity*2),
		stop: make(chan struct{}),
	}
	go func() {
		defer close(c.ch)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		offsets := [2]int64{}
		part := 0
		for {
			select {
			case <-c.stop:
				return
			case <-tick.C:
			}
			msg := Message{
				Topic:     topic,
				Partition: part,
				Offset:    offsets[part],
				Value:     []byte("v"),
			}
			offsets[part]++
			select {
			case <-c.stop:
				return
			case c.ch <- msg:
			default:
				// Channel full — drop, same as real Kafka consumer back-pressure
				// not blocking the producer clock.
			}
			part = 1 - part // alternate
		}
	}()
	return c
}

func (c *twoPartitionConsumer) Messages() <-chan Message { return c.ch }

func (c *twoPartitionConsumer) Close() error {
	c.closed.Do(func() { close(c.stop) })
	return nil
}

func (c *twoPartitionConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

func (c *twoPartitionConsumer) commitsByPartition() map[int]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := map[int]int{}
	for _, m := range c.commits {
		counts[m.Partition]++
	}
	return counts
}

// TestKafkaAggregateHeadOfLine verifies that one blocked partition does not
// starve another partition served by the same consumer read loop.
func TestKafkaAggregateHeadOfLine(t *testing.T) {
	const (
		maxSize    = 10
		flushEvery = 100 * time.Millisecond
		// Partition 0's emit blocks for the entire window — it never returns.
		part0EmitBlocks = 3 * time.Second
		produceInterval = 2 * time.Millisecond // 1 message every 2ms, alternating
		window          = 3 * time.Second

		// If NOT blocked: partition 1 receives a message every ~4ms (alternating),
		// fills a batch of 10 in ~40ms, emits immediately, and repeats.
		// Over 3s: ~3000/40 = 75 flushes. We use a conservative floor of 10.
		minPart1EmitsExpected = 10
	)

	orig := newConsumer
	consumer := newTwoPartitionConsumer("t", maxSize*2, produceInterval)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var part0Emits, part1Emits atomic.Int32

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		partition, _ := ev.Data["partition"].(int)
		if partition == 0 {
			part0Emits.Add(1)
			// Block until the context is cancelled or the window expires.
			// This simulates a very slow downstream for partition 0's batches.
			select {
			case <-ctx.Done():
			case <-time.After(part0EmitBlocks):
			}
			return "", nil
		}
		// Partition 1: fast path, returns immediately.
		part1Emits.Add(1)
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

	p0 := part0Emits.Load()
	p1 := part1Emits.Load()
	commits := consumer.commitsByPartition()

	t.Logf("partition 0: emits=%d commits=%d", p0, commits[0])
	t.Logf("partition 1: emits=%d commits=%d", p1, commits[1])
	t.Logf("H1 (HOL blocking): partition 1 emits want>=%d, got=%d",
		minPart1EmitsExpected, p1)

	// Harness guard: partition 0 must have triggered at least one emit,
	// proving the HOL condition was set up.
	if p0 == 0 {
		t.Fatal("partition 0 never emitted — harness did not set up the blocking " +
			"condition, so the result below proves nothing")
	}

	// Regression assertion: partition 1 must keep making progress while
	// partition 0 is blocked.
	if int(p1) < minPart1EmitsExpected {
		t.Errorf("partition 1 emitted %d times (want >= %d) while "+
			"partition 0's downstream was blocked. The single reader in "+
			"activateAggregate stopped on submit() after partition 0's "+
			"aggregator channel (cap=%d) filled, starving other partitions "+
			"for the remaining ~%.1fs of the %s window.",
			p1, minPart1EmitsExpected, maxSize,
			float64(window-80*time.Millisecond)/float64(time.Second),
			window)
	}
}

// TestKafkaAggregateHeadOfLineInverse is the discriminator / reverse check.
//
// Same setup as TestKafkaAggregateHeadOfLine but with partition 0's emit also
// returning immediately. Without the blocking condition, partition 1 MUST reach
// minPart1EmitsExpected — otherwise the harness itself is broken and the HOL
// probe above is meaningless.
func TestKafkaAggregateHeadOfLineInverse(t *testing.T) {
	const (
		maxSize               = 10
		flushEvery            = 100 * time.Millisecond
		produceInterval       = 2 * time.Millisecond
		window                = 3 * time.Second
		minPart1EmitsExpected = 10
	)

	orig := newConsumer
	consumer := newTwoPartitionConsumer("t", maxSize*2, produceInterval)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var part0Emits, part1Emits atomic.Int32

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		partition, _ := ev.Data["partition"].(int)
		if partition == 0 {
			part0Emits.Add(1)
		} else {
			part1Emits.Add(1)
		}
		// Both partitions return immediately.
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

	p0 := part0Emits.Load()
	p1 := part1Emits.Load()
	commits := consumer.commitsByPartition()

	t.Logf("partition 0: emits=%d commits=%d", p0, commits[0])
	t.Logf("partition 1: emits=%d commits=%d", p1, commits[1])
	t.Logf("Inverse (no blocking): partition 1 emits want>=%d, got=%d",
		minPart1EmitsExpected, p1)

	// Without blocking, partition 1 MUST reach the threshold. If this test is
	// RED, the harness is broken and the HOL test's result cannot be trusted.
	if int(p1) < minPart1EmitsExpected {
		t.Errorf("inverse harness broken: partition 1 emitted only %d times "+
			"(want >= %d) even with no blocking condition. The HOL test result "+
			"cannot be trusted — fix the harness.", p1, minPart1EmitsExpected)
	}
}
