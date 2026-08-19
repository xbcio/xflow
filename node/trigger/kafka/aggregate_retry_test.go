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

// steadyConsumer feeds messages continuously so the aggregator keeps receiving
// arrivals while a flush is failing. That is the condition the divergence needed:
// the bug was invisible to a fixed-size scripted batch, because it took a NEW
// message arriving against an already-full buffer to trigger the re-flush.
type steadyConsumer struct {
	ch     chan Message
	stop   chan struct{}
	closed sync.Once

	mu      sync.Mutex
	commits []Message
}

func newSteadyConsumer(topic string, partition int) *steadyConsumer {
	c := &steadyConsumer{ch: make(chan Message, 1024), stop: make(chan struct{})}
	go func() {
		defer close(c.ch)
		for offset := int64(0); ; offset++ {
			select {
			case <-c.stop:
				return
			case c.ch <- Message{Topic: topic, Partition: partition, Offset: offset, Value: []byte("v")}:
			}
			select {
			case <-c.stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return c
}

func (c *steadyConsumer) Messages() <-chan Message { return c.ch }

func (c *steadyConsumer) Close() error {
	c.closed.Do(func() { close(c.stop) })
	return nil
}

func (c *steadyConsumer) CommitMessages(_ context.Context, msgs ...Message) error {
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	c.mu.Unlock()
	return nil
}

// TestKafkaAggregateFailedFlushDoesNotReflushPerMessage is the regression test
// for the divergent retry loop.
//
// Mechanism: flush failure leaves the buffer intact (deliberately — offsets must
// not be committed). Before the fix, that meant the buffer stayed at MaxSize, so
// the size threshold was satisfied by the NEXT arriving message and every
// arrival after it re-ran the entire buffer downstream. Retries also grew the
// batch, so each attempt cost more than the last.
//
// Measured against live traffic before the fix: 46.8 downstream evals per
// committed record where 2 were expected — 95.7% of capacity spent on messages
// that never committed.
//
// The assertion is a RATE, not a count: with a permanently failing downstream
// and a 200ms flush interval, a correct aggregator retries on the timer (~5/s),
// while the buggy one retries once per arriving message (~1000/s at 1ms
// spacing). The bound sits far above the timer rate and far below the
// per-message rate, so it discriminates without being timing-fragile.
func TestKafkaAggregateFailedFlushDoesNotReflushPerMessage(t *testing.T) {
	orig := newConsumer
	consumer := newSteadyConsumer("t", 0)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var emits atomic.Int32
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		emits.Add(1)
		return "", errors.New("downstream permanently unavailable")
	})

	// MaxSize=10 so the buffer fills fast; FlushInterval=200ms so the timer
	// retry rate is clearly separated from the per-arrival rate.
	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(10, 200*time.Millisecond)
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

	const window = 2 * time.Second
	time.Sleep(window)
	got := emits.Load()

	// Timer-driven retries over 2s at 200ms ≈ 10, plus the initial size-triggered
	// attempt. 40 leaves generous headroom for scheduling jitter while staying
	// two orders of magnitude below the per-message behaviour.
	const maxAttempts = 40
	if got > maxAttempts {
		t.Errorf("downstream called %d times in %s with a permanently failing flush, "+
			"want <= %d. The aggregator is re-flushing the buffer on every arriving "+
			"message instead of on the retry timer: capacity is being spent "+
			"re-processing messages that never commit.", got, window, maxAttempts)
	}
	// Guard the assertion itself: zero attempts would satisfy the bound above
	// while proving nothing, which is how a broken harness passes.
	if got == 0 {
		t.Error("downstream was never called; the harness produced no flush attempt, " +
			"so the rate bound above proves nothing")
	}

	// Nothing may commit while every flush fails.
	consumer.mu.Lock()
	commits := len(consumer.commits)
	consumer.mu.Unlock()
	if commits != 0 {
		t.Errorf("committed %d offset(s) despite every flush failing; those messages "+
			"would be lost rather than redelivered", commits)
	}
}

// TestKafkaAggregateRetryBufferIsBounded proves the buffer cap holds while a
// flush retry is pending.
//
// Holding messages for a timed retry (rather than re-flushing per arrival) is
// what creates the need for this bound: a downstream that stays broken would
// otherwise accumulate every arriving message until the process ran out of
// memory. Shedding is safe because the offsets were never committed — Kafka
// redelivers from the retained head.
func TestKafkaAggregateRetryBufferIsBounded(t *testing.T) {
	orig := newConsumer
	consumer := newSteadyConsumer("t", 0)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	obsFake := &recordingBatchObserver{}
	SetObserver(obsFake)
	t.Cleanup(func() { SetObserver(nil) })

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("downstream permanently unavailable")
	})

	const maxSize = 10
	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(maxSize, 200*time.Millisecond)
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

	// Long enough to produce far more messages (~2000 at 1ms) than the cap
	// (maxSize * maxBufferedBatches = 40).
	time.Sleep(2 * time.Second)

	// Every attempted flush is now observed, so the largest observed size is the
	// buffer's high-water mark.
	obsFake.mu.Lock()
	sizes := append([]int(nil), obsFake.sizes...)
	obsFake.mu.Unlock()
	if len(sizes) == 0 {
		t.Fatal("no flush attempt was observed; OnBatchFlushed must fire on attempts, " +
			"not only on successes, or this assertion cannot see a failing batch at all")
	}
	high := 0
	for _, s := range sizes {
		if s > high {
			high = s
		}
	}
	cap := maxSize * maxBufferedBatches
	if high > cap {
		t.Errorf("largest attempted batch = %d, want <= %d (max_size %d x %d). The "+
			"retry buffer is unbounded: a downstream that stays broken grows it until "+
			"the process dies.", high, cap, maxSize, maxBufferedBatches)
	}
}
