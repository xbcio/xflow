package trigger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// replayableKafkaConsumer sends a scripted batch, then optionally replays it to
// simulate Kafka redelivery after an uncommitted flush failure.
type replayableKafkaConsumer struct {
	ch        chan KafkaMessage
	mu        sync.Mutex
	commits   []KafkaMessage
	notify    chan struct{}
	commitErr error // if non-nil, CommitMessages returns this
}

func newReplayableKafkaConsumer(initial []KafkaMessage, replay []KafkaMessage) *replayableKafkaConsumer {
	ch := make(chan KafkaMessage, len(initial)+len(replay))
	for _, m := range initial {
		ch <- m
	}
	for _, m := range replay {
		ch <- m
	}
	return &replayableKafkaConsumer{ch: ch, notify: make(chan struct{})}
}

func (c *replayableKafkaConsumer) Messages() <-chan KafkaMessage { return c.ch }
func (c *replayableKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

func (c *replayableKafkaConsumer) CommitMessages(_ context.Context, msgs ...KafkaMessage) error {
	if c.commitErr != nil {
		return c.commitErr
	}
	c.mu.Lock()
	c.commits = append(c.commits, msgs...)
	close(c.notify)
	c.notify = make(chan struct{})
	c.mu.Unlock()
	return nil
}

func (c *replayableKafkaConsumer) waitForCommitCount(n int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		if len(c.commits) >= n {
			c.mu.Unlock()
			return true
		}
		notify := c.notify
		c.mu.Unlock()
		select {
		case <-notify:
		case <-deadline.C:
			return false
		}
	}
}

func (c *replayableKafkaConsumer) commitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.commits)
}

func (c *replayableKafkaConsumer) committedOffsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, 0, len(c.commits))
	for _, m := range c.commits {
		out = append(out, m.Offset)
	}
	return out
}

// TestKafkaAggregateP01Regression_EmitFailThenReplay is the core P0-1
// regression test for the aggregate path. It simulates:
//  1. A batch of 2 messages arrives and fills the aggregator buffer.
//  2. First flush: Emit FAILS → offset NOT committed → buffer NOT cleared.
//  3. The aggregator retries on the next flush interval.
//  4. Second flush: Emit succeeds → offset committed.
//
// Under the OLD pre-emit dedup scheme, after step 2 the messages were marked
// "seen" by SETNX. If Kafka redelivered them (because offset was not committed),
// dedup would have eaten them — permanent event loss. The fix removes pre-emit
// dedup: the aggregator simply retries flush until Emit succeeds.
func TestKafkaAggregateP01Regression_EmitFailThenReplay(t *testing.T) {
	orig := newKafkaConsumer
	msgs := []KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 10, Value: []byte("a")},
		{Topic: "orders", Partition: 0, Offset: 11, Value: []byte("b")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	var emitCalls atomic.Int32
	rt := newFakeTriggerRuntime()
	rt.emitFunc = func(_ context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
		n := emitCalls.Add(1)
		if n == 1 {
			return "", errors.New("transient network error")
		}
		return "exec-ok", nil
	}

	// MaxSize=2 so the batch flushes immediately when both messages arrive.
	// FlushInterval=20ms so retry happens quickly after the first failure.
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(2, 20*time.Millisecond)
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

	// Wait for commit — proves the batch eventually succeeded.
	if !consumer.waitForCommitCount(2, 2*time.Second) {
		t.Fatalf("commit count = %d, want 2 (batch of 2 messages); emit calls = %d",
			consumer.commitCount(), emitCalls.Load())
	}

	// Verify Emit was called at least twice (first fail, then success).
	if got := emitCalls.Load(); got < 2 {
		t.Fatalf("emit calls = %d, want >= 2 (first fails, then retries)", got)
	}

	// Verify committed offsets include both messages.
	offsets := consumer.committedOffsets()
	if len(offsets) < 2 {
		t.Fatalf("committed offsets = %v, want at least [10 11]", offsets)
	}
}

// TestKafkaAggregateBatchEmitSuccess_CommitsAllOffsets verifies that when Emit
// succeeds for a full batch, all offsets are committed in a single call.
func TestKafkaAggregateBatchEmitSuccess_CommitsAllOffsets(t *testing.T) {
	orig := newKafkaConsumer
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 100, Value: []byte("x")},
		{Topic: "t", Partition: 0, Offset: 101, Value: []byte("y")},
		{Topic: "t", Partition: 0, Offset: 102, Value: []byte("z")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(3, time.Hour)
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

	if !consumer.waitForCommitCount(3, time.Second) {
		t.Fatalf("commit count = %d, want 3", consumer.commitCount())
	}
	offsets := consumer.committedOffsets()
	if len(offsets) != 3 || offsets[0] != 100 || offsets[1] != 101 || offsets[2] != 102 {
		t.Fatalf("committed offsets = %v, want [100 101 102]", offsets)
	}
}

// TestKafkaAggregateBatchEmitFail_NoCommit verifies that when Emit fails, no
// offset is committed — the messages remain uncommitted for Kafka redelivery.
func TestKafkaAggregateBatchEmitFail_NoCommit(t *testing.T) {
	orig := newKafkaConsumer
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 200, Value: []byte("a")},
		{Topic: "t", Partition: 0, Offset: 201, Value: []byte("b")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("permanent failure")
	}
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for at least one emit attempt.
	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want >= 1", rt.emitCount())
	}
	// Give a brief window for any erroneous commits.
	time.Sleep(30 * time.Millisecond)
	_ = sub.Close(context.Background())

	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0 (emit failed → no commit)", got)
	}
}

// TestKafkaAggregateCommitFails_NoPanic verifies that when Emit succeeds but
// the subsequent commit fails, the aggregator does not panic and the offset
// remains uncommitted (safe degradation: Kafka redelivery will occur).
func TestKafkaAggregateCommitFails_NoPanic(t *testing.T) {
	orig := newKafkaConsumer
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 300, Value: []byte("c")},
		{Topic: "t", Partition: 0, Offset: 301, Value: []byte("d")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	consumer.commitErr = errors.New("commit broker unavailable")
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Emit succeeds — wait for at least one emit.
	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want >= 1", rt.emitCount())
	}
	// Should not panic. Close cleanly.
	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	// No commits recorded because commitErr is set.
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0 (commit error → no recorded commits)", got)
	}
}

// TestKafkaAggregateNoPreEmitDedup verifies that the aggregator does NOT call
// Dedup before buffering messages. This is the mechanical complement to the
// P0-1 regression test above: even if the runtime's Dedup returns false (would
// have suppressed the message under the old scheme), the message is still
// buffered and emitted.
func TestKafkaAggregateNoPreEmitDedup(t *testing.T) {
	orig := newKafkaConsumer
	msgs := []KafkaMessage{
		{Topic: "t", Partition: 0, Offset: 50, Value: []byte("nodedup")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	// If dedup were still called, it would return false (suppress the message).
	rt.dedupFunc = func(context.Context, string, time.Duration) (bool, error) {
		return false, nil
	}

	// FlushInterval short so the single message is flushed quickly.
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(100, 20*time.Millisecond)
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

	// The message must be emitted despite dedup returning false.
	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1 (pre-emit dedup must not suppress messages)", rt.emitCount())
	}
	// And Dedup must not have been consulted at all.
	if rt.waitDedup(50 * time.Millisecond) {
		t.Fatal("aggregator called Dedup; pre-emit dedup must be removed (P0-1)")
	}
}
