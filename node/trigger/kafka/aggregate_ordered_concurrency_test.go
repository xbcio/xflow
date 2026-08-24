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

func TestKafkaAggregateConcurrentEmitCommitsOnlyContiguousPrefix(t *testing.T) {
	orig := newConsumer
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Value: []byte("a")},
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte("b")},
		{Topic: "t", Partition: 0, Offset: 2, Value: []byte("c")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	releaseHead := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHead) }) }
	started := make(chan int64, len(msgs))
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["start_offset"].(int64)
		started <- offset
		if offset == 0 {
			<-releaseHead
		}
		return "exec", nil
	})

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		MaxInflight(3).
		AggregateByPartition(1, time.Second)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		release()
		_ = sub.Close(context.Background())
	}()

	seen := make(map[int64]bool, len(msgs))
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(seen) < len(msgs) {
		select {
		case offset := <-started:
			seen[offset] = true
		case <-deadline.C:
			release()
			t.Fatalf("only offsets %v began while the head was blocked; want all three batches in flight", seen)
		}
	}
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("committed %v while offset 0 was still running; only a contiguous successful prefix may commit", consumer.committedOffsets())
	}

	release()
	if !consumer.waitForCommitCount(3, time.Second) {
		t.Fatalf("committed offsets = %v, want [0 1 2] after the head succeeds", consumer.committedOffsets())
	}
	if got := consumer.committedOffsets(); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("committed offsets = %v, want strict offset order [0 1 2]", got)
	}
}

func TestKafkaAggregateFailedHeadRetriesWithoutReemittingFollowers(t *testing.T) {
	orig := newConsumer
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Value: []byte("a")},
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte("b")},
		{Topic: "t", Partition: 0, Offset: 2, Value: []byte("c")},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var mu sync.Mutex
	calls := map[int64]int{}
	allFirstAttempts := make(chan struct{})
	var allOnce sync.Once
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["start_offset"].(int64)
		mu.Lock()
		calls[offset]++
		attempt := calls[offset]
		if calls[0] > 0 && calls[1] > 0 && calls[2] > 0 {
			allOnce.Do(func() { close(allFirstAttempts) })
		}
		mu.Unlock()
		if offset == 0 && attempt == 1 {
			return "", errors.New("transient head failure")
		}
		return "exec", nil
	})

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		MaxInflight(3).
		AggregateByPartition(1, 200*time.Millisecond)
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

	select {
	case <-allFirstAttempts:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("followers did not execute while the failed head waited for its retry interval")
	}
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("committed %v across a failed head batch", consumer.committedOffsets())
	}
	if !consumer.waitForCommitCount(3, time.Second) {
		t.Fatalf("committed offsets = %v, want the full prefix after head retry", consumer.committedOffsets())
	}

	mu.Lock()
	defer mu.Unlock()
	if calls[0] != 2 {
		t.Errorf("head emit calls = %d, want 2 (failure plus retry)", calls[0])
	}
	if calls[1] != 1 || calls[2] != 1 {
		t.Errorf("follower emit calls = [%d %d], want [1 1]; successful followers must wait for commit, not re-execute", calls[1], calls[2])
	}
}

type failOnceCommitConsumer struct {
	*replayableKafkaConsumer
	attempts atomic.Int32
}

func (c *failOnceCommitConsumer) CommitMessages(ctx context.Context, msgs ...Message) error {
	if c.attempts.Add(1) == 1 {
		return errors.New("transient commit failure")
	}
	return c.replayableKafkaConsumer.CommitMessages(ctx, msgs...)
}

func TestKafkaAggregateCommitFailureRetriesCommitWithoutReemit(t *testing.T) {
	orig := newConsumer
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Value: []byte("a")},
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte("b")},
	}
	consumer := &failOnceCommitConsumer{replayableKafkaConsumer: newReplayableKafkaConsumer(msgs, nil)}
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		MaxInflight(2).
		AggregateByPartition(1, 20*time.Millisecond)
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

	if !consumer.waitForCommitCount(2, time.Second) {
		t.Fatalf("commit attempts = %d, committed offsets = %v; a failed commit must retain and retry the successful prefix", consumer.attempts.Load(), consumer.committedOffsets())
	}
	if got := rt.EmitCount(); got != 2 {
		t.Fatalf("emit count = %d, want 2; commit retry must not repeat an already successful side effect", got)
	}
	if got := consumer.attempts.Load(); got < 2 {
		t.Fatalf("commit attempts = %d, want at least 2", got)
	}
}

func TestKafkaAggregateCloseReturnsWithUncooperativeEmit(t *testing.T) {
	orig := newConsumer
	consumer := newReplayableKafkaConsumer([]Message{{Topic: "t", Partition: 0, Offset: 0}}, nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	emitStarted := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		startedOnce.Do(func() { close(emitStarted) })
		<-release // deliberately ignores context cancellation
		return "exec", nil
	})

	tr := New().Brokers("localhost:9092").Topic("t").Group("g").AggregateByPartition(1, time.Second)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-emitStarted:
	case <-time.After(time.Second):
		t.Fatal("emit never started")
	}

	closed := make(chan error, 1)
	start := time.Now()
	go func() { closed <- sub.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("Close took %s, want under 1s", elapsed)
		}
	case <-time.After(900 * time.Millisecond):
		releaseOnce.Do(func() { close(release) })
		<-closed
		t.Fatal("Close did not return within 900ms while Emit ignored cancellation")
	}
	releaseOnce.Do(func() { close(release) })
}

func TestKafkaAggregateAllInvalidStreamFlushesAtSize(t *testing.T) {
	orig := newConsumer
	const count = 8
	msgs := make([]Message, 0, count)
	for i := 0; i < count; i++ {
		msgs = append(msgs, Message{Topic: "invalid", Partition: 0, Offset: int64(i), Value: []byte(`{}`)})
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "kafka", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 2,
		Aggregate: AggregateConfig{
			Enabled: true, By: aggregateByPartition, MaxSize: 2,
			FlushInterval: time.Hour, Dedup: aggregateDedupMessage,
		},
		MessageSchema: &MessageSchema{RequiredFields: []string{"required"}},
	}
	sub := activateAggregate(context.Background(), in, cfg, consumer, nil)
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(count, time.Second) {
		t.Fatalf("committed offsets = %v, want all invalid offsets without waiting for the one-hour timer", consumer.committedOffsets())
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0 for an all-invalid stream", got)
	}
}

type failOncePublisher struct {
	mu       sync.Mutex
	attempts map[int64]int
}

func (p *failOncePublisher) Publish(_ context.Context, _ string, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.attempts == nil {
		p.attempts = make(map[int64]int)
	}
	p.attempts[msg.Offset]++
	if msg.Offset == 1 && p.attempts[msg.Offset] == 1 {
		return errors.New("transient dlq failure")
	}
	return nil
}

func (*failOncePublisher) Close() error { return nil }

func (p *failOncePublisher) calls(offset int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts[offset]
}

func TestKafkaAggregateDeadLetterRetryDoesNotRepublishResolvedPrefix(t *testing.T) {
	orig := newConsumer
	msgs := []Message{
		{Topic: "invalid", Partition: 0, Offset: 0, Value: []byte(`{}`)},
		{Topic: "invalid", Partition: 0, Offset: 1, Value: []byte(`{}`)},
	}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	publisher := &failOncePublisher{}
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "kafka", Runtime: triggertest.NewFakeRuntime()}
	cfg := ConsumerConfig{
		MaxInflight: 1,
		Aggregate: AggregateConfig{
			Enabled: true, By: aggregateByPartition, MaxSize: 2,
			FlushInterval: 20 * time.Millisecond, Dedup: aggregateDedupMessage,
		},
		MessageSchema: &MessageSchema{
			RequiredFields: []string{"required"}, OnInvalid: onInvalidDeadLetter,
			DeadLetterTopic: "invalid-dlq",
		},
	}
	sub := activateAggregate(context.Background(), in, cfg, consumer, publisher)
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(2, time.Second) {
		t.Fatalf("committed offsets = %v, want both after the transient DLQ failure recovers", consumer.committedOffsets())
	}
	if got := publisher.calls(0); got != 1 {
		t.Fatalf("offset 0 publish calls = %d, want 1; a resolved DLQ prefix must survive retry", got)
	}
	if got := publisher.calls(1); got != 2 {
		t.Fatalf("offset 1 publish calls = %d, want 2 (failure plus retry)", got)
	}
}
