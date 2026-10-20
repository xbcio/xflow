package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// --- harness -------------------------------------------------------------

func legacyEmitParams(topic string) map[string]any {
	return map[string]any{
		"brokers": []any{"localhost:9092"},
		"topic":   topic,
		"group":   "workers",
	}
}

// activateLegacyEmit runs the real Activate path (so the params parser and the
// publisher-construction decision are exercised) against a fake consumer.
// publisher may be nil for the tests that configure no dead-letter topic.
func activateLegacyEmit(t *testing.T, ctx context.Context, params map[string]any, consumer Consumer, rt types.TriggerRuntime, publisher DeadLetterPublisher) types.TriggerSubscription {
	t.Helper()
	origConsumer := newConsumer
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = origConsumer })
	if publisher != nil {
		origPublisher := newDeadLetterPublisher
		newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) { return publisher, nil }
		t.Cleanup(func() { newDeadLetterPublisher = origPublisher })
	}
	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     params,
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sub.Close(closeCtx)
	})
	return sub
}

// emitAttemptLog records the emit attempts the trigger made, in order, so a
// test can prove the partition held the SAME record instead of moving on.
type emitAttemptLog struct {
	mu       sync.Mutex
	offsets  []int64
	failures map[int64]int
}

func newEmitAttemptLog() *emitAttemptLog {
	return &emitAttemptLog{failures: make(map[int64]int)}
}

func (l *emitAttemptLog) record(offset int64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.offsets = append(l.offsets, offset)
	n := l.failures[offset]
	l.failures[offset]++
	return n
}

func (l *emitAttemptLog) snapshot() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int64(nil), l.offsets...)
}

// scriptedDeadLetterPublisher fails its first `failures` Publish calls, then
// records subsequent ones. The changed channel wakes waiters, mirroring the
// package's other publisher fakes without giving the tests a polling loop.
type scriptedDeadLetterPublisher struct {
	mu        sync.Mutex
	failures  int
	attempts  int
	published []Message
	topics    []string
	reasons   []string
	changed   chan struct{}
}

func newScriptedDeadLetterPublisher(failures int) *scriptedDeadLetterPublisher {
	return &scriptedDeadLetterPublisher{failures: failures, changed: make(chan struct{})}
}

func (p *scriptedDeadLetterPublisher) Publish(_ context.Context, topic string, msg Message, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.failures > 0 {
		p.failures--
		p.wakeLocked()
		return errors.New("dead-letter broker unavailable")
	}
	p.published = append(p.published, msg)
	p.topics = append(p.topics, topic)
	p.reasons = append(p.reasons, reason)
	p.wakeLocked()
	return nil
}

func (p *scriptedDeadLetterPublisher) Close() error { return nil }

func (p *scriptedDeadLetterPublisher) wakeLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// waitFor blocks until cond (evaluated with p.mu held) is true. cond must only
// read the publisher's fields directly.
func (p *scriptedDeadLetterPublisher) waitFor(t *testing.T, what string, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		p.mu.Lock()
		ok := cond()
		changed := p.changed
		p.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func (p *scriptedDeadLetterPublisher) snapshot() (attempts int, published []Message, topics, reasons []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts,
		append([]Message(nil), p.published...),
		append([]string(nil), p.topics...),
		append([]string(nil), p.reasons...)
}

// --- the fix: a failed emit holds the partition --------------------------

// TestLegacyEmitRetriesSameRecordAndKeepsOrder is the regression probe for the
// legacy path's silent skip. The first record's emit fails twice; without the
// fix the worker took the second record, committed its higher offset, and Kafka
// never redelivered the first one (replay starts at the committed offset).
func TestLegacyEmitRetriesSameRecordAndKeepsOrder(t *testing.T) {
	const topic = "emit-retry-retry"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 10, Value: []byte("a")},
		{Topic: topic, Partition: 0, Offset: 11, Value: []byte("b")},
	}, nil)

	log := newEmitAttemptLog()
	secondFailure := make(chan struct{})
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["offset"].(int64)
		n := log.record(offset)
		if offset == 10 && n < 2 {
			if n == 1 {
				// Close only after this (second) failure has been recorded, so a
				// commit observed after the signal would be the bug.
				close(secondFailure)
			}
			return "", errors.New("downstream unavailable")
		}
		return "exec-ok", nil
	})

	activateLegacyEmit(t, context.Background(), legacyEmitParams(topic), consumer, rt, nil)

	select {
	case <-secondFailure:
	case <-time.After(3 * time.Second):
		t.Fatal("the first record was not retried a second time: the worker moved on after one failed emit")
	}
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d while the first record is still being retried, want 0 "+
			"(a commit here is the offset that sweeps past it)", n)
	}
	if !consumer.waitForCommitCount(2, 3*time.Second) {
		t.Fatalf("committed offsets = %v, want [10 11]", consumer.committedOffsets())
	}
	if got := consumer.committedOffsets(); len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Fatalf("committed offsets = %v, want [10 11] in order", got)
	}
	want := []int64{10, 10, 10, 11}
	if got := log.snapshot(); len(got) != len(want) {
		t.Fatalf("emit attempts = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("emit attempts = %v, want %v", got, want)
			}
		}
	}
}

// TestLegacyEmitBudgetExhaustedDeadLettersRecord pins the bounded policy: after
// max_attempts failed emits the record is republished with its last error, and
// only then is its offset committed, so the next record proceeds.
func TestLegacyEmitBudgetExhaustedDeadLettersRecord(t *testing.T) {
	const topic = "emit-retry-dlq"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 20, Value: []byte("poison")},
		{Topic: topic, Partition: 0, Offset: 21, Value: []byte("good")},
	}, nil)
	publisher := newScriptedDeadLetterPublisher(0)

	log := newEmitAttemptLog()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["offset"].(int64)
		log.record(offset)
		if offset == 20 {
			return "", errors.New("poison record")
		}
		return "exec-ok", nil
	})

	params := legacyEmitParams(topic)
	params["emit_retry"] = map[string]any{
		"on_failure":        "dead_letter",
		"dead_letter_topic": topic + "-dlq",
		"max_attempts":      2,
	}
	activateLegacyEmit(t, context.Background(), params, consumer, rt, publisher)

	publisher.waitFor(t, "the record to be republished", func() bool {
		return len(publisher.published) == 1
	}, 3*time.Second)
	if !consumer.waitForCommitCount(2, 3*time.Second) {
		t.Fatalf("committed offsets = %v, want [20 21]", consumer.committedOffsets())
	}

	attempts, published, topics, reasons := publisher.snapshot()
	if attempts != 1 {
		t.Fatalf("publish attempts = %d, want 1", attempts)
	}
	if len(published) != 1 || published[0].Offset != 20 || published[0].Topic != topic {
		t.Fatalf("published = %+v, want the original record at %s/0/20", published, topic)
	}
	if len(topics) != 1 || topics[0] != topic+"-dlq" {
		t.Fatalf("published topics = %v, want [%s-dlq]", topics, topic)
	}
	if len(reasons) != 1 || !strings.HasPrefix(reasons[0], deadLetterReasonEmit) ||
		!strings.Contains(reasons[0], "poison record") {
		t.Fatalf("published reason = %v, want the emit reason carrying the last error", reasons)
	}
	if got := log.snapshot(); len(got) != 3 || got[0] != 20 || got[1] != 20 || got[2] != 21 {
		t.Fatalf("emit attempts = %v, want [20 20 21] (two failures, then the next record)", got)
	}
	if got := consumer.committedOffsets(); len(got) != 2 || got[0] != 20 || got[1] != 21 {
		t.Fatalf("committed offsets = %v, want [20 21]", got)
	}
}

// TestLegacyEmitMaxBlockDeadLettersSlowEmitter pins the wall-clock half of the
// budget. max_attempts is effectively unreachable; a slow downstream spends the
// max_block window across a couple of attempts, and the record is parked with
// nothing committed before the republish succeeded.
func TestLegacyEmitMaxBlockDeadLettersSlowEmitter(t *testing.T) {
	const topic = "emit-retry-max-block"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 60, Value: []byte("slow")},
		{Topic: topic, Partition: 0, Offset: 61, Value: []byte("good")},
	}, nil)
	publisher := newScriptedDeadLetterPublisher(1)

	log := newEmitAttemptLog()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["offset"].(int64)
		log.record(offset)
		if offset == 60 {
			// Each attempt spends most of the block budget, so the second one
			// crosses max_block long before max_attempts is anywhere near.
			time.Sleep(15 * time.Millisecond)
			return "", errors.New("slow downstream")
		}
		return "exec-ok", nil
	})

	params := legacyEmitParams(topic)
	params["emit_retry"] = map[string]any{
		"on_failure":        "dead_letter",
		"dead_letter_topic": topic + "-dlq",
		"max_attempts":      1000,
		"max_block":         "20ms",
	}
	activateLegacyEmit(t, context.Background(), params, consumer, rt, publisher)

	// A failed republish proves the emit loop stopped and the DLQ phase is
	// active while the record is still uncommitted.
	publisher.waitFor(t, "a failed republish", func() bool {
		return publisher.attempts >= 1 && len(publisher.published) == 0
	}, 3*time.Second)
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d while the record is not durably parked, want 0", n)
	}
	got := log.snapshot()
	if len(got) == 0 || len(got) >= 10 {
		t.Fatalf("emit attempts = %v, want a few (max_block is 20ms, max_attempts is 1000): "+
			"the wall-clock budget, not the attempt count, must end the retry", got)
	}
	for _, offset := range got {
		if offset != 60 {
			t.Fatalf("emit attempts = %v, want only the held record at offset 60", got)
		}
	}

	publisher.waitFor(t, "the republish to succeed", func() bool {
		return len(publisher.published) == 1
	}, 3*time.Second)
	if !consumer.waitForCommitCount(2, 3*time.Second) {
		t.Fatalf("committed offsets = %v, want [60 61]", consumer.committedOffsets())
	}
	if got := consumer.committedOffsets(); len(got) != 2 || got[0] != 60 || got[1] != 61 {
		t.Fatalf("committed offsets = %v, want [60 61]", got)
	}
}

// TestLegacyEmitDeadLetterFailureHoldsPartition pins the fail-closed half: a
// republish that fails is backpressure, not a drop. Nothing behind the record is
// emitted and nothing commits until the record is durably parked.
func TestLegacyEmitDeadLetterFailureHoldsPartition(t *testing.T) {
	const topic = "emit-retry-hold"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 30, Value: []byte("poison")},
		{Topic: topic, Partition: 0, Offset: 31, Value: []byte("good")},
	}, nil)
	publisher := newScriptedDeadLetterPublisher(2)

	log := newEmitAttemptLog()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["offset"].(int64)
		log.record(offset)
		if offset == 30 {
			return "", errors.New("poison record")
		}
		return "exec-ok", nil
	})

	params := legacyEmitParams(topic)
	params["emit_retry"] = map[string]any{
		"on_failure":        "dead_letter",
		"dead_letter_topic": topic + "-dlq",
		"max_attempts":      1,
		"max_block":         "5s",
	}
	activateLegacyEmit(t, context.Background(), params, consumer, rt, publisher)

	publisher.waitFor(t, "two failed republishes", func() bool {
		return publisher.attempts >= 2 && len(publisher.published) == 0
	}, 3*time.Second)
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d while the republish is failing, want 0", n)
	}
	if got := log.snapshot(); len(got) != 1 || got[0] != 30 {
		t.Fatalf("emit attempts = %v, want [30]: the next record must not be emitted while the "+
			"first is not durably parked", got)
	}

	publisher.waitFor(t, "the republish to succeed", func() bool {
		return len(publisher.published) == 1
	}, 3*time.Second)
	if !consumer.waitForCommitCount(2, 3*time.Second) {
		t.Fatalf("committed offsets = %v, want [30 31]", consumer.committedOffsets())
	}
	if got := log.snapshot(); len(got) != 2 || got[0] != 30 || got[1] != 31 {
		t.Fatalf("emit attempts = %v, want [30 31]", got)
	}
	_, published, _, _ := publisher.snapshot()
	if len(published) != 1 || published[0].Offset != 30 {
		t.Fatalf("published = %+v, want the held record at offset 30", published)
	}
}

// TestLegacyEmitRetryStopsOnContextCancel proves cancellation ends the hold
// promptly and commits nothing: the record stays for the next generation.
func TestLegacyEmitRetryStopsOnContextCancel(t *testing.T) {
	const topic = "emit-retry-cancel"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 40, Value: []byte("a")},
		{Topic: topic, Partition: 0, Offset: 41, Value: []byte("b")},
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cancelOnce sync.Once
	log := newEmitAttemptLog()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		offset := ev.Data["offset"].(int64)
		log.record(offset)
		cancelOnce.Do(cancel)
		return "", errors.New("downstream unavailable")
	})

	sub := activateLegacyEmit(t, ctx, legacyEmitParams(topic), consumer, rt, nil)
	health, ok := sub.(types.SubscriptionHealth)
	if !ok {
		t.Fatalf("kafka subscription does not expose Done(); cannot prove it stopped")
	}

	select {
	case <-health.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the subscription did not stop after its context was canceled")
	}
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d after cancellation, want 0", n)
	}
	time.Sleep(100 * time.Millisecond)
	got := log.snapshot()
	if len(got) != 1 || got[0] != 40 {
		t.Fatalf("emit attempts = %v after cancellation, want exactly [40]: no retry and no later record", got)
	}
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d after cancellation, want 0", n)
	}
}

// TestLegacyEmitWithoutDeadLetterNeverSkips pins the no-destination default:
// retry forever, hold the partition, never commit, never touch the next record.
// A bounded retry here would have to lose the record, so none is applied.
func TestLegacyEmitWithoutDeadLetterNeverSkips(t *testing.T) {
	const topic = "emit-retry-unbounded"
	consumer := newReplayableKafkaConsumer([]Message{
		{Topic: topic, Partition: 0, Offset: 50, Value: []byte("a")},
		{Topic: topic, Partition: 0, Offset: 51, Value: []byte("b")},
	}, nil)

	log := newEmitAttemptLog()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, ev *types.TriggerEvent) (types.ExecutionID, error) {
		log.record(ev.Data["offset"].(int64))
		return "", errors.New("downstream unavailable")
	})

	activateLegacyEmit(t, context.Background(), legacyEmitParams(topic), consumer, rt, nil)

	if !rt.WaitForEmitCount(3, 3*time.Second) {
		t.Fatalf("emit attempts = %d, want at least 3: the record must be retried, not abandoned", rt.EmitCount())
	}
	time.Sleep(100 * time.Millisecond)
	if n := consumer.commitCount(); n != 0 {
		t.Fatalf("commitCount = %d, want 0: committing past an undelivered record is the loss this must not do", n)
	}
	for _, offset := range log.snapshot() {
		if offset != 50 {
			t.Fatalf("emit attempts include offset %d; the partition must hold offset 50, not move on", offset)
		}
	}
}

// --- configuration -------------------------------------------------------

func TestEmitRetryFromParams(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    EmitRetryConfig
		wantErr bool
	}{
		{name: "absent", in: nil, want: EmitRetryConfig{}},
		{name: "explicit_retry_is_the_zero_value",
			in:   map[string]any{"on_failure": "retry"},
			want: EmitRetryConfig{}},
		{name: "dead_letter_without_topic_fails_closed",
			in:      map[string]any{"on_failure": "dead_letter"},
			wantErr: true},
		{name: "dead_letter_with_bounds",
			in: map[string]any{
				"on_failure":        "dead_letter",
				"dead_letter_topic": "events-dlq",
				"max_attempts":      float64(3),
				"max_block":         "2s",
			},
			want: EmitRetryConfig{
				OnFailure:       onEmitFailureDeadLetter,
				DeadLetterTopic: "events-dlq",
				MaxAttempts:     3,
				MaxBlock:        2 * time.Second,
			}},
		{name: "unknown_policy", in: map[string]any{"on_failure": "give_up"}, wantErr: true},
		{name: "non_positive_attempts", in: map[string]any{"max_attempts": float64(0)}, wantErr: true},
		{name: "unparseable_block", in: map[string]any{"max_block": "soon"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := emitRetryFromParams(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("emitRetryFromParams(%v) = %+v, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("emitRetryFromParams(%v): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("emitRetryFromParams(%v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

// TestEmitRetryBuilderRoundTripsThroughParams guards the DSL path: RawParams is
// the only way a builder setting reaches the runtime, and an unset block must
// stay absent so existing definitions keep their hash.
func TestEmitRetryBuilderRoundTripsThroughParams(t *testing.T) {
	if _, ok := New().RawParams().(map[string]any)["emit_retry"]; ok {
		t.Fatal("New() serialized emit_retry; an untouched definition must keep its hash")
	}

	node := New().DeadLetterOnEmitFailure("emit-dlq").EmitRetryPolicy(4, 2*time.Second)
	raw, ok := node.RawParams().(map[string]any)["emit_retry"]
	if !ok {
		t.Fatal("DeadLetterOnEmitFailure did not serialize emit_retry; the setting would silently not apply")
	}
	got, err := emitRetryFromParams(raw)
	if err != nil {
		t.Fatalf("emitRetryFromParams(%v): %v", raw, err)
	}
	want := EmitRetryConfig{
		OnFailure:       onEmitFailureDeadLetter,
		DeadLetterTopic: "emit-dlq",
		MaxAttempts:     4,
		MaxBlock:        2 * time.Second,
	}
	if got != want {
		t.Fatalf("round-tripped config = %+v, want %+v", got, want)
	}
}

// TestEmitRetryBuilderWithoutTopicFailsClosed pins that a dead_letter policy
// with no destination is rejected at activation instead of quietly retrying
// forever.
func TestEmitRetryBuilderWithoutTopicFailsClosed(t *testing.T) {
	raw := New().DeadLetterOnEmitFailure("").RawParams().(map[string]any)["emit_retry"]
	if _, err := emitRetryFromParams(raw); err == nil {
		t.Fatalf("emitRetryFromParams(%v) = nil error; a dead-letter policy without a topic must be rejected", raw)
	}
}
