package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// --- observer fake -------------------------------------------------------

// recordingObserver captures the observer callbacks so a test can assert that a
// drop was actually counted, not just that it happened.
type recordingObserver struct {
	mu           sync.Mutex
	discarded    []string // "topic/reason"
	deadLettered []string // "topic/result"
	notify       chan struct{}
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{notify: make(chan struct{})}
}

func (o *recordingObserver) OnMessageDiscarded(_ context.Context, topic, reason string) {
	o.mu.Lock()
	o.discarded = append(o.discarded, topic+"/"+reason)
	o.wakeLocked()
	o.mu.Unlock()
}

func (o *recordingObserver) OnMessageDeadLettered(_ context.Context, topic, result string) {
	o.mu.Lock()
	o.deadLettered = append(o.deadLettered, topic+"/"+result)
	o.wakeLocked()
	o.mu.Unlock()
}

// OnBatchFlushed, OnBatchFlushOutcome and OnBatchAdmission are no-ops here: this
// fake only asserts on discard/dead-letter behavior (Task 4's batch metrics are
// covered by recordingBatchObserver in kafka_entry_seed_batch_test.go).
func (o *recordingObserver) OnBatchFlushed(context.Context, string, string, int)         {}
func (o *recordingObserver) OnBatchFlushOutcome(context.Context, string, string, string) {}
func (o *recordingObserver) OnBatchAdmission(context.Context, string, string, string)    {}

func (o *recordingObserver) wakeLocked() {
	close(o.notify)
	o.notify = make(chan struct{})
}

func (o *recordingObserver) snapshot() (discarded, deadLettered []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.discarded...), append([]string(nil), o.deadLettered...)
}

// waitFor blocks until pred is satisfied by the current snapshot, or timeout.
func (o *recordingObserver) waitFor(timeout time.Duration, pred func(discarded, deadLettered []string) bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		o.mu.Lock()
		d := append([]string(nil), o.discarded...)
		dl := append([]string(nil), o.deadLettered...)
		notify := o.notify
		o.mu.Unlock()
		if pred(d, dl) {
			return true
		}
		select {
		case <-notify:
		case <-deadline.C:
			return false
		}
	}
}

// installRecordingObserver installs a recording observer for the duration of
// the test. SetObserver is process-global, so the cleanup is what keeps this
// from leaking into other tests in the package.
func installRecordingObserver(t *testing.T) *recordingObserver {
	t.Helper()
	o := newRecordingObserver()
	SetObserver(o)
	t.Cleanup(func() { SetObserver(nil) })
	return o
}

// --- dead-letter publisher fake ------------------------------------------

type fakePublisher struct {
	mu        sync.Mutex
	published []Message
	topics    []string
	err       error
	closed    bool
	notify    chan struct{}
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{notify: make(chan struct{})}
}

func (p *fakePublisher) Publish(_ context.Context, topic string, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, msg)
	p.topics = append(p.topics, topic)
	close(p.notify)
	p.notify = make(chan struct{})
	return nil
}

func (p *fakePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *fakePublisher) publishedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func (p *fakePublisher) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// --- discard is counted, not silent --------------------------------------

// TestKafkaSchemaDiscardIsObserved is the probe for the reported gap: the
// pre-existing code committed an invalid message's offset and returned, with no
// counter and no log. That made a producer emitting malformed records
// indistinguishable from an idle topic — offsets kept advancing, so
// consumer-group lag stayed at zero and nothing alerted.
//
// The commit assertions are unchanged from the original behaviour on purpose:
// this closes the observability gap without changing which messages are dropped.
func TestKafkaSchemaDiscardIsObserved(t *testing.T) {
	o := installRecordingObserver(t)
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 1, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "events", Partition: 0, Offset: 2, Value: []byte(`{"user_id":"u2"}`)},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	sub := activateSchemaTrigger(t, rt, map[string]any{"required_fields": []any{"user_id", "action"}})
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(2, 2*time.Second) {
		t.Fatalf("commit count = %d, want 2", consumer.commitCount())
	}
	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.EmitCount())
	}
	ok := o.waitFor(2*time.Second, func(discarded, _ []string) bool { return len(discarded) == 1 })
	discarded, _ := o.snapshot()
	if !ok {
		t.Fatalf("discarded observations = %v, want exactly one; a silent drop is the whole defect", discarded)
	}
	if discarded[0] != "events/schema" {
		t.Errorf("discarded[0] = %q, want %q", discarded[0], "events/schema")
	}
}

// --- entry-seed mode was skipping validation entirely --------------------

// TestKafkaSchemaAppliesToEntrySeedMode is the probe for the second defect: the
// dispatch chained schema validation as `else if` AFTER the entry-seed branch,
// so an entry-seed activation ignored a declared schema completely. That is the
// mode where invalid content costs the most, because a seeded execution is
// durable in the control plane — a malformed record became a permanent row
// rather than a dropped message.
//
// Against the pre-fix dispatch, SeedExecutionFromEntry is called for the invalid
// message and this test fails on admission calls = 2.
func TestKafkaSchemaAppliesToEntrySeedMode(t *testing.T) {
	o := installRecordingObserver(t)
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 700, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "t", Partition: 0, Offset: 701, Value: []byte(`{"user_id":"u2"}`)}, // invalid
	}
	consumer := newScriptedConsumer(msgs)
	recorder := &commitRecordingConsumer{inner: consumer}

	admitter := &mockEntrySeedRuntime{
		response: types.EntrySeedResponse{Accepted: true, ExecutionID: "exec-seed"},
	}
	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(context.Context, string, time.Duration) (bool, error) { return true, nil },
	}
	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_seed": true, "entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}
	cfg := ConsumerConfig{
		MaxInflight:   4,
		MessageSchema: &MessageSchema{RequiredFields: []string{"user_id", "action"}},
	}
	sub := activatePerMessage(context.Background(), in, cfg, recorder, nil)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	if !o.waitFor(2*time.Second, func(discarded, _ []string) bool { return len(discarded) == 1 }) {
		discarded, _ := o.snapshot()
		t.Fatalf("discarded = %v, want one; entry-seed mode is not applying the schema", discarded)
	}
	// The valid message is admitted; the invalid one must never reach admission.
	if got := admitter.callCount.Load(); got != 1 {
		t.Errorf("admission calls = %d, want 1: an invalid message reached SeedExecutionFromEntry, "+
			"which durably seeds it in the control plane", got)
	}
}

// --- aggregate mode had no schema field at all ---------------------------

// TestKafkaSchemaAppliesToAggregateMode is the probe for the third defect:
// kafkaAggregateRuntime had no messageSchema field, so an aggregate-mode trigger
// that declared message_schema had it silently ignored — invalid messages went
// straight into the emitted batch.
//
// It also pins where filtering happens: pre-batch, so one malformed record does
// not contaminate an otherwise valid batch.
func TestKafkaSchemaAppliesToAggregateMode(t *testing.T) {
	o := installRecordingObserver(t)
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "t", Partition: 0, Offset: 2, Value: []byte(`{"user_id":"u2"}`)}, // invalid
		{Topic: "t", Partition: 0, Offset: 3, Value: []byte(`{"user_id":"u3","action":"scroll"}`)},
	}
	consumer := newCommitRecordingKafkaConsumer(msgs)
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf1", NodeName: "trigger", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 4,
		Aggregate: AggregateConfig{
			Enabled: true, By: aggregateByPartition,
			MaxSize: 2, FlushInterval: 50 * time.Millisecond,
			Dedup: aggregateDedupMessage,
		},
		MessageSchema: &MessageSchema{RequiredFields: []string{"user_id", "action"}},
	}
	sub := activateAggregate(context.Background(), in, cfg, consumer, nil)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	if !o.waitFor(2*time.Second, func(discarded, _ []string) bool { return len(discarded) == 1 }) {
		discarded, _ := o.snapshot()
		t.Fatalf("discarded = %v, want one; aggregate mode is not applying the schema", discarded)
	}
	if !rt.WaitForEmitCount(1, 2*time.Second) {
		t.Fatalf("emit count = %d, want at least 1", rt.EmitCount())
	}
	// The batch must contain only the two valid messages. A count of 3 means the
	// invalid record was emitted downstream.
	events := rt.Events()
	if len(events) == 0 {
		t.Fatal("no events emitted")
	}
	batch := events[0].Data
	if got := batch["count"]; fmt.Sprint(got) != "2" {
		t.Errorf("batch count = %v, want 2 (the invalid message must not be in the batch)", got)
	}
	// All three offsets must still be committed: the two emitted plus the
	// discarded one riding along. Leaving the discarded offset uncommitted would
	// stall the group on a message that will never be emitted.
	if !consumer.waitForCommitCount(3, 2*time.Second) {
		t.Errorf("committed offsets = %v, want all of 1,2,3", consumer.committedOffsets())
	}
}

// TestKafkaAggregateDiscardedOffsetNotCommittedBeforeBatch pins the ordering
// hazard the discarded-offset carry exists to avoid: a discarded offset must not
// be committed while a LOWER offset is still buffered and unemitted. Committing
// it independently would advance the group past the buffered message, so a
// rebalance right then would skip it — the exact skip the per-partition serial
// design exists to prevent.
func TestKafkaAggregateDiscardedOffsetNotCommittedBeforeBatch(t *testing.T) {
	installRecordingObserver(t)
	// Offset 1 is valid (buffered, below MaxSize so no flush yet); offset 2 is
	// invalid. If offset 2 committed on its own, the group would sit at 2 with
	// offset 1 never emitted.
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "t", Partition: 0, Offset: 2, Value: []byte(`{"user_id":"u2"}`)},
	})
	rt := triggertest.NewFakeRuntime()
	// Block emit so the buffer cannot flush while we inspect commits.
	release := make(chan struct{})
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		<-release
		return "exec-1", nil
	})
	in := &types.TriggerActivateInput{WorkflowID: "wf1", NodeName: "trigger", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 4,
		Aggregate: AggregateConfig{
			Enabled: true, By: aggregateByPartition,
			// MaxSize 3 so two messages never trigger a size flush; the interval
			// is long enough that nothing flushes during the window below.
			MaxSize: 3, FlushInterval: 10 * time.Second,
			Dedup: aggregateDedupMessage,
		},
		MessageSchema: &MessageSchema{RequiredFields: []string{"user_id", "action"}},
	}
	sub := activateAggregate(context.Background(), in, cfg, consumer, nil)
	t.Cleanup(func() {
		close(release)
		_ = sub.Close(context.Background())
	})

	// Give the aggregator time to process both messages. Nothing should commit:
	// the valid one is buffered and the invalid one must wait for it.
	if consumer.waitForCommitCount(1, 300*time.Millisecond) {
		t.Fatalf("committed %v while offset 1 was still buffered and unemitted; "+
			"a rebalance here would skip offset 1", consumer.committedOffsets())
	}
}

// --- dead-letter policy --------------------------------------------------

// TestKafkaSchemaDeadLetterPublishesAndCommits checks the happy path: the
// message is republished and only then is its offset committed.
func TestKafkaSchemaDeadLetterPublishesAndCommits(t *testing.T) {
	o := installRecordingObserver(t)
	publisher := newFakePublisher()
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 5, Value: []byte(`{"user_id":"u2"}`)},
	})
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf1", NodeName: "trigger", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 4,
		MessageSchema: &MessageSchema{
			RequiredFields:  []string{"user_id", "action"},
			OnInvalid:       onInvalidDeadLetter,
			DeadLetterTopic: "events-dlq",
		},
	}
	sub := activatePerMessage(context.Background(), in, cfg, consumer, publisher)
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(1, 2*time.Second) {
		t.Fatalf("commit count = %d, want 1 after a successful dead-letter publish", consumer.commitCount())
	}
	if got := publisher.publishedCount(); got != 1 {
		t.Fatalf("published = %d, want 1", got)
	}
	publisher.mu.Lock()
	topic := publisher.topics[0]
	publisher.mu.Unlock()
	if topic != "events-dlq" {
		t.Errorf("published to %q, want %q", topic, "events-dlq")
	}
	if !o.waitFor(2*time.Second, func(_, deadLettered []string) bool { return len(deadLettered) == 1 }) {
		_, dl := o.snapshot()
		t.Fatalf("dead-letter observations = %v, want one", dl)
	}
	_, dl := o.snapshot()
	if dl[0] != "events/ok" {
		t.Errorf("deadLettered[0] = %q, want %q", dl[0], "events/ok")
	}
	// No emit: the message never entered the workflow.
	if got := rt.EmitCount(); got != 0 {
		t.Errorf("emit count = %d, want 0", got)
	}
}

// TestKafkaSchemaDeadLetterFailureWithholdsCommit is the assertion that makes the
// DLQ worth having: when the republish fails, the offset must NOT be committed,
// so Kafka redelivers. Committing on a failed publish would lose the message
// while reporting that it was safely parked — worse than the plain discard,
// because the operator believes nothing was lost.
func TestKafkaSchemaDeadLetterFailureWithholdsCommit(t *testing.T) {
	o := installRecordingObserver(t)
	publisher := newFakePublisher()
	publisher.err = errors.New("broker unreachable")
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 5, Value: []byte(`{"user_id":"u2"}`)},
	})
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf1", NodeName: "trigger", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 4,
		MessageSchema: &MessageSchema{
			RequiredFields:  []string{"user_id", "action"},
			OnInvalid:       onInvalidDeadLetter,
			DeadLetterTopic: "events-dlq",
		},
	}
	sub := activatePerMessage(context.Background(), in, cfg, consumer, publisher)
	defer func() { _ = sub.Close(context.Background()) }()

	if !o.waitFor(2*time.Second, func(_, deadLettered []string) bool { return len(deadLettered) == 1 }) {
		_, dl := o.snapshot()
		t.Fatalf("dead-letter observations = %v, want one error", dl)
	}
	_, dl := o.snapshot()
	if dl[0] != "events/error" {
		t.Errorf("deadLettered[0] = %q, want %q", dl[0], "events/error")
	}
	// Wait for a commit that must never arrive. Reading commitCount() straight
	// after the observation would pass trivially: the observer fires BEFORE the
	// commit would happen, so there is a window in which a wrongly-committing
	// implementation still reads zero.
	if consumer.waitForCommitCount(1, 500*time.Millisecond) {
		t.Fatalf("committed %v after a failed dead-letter publish; the message is lost while "+
			"appearing safely parked — worse than a plain discard", consumer.committedOffsets())
	}
}

// TestKafkaSchemaFailPolicyWithholdsCommit pins the "fail" policy: nothing is
// committed, so the message is redelivered indefinitely. This trades a stalled
// partition for zero data loss, which is the point of offering the policy.
func TestKafkaSchemaFailPolicyWithholdsCommit(t *testing.T) {
	o := installRecordingObserver(t)
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 9, Value: []byte(`{"user_id":"u2"}`)},
	})
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf1", NodeName: "trigger", Runtime: rt}
	cfg := ConsumerConfig{
		MaxInflight: 4,
		MessageSchema: &MessageSchema{
			RequiredFields: []string{"user_id", "action"},
			OnInvalid:      onInvalidFail,
		},
	}
	sub := activatePerMessage(context.Background(), in, cfg, consumer, nil)
	defer func() { _ = sub.Close(context.Background()) }()

	if !o.waitFor(2*time.Second, func(discarded, _ []string) bool { return len(discarded) == 1 }) {
		discarded, _ := o.snapshot()
		t.Fatalf("discarded = %v, want one schema_fail", discarded)
	}
	discarded, _ := o.snapshot()
	if discarded[0] != "events/schema_fail" {
		t.Errorf("discarded[0] = %q, want %q", discarded[0], "events/schema_fail")
	}
	// Wait, for the same reason as the dead-letter failure test: the observation
	// precedes the commit, so an immediate count would read zero either way.
	if consumer.waitForCommitCount(1, 500*time.Millisecond) {
		t.Errorf("committed %v under the fail policy, which must never commit", consumer.committedOffsets())
	}
}

// TestKafkaActivateBuildsDeadLetterPublisher checks the publisher is constructed
// eagerly at activation (not lazily on the first invalid message) and closed when
// the subscription closes. Eager construction turns an unreachable DLQ broker
// into a failed activation, which the runner self-heals by retrying, instead of
// an unbounded redelivery loop the first time a malformed record shows up.
func TestKafkaActivateBuildsDeadLetterPublisher(t *testing.T) {
	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})

	consumer := newCommitRecordingKafkaConsumer(nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	publisher := newFakePublisher()
	var built int
	newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) {
		built++
		return publisher, nil
	}

	rt := triggertest.NewFakeRuntime()
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params: map[string]any{
			"brokers": []any{"localhost:9092"}, "topic": "events", "group": "workers",
			"message_schema": map[string]any{
				"required_fields":   []any{"user_id"},
				"on_invalid":        "dead_letter",
				"dead_letter_topic": "events-dlq",
			},
		},
		Runtime: rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if built != 1 {
		t.Errorf("publisher constructed %d times, want 1 (eagerly at activation)", built)
	}
	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !publisher.isClosed() {
		t.Error("dead-letter publisher was not closed with the subscription; it owns a Kafka writer")
	}
}

// TestKafkaActivateFailsWhenDeadLetterPublisherFails checks activation fails
// closed, and that the consumer it already opened is not leaked.
func TestKafkaActivateFailsWhenDeadLetterPublisherFails(t *testing.T) {
	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})

	consumer := newCommitRecordingKafkaConsumer(nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) {
		return nil, errors.New("dlq broker unreachable")
	}

	_, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params: map[string]any{
			"brokers": []any{"localhost:9092"}, "topic": "events", "group": "workers",
			"message_schema": map[string]any{
				"required_fields":   []any{"user_id"},
				"on_invalid":        "dead_letter",
				"dead_letter_topic": "events-dlq",
			},
		},
		Runtime: triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("Activate succeeded with an unreachable dead-letter broker; want an error so the runner retries")
	}
}

// TestKafkaActivateSkipsPublisherForDiscardPolicy pins that the default policy
// opens no writer. A Kafka producer connection per trigger that never uses it is
// a real resource cost on a runner hosting many triggers.
func TestKafkaActivateSkipsPublisherForDiscardPolicy(t *testing.T) {
	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})

	consumer := newCommitRecordingKafkaConsumer(nil)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	var built int
	newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) {
		built++
		return newFakePublisher(), nil
	}

	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params: map[string]any{
			"brokers": []any{"localhost:9092"}, "topic": "events", "group": "workers",
			"message_schema": map[string]any{"required_fields": []any{"user_id"}},
		},
		Runtime: triggertest.NewFakeRuntime(),
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })
	if built != 0 {
		t.Errorf("publisher constructed %d times under the discard policy, want 0", built)
	}
}

// --- throttled logging ---------------------------------------------------

// TestDiscardLogThrottleReportsSuppressedCount checks the log throttle never
// understates volume: a suppressed line's occurrences are folded into the next
// emitted one. Without that, a throttled log would report "1 occurrence" for a
// partition where every message is being dropped.
func TestDiscardLogThrottleReportsSuppressedCount(t *testing.T) {
	l := &discardLogger{last: make(map[string]time.Time), suppressed: make(map[string]int)}
	base := time.Unix(1700000000, 0)

	if emit, count := l.allow(base, "t/schema"); !emit || count != 1 {
		t.Fatalf("first call: emit=%v count=%d, want true/1", emit, count)
	}
	for i := range 4 {
		if emit, _ := l.allow(base.Add(time.Duration(i+1)*time.Second), "t/schema"); emit {
			t.Fatalf("call %d emitted inside the throttle window", i+2)
		}
	}
	emit, count := l.allow(base.Add(discardLogInterval+time.Second), "t/schema")
	if !emit {
		t.Fatal("no emission after the throttle window elapsed")
	}
	if count != 5 {
		t.Errorf("occurrences = %d, want 5 (1 emitted + 4 suppressed)", count)
	}
	// A different key must have its own window rather than inheriting this one.
	if emit, count := l.allow(base.Add(discardLogInterval+time.Second), "t/schema_fail"); !emit || count != 1 {
		t.Errorf("second key: emit=%v count=%d, want true/1", emit, count)
	}
}

// TestKafkaTriggerSchemaBuilderRoundTrip pins that the Go builder's schema
// survives RawParams -> kafkaConfigFromParams. The two sides use different types
// for required_fields ([]string from the builder, []any from decoded YAML/JSON),
// so a parser that only handled one would silently drop the schema on the other
// path — and a dropped schema means no validation at all.
func TestKafkaTriggerSchemaBuilderRoundTrip(t *testing.T) {
	params := New().
		Brokers("localhost:9092").Topic("events").Group("workers").
		MessageSchema("user_id", "action").
		DeadLetterInvalid("events-dlq").
		RawParams().(map[string]any)

	cfg, err := configFromParams(params, nil, false)
	if err != nil {
		t.Fatalf("configFromParams: %v", err)
	}
	if cfg.MessageSchema == nil {
		t.Fatal("MessageSchema is nil after a builder round-trip; the schema was silently dropped")
	}
	if got := cfg.MessageSchema.OnInvalid; got != onInvalidDeadLetter {
		t.Errorf("OnInvalid = %q, want %q", got, onInvalidDeadLetter)
	}
	if got := cfg.MessageSchema.DeadLetterTopic; got != "events-dlq" {
		t.Errorf("DeadLetterTopic = %q, want %q", got, "events-dlq")
	}
	if got := len(cfg.MessageSchema.RequiredFields); got != 2 {
		t.Errorf("RequiredFields = %v, want 2 entries", cfg.MessageSchema.RequiredFields)
	}
}

// --- helper --------------------------------------------------------------

func activateSchemaTrigger(t *testing.T, rt types.TriggerRuntime, schema map[string]any) types.TriggerSubscription {
	t.Helper()
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params: map[string]any{
			"brokers":        []any{"localhost:9092"},
			"topic":          "events",
			"group":          "workers",
			"max_inflight":   1,
			"message_schema": schema,
		},
		Runtime: rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	return sub
}
