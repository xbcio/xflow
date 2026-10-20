package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// orderLog is a mutex-guarded event sequence shared by the fakes below. It
// exists so a test can prove ORDER — publish before commit before the next
// emit — rather than merely that each event happened.
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *orderLog) add(entry string) {
	l.mu.Lock()
	l.entries = append(l.entries, entry)
	l.mu.Unlock()
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

// orderedCommitConsumer records each commit in the shared order log before
// delegating to the standard fake, so a commit can be placed relative to a
// dead-letter publish and to the emits around it.
type orderedCommitConsumer struct {
	*commitRecordingKafkaConsumer
	order *orderLog
}

func (c *orderedCommitConsumer) CommitMessages(ctx context.Context, messages ...Message) error {
	for _, msg := range messages {
		c.order.add(fmt.Sprintf("commit:%d", msg.Offset))
	}
	return c.commitRecordingKafkaConsumer.CommitMessages(ctx, messages...)
}

func newOrderedCommitConsumer(messages []Message, order *orderLog) *orderedCommitConsumer {
	return &orderedCommitConsumer{
		commitRecordingKafkaConsumer: newCommitRecordingKafkaConsumer(messages),
		order:                        order,
	}
}

// TestKafkaSchemaFailHoldsPartition is the probe for the loss the one-shot
// policy call allowed: offset 10 fails validation under on_invalid=fail, so it
// is never committed. The old dispatch returned to the worker loop anyway, and
// the emit and commit of offset 11 then advanced the partition's committed
// offset past 10 — Kafka replays only from the committed offset, so offset 10
// was lost in this process despite the policy saying "do not commit".
//
// With the hold, offsets 11 and 12 are never emitted and never committed while
// the worker sits on 10, and the schema_fail counter is incremented once rather
// than once per retry round.
func TestKafkaSchemaFailHoldsPartition(t *testing.T) {
	o := installRecordingObserver(t)
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 10, Value: []byte(`{"user_id":"u2"}`)}, // invalid
		{Topic: "events", Partition: 0, Offset: 11, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "events", Partition: 0, Offset: 12, Value: []byte(`{"user_id":"u1","action":"scroll"}`)},
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
	closedAlready := false
	defer func() {
		if !closedAlready {
			_ = sub.Close(context.Background())
		}
	}()

	if !o.waitFor(2*time.Second, func(discarded, _ []string) bool { return len(discarded) >= 1 }) {
		discarded, _ := o.snapshot()
		t.Fatalf("discarded = %v, want one schema_fail", discarded)
	}
	// The window in which the pre-fix dispatch would emit and commit 11/12.
	if consumer.waitForCommitCount(1, 300*time.Millisecond) {
		t.Fatalf("committed %v while offset 10 was withheld by on_invalid=fail; "+
			"the later commit sweeps past the uncommitted record and it is never redelivered",
			consumer.committedOffsets())
	}
	if rt.WaitForEmitCount(1, 100*time.Millisecond) {
		t.Fatalf("emit count = %d behind a fail-held record; neither a commit nor an emit may pass it",
			rt.EmitCount())
	}
	discarded, _ := o.snapshot()
	if len(discarded) != 1 || discarded[0] != "events/schema_fail" {
		t.Fatalf("discarded = %v, want exactly one events/schema_fail: the hold must count the "+
			"policy decision once, not once per wait round", discarded)
	}

	// Cancellation is the only thing that ends the hold, and it must neither
	// hang nor commit anything.
	closed := make(chan error, 1)
	start := time.Now()
	go func() { closed <- sub.Close(context.Background()) }()
	select {
	case err := <-closed:
		closedAlready = true
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("Close took %s; deliverInvalid must exit on cancellation", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return; a fail-held worker is ignoring cancellation")
	}
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d after cancellation, want 0", got)
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d after cancellation, want 0: offsets 11/12 must never be processed "+
			"behind the held record", got)
	}
	if discarded, _ = o.snapshot(); len(discarded) != 1 {
		t.Errorf("schema_fail count = %d, want 1: the hold must not re-count the same record", len(discarded))
	}
}

// TestKafkaSchemaDeadLetterRetryHoldsThenPublishesBeforeCommit covers the other
// half of the same defect: with on_invalid=dead_letter and a failing publisher,
// the old dispatch withheld the commit and then moved on, so offset 11's commit
// swept past offset 10 while the operator believed it was safely parked.
//
// The fix retries the SAME record, so the probe holds the third publish attempt
// (the first two fail) at the publisher boundary and asserts that nothing was
// committed or emitted while the record was not yet durably parked. Releasing
// it proves the ordering that makes the policy meaningful: publish, then
// commit, then the following offsets — never the other way around.
func TestKafkaSchemaDeadLetterRetryHoldsThenPublishesBeforeCommit(t *testing.T) {
	o := installRecordingObserver(t)
	publisher := newFakePublisher()
	publisher.err = errors.New("dead-letter broker unreachable")
	publisher.recoverAfter = 2
	order := &orderLog{}
	publishSucceeded := make(chan struct{})
	release := make(chan struct{})
	publisher.onPublish = func(msg Message) {
		order.add(fmt.Sprintf("publish:%d", msg.Offset))
		close(publishSucceeded)
		<-release
	}

	consumer := newOrderedCommitConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 10, Value: []byte(`{"user_id":"u2"}`)}, // invalid
		{Topic: "events", Partition: 0, Offset: 11, Value: []byte(`{"user_id":"u1","action":"click"}`)},
		{Topic: "events", Partition: 0, Offset: 12, Value: []byte(`{"user_id":"u1","action":"scroll"}`)},
	}, order)
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
		order.add(fmt.Sprintf("emit:%v", event.Data["offset"]))
		return "exec", nil
	})
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
	var releaseOnce sync.Once
	releasePublisher := func() { releaseOnce.Do(func() { close(release) }) }
	// Release before closing so a failed assertion cannot leave the worker
	// blocked inside the publisher while Close waits for it.
	defer func() {
		releasePublisher()
		_ = sub.Close(context.Background())
	}()

	select {
	case <-publishSucceeded:
	case <-time.After(2 * time.Second):
		t.Fatalf("publish attempts = %d, dead-letter observations = %v; the failed republish was "+
			"not retried for the same record", publisher.callCount(), mustDeadLettered(o))
	}

	// The first two attempts failed and the third is parked at the publisher
	// boundary: this is the window the old code used to step over the record.
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d while the record was not yet durably parked; "+
			"committing here loses it while reporting it as dead-lettered", got)
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d while offset 10 was unpublished; no later record may be processed", got)
	}
	if got := publisher.callCount(); got != 3 {
		t.Fatalf("publish calls = %d, want 3 (two failures plus the successful retry)", got)
	}
	_, deadLettered := o.snapshot()
	if want := []string{"events/error", "events/error"}; !reflect.DeepEqual(deadLettered, want) {
		t.Fatalf("dead-letter observations = %v, want %v (one per failed attempt)", deadLettered, want)
	}

	releasePublisher()
	if !consumer.waitForCommitCount(3, 2*time.Second) {
		t.Fatalf("committed offsets = %v, want 10, 11 and 12 after the publish succeeded", consumer.committedOffsets())
	}
	if got, want := consumer.committedOffsets(), []int64{10, 11, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed offsets = %v, want %v in order", got, want)
	}
	if got, want := order.snapshot(), []string{
		"publish:10", "commit:10",
		"emit:11", "commit:11",
		"emit:12", "commit:12",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event order = %v, want %v: the record must be durably parked BEFORE its offset "+
			"is committed, and only then may the partition move on", got, want)
	}
}

func mustDeadLettered(o *recordingObserver) []string {
	_, deadLettered := o.snapshot()
	return deadLettered
}
