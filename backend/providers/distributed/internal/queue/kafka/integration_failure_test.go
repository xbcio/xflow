package kafka

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// A handler that returns types.ErrPermanent is not retried: the message goes to
// the dead-letter topic, tagged with where it came from and why, and the source
// offset is committed so it is not delivered again.
func TestKafkaIntegrationPermanentErrorGoesToDLQ(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)
	c := &collector{fail: func(delivery, int) error { return &permanentErr{} }}
	startConsumer(t, tr, c.handle)
	if err := tr.Enqueue(t.Context(), itTask("doomed", false)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	dlq := e.readTopic(e.names.DLQ(), 3, 1, 30*time.Second)
	if len(dlq) != 1 {
		t.Fatalf("dead-letter topic holds %d messages, want 1", len(dlq))
	}
	if src, _ := headerValue(dlq[0], hdrSource); src != e.names.Default() {
		t.Errorf("%s = %q, want %q", hdrSource, src, e.names.Default())
	}
	if msg, _ := headerValue(dlq[0], hdrError); !strings.Contains(msg, "permanent") {
		t.Errorf("%s = %q, want it to carry the handler's error", hdrError, msg)
	}
	waitFor(t, "the source offset to be committed", 30*time.Second, func() bool {
		return e.committed(groupDefault, e.names.Default(), 3) == 1
	})
	if n := c.ids()["doomed"]; n != 1 {
		t.Errorf("handler ran %d times, want once: a permanent error must not be retried", n)
	}
	if retries := e.readTopic(e.names.Delay(), 3, -1, 1500*time.Millisecond); len(retries) != 0 {
		t.Errorf("delay topic holds %d messages, want none", len(retries))
	}
}

// permanentErr wraps types.ErrPermanent the way a handler would.
type permanentErr struct{}

func (*permanentErr) Error() string { return "permanent: invalid input" }
func (*permanentErr) Unwrap() error { return types.ErrPermanent }

// A message that is not a task at all cannot be retried into success, so it is
// dead-lettered as is.
func TestKafkaIntegrationUndecodableGoesToDLQ(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)
	c := &collector{}
	startConsumer(t, tr, c.handle)

	garbage := kafkago.Message{Topic: e.names.Default(), Key: []byte("k"), Value: []byte("this is not json")}
	if err := tr.publish(t.Context(), garbage); err != nil {
		t.Fatalf("publish: %v", err)
	}
	dlq := e.readTopic(e.names.DLQ(), 3, 1, 30*time.Second)
	if len(dlq) != 1 {
		t.Fatalf("dead-letter topic holds %d messages, want 1", len(dlq))
	}
	if string(dlq[0].Value) != "this is not json" {
		t.Errorf("dead-letter value = %q, want the original bytes", dlq[0].Value)
	}
	if msg, _ := headerValue(dlq[0], hdrError); !strings.Contains(msg, "decode") {
		t.Errorf("%s = %q, want it to say the task could not be decoded", hdrError, msg)
	}
	waitFor(t, "the garbage offset to be committed", 30*time.Second, func() bool {
		return e.committed(groupDefault, e.names.Default(), 3) == 1
	})
	if n := len(c.all()); n != 0 {
		t.Errorf("handler ran %d times for an undecodable message", n)
	}
}

// topicExists asks the broker, without creating anything, whether topic exists.
func (e *itEnv) topicExists(topic string) bool {
	e.t.Helper()
	tr := &kafkago.Transport{}
	defer tr.CloseIdleConnections()
	client := &kafkago.Client{Addr: kafkago.TCP(e.brokers...), Timeout: 10 * time.Second, Transport: tr}
	res, err := client.Metadata(e.t.Context(), &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		e.t.Fatalf("metadata %s: %v", topic, err)
	}
	for _, tp := range res.Topics {
		if tp.Name == topic {
			return tp.Error == nil
		}
	}
	return false
}

// What a produce to a deployment whose topics were never created really
// returns, and how the transport classifies it. The broker auto-creates topics
// by default, so the test also checks that the producer did not create one.
func TestKafkaIntegrationEnqueueMissingTopic(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(func(c *Config) { c.CreateTopics, c.Partitions, c.ReplicationFactor = false, 0, 0 })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := tr.Enqueue(ctx, itTask("nowhere", false))
	t.Logf("Enqueue to a missing topic: %T: %v", err, err)
	if err == nil {
		t.Fatal("Enqueue to a missing topic succeeded")
	}
	if got := classify(t.Context(), err); got != kindPermanent {
		t.Errorf("classify = %v, want kindPermanent", got)
	}
	tryErr := tr.TryEnqueue(ctx, itTask("nowhere", false))
	t.Logf("TryEnqueue to a missing topic: %T: %v", tryErr, tryErr)
	if tryErr == nil || errors.Is(tryErr, engine.ErrQueueFull) {
		t.Errorf("TryEnqueue = %v, want a permanent error that is not ErrQueueFull", tryErr)
	}
	if e.topicExists(e.names.Default()) {
		t.Error("the producer made the broker auto-create the default topic")
	}
}

// A task bigger than the broker's message.max.bytes (about 1 MiB) passes the
// writer, whose own limit is 8 MiB, and is refused by the broker.
func TestKafkaIntegrationEnqueueTooLarge(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)

	big := itTask("big", false)
	big.NodeName = strings.Repeat("x", 3<<20)
	err := tr.Enqueue(t.Context(), big)
	t.Logf("Enqueue of a 3 MiB task: %T: %v", err, err)
	if err == nil {
		t.Fatal("Enqueue of a 3 MiB task succeeded; the broker's message.max.bytes is larger than assumed")
	}
	if !isMessageTooLarge(err) {
		t.Errorf("isMessageTooLarge = false for %T: %v", err, err)
	}
	if got := classify(t.Context(), err); got != kindPermanent {
		t.Errorf("classify = %v, want kindPermanent", got)
	}
	tryErr := tr.TryEnqueue(t.Context(), big)
	t.Logf("TryEnqueue of a 3 MiB task: %T: %v", tryErr, tryErr)
	if tryErr == nil || errors.Is(tryErr, engine.ErrQueueFull) {
		t.Errorf("TryEnqueue = %v, want a permanent error that is not ErrQueueFull", tryErr)
	}
}

// A handler that overruns HandlerTimeout has its context cancelled and the task
// is retried, not dead-lettered. The first delivery blocks until its context
// ends; the second succeeds.
func TestKafkaIntegrationHandlerTimeoutIsRetried(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(func(c *Config) { c.HandlerTimeout = 500 * time.Millisecond })
	e.ensure(tr)

	var timedOut atomic.Bool
	c := &collector{}
	startConsumer(t, tr, func(ctx context.Context, task *engine.Task) error {
		_ = c.handle(ctx, task)
		if c.ids()[task.ExecutionID] == 1 {
			<-ctx.Done()
			timedOut.Store(true)
			return ctx.Err()
		}
		return nil
	})
	if err := tr.Enqueue(t.Context(), itTask("slow", false)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "the retry after the timeout", 60*time.Second, func() bool { return c.ids()["slow"] >= 2 })
	if !timedOut.Load() {
		t.Error("the first delivery's context was not cancelled by HandlerTimeout")
	}
	if dlq := e.readTopic(e.names.DLQ(), 3, -1, 2*time.Second); len(dlq) != 0 {
		t.Errorf("dead-letter topic holds %d messages, want none: a timeout is retryable", len(dlq))
	}
	// The relay puts the retry back on the default topic as a second message, so
	// once both deliveries are done the group has committed past both.
	waitFor(t, "both offsets to be committed", 30*time.Second, func() bool {
		got := e.committed(groupDefault, e.names.Default(), 3)
		t.Logf("committed on the default topic: %d", got)
		return got == 2
	})
}
