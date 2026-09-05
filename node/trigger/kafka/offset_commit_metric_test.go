package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// commitObserver records OnOffsetCommit samples.
//
// It embeds noopObserver so the rest of the interface stays out of the way, and
// records the DURATION rather than only the labels: the duration is the whole
// reason this callback was added, and an observer that kept only topic/result
// would pass just as happily against an implementation that reported zero.
type commitObserver struct {
	noopObserver
	mu      sync.Mutex
	samples []commitSample
}

type commitSample struct {
	topic    string
	result   string
	messages int
	d        time.Duration
}

func (o *commitObserver) OnOffsetCommit(_ context.Context, topic, result string, messages int, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.samples = append(o.samples, commitSample{topic: topic, result: result, messages: messages, d: d})
}

func (o *commitObserver) snapshot() []commitSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]commitSample(nil), o.samples...)
}

// pausingCommitter is a Consumer that implements messageCommitter and spends a
// known amount of wall time inside CommitMessages.
type pausingCommitter struct {
	delay time.Duration
	err   error
	ch    chan Message
}

func newPausingCommitter(delay time.Duration, err error) *pausingCommitter {
	return &pausingCommitter{delay: delay, err: err, ch: make(chan Message)}
}

func (c *pausingCommitter) Messages() <-chan Message { return c.ch }
func (c *pausingCommitter) Close() error             { close(c.ch); return nil }
func (c *pausingCommitter) CommitMessages(_ context.Context, _ ...Message) error {
	time.Sleep(c.delay)
	return c.err
}

// plainConsumer deliberately does NOT implement messageCommitter, which is the
// shape of most fakes in this package and of any consumer that does not model
// offsets.
type plainConsumer struct{ ch chan Message }

func (c *plainConsumer) Messages() <-chan Message { return c.ch }
func (c *plainConsumer) Close() error             { close(c.ch); return nil }

func installCommitObserver(t *testing.T) *commitObserver {
	t.Helper()
	o := &commitObserver{}
	SetObserver(o)
	t.Cleanup(func() { SetObserver(nil) })
	return o
}

// TestCommitMessagesReportsTheBrokerRoundTrip pins that OnOffsetCommit carries
// a duration that actually brackets the CommitMessages call, plus the topic,
// the offset count and an "ok" result.
//
// The duration lower bound is the load-bearing assertion. Every other field can
// be produced by an implementation that reports before or after the round trip
// instead of around it, and such an implementation would report a duration near
// zero — which is precisely the reading that would have kept this hop looking
// free. A lower bound only: an upper bound would turn a busy machine into a
// test failure, and this suite already has timing assertions that behave that
// way under load.
func TestCommitMessagesReportsTheBrokerRoundTrip(t *testing.T) {
	o := installCommitObserver(t)
	const delay = 30 * time.Millisecond
	consumer := newPausingCommitter(delay, nil)

	msgs := []Message{
		{Topic: "orders", Partition: 3, Offset: 10},
		{Topic: "orders", Partition: 3, Offset: 11},
		{Topic: "orders", Partition: 3, Offset: 12},
	}
	if err := commitMessages(context.Background(), consumer, msgs...); err != nil {
		t.Fatalf("commitMessages: %v", err)
	}

	samples := o.snapshot()
	if len(samples) != 1 {
		t.Fatalf("OnOffsetCommit samples = %d, want 1", len(samples))
	}
	got := samples[0]
	if got.topic != "orders" {
		t.Errorf("topic = %q, want %q", got.topic, "orders")
	}
	if got.result != "ok" {
		t.Errorf("result = %q, want %q", got.result, "ok")
	}
	if got.messages != 3 {
		t.Errorf("messages = %d, want 3", got.messages)
	}
	if got.d < delay {
		t.Errorf("duration = %v, want >= %v; the sample does not bracket the commit call",
			got.d, delay)
	}
}

// TestCommitMessagesReportsErrorResult pins that a failed commit is reported
// under its own result label rather than merged into the success distribution.
//
// It also pins that the size is still reported: a failed commit's offsets are
// the offsets that will be retried, so dropping the sample would understate the
// work the pipeline is redoing.
func TestCommitMessagesReportsErrorResult(t *testing.T) {
	o := installCommitObserver(t)
	wantErr := errors.New("broker unavailable")
	consumer := newPausingCommitter(0, wantErr)

	err := commitMessages(context.Background(), consumer, Message{Topic: "orders", Offset: 1})
	if !errors.Is(err, wantErr) {
		t.Fatalf("commitMessages err = %v, want %v", err, wantErr)
	}

	samples := o.snapshot()
	if len(samples) != 1 {
		t.Fatalf("OnOffsetCommit samples = %d, want 1", len(samples))
	}
	if samples[0].result != "error" {
		t.Errorf("result = %q, want %q", samples[0].result, "error")
	}
	if samples[0].messages != 1 {
		t.Errorf("messages = %d, want 1", samples[0].messages)
	}
}

// TestCommitMessagesDoesNotReportNonCommits pins the two early returns as
// NON-observations.
//
// Neither is a commit, and reporting them would be worse than reporting
// nothing: both return instantly, so every such call would deposit a
// near-zero sample. The bulk of this package's tests run against consumers with
// no messageCommitter at all, so under a naive implementation the histogram's
// mean would be dominated by calls that never touched a broker — and the
// distribution the metric exists to show would be unreadable in exactly the
// environment where someone first looks at it.
func TestCommitMessagesDoesNotReportNonCommits(t *testing.T) {
	t.Run("empty message set", func(t *testing.T) {
		o := installCommitObserver(t)
		if err := commitMessages(context.Background(), newPausingCommitter(0, nil)); err != nil {
			t.Fatalf("commitMessages: %v", err)
		}
		if n := len(o.snapshot()); n != 0 {
			t.Errorf("OnOffsetCommit samples = %d, want 0 for an empty commit set", n)
		}
	})

	t.Run("consumer without messageCommitter", func(t *testing.T) {
		o := installCommitObserver(t)
		consumer := &plainConsumer{ch: make(chan Message)}
		if err := commitMessages(context.Background(), consumer, Message{Topic: "orders"}); err != nil {
			t.Fatalf("commitMessages: %v", err)
		}
		if n := len(o.snapshot()); n != 0 {
			t.Errorf("OnOffsetCommit samples = %d, want 0 when the consumer cannot commit", n)
		}
	})
}
