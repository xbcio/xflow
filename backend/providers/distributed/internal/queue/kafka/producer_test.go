package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// fakeWriter records what the transport writes. A non-nil err is returned from
// every WriteMessages call; the messages are still recorded so a test can see
// what was attempted.
type fakeWriter struct {
	mu       sync.Mutex
	msgs     []kafkago.Message
	err      error
	closes   int
	closeErr error
}

func (w *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	if w.err != nil {
		return w.err
	}
	return ctx.Err()
}

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closes++
	return w.closeErr
}

func (w *fakeWriter) written() []kafkago.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kafkago.Message(nil), w.msgs...)
}

func (w *fakeWriter) closeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closes
}

type enqueueCall struct {
	op  string
	err error
}

type recordingObserver struct {
	mu    sync.Mutex
	calls []enqueueCall
}

func (o *recordingObserver) OnEnqueue(op string, _ time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, enqueueCall{op: op, err: err})
}

func (o *recordingObserver) recorded() []enqueueCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]enqueueCall(nil), o.calls...)
}

type warnRecord struct {
	msg  string
	args []any
}

// recordingLogger implements engine.Logger and keeps only the Warn calls.
type recordingLogger struct {
	mu    sync.Mutex
	warns []warnRecord
}

func (l *recordingLogger) Debug(string, ...any)  {}
func (l *recordingLogger) Debugf(string, ...any) {}
func (l *recordingLogger) Info(string, ...any)   {}
func (l *recordingLogger) Infof(string, ...any)  {}
func (l *recordingLogger) Warnf(string, ...any)  {}
func (l *recordingLogger) Error(string, ...any)  {}
func (l *recordingLogger) Errorf(string, ...any) {}
func (l *recordingLogger) Panic(string, ...any)  {}
func (l *recordingLogger) Panicf(string, ...any) {}
func (l *recordingLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, warnRecord{msg: msg, args: args})
}

func (l *recordingLogger) warned() []warnRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]warnRecord(nil), l.warns...)
}

var _ engine.Logger = (*recordingLogger)(nil)

// newTestTransport builds a transport over a fake writer, with its observer.
func newTestTransport(t *testing.T, suffix string) (*Transport, *fakeWriter, *recordingObserver) {
	t.Helper()
	w := &fakeWriter{}
	obs := &recordingObserver{}
	tr, err := New(Config{Brokers: []string{"broker:9092"}}, suffix, withWriter(w), WithObserver(obs))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr, w, obs
}

func batchTask() *engine.Task {
	return &engine.Task{
		ExecutionID: "exec-9",
		NodeName:    "map/_batch/3",
		Type:        engine.TaskTypeNodeBatch,
		UnitIdx:     engine.UnitIdxUnknown,
	}
}

func TestEnqueue_RoutesByTaskType(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	names, _ := newTopicNames("")

	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := tr.Enqueue(context.Background(), batchTask()); err != nil {
		t.Fatalf("Enqueue batch: %v", err)
	}

	got := w.written()
	if len(got) != 2 {
		t.Fatalf("wrote %d messages, want 2", len(got))
	}
	if got[0].Topic != names.Default() {
		t.Errorf("ordinary task topic = %q, want %q", got[0].Topic, names.Default())
	}
	if got[1].Topic != names.Batch() {
		t.Errorf("batch task topic = %q, want %q", got[1].Topic, names.Batch())
	}
	if len(got[0].Headers) != 0 || len(got[1].Headers) != 0 {
		t.Errorf("immediate enqueue must not carry relay headers: %+v / %+v", got[0].Headers, got[1].Headers)
	}
}

func TestEnqueue_UsesDeploymentSuffixTopics(t *testing.T) {
	tr, w, _ := newTestTransport(t, "tenant-a")
	names, err := newTopicNames("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := w.written()[0].Topic; got != names.Default() {
		t.Errorf("topic = %q, want %q", got, names.Default())
	}
}

func TestEnqueue_Key(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatal(err)
	}
	if err := tr.Enqueue(context.Background(), batchTask()); err != nil {
		t.Fatal(err)
	}
	got := w.written()
	if string(got[0].Key) != "exec-1" {
		t.Errorf("ordinary key = %q, want exec-1", got[0].Key)
	}
	if string(got[1].Key) != "exec-9/map/_batch/3" {
		t.Errorf("batch key = %q, want exec-9/map/_batch/3", got[1].Key)
	}
}

func TestEnqueue_NamespaceTravelsInBody(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")
	if err := tr.Enqueue(ctx, sampleTask()); err != nil {
		t.Fatal(err)
	}
	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatal(err)
	}
	got := w.written()
	if _, ns, err := decodeTask(got[0]); err != nil || ns != "tenant-a" {
		t.Errorf("decoded namespace = %q, %v; want tenant-a", ns, err)
	}
	if _, ns, err := decodeTask(got[1]); err != nil || ns != namespace.Default {
		t.Errorf("decoded namespace = %q, %v; want the default", ns, err)
	}
}

func TestEnqueue_BodyKeepsSchedulerMetadata(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatal(err)
	}
	task, _, err := decodeTask(w.written()[0])
	if err != nil {
		t.Fatal(err)
	}
	if task.AutoDepth != 2 || task.ActivationID != 7 || task.UnitIdx != 5 || task.Generation != 4 {
		t.Errorf("metadata lost: %+v", task)
	}
}

func TestEnqueue_ReturnsWriterErrorUnmapped(t *testing.T) {
	errs := []error{
		kafkago.LeaderNotAvailable,
		kafkago.MessageSizeTooLarge,
		fmt.Errorf("produce: %w", kafkago.NotEnoughReplicas),
		errors.New("connection reset"),
	}
	for _, want := range errs {
		tr, w, _ := newTestTransport(t, "")
		w.err = want
		got := tr.Enqueue(context.Background(), sampleTask())
		if got != want {
			t.Errorf("Enqueue returned %v, want exactly %v", got, want)
		}
		if errors.Is(got, engine.ErrQueueFull) {
			t.Errorf("Enqueue mapped %v to ErrQueueFull; direct enqueues have no outbox to retry from", want)
		}
	}
}

func TestEnqueue_NilTask(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	err := tr.Enqueue(context.Background(), nil)
	if err == nil {
		t.Fatal("Enqueue(nil) returned no error")
	}
	if n := len(w.written()); n != 0 {
		t.Errorf("wrote %d messages for a nil task", n)
	}
	if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue" || calls[0].err == nil {
		t.Errorf("observer calls = %+v, want one failed enqueue", calls)
	}
}

func TestEnqueue_CallerContextErrorReturnedAsIs(t *testing.T) {
	tr, _, _ := newTestTransport(t, "")
	err := tr.Enqueue(canceledCtx(), sampleTask())
	if err != context.Canceled {
		t.Errorf("Enqueue under a canceled context = %v, want context.Canceled", err)
	}
	if errors.Is(err, engine.ErrQueueFull) {
		t.Error("a canceled caller must not read as backpressure")
	}
}

func TestEnqueue_ObserverOps(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	if err := tr.Enqueue(context.Background(), sampleTask()); err != nil {
		t.Fatal(err)
	}
	w.err = kafkago.LeaderNotAvailable
	if err := tr.Enqueue(context.Background(), sampleTask()); err == nil {
		t.Fatal("expected the injected error")
	}
	calls := obs.recorded()
	if len(calls) != 2 {
		t.Fatalf("observer calls = %+v, want 2", calls)
	}
	if calls[0].op != "enqueue" || calls[0].err != nil {
		t.Errorf("first call = %+v, want a successful enqueue", calls[0])
	}
	if calls[1].op != "enqueue" || calls[1].err != kafkago.LeaderNotAvailable {
		t.Errorf("second call = %+v, want enqueue with the writer error", calls[1])
	}
}

func TestTryEnqueue_Success(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	if err := tr.TryEnqueue(context.Background(), batchTask()); err != nil {
		t.Fatalf("TryEnqueue: %v", err)
	}
	names, _ := newTopicNames("")
	got := w.written()
	if len(got) != 1 || got[0].Topic != names.Batch() || string(got[0].Key) != "exec-9/map/_batch/3" {
		t.Errorf("written = %+v", got)
	}
	if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue" || calls[0].err != nil {
		t.Errorf("observer calls = %+v", calls)
	}
}

func TestTryEnqueue_RetryableBecomesQueueFull(t *testing.T) {
	retryable := []error{
		kafkago.LeaderNotAvailable,
		kafkago.NotEnoughReplicas,
		errors.New("connection reset by peer"),
		kafkago.WriteErrors{kafkago.LeaderNotAvailable, nil},
	}
	for _, cause := range retryable {
		tr, w, obs := newTestTransport(t, "")
		w.err = cause
		err := tr.TryEnqueue(context.Background(), sampleTask())
		if !errors.Is(err, engine.ErrQueueFull) {
			t.Errorf("TryEnqueue(%v) = %v, want ErrQueueFull in the chain", cause, err)
			continue
		}
		if kerr, ok := cause.(kafkago.Error); ok && !errors.Is(err, kerr) {
			t.Errorf("TryEnqueue(%v) lost the cause: %v", cause, err)
		}
		if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue" || !errors.Is(calls[0].err, engine.ErrQueueFull) {
			t.Errorf("observer calls = %+v", calls)
		}
	}
}

func TestTryEnqueue_KeepsCauseReachable(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	w.err = kafkago.LeaderNotAvailable
	err := tr.TryEnqueue(context.Background(), sampleTask())
	if !errors.Is(err, engine.ErrQueueFull) || !errors.Is(err, kafkago.LeaderNotAvailable) {
		t.Fatalf("TryEnqueue = %v, want both ErrQueueFull and LeaderNotAvailable to match", err)
	}
}

func TestTryEnqueue_PermanentReturnedAsIs(t *testing.T) {
	for _, want := range []error{kafkago.MessageSizeTooLarge, kafkago.TopicAuthorizationFailed, kafkago.UnknownTopicOrPartition} {
		tr, w, _ := newTestTransport(t, "")
		w.err = want
		got := tr.TryEnqueue(context.Background(), sampleTask())
		if got != want {
			t.Errorf("TryEnqueue returned %v, want exactly %v", got, want)
		}
		if errors.Is(got, engine.ErrQueueFull) {
			t.Errorf("a permanent failure (%v) must not read as backpressure", want)
		}
	}
}

func TestTryEnqueue_CallerContextReturnedAsIs(t *testing.T) {
	tr, _, _ := newTestTransport(t, "")
	err := tr.TryEnqueue(canceledCtx(), sampleTask())
	if err != context.Canceled {
		t.Errorf("TryEnqueue under a canceled context = %v, want context.Canceled", err)
	}
	err = tr.TryEnqueue(expiredCtx(), sampleTask())
	if err != context.DeadlineExceeded {
		t.Errorf("TryEnqueue past its deadline = %v, want context.DeadlineExceeded", err)
	}
}

func TestTryEnqueue_EncodeErrorIsNotBackpressure(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	err := tr.TryEnqueue(context.Background(), nil)
	if err == nil || errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue(nil) = %v, want a plain error that is not ErrQueueFull", err)
	}
	if n := len(w.written()); n != 0 {
		t.Errorf("wrote %d messages for a nil task", n)
	}
}

func TestEnqueueDelayed_RoutesToDelayTopic(t *testing.T) {
	tr, w, obs := newTestTransport(t, "tenant-a")
	names, _ := newTopicNames("tenant-a")

	before := time.Now()
	if err := tr.EnqueueDelayed(context.Background(), sampleTask(), 30*time.Second); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	if err := tr.EnqueueDelayed(context.Background(), batchTask(), time.Minute); err != nil {
		t.Fatalf("EnqueueDelayed batch: %v", err)
	}
	after := time.Now()

	got := w.written()
	if len(got) != 2 {
		t.Fatalf("wrote %d messages, want 2", len(got))
	}
	cases := []struct {
		msg    kafkago.Message
		target string
		key    string
		delay  time.Duration
	}{
		{got[0], names.Default(), "exec-1", 30 * time.Second},
		{got[1], names.Batch(), "exec-9/map/_batch/3", time.Minute},
	}
	for i, c := range cases {
		if c.msg.Topic != names.Delay() {
			t.Errorf("message %d topic = %q, want the delay topic %q", i, c.msg.Topic, names.Delay())
		}
		if string(c.msg.Key) != c.key {
			t.Errorf("message %d key = %q, want %q", i, c.msg.Key, c.key)
		}
		if v, ok := headerValue(c.msg, hdrTarget); !ok || v != c.target {
			t.Errorf("message %d %s = %q, %v; want %q", i, hdrTarget, v, ok, c.target)
		}
		if v, ok := headerValue(c.msg, hdrAttempt); !ok || v != "0" {
			t.Errorf("message %d %s = %q, %v; want 0", i, hdrAttempt, v, ok)
		}
		due, ok := notBeforeOf(c.msg)
		if !ok {
			t.Errorf("message %d has no usable %s header", i, hdrNotBefore)
			continue
		}
		lo := before.Add(c.delay).UnixMilli()
		hi := after.Add(c.delay).UnixMilli()
		if due.UnixMilli() < lo || due.UnixMilli() > hi {
			t.Errorf("message %d due at %d ms, want within [%d, %d]", i, due.UnixMilli(), lo, hi)
		}
		if _, ns, err := decodeTask(c.msg); err != nil || ns != namespace.Default {
			t.Errorf("message %d body = namespace %q, %v", i, ns, err)
		}
	}
	if calls := obs.recorded(); len(calls) != 2 || calls[0].op != "enqueue_delayed" || calls[1].op != "enqueue_delayed" {
		t.Errorf("observer calls = %+v, want two enqueue_delayed", calls)
	}
}

func TestEnqueueDelayed_NamespaceTravelsInBody(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	ctx := namespace.WithNamespace(context.Background(), "tenant-b")
	if err := tr.EnqueueDelayed(ctx, sampleTask(), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ns, err := decodeTask(w.written()[0]); err != nil || ns != "tenant-b" {
		t.Errorf("decoded namespace = %q, %v; want tenant-b", ns, err)
	}
}

func TestEnqueueDelayed_NonPositiveDelayIsTryEnqueue(t *testing.T) {
	for _, delay := range []time.Duration{0, -time.Second} {
		tr, w, obs := newTestTransport(t, "")
		names, _ := newTopicNames("")
		if err := tr.EnqueueDelayed(context.Background(), batchTask(), delay); err != nil {
			t.Fatalf("EnqueueDelayed(%v): %v", delay, err)
		}
		got := w.written()
		if len(got) != 1 || got[0].Topic != names.Batch() {
			t.Errorf("delay %v: written = %+v, want one message on the batch topic", delay, got)
		}
		if len(got) == 1 && len(got[0].Headers) != 0 {
			t.Errorf("delay %v: headers = %+v, want none", delay, got[0].Headers)
		}
		if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue" {
			t.Errorf("delay %v: observer calls = %+v, want one enqueue", delay, calls)
		}

		// Like TryEnqueue, a non-positive delay reports a retryable failure as
		// backpressure, still under the "enqueue" operation.
		w.err = kafkago.LeaderNotAvailable
		err := tr.EnqueueDelayed(context.Background(), sampleTask(), delay)
		if !errors.Is(err, engine.ErrQueueFull) || !errors.Is(err, kafkago.LeaderNotAvailable) {
			t.Errorf("delay %v: error = %v, want ErrQueueFull wrapping LeaderNotAvailable", delay, err)
		}
		if calls := obs.recorded(); len(calls) != 2 || calls[1].op != "enqueue" {
			t.Errorf("delay %v: observer calls = %+v, want a second enqueue", delay, calls)
		}
	}
}

func TestEnqueueDelayed_RetryableBecomesQueueFull(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	w.err = kafkago.LeaderNotAvailable
	err := tr.EnqueueDelayed(context.Background(), sampleTask(), time.Minute)
	if !errors.Is(err, engine.ErrQueueFull) || !errors.Is(err, kafkago.LeaderNotAvailable) {
		t.Fatalf("EnqueueDelayed = %v, want ErrQueueFull wrapping LeaderNotAvailable", err)
	}
	if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue_delayed" || !errors.Is(calls[0].err, engine.ErrQueueFull) {
		t.Errorf("observer calls = %+v", calls)
	}
}

func TestEnqueueDelayed_PermanentAndCallerReturnedAsIs(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	w.err = kafkago.MessageSizeTooLarge
	got := tr.EnqueueDelayed(context.Background(), sampleTask(), time.Minute)
	if got != kafkago.MessageSizeTooLarge || errors.Is(got, engine.ErrQueueFull) {
		t.Errorf("permanent error = %v, want the raw MessageSizeTooLarge", got)
	}

	tr2, _, _ := newTestTransport(t, "")
	got = tr2.EnqueueDelayed(canceledCtx(), sampleTask(), time.Minute)
	if got != context.Canceled {
		t.Errorf("canceled caller = %v, want context.Canceled", got)
	}
}

// Mutation note: dropping the delay > limit guard in EnqueueDelayed turns the
// over-limit rows red; changing > to >= turns the exactly-at-limit row red;
// wrapping ErrDelayTooLong in engine.ErrQueueFull turns the sentinel check red.
func TestEnqueueDelayed_MaxEnqueueDelay(t *testing.T) {
	const limit = 10 * time.Minute
	newTr := func(t *testing.T) (*Transport, *fakeWriter, *recordingObserver) {
		t.Helper()
		w := &fakeWriter{}
		obs := &recordingObserver{}
		tr, err := New(Config{Brokers: []string{"broker:9092"}, MaxEnqueueDelay: limit}, "", withWriter(w), WithObserver(obs))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return tr, w, obs
	}

	t.Run("exactly at the limit is accepted", func(t *testing.T) {
		tr, w, _ := newTr(t)
		if err := tr.EnqueueDelayed(context.Background(), sampleTask(), limit); err != nil {
			t.Fatalf("EnqueueDelayed(limit) = %v, want nil", err)
		}
		names, _ := newTopicNames("")
		if got := w.written(); len(got) != 1 || got[0].Topic != names.Delay() {
			t.Errorf("written = %+v, want one message on the delay topic", got)
		}
	})

	for _, over := range []time.Duration{limit + time.Nanosecond, limit + time.Second, 24 * time.Hour} {
		t.Run("over the limit "+over.String(), func(t *testing.T) {
			tr, w, obs := newTr(t)
			err := tr.EnqueueDelayed(context.Background(), sampleTask(), over)
			if !errors.Is(err, ErrDelayTooLong) {
				t.Fatalf("EnqueueDelayed(%v) = %v, want ErrDelayTooLong", over, err)
			}
			if errors.Is(err, engine.ErrQueueFull) {
				t.Errorf("EnqueueDelayed(%v) = %v: a too-long delay is not backpressure and must not match ErrQueueFull", over, err)
			}
			for _, want := range []string{"MaxEnqueueDelay", limit.String(), over.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
			if n := len(w.written()); n != 0 {
				t.Errorf("wrote %d messages for a rejected delay, want none", n)
			}
			calls := obs.recorded()
			if len(calls) != 1 || calls[0].op != "enqueue_delayed" || !errors.Is(calls[0].err, ErrDelayTooLong) {
				t.Errorf("observer calls = %+v, want one failed enqueue_delayed carrying ErrDelayTooLong", calls)
			}
		})
	}

	// A non-positive delay never reaches the guard: it is a plain TryEnqueue,
	// even when the configured limit is tiny relative to nothing.
	for _, delay := range []time.Duration{0, -time.Hour} {
		t.Run("non-positive "+delay.String()+" is TryEnqueue", func(t *testing.T) {
			tr, w, obs := newTr(t)
			if err := tr.EnqueueDelayed(context.Background(), sampleTask(), delay); err != nil {
				t.Fatalf("EnqueueDelayed(%v) = %v, want nil", delay, err)
			}
			if got := w.written(); len(got) != 1 || len(got[0].Headers) != 0 {
				t.Errorf("written = %+v, want one undelayed message", got)
			}
			if calls := obs.recorded(); len(calls) != 1 || calls[0].op != "enqueue" {
				t.Errorf("observer calls = %+v, want one enqueue", calls)
			}
		})
	}
}

// Zero MaxEnqueueDelay selects the documented 15 minute default: the edge of
// the default is accepted, one nanosecond past it is not.
func TestEnqueueDelayed_DefaultMaxEnqueueDelay(t *testing.T) {
	if defaultMaxEnqueueDelay != 15*time.Minute {
		t.Fatalf("defaultMaxEnqueueDelay = %v; the Config doc says 15m", defaultMaxEnqueueDelay)
	}
	tr, _, _ := newTestTransport(t, "")
	if err := tr.EnqueueDelayed(context.Background(), sampleTask(), defaultMaxEnqueueDelay); err != nil {
		t.Fatalf("EnqueueDelayed at the default limit = %v, want nil", err)
	}
	err := tr.EnqueueDelayed(context.Background(), sampleTask(), defaultMaxEnqueueDelay+time.Nanosecond)
	if !errors.Is(err, ErrDelayTooLong) {
		t.Fatalf("EnqueueDelayed past the default limit = %v, want ErrDelayTooLong", err)
	}
	// The engine's longest delay (5m cap + 20% jitter) is far inside the default.
	if err := tr.EnqueueDelayed(context.Background(), sampleTask(), 6*time.Minute); err != nil {
		t.Errorf("EnqueueDelayed(6m) = %v: the engine's own retry delays must always be accepted", err)
	}
}

func TestEnqueueDelayed_NilTask(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	err := tr.EnqueueDelayed(context.Background(), nil, time.Minute)
	if err == nil || errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("EnqueueDelayed(nil) = %v, want a plain error", err)
	}
	if n := len(w.written()); n != 0 {
		t.Errorf("wrote %d messages for a nil task", n)
	}
}

func TestPublish_PassesMessagesAndErrorThrough(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	msgs := []kafkago.Message{{Topic: "a", Value: []byte("1")}, {Topic: "b", Value: []byte("2")}}
	if err := tr.publish(context.Background(), msgs...); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := w.written()
	if len(got) != 2 || got[0].Topic != "a" || got[1].Topic != "b" {
		t.Errorf("written = %+v", got)
	}

	w.err = kafkago.LeaderNotAvailable
	if err := tr.publish(context.Background(), msgs[0]); err != kafkago.LeaderNotAvailable {
		t.Errorf("publish error = %v, want the raw writer error", err)
	}
	if calls := obs.recorded(); len(calls) != 0 {
		t.Errorf("publish is the consumer-side path and must not report enqueue metrics: %+v", calls)
	}
}
