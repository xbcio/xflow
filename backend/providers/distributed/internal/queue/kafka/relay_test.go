package kafka

import (
	"context"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// waiter stands in for Transport.waitUntil. It records the time each call was
// asked to wait for on calls. With a nil gate it returns at once; otherwise it
// blocks until the test sends a verdict on gate (true: the message is due) or
// the context ends.
type waiter struct {
	calls chan time.Time
	gate  chan bool
}

func newWaiter(gated bool) *waiter {
	w := &waiter{calls: make(chan time.Time, 64)}
	if gated {
		w.gate = make(chan bool)
	}
	return w
}

func (w *waiter) wait(ctx context.Context, until time.Time) bool {
	w.calls <- until
	if w.gate == nil {
		return ctx.Err() == nil
	}
	select {
	case ok := <-w.gate:
		return ok
	case <-ctx.Done():
		return false
	}
}

// relayHarness is a harness whose transport waits through a waiter.
func relayHarness(t *testing.T, failFirst int, gated bool) (*harness, *waiter) {
	t.Helper()
	h := newHarness(t, failFirst, nil)
	w := newWaiter(gated)
	h.tr.waitUntil = w.wait
	return h, w
}

// delayMsg builds a message as it sits on the delay topic.
func (h *harness) delayMsg(partition int, offset int64, target string, due time.Time, extra ...kafkago.Header) kafkago.Message {
	hdrs := []kafkago.Header{
		hdr(hdrTarget, target),
		hdr(hdrNotBefore, strconv.FormatInt(due.UnixMilli(), 10)),
		hdr(hdrAttempt, "0"),
	}
	// An extra header replaces a default of the same name, as setHeader does,
	// so a test can set the attempt without producing a duplicate.
	for _, e := range extra {
		hdrs = setHeader(hdrs, e.Key, string(e.Value))
	}
	return taskMsg(h.t, h.names.Delay(), partition, offset, &engine.Task{ExecutionID: "e"}, "ns", hdrs...)
}

func startRelayOnly(h *harness) func() {
	return h.start(queue.ConsumerConfig{Concurrency: 1}, nilHandler)
}

func TestRelayForwardsDueMessageWithHeadersIntact(t *testing.T) {
	h, w := relayHarness(t, 0, false)
	startRelayOnly(h)
	due := time.Now().Add(-time.Minute)
	in := h.delayMsg(2, 11, h.names.Batch(), due, hdr(hdrAttempt, "3"), hdr("keep", "me"))
	h.feed(h.names.Delay(), in)

	out := h.awaitWrite()
	if out.Topic != h.names.Batch() {
		t.Fatalf("forwarded to %q, want the target %q", out.Topic, h.names.Batch())
	}
	if string(out.Key) != string(in.Key) || string(out.Value) != string(in.Value) {
		t.Errorf("key/value changed: %q/%q vs %q/%q", out.Key, out.Value, in.Key, in.Value)
	}
	if got := hdrOf(t, out, hdrAttempt); got != "3" {
		t.Errorf("attempt = %q, want 3 to survive the relay", got)
	}
	if !reflect.DeepEqual(out.Headers, in.Headers) {
		t.Errorf("headers = %+v, want all of %+v", out.Headers, in.Headers)
	}
	if got := recv(t, w.calls, "the wait"); got.UnixMilli() != due.UnixMilli() {
		t.Errorf("asked to wait until %v, want the not-before time %v", got, due)
	}
	h.reader(h.names.Delay()).waitCommitted(t, 2, 11)
}

func TestRelayWaitsUntilDueBeforeForwarding(t *testing.T) {
	h, w := relayHarness(t, 0, true)
	startRelayOnly(h)
	due := time.Now().Add(time.Hour)
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), due))

	if got := recv(t, w.calls, "the wait"); got.UnixMilli() != due.UnixMilli() {
		t.Fatalf("waiting until %v, want %v", got, due)
	}
	// The relay is parked in wait: nothing may be written or committed yet.
	if n := len(h.w.written()); n != 0 {
		t.Fatalf("%d messages forwarded before the message was due", n)
	}
	if got := h.reader(h.names.Delay()).commitCalls(); len(got) != 0 {
		t.Fatalf("committed %v before the message was due", got)
	}
	w.gate <- true
	if m := h.awaitWrite(); m.Topic != h.names.Default() {
		t.Errorf("forwarded to %q, want %q", m.Topic, h.names.Default())
	}
	h.reader(h.names.Delay()).waitCommitted(t, 0, 0)
}

func TestRelayForwardsMessageWithoutNotBeforeImmediately(t *testing.T) {
	h, w := relayHarness(t, 0, true) // a gated waiter would hang if it were asked
	startRelayOnly(h)
	msg := taskMsg(t, h.names.Delay(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrTarget, h.names.Default()))
	h.feed(h.names.Delay(), msg)
	if m := h.awaitWrite(); m.Topic != h.names.Default() {
		t.Errorf("forwarded to %q, want %q", m.Topic, h.names.Default())
	}
	select {
	case until := <-w.calls:
		t.Errorf("waited until %v for a message with no not-before header", until)
	default:
	}
}

// The forwarded copy must be written before the offset is committed.
func TestRelayCommitsOnlyAfterForwarding(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	h.onReader = func(topic string, fr *fakeReader) {
		if topic == h.names.Delay() {
			fr.commitHook = func() {
				if n := len(h.w.written()); n == 0 {
					t.Error("offset committed before the forwarded copy was written")
				}
			}
		}
	}
	startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 6, h.names.Default(), time.Now().Add(-time.Second)))
	h.reader(h.names.Delay()).waitCommitted(t, 0, 6)
}

func TestRelayDeadLettersMessageWithoutValidTarget(t *testing.T) {
	cases := map[string][]kafkago.Header{
		"missing":      nil,
		"unknown":      {hdr(hdrTarget, "some.other.topic")},
		"delay itself": {hdr(hdrTarget, "xflow.task.delay")},
		"dlq":          {hdr(hdrTarget, "xflow.task.dlq")},
		"empty":        {hdr(hdrTarget, "")},
	}
	for name, hdrs := range cases {
		h, w := relayHarness(t, 0, true)
		startRelayOnly(h)
		hdrs = append(hdrs, hdr(hdrNotBefore, strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)))
		msg := taskMsg(t, h.names.Delay(), 0, 4, &engine.Task{ExecutionID: "e"}, "ns", hdrs...)
		h.feed(h.names.Delay(), msg)

		m := h.awaitWrite()
		if m.Topic != h.names.DLQ() {
			t.Fatalf("%s: written to %q, want the DLQ", name, m.Topic)
		}
		if got := hdrOf(t, m, hdrSource); got != h.names.Delay() {
			t.Errorf("%s: source = %q, want the delay topic", name, got)
		}
		if got := hdrOf(t, m, hdrError); got != "delay message has no valid target" {
			t.Errorf("%s: error = %q", name, got)
		}
		h.reader(h.names.Delay()).waitCommitted(t, 0, 4)
		select {
		case until := <-w.calls:
			t.Errorf("%s: waited until %v although the message can never be delivered", name, until)
		default:
		}
	}
}

func TestRelayForwardFailureRetriesWithoutCommitting(t *testing.T) {
	h, _ := relayHarness(t, 2, false)
	h.p.gate = make(chan struct{})
	startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 3, h.names.Default(), time.Now().Add(-time.Second)))
	r := h.reader(h.names.Delay())

	for i, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond} {
		if got := recv(t, h.p.paused, "a forward retry pause"); got != want {
			t.Errorf("pause %d = %v, want %v", i, got, want)
		}
		if got := r.commitCalls(); len(got) != 0 {
			t.Fatalf("committed %v while the forward was still failing", got)
		}
		h.p.gate <- struct{}{}
	}
	if m := h.awaitWrite(); m.Topic != h.names.Default() {
		t.Errorf("forwarded to %q, want %q", m.Topic, h.names.Default())
	}
	r.waitCommitted(t, 0, 3)
	if n := len(h.w.written()); n != 1 {
		t.Errorf("writer holds %d messages, want the one forwarded copy", n)
	}
}

// A message is forwarded before the next one is touched, in consumption order,
// even when the first one has to wait.
func TestRelayHandlesMessagesSerially(t *testing.T) {
	h, w := relayHarness(t, 0, true)
	startRelayOnly(h)
	late := time.Now().Add(time.Hour)
	soon := time.Now().Add(time.Minute)
	first := h.delayMsg(0, 0, h.names.Default(), late)
	second := h.delayMsg(0, 1, h.names.Batch(), soon)
	h.feed(h.names.Delay(), first, second)

	if got := recv(t, w.calls, "the first wait"); got.UnixMilli() != late.UnixMilli() {
		t.Fatalf("first wait until %v, want %v", got, late)
	}
	// The second message is due sooner but sits behind the first.
	select {
	case until := <-w.calls:
		t.Fatalf("the relay moved on to the next message (wait until %v) before the first was forwarded", until)
	default:
	}
	if n := len(h.w.written()); n != 0 {
		t.Fatalf("%d messages forwarded while the head was still waiting", n)
	}
	w.gate <- true
	if m := h.awaitWrite(); m.Topic != h.names.Default() {
		t.Fatalf("first forwarded to %q, want %q", m.Topic, h.names.Default())
	}
	if got := recv(t, w.calls, "the second wait"); got.UnixMilli() != soon.UnixMilli() {
		t.Fatalf("second wait until %v, want %v", got, soon)
	}
	w.gate <- true
	if m := h.awaitWrite(); m.Topic != h.names.Batch() {
		t.Fatalf("second forwarded to %q, want %q", m.Topic, h.names.Batch())
	}
	h.reader(h.names.Delay()).waitCommitted(t, 0, 1)
}

func TestRelayForwardsInConsumptionOrder(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	startRelayOnly(h)
	var want []string
	for i := range 20 {
		task := &engine.Task{ExecutionID: "e", NodeName: strconv.Itoa(i)}
		want = append(want, strconv.Itoa(i))
		h.feed(h.names.Delay(), taskMsg(t, h.names.Delay(), 0, int64(i), task, "ns",
			hdr(hdrTarget, h.names.Default()), hdr(hdrNotBefore, "1")))
	}
	var got []string
	for range 20 {
		task, _, err := decodeTask(h.awaitWrite())
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, task.NodeName)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("forward order = %v, want %v", got, want)
	}
	h.reader(h.names.Delay()).waitCommitted(t, 0, 19)
}

// Messages of several partitions each commit on their own offset.
func TestRelayCommitsEachPartition(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	startRelayOnly(h)
	h.feed(h.names.Delay(),
		h.delayMsg(0, 5, h.names.Default(), time.Now()),
		h.delayMsg(1, 9, h.names.Default(), time.Now()),
		h.delayMsg(0, 6, h.names.Default(), time.Now()))
	r := h.reader(h.names.Delay())
	r.waitCommitted(t, 0, 6)
	r.waitCommitted(t, 1, 9)
}

func TestRelayStopWhileWaitingDoesNotForwardOrCommit(t *testing.T) {
	h, w := relayHarness(t, 0, true)
	stop := startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(time.Hour)))
	recv(t, w.calls, "the wait")

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	recv(t, done, "stop to return")

	if n := len(h.w.written()); n != 0 {
		t.Errorf("%d messages forwarded by a relay that was stopped while waiting", n)
	}
	if got := h.reader(h.names.Delay()).commitCalls(); len(got) != 0 {
		t.Errorf("committed %v for a message that was never forwarded", got)
	}
}

func TestRelayStopDuringFailingForwardDoesNotCommit(t *testing.T) {
	h, _ := relayHarness(t, 1<<30, false)
	h.p.gate = make(chan struct{})
	stop := startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(-time.Second)))
	recv(t, h.p.paused, "the first forward retry pause")

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	recv(t, done, "stop to return")
	if got := h.reader(h.names.Delay()).commitCalls(); len(got) != 0 {
		t.Errorf("committed %v although the forward never succeeded", got)
	}
}

func TestRelayFetchErrorRetriesAndEOFEnds(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	h.onReader = func(topic string, r *fakeReader) {
		if topic == h.names.Delay() {
			r.fetchErrs = []error{io.ErrUnexpectedEOF, io.ErrNoProgress}
		}
	}
	startRelayOnly(h)
	for i, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond} {
		if got := recv(t, h.p.paused, "a relay fetch pause"); got != want {
			t.Errorf("pause %d = %v, want %v", i, got, want)
		}
	}
	// After the errors the relay is fetching normally again.
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(-time.Second)))
	h.awaitWrite()
	h.reader(h.names.Delay()).waitCommitted(t, 0, 0)
}

// io.EOF means the reader was closed, so the relay returns instead of retrying.
func TestRelayExitsOnEOF(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	h.onReader = func(topic string, r *fakeReader) {
		if topic == h.names.Delay() {
			r.fetchErrs = []error{io.EOF}
		}
	}
	startRelayOnly(h)
	// Once the relay has exited nothing consumes the delay feed any more.
	r := h.reader(h.names.Delay())
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(-time.Second)))
	select {
	case m := <-h.w.wrote:
		t.Fatalf("a relay that saw io.EOF still forwarded %q", m.Topic)
	case d := <-h.p.paused:
		t.Fatalf("a relay that saw io.EOF paused %v and retried", d)
	case <-time.After(100 * time.Millisecond):
	}
	if got := len(r.in); got != 1 {
		t.Errorf("%d messages left in the feed, want the one nobody fetched", got)
	}
}

func TestRelayLeavesNoGoroutinesAfterStop(t *testing.T) {
	before := runtime.NumGoroutine()
	h, w := relayHarness(t, 0, true)
	stop := startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(time.Hour)))
	recv(t, w.calls, "the wait")
	stop()
	deadline := time.Now().Add(consumerGuard)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if after := runtime.NumGoroutine(); after > before {
		buf := make([]byte, 1<<16)
		t.Errorf("goroutines: %d before, %d after stop\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

// The default waiter really waits and honors cancellation.
func TestSleepUntil(t *testing.T) {
	if !sleepUntil(t.Context(), time.Now().Add(-time.Hour)) {
		t.Error("sleepUntil(past) = false, want an immediate true")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if sleepUntil(ctx, time.Now().Add(time.Hour)) {
		t.Error("sleepUntil with a cancelled context = true, want false")
	}
	if sleepUntil(ctx, time.Now().Add(-time.Hour)) {
		t.Error("sleepUntil(past) with a cancelled context = true, want false")
	}
	if !sleepFor(t.Context(), 5*time.Millisecond) {
		t.Error("sleepFor(5ms) = false, want true after the pause")
	}
	if !sleepFor(t.Context(), 0) || !sleepFor(t.Context(), -time.Second) {
		t.Error("sleepFor(<=0) = false, want true")
	}
}

// A due message whose forwarded copy is too large for the target topic goes to
// the dead-letter topic with a reason, and is then committed.
func TestRelayForwardTooLargeGoesToDLQ(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	w := newSizeWriter(nil)
	w.reject = tooLargeFor(h.names.Default())
	h.tr.writer = w
	startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(3, 14, h.names.Default(), time.Now().Add(-time.Second)))

	m := recvWrite(t, w)
	if m.Topic != h.names.DLQ() {
		t.Fatalf("written to %q, want the DLQ", m.Topic)
	}
	if got := hdrOf(t, m, hdrError); got != "forwarded copy too large" {
		t.Errorf("error = %q", got)
	}
	if got := hdrOf(t, m, hdrSource); got != h.names.Delay() {
		t.Errorf("source = %q, want the delay topic", got)
	}
	h.reader(h.names.Delay()).waitCommitted(t, 3, 14)
}

// A forward that fails for any other reason is still retried, never dead-lettered.
func TestRelayForwardOtherErrorsKeepRetrying(t *testing.T) {
	h, _ := relayHarness(t, 0, false)
	h.p.gate = make(chan struct{})
	w := newSizeWriter(func(kafkago.Message) error { return kafkago.UnknownTopicOrPartition })
	h.tr.writer = w
	startRelayOnly(h)
	h.feed(h.names.Delay(), h.delayMsg(0, 0, h.names.Default(), time.Now().Add(-time.Second)))
	for range 3 {
		recv(t, h.p.paused, "a forward retry pause")
		h.p.gate <- struct{}{}
	}
	if got := h.reader(h.names.Delay()).commitCalls(); len(got) != 0 {
		t.Errorf("committed %v while forwarding kept failing", got)
	}
	for _, a := range w.attempts() {
		if a.Topic != h.names.Default() {
			t.Errorf("wrote to %q, want only forward attempts to %q", a.Topic, h.names.Default())
		}
	}
}
