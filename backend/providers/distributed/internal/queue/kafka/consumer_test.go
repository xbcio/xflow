package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// consumerGuard only bounds how long a broken test may hang; no assertion
// depends on it elapsing.
const consumerGuard = 10 * time.Second

var errFlaky = errors.New("flaky: write refused")

// fakeReader is a messageReader that hands out messages fed by the test, records
// every commit, and blocks until its context ends once it has nothing to give.
type fakeReader struct {
	in        chan kafkago.Message
	closed    chan struct{}
	closeOnce sync.Once
	// commitSig gets a token per commit; fetchEnded closes when a fetch returns
	// because its context ended.
	commitSig    chan struct{}
	fetchEnded   chan struct{}
	fetchEndOnce sync.Once
	mu           sync.Mutex
	commits      [][]kafkago.Message
	closes       int
	fetchErrs    []error
	commitHook   func()
}

// newFakeReader: feedCap 0 makes feeding a synchronous hand-off, so a send
// returns only once the consumer has actually fetched the message.
func newFakeReader(feedCap int) *fakeReader {
	return &fakeReader{
		in:         make(chan kafkago.Message, feedCap),
		closed:     make(chan struct{}),
		commitSig:  make(chan struct{}, 1024),
		fetchEnded: make(chan struct{}),
	}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if len(r.fetchErrs) > 0 {
		err := r.fetchErrs[0]
		r.fetchErrs = r.fetchErrs[1:]
		r.mu.Unlock()
		return kafkago.Message{}, err
	}
	r.mu.Unlock()
	select {
	case m := <-r.in:
		return m, nil
	case <-ctx.Done():
		r.fetchEndOnce.Do(func() { close(r.fetchEnded) })
		return kafkago.Message{}, ctx.Err()
	case <-r.closed:
		return kafkago.Message{}, io.EOF
	}
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	hook := r.commitHook
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	r.mu.Lock()
	r.commits = append(r.commits, append([]kafkago.Message(nil), msgs...))
	r.mu.Unlock()
	select {
	case r.commitSig <- struct{}{}:
	default:
	}
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	r.closes++
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *fakeReader) commitCalls() [][]kafkago.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]kafkago.Message(nil), r.commits...)
}

// committed returns the highest offset committed so far, per partition.
func (r *fakeReader) committed() map[int]int64 {
	out := map[int]int64{}
	for _, call := range r.commitCalls() {
		for _, m := range call {
			if cur, ok := out[m.Partition]; !ok || m.Offset > cur {
				out[m.Partition] = m.Offset
			}
		}
	}
	return out
}

func (r *fakeReader) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

// waitCommitted blocks until the partition's committed offset is at least want.
func (r *fakeReader) waitCommitted(t *testing.T, partition int, want int64) {
	t.Helper()
	timeout := time.After(consumerGuard)
	for {
		if got, ok := r.committed()[partition]; ok && got >= want {
			return
		}
		select {
		case <-r.commitSig:
		case <-timeout:
			t.Fatalf("partition %d: committed %v, want at least %d", partition, r.committed(), want)
		}
	}
}

// recv waits for a value on ch, failing the test if none arrives.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(consumerGuard):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// flakyWriter is a messageWriter that refuses the first failFirst calls and
// afterwards records every message and announces it on wrote.
type flakyWriter struct {
	mu        sync.Mutex
	failFirst int
	calls     int
	msgs      []kafkago.Message
	wrote     chan kafkago.Message
}

func newFlakyWriter(failFirst int) *flakyWriter {
	return &flakyWriter{failFirst: failFirst, wrote: make(chan kafkago.Message, 256)}
}

func (w *flakyWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls <= w.failFirst {
		return errFlaky
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, m := range msgs {
		w.msgs = append(w.msgs, m)
		w.wrote <- m
	}
	return nil
}

func (w *flakyWriter) Close() error { return nil }

func (w *flakyWriter) written() []kafkago.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kafkago.Message(nil), w.msgs...)
}

// pauser stands in for Transport.pause. It records every requested pause on
// paused. When gate is non-nil each pause also blocks until the test sends a
// token on it or the context ends, which freezes a retry loop between attempts.
type pauser struct {
	paused chan time.Duration
	gate   chan struct{}
}

func newPauser() *pauser { return &pauser{paused: make(chan time.Duration, 64)} }

func (p *pauser) pause(ctx context.Context, d time.Duration) bool {
	select {
	case p.paused <- d:
	default:
	}
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return false
		}
	}
	return ctx.Err() == nil
}

// harness wires a Transport to fake readers, a flaky writer and a controllable
// pause, so a consumer runs end to end without a broker.
type harness struct {
	t     *testing.T
	tr    *Transport
	w     *flakyWriter
	p     *pauser
	names topicNames

	// onReader, if set, runs on every reader the factory builds, before the
	// consumer sees it.
	onReader func(topic string, r *fakeReader)

	mu      sync.Mutex
	readers map[string]*fakeReader
	groups  map[string]string
}

// newHarness: mutate adjusts the Config; failFirst is how many writes fail.
func newHarness(t *testing.T, failFirst int, mutate func(*Config)) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		w:       newFlakyWriter(failFirst),
		p:       newPauser(),
		readers: map[string]*fakeReader{},
		groups:  map[string]string{},
	}
	cfg := Config{Brokers: []string{"broker:9092"}, MaxRetries: 3, ShutdownTimeout: 5 * time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	tr, err := New(cfg, "", withWriter(h.w), withReaderFactory(h.newReader))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tr.pause = h.p.pause
	h.tr = tr
	h.names = tr.names
	return h
}

func (h *harness) newReader(topic, group string) messageReader {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := newFakeReader(256)
	if h.onReader != nil {
		h.onReader(topic, r)
	}
	h.readers[topic] = r
	h.groups[topic] = group
	return r
}

// feed queues messages on the fake reader of topic.
func (h *harness) feed(topic string, msgs ...kafkago.Message) {
	h.t.Helper()
	r := h.reader(topic)
	for _, m := range msgs {
		r.in <- m
	}
}

func (h *harness) reader(topic string) *fakeReader {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.readers[topic]
	if r == nil {
		h.t.Fatalf("no reader was built for topic %q", topic)
	}
	return r
}

// start runs the consumer and stops it when the test ends.
func (h *harness) start(cfg queue.ConsumerConfig, handler queue.TaskHandler) func() {
	h.t.Helper()
	stop, err := h.tr.StartConsumer(cfg, handler)
	if err != nil {
		h.t.Fatalf("StartConsumer: %v", err)
	}
	h.t.Cleanup(stop)
	return stop
}

// taskMsg encodes a task into the message a producer would have written to topic.
func taskMsg(t *testing.T, topic string, partition int, offset int64, task *engine.Task, ns namespace.Namespace, hdrs ...kafkago.Header) kafkago.Message {
	t.Helper()
	msg, err := encodeTask(topic, task, ns)
	if err != nil {
		t.Fatalf("encodeTask: %v", err)
	}
	msg.Partition, msg.Offset, msg.Headers = partition, offset, hdrs
	return msg
}

func hdr(key, val string) kafkago.Header { return kafkago.Header{Key: key, Value: []byte(val)} }

var nilHandler queue.TaskHandler = func(context.Context, *engine.Task) error { return nil }

func TestConsumerWorkerSplit(t *testing.T) {
	cases := []struct{ n, wantDefault, wantBatch int }{
		{-3, 1, 1}, {0, 1, 1}, {1, 1, 1}, {2, 1, 1}, {8, 7, 1},
		{9, 8, 1}, {10, 9, 1}, {17, 16, 1}, {18, 16, 2}, {27, 24, 3},
	}
	for _, c := range cases {
		d, b := workerSplit(c.n)
		if d != c.wantDefault || b != c.wantBatch {
			t.Errorf("workerSplit(%d) = %d,%d, want %d,%d", c.n, d, b, c.wantDefault, c.wantBatch)
		}
	}
}

// With Concurrency 1 each topic still gets a worker, so two handlers run at
// once. The test proves it by holding both inside the handler together.
func TestConsumerConcurrencyOneRunsTwoWorkers(t *testing.T) {
	h := newHarness(t, 0, nil)
	entered := make(chan string, 4)
	release := make(chan struct{})
	h.start(queue.ConsumerConfig{Concurrency: 1}, func(_ context.Context, task *engine.Task) error {
		entered <- string(task.ExecutionID)
		<-release
		return nil
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "d"}, "ns"))
	h.feed(h.names.Batch(), taskMsg(t, h.names.Batch(), 0, 0, batchTask(), "ns"))
	got := map[string]bool{recv(t, entered, "first handler"): true, recv(t, entered, "second handler"): true}
	if len(got) != 2 {
		t.Fatalf("handlers entered = %v, want one default and one batch task running together", got)
	}
	close(release)
}

func TestConsumerWorkerCountMatchesSplit(t *testing.T) {
	for _, n := range []int{1, 2, 9, 10, 18} {
		wantDefault, wantBatch := workerSplit(n)
		h := newHarness(t, 0, nil)
		var mu sync.Mutex
		running := map[string]int{}
		peak := map[string]int{}
		reached := make(chan string, 64)
		release := make(chan struct{})
		h.start(queue.ConsumerConfig{Concurrency: n}, func(_ context.Context, task *engine.Task) error {
			kind := "default"
			if task.Type == engine.TaskTypeNodeBatch {
				kind = "batch"
			}
			mu.Lock()
			running[kind]++
			peak[kind] = max(peak[kind], running[kind])
			mu.Unlock()
			reached <- kind
			<-release
			mu.Lock()
			running[kind]--
			mu.Unlock()
			return nil
		})
		total := wantDefault + wantBatch
		for i := range total + 4 {
			h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, int64(i), &engine.Task{ExecutionID: "d"}, "ns"))
			h.feed(h.names.Batch(), taskMsg(t, h.names.Batch(), 0, int64(i), batchTask(), "ns"))
		}
		for range total {
			recv(t, reached, "a worker to start")
		}
		mu.Lock()
		gotDefault, gotBatch := running["default"], running["batch"]
		mu.Unlock()
		if gotDefault != wantDefault || gotBatch != wantBatch {
			t.Errorf("Concurrency %d: running default=%d batch=%d, want %d and %d", n, gotDefault, gotBatch, wantDefault, wantBatch)
		}
		close(release)
	}
}

func TestBackoff(t *testing.T) {
	const limit = 5 * time.Minute
	cases := []struct {
		attempt int
		limit   time.Duration
		want    time.Duration
	}{
		{math.MinInt, limit, time.Second}, {-1, limit, time.Second}, {0, limit, time.Second},
		{1, limit, time.Second}, {2, limit, 2 * time.Second}, {3, limit, 4 * time.Second},
		{4, limit, 8 * time.Second}, {8, limit, 128 * time.Second}, {9, limit, 256 * time.Second},
		{10, limit, limit}, {11, limit, limit}, {63, limit, limit}, {64, limit, limit},
		{1_000_000, limit, limit}, {math.MaxInt, limit, limit},
		{5, 3 * time.Second, 3 * time.Second}, {1, 500 * time.Millisecond, 500 * time.Millisecond},
		{3, time.Duration(math.MaxInt64), 4 * time.Second},
	}
	for _, c := range cases {
		if got := backoff(c.attempt, c.limit); got != c.want {
			t.Errorf("backoff(%d, %v) = %v, want %v", c.attempt, c.limit, got, c.want)
		}
	}
}

func TestBackoffNeverOverflowsAndNeverDecreases(t *testing.T) {
	limit := time.Duration(math.MaxInt64)
	prev := time.Duration(0)
	for attempt := 1; attempt <= 200; attempt++ {
		got := backoff(attempt, limit)
		if got <= 0 {
			t.Fatalf("backoff(%d) = %v, want a positive duration", attempt, got)
		}
		if got < prev {
			t.Fatalf("backoff(%d) = %v is below backoff(%d) = %v", attempt, got, attempt-1, prev)
		}
		prev = got
	}
}

func TestConfigConsumerDefaults(t *testing.T) {
	got := Config{Brokers: []string{"b:9092"}}.withConsumerDefaults()
	if got.SessionTimeout != 30*time.Second || got.HeartbeatInterval != 3*time.Second ||
		got.RebalanceTimeout != 60*time.Second || got.ShutdownTimeout != 30*time.Second ||
		got.MaxRetryBackoff != 5*time.Minute || got.HandlerTimeout != 30*time.Minute {
		t.Errorf("duration defaults = %+v", got)
	}
	if got.FetchMaxBytes != 8<<20 || got.MaxRetries != 25 || got.MaxPending != 1024 || got.StartFromLatest {
		t.Errorf("other defaults = %+v", got)
	}
	if again := got.withConsumerDefaults(); !reflect.DeepEqual(again, got) {
		t.Errorf("withConsumerDefaults is not idempotent: %+v vs %+v", again, got)
	}
}

func TestConfigConsumerDefaultsKeepAndFallBack(t *testing.T) {
	set := Config{
		BatchBytes: 1 << 20, SessionTimeout: time.Minute, HeartbeatInterval: time.Second,
		RebalanceTimeout: 2 * time.Minute, MaxRetries: 7, MaxPending: 9,
		ShutdownTimeout: time.Second, MaxRetryBackoff: time.Second, StartFromLatest: true,
		HandlerTimeout: time.Minute,
	}.withConsumerDefaults()
	if set.FetchMaxBytes != 1<<20 {
		t.Errorf("FetchMaxBytes = %d, want BatchBytes %d", set.FetchMaxBytes, 1<<20)
	}
	if set.SessionTimeout != time.Minute || set.HeartbeatInterval != time.Second || set.MaxRetries != 7 ||
		set.MaxPending != 9 || set.RebalanceTimeout != 2*time.Minute || set.ShutdownTimeout != time.Second ||
		set.MaxRetryBackoff != time.Second || !set.StartFromLatest || set.HandlerTimeout != time.Minute {
		t.Errorf("explicit values were changed: %+v", set)
	}
	neg := Config{SessionTimeout: -1, MaxRetries: -5, MaxPending: -1, FetchMaxBytes: -9, HandlerTimeout: -1}.withConsumerDefaults()
	if neg.SessionTimeout != 30*time.Second || neg.MaxRetries != 25 || neg.MaxPending != 1024 || neg.FetchMaxBytes != 8<<20 ||
		neg.HandlerTimeout != 30*time.Minute {
		t.Errorf("negative values did not fall back: %+v", neg)
	}
	if over := (Config{BatchBytes: 1 << 20, FetchMaxBytes: 4 << 20}).withConsumerDefaults(); over.FetchMaxBytes != 4<<20 {
		t.Errorf("FetchMaxBytes = %d, want the explicit 4 MiB", over.FetchMaxBytes)
	}
}

func TestReaderConfigDefaults(t *testing.T) {
	cfg := Config{Brokers: []string{"a:9092", "b:9092"}}
	rc, err := readerConfig(cfg, "xflow.task.default", "xflow.consumer.default")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rc.Brokers, cfg.Brokers) || rc.Topic != "xflow.task.default" || rc.GroupID != "xflow.consumer.default" {
		t.Errorf("brokers/topic/group = %v %q %q", rc.Brokers, rc.Topic, rc.GroupID)
	}
	if rc.MinBytes != 1 || rc.MaxBytes != 8<<20 || rc.MaxWait != 500*time.Millisecond {
		t.Errorf("fetch sizing = min %d max %d wait %v", rc.MinBytes, rc.MaxBytes, rc.MaxWait)
	}
	if rc.CommitInterval != 0 {
		t.Errorf("CommitInterval = %v, want 0 (synchronous commits)", rc.CommitInterval)
	}
	if rc.StartOffset != kafkago.FirstOffset {
		t.Errorf("StartOffset = %d, want FirstOffset %d", rc.StartOffset, kafkago.FirstOffset)
	}
	if rc.SessionTimeout != 30*time.Second || rc.HeartbeatInterval != 3*time.Second || rc.RebalanceTimeout != 60*time.Second {
		t.Errorf("group timings = %v %v %v", rc.SessionTimeout, rc.HeartbeatInterval, rc.RebalanceTimeout)
	}
	if !rc.WatchPartitionChanges || rc.PartitionWatchInterval != 10*time.Second {
		t.Errorf("partition watch = %v every %v, want enabled every 10s", rc.WatchPartitionChanges, rc.PartitionWatchInterval)
	}
	if len(rc.GroupBalancers) != 1 {
		t.Fatalf("GroupBalancers = %v, want exactly Range", rc.GroupBalancers)
	}
	if _, ok := rc.GroupBalancers[0].(kafkago.RangeGroupBalancer); !ok {
		t.Errorf("balancer = %T, want RangeGroupBalancer", rc.GroupBalancers[0])
	}
	d := rc.Dialer
	if d == nil || d.Timeout != 10*time.Second || !d.DualStack || d.TLS != nil || d.SASLMechanism != nil {
		t.Errorf("Dialer = %+v, want 10s timeout, dual stack, no TLS, no SASL", d)
	}
	if err := rc.Validate(); err != nil {
		t.Errorf("kafka-go rejects the config: %v", err)
	}
}

func TestReaderConfigOverrides(t *testing.T) {
	tlsCfg := &tls.Config{ServerName: "kafka.example"}
	cfg := Config{
		Brokers: []string{"a:9092"}, TLS: tlsCfg, SASLMechanism: "scram-sha-512",
		SASLUsername: "u", SASLPassword: "p", BatchBytes: 2 << 20, FetchMaxBytes: 3 << 20,
		SessionTimeout: 40 * time.Second, HeartbeatInterval: 4 * time.Second,
		RebalanceTimeout: 90 * time.Second, StartFromLatest: true,
	}
	rc, err := readerConfig(cfg, "t", "g")
	if err != nil {
		t.Fatal(err)
	}
	if rc.MaxBytes != 3<<20 {
		t.Errorf("MaxBytes = %d, want the explicit FetchMaxBytes", rc.MaxBytes)
	}
	if rc.StartOffset != kafkago.LastOffset {
		t.Errorf("StartOffset = %d, want LastOffset %d", rc.StartOffset, kafkago.LastOffset)
	}
	if rc.SessionTimeout != 40*time.Second || rc.HeartbeatInterval != 4*time.Second || rc.RebalanceTimeout != 90*time.Second {
		t.Errorf("group timings = %v %v %v", rc.SessionTimeout, rc.HeartbeatInterval, rc.RebalanceTimeout)
	}
	if rc.Dialer.TLS != tlsCfg || rc.Dialer.SASLMechanism == nil || rc.Dialer.SASLMechanism.Name() != "SCRAM-SHA-512" {
		t.Errorf("Dialer = %+v, want the TLS config and SCRAM-SHA-512", rc.Dialer)
	}
	if fromBatch, _ := readerConfig(Config{Brokers: []string{"a:9092"}, BatchBytes: 2 << 20}, "t", "g"); fromBatch.MaxBytes != 2<<20 {
		t.Errorf("MaxBytes = %d, want BatchBytes when FetchMaxBytes is unset", fromBatch.MaxBytes)
	}
}

func TestReaderConfigRejectsBadSASL(t *testing.T) {
	cfg := Config{Brokers: []string{"a:9092"}, SASLMechanism: "gssapi", SASLUsername: "u", SASLPassword: "p"}
	if _, err := readerConfig(cfg, "t", "g"); err == nil {
		t.Error("readerConfig accepted an unsupported SASL mechanism")
	}
	if r, err := newGroupReader(cfg, "t", "g"); err == nil || r != nil {
		t.Errorf("newGroupReader = %v, %v; want an error and no reader", r, err)
	}
}

func TestNewGroupReaderBuildsRealReader(t *testing.T) {
	r, err := newGroupReader(Config{Brokers: []string{"127.0.0.1:1"}}, "t", "g")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.(*kafkago.Reader); !ok {
		t.Fatalf("reader = %T, want *kafka.Reader", r)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestNewRejectsBadSASLEvenWithInjectedWriter(t *testing.T) {
	cfg := Config{Brokers: []string{"b:9092"}, SASLMechanism: "plain", SASLPassword: "p"}
	if tr, err := New(cfg, "", withWriter(&fakeWriter{})); err == nil || tr != nil {
		t.Errorf("New = %v, %v; want an error and no transport", tr, err)
	}
}

func TestStartConsumerBuildsThreeGroupReaders(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, nilHandler)
	h.mu.Lock()
	defer h.mu.Unlock()
	want := map[string]string{
		"xflow.task.default": "xflow.consumer.default",
		"xflow.task.batch":   "xflow.consumer.batch",
		"xflow.task.delay":   "xflow.consumer.delay",
	}
	if !reflect.DeepEqual(h.groups, want) {
		t.Errorf("topic->group = %v, want %v", h.groups, want)
	}
}

func TestStartConsumerRequiresHandler(t *testing.T) {
	h := newHarness(t, 0, nil)
	if stop, err := h.tr.StartConsumer(queue.ConsumerConfig{}, nil); err == nil || stop != nil {
		t.Errorf("StartConsumer(nil handler) = %v, %v; want an error", stop != nil, err)
	}
}

func hdrOf(t *testing.T, m kafkago.Message, key string) string {
	t.Helper()
	v, ok := headerValue(m, key)
	if !ok {
		t.Fatalf("message has no %s header: %+v", key, m.Headers)
	}
	return v
}

func TestConsumerSuccessCommitsOffset(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, nilHandler)
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 3, 41, &engine.Task{ExecutionID: "e"}, "ns"))
	r := h.reader(h.names.Default())
	r.waitCommitted(t, 3, 41)
	call := r.commitCalls()[0]
	if len(call) != 1 || call[0].Topic != h.names.Default() || call[0].Partition != 3 || call[0].Offset != 41 {
		t.Errorf("commit = %+v, want topic %s partition 3 offset 41", call, h.names.Default())
	}
	if w := h.w.written(); len(w) != 0 {
		t.Errorf("a successful task produced %d messages, want none", len(w))
	}
}

// Offset 2 finishes first and must not be committed ahead of 0 and 1; once 0
// and 1 finish, the commit jumps straight to 2.
//
// Concurrency 4 gives the default topic three workers. A fourth message waits
// for a free one, so the moment its handler starts proves that the worker which
// ran offset 2 has already registered the completion: no sleep is needed to know
// a wrong early commit would have been issued by then.
func TestConsumerOutOfOrderCompletionCommitsWatermark(t *testing.T) {
	h := newHarness(t, 0, nil)
	gates := map[string]chan struct{}{
		"e0": make(chan struct{}), "e1": make(chan struct{}),
		"e2": make(chan struct{}), "e3": make(chan struct{}),
	}
	entered := make(chan string, 4)
	h.start(queue.ConsumerConfig{Concurrency: 4}, func(_ context.Context, task *engine.Task) error {
		id := string(task.ExecutionID)
		entered <- id
		<-gates[id]
		return nil
	})
	for i, id := range []string{"e0", "e1", "e2", "e3"} {
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, int64(i), &engine.Task{ExecutionID: types.ExecutionID(id)}, "ns"))
	}
	for range 3 {
		recv(t, entered, "handler to start")
	}
	r := h.reader(h.names.Default())
	close(gates["e2"])
	if id := recv(t, entered, "the fourth handler"); id != "e3" {
		t.Fatalf("fourth handler = %s, want e3", id)
	}
	if got := r.commitCalls(); len(got) != 0 {
		t.Fatalf("committed %v while offsets 0 and 1 were still running", got)
	}
	close(gates["e0"])
	r.waitCommitted(t, 0, 0)
	if got := r.committed()[0]; got != 0 {
		t.Fatalf("committed = %d with offset 1 still running, want 0", got)
	}
	close(gates["e1"])
	r.waitCommitted(t, 0, 2)
	for _, call := range r.commitCalls() {
		for _, m := range call {
			if m.Offset == 1 {
				t.Errorf("offset 1 was committed on its own, not as part of the jump to 2: %v", r.commitCalls())
			}
		}
	}
	close(gates["e3"])
	r.waitCommitted(t, 0, 3)
}

// failWith returns a handler that fails every task with err.
func failWith(err error) queue.TaskHandler {
	return func(context.Context, *engine.Task) error { return err }
}

// awaitWrite waits for the next message the fake writer accepts.
func (h *harness) awaitWrite() kafkago.Message {
	h.t.Helper()
	return recv(h.t, h.w.wrote, "a message to be written")
}

func TestConsumerRetryableErrorParksOnDelayTopic(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("downstream 503")))
	orig := taskMsg(t, h.names.Default(), 1, 7, &engine.Task{ExecutionID: "e"}, "ns", hdr("keep", "me"))
	before := time.Now()
	h.feed(h.names.Default(), orig)

	m := h.awaitWrite()
	if m.Topic != h.names.Delay() {
		t.Fatalf("retry written to %q, want the delay topic %q", m.Topic, h.names.Delay())
	}
	if string(m.Key) != string(orig.Key) || string(m.Value) != string(orig.Value) {
		t.Errorf("key/value changed: %q/%q vs %q/%q", m.Key, m.Value, orig.Key, orig.Value)
	}
	if got := hdrOf(t, m, hdrAttempt); got != "1" {
		t.Errorf("attempt = %q, want 1", got)
	}
	if got := hdrOf(t, m, hdrTarget); got != h.names.Default() {
		t.Errorf("target = %q, want %q", got, h.names.Default())
	}
	if got := hdrOf(t, m, "keep"); got != "me" {
		t.Errorf("custom header = %q, want it preserved", got)
	}
	due, ok := notBeforeOf(m)
	if !ok || !due.After(before) {
		t.Errorf("not-before = %v (ok %v), want a time after %v", due, ok, before)
	}
	h.reader(h.names.Default()).waitCommitted(t, 1, 7)
	if n := len(h.w.written()); n != 1 {
		t.Errorf("writer got %d messages, want just the delay copy", n)
	}
}

func TestConsumerRetryKeepsTargetOfRelayedMessage(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.MaxRetries = 10 })
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("again")))
	// A message the relay forwarded still carries its stale target and attempt.
	h.feed(h.names.Batch(), taskMsg(t, h.names.Batch(), 0, 0, batchTask(), "ns",
		hdr(hdrAttempt, "4"), hdr(hdrTarget, h.names.Batch()), hdr(hdrNotBefore, "1")))
	m := h.awaitWrite()
	if got := hdrOf(t, m, hdrAttempt); got != "5" {
		t.Errorf("attempt = %q, want 5", got)
	}
	if got := hdrOf(t, m, hdrTarget); got != h.names.Batch() {
		t.Errorf("target = %q, want the batch topic", got)
	}
	if due, _ := notBeforeOf(m); due.UnixMilli() <= 1 {
		t.Errorf("not-before was not refreshed: %v", due)
	}
	if n := countHeader(m, hdrAttempt) + countHeader(m, hdrTarget) + countHeader(m, hdrNotBefore); n != 3 {
		t.Errorf("headers = %+v, want each bookkeeping header exactly once", m.Headers)
	}
}

func countHeader(m kafkago.Message, key string) int {
	n := 0
	for _, h := range m.Headers {
		if h.Key == key {
			n++
		}
	}
	return n
}

func TestConsumerRetryBackoffGrowsWithAttempt(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.MaxRetries = 10 })
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("slow down")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrAttempt, "2")))
	m := h.awaitWrite()
	due, _ := notBeforeOf(m)
	delay := time.Until(due)
	if delay < 3*time.Second || delay > 4*time.Second {
		t.Errorf("attempt 3 is due in %v, want about %v", delay, backoff(3, 5*time.Minute))
	}
}

func TestConsumerAttemptBeyondMaxRetriesGoesToDLQ(t *testing.T) {
	h := newHarness(t, 0, nil) // MaxRetries 3
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("still failing")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 5, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrAttempt, "3")))
	m := h.awaitWrite()
	if m.Topic != h.names.DLQ() {
		t.Fatalf("exhausted task written to %q, want the DLQ", m.Topic)
	}
	if got := hdrOf(t, m, hdrSource); got != h.names.Default() {
		t.Errorf("source = %q, want %q", got, h.names.Default())
	}
	if got := hdrOf(t, m, hdrError); got != "still failing" {
		t.Errorf("error header = %q", got)
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 5)
}

func TestConsumerLastAllowedAttemptStillRetries(t *testing.T) {
	h := newHarness(t, 0, nil) // MaxRetries 3: attempt 2 -> 3 is the last retry
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("x")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrAttempt, "2")))
	if m := h.awaitWrite(); m.Topic != h.names.Delay() {
		t.Errorf("attempt 3 of 3 written to %q, want the delay topic", m.Topic)
	}
}

func TestConsumerPermanentErrorGoesToDLQWithTruncatedError(t *testing.T) {
	h := newHarness(t, 0, nil)
	long := strings.Repeat("é", 2000) // multi-byte, so a byte cut could split a rune
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(fmt.Errorf("%w: %s", types.ErrPermanent, long)))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 2, 9, &engine.Task{ExecutionID: "e"}, "ns"))
	m := h.awaitWrite()
	if m.Topic != h.names.DLQ() {
		t.Fatalf("permanent failure written to %q, want the DLQ", m.Topic)
	}
	e := hdrOf(t, m, hdrError)
	if len(e) > maxErrorHeaderBytes || len(e) < maxErrorHeaderBytes-3 || !utf8.ValidString(e) {
		t.Errorf("error header: %d bytes, valid utf8 %v; want a rune-safe cut at about %d", len(e), utf8.ValidString(e), maxErrorHeaderBytes)
	}
	if !strings.HasPrefix(e, types.ErrPermanent.Error()) {
		t.Errorf("error header does not start with the cause: %.40q", e)
	}
	h.reader(h.names.Default()).waitCommitted(t, 2, 9)
	if got := len(h.w.written()); got != 1 {
		t.Errorf("writer got %d messages, want only the DLQ copy", got)
	}
}

func TestConsumerUndecodableMessageGoesToDLQ(t *testing.T) {
	h := newHarness(t, 0, nil)
	called := make(chan struct{}, 1)
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(context.Context, *engine.Task) error {
		called <- struct{}{}
		return nil
	})
	bad := kafkago.Message{Topic: h.names.Default(), Partition: 0, Offset: 0, Key: []byte("k"), Value: []byte("{not json")}
	h.feed(h.names.Default(), bad)
	m := h.awaitWrite()
	if m.Topic != h.names.DLQ() || string(m.Value) != "{not json" || string(m.Key) != "k" {
		t.Errorf("dead letter = %+v", m)
	}
	if got := hdrOf(t, m, hdrError); !strings.Contains(got, "decode task") {
		t.Errorf("error header = %q, want the decode failure", got)
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 0)
	select {
	case <-called:
		t.Error("handler ran for an undecodable message")
	default:
	}
}

func TestConsumerTransientDropsFailedTask(t *testing.T) {
	for name, err := range map[string]error{
		"retryable": errors.New("boom"),
		"permanent": fmt.Errorf("%w: no", types.ErrPermanent),
	} {
		h := newHarness(t, 0, nil)
		h.start(queue.ConsumerConfig{Concurrency: 2, Transient: true}, failWith(err))
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 3, &engine.Task{ExecutionID: "e"}, "ns"))
		h.reader(h.names.Default()).waitCommitted(t, 0, 3)
		if w := h.w.written(); len(w) != 0 {
			t.Errorf("%s: transient mode wrote %d messages, want none (no retry, no DLQ)", name, len(w))
		}
	}
}

func TestConsumerPanicIsRetried(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(context.Context, *engine.Task) error {
		panic("secret task data")
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	m := h.awaitWrite()
	if m.Topic != h.names.Delay() || hdrOf(t, m, hdrAttempt) != "1" {
		t.Errorf("panic produced %q attempt %q, want a retry on the delay topic", m.Topic, hdrOf(t, m, hdrAttempt))
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 0)
}

func TestConsumerPanicErrorTextHasNoPanicValue(t *testing.T) {
	h := newHarness(t, 0, nil) // MaxRetries 3
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(context.Context, *engine.Task) error {
		panic("secret task data")
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrAttempt, "3")))
	e := hdrOf(t, h.awaitWrite(), hdrError)
	if !strings.Contains(e, "panic") || strings.Contains(e, "secret") {
		t.Errorf("error header = %q, want it to say panic without the panic value", e)
	}
}

func TestConsumerPanicDoesNotKillWorker(t *testing.T) {
	h := newHarness(t, 0, nil)
	var once sync.Once
	h.start(queue.ConsumerConfig{Concurrency: 1}, func(_ context.Context, task *engine.Task) error {
		var err error
		once.Do(func() { panic("first") })
		return err
	})
	for i := range 2 {
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, int64(i), &engine.Task{ExecutionID: "e"}, "ns"))
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 1)
}

// A handler that overruns HandlerTimeout has its context cancelled and the task
// is retried, even when the handler reports the cancellation as a permanent
// error.
func TestConsumerHandlerTimeoutIsRetried(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.HandlerTimeout = 20 * time.Millisecond })
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		<-ctx.Done()
		return fmt.Errorf("%w: %w", types.ErrPermanent, ctx.Err())
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	m := h.awaitWrite()
	if m.Topic != h.names.Delay() || hdrOf(t, m, hdrAttempt) != "1" {
		t.Errorf("timeout produced %q attempt %q, want a retry on the delay topic", m.Topic, hdrOf(t, m, hdrAttempt))
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 0)
}

func TestConsumerHandlerTimeoutErrorIsDeadLettered(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.HandlerTimeout = 20 * time.Millisecond }) // MaxRetries 3
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		<-ctx.Done()
		return ctx.Err()
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns", hdr(hdrAttempt, "3")))
	m := h.awaitWrite()
	if m.Topic != h.names.DLQ() {
		t.Fatalf("out of attempts wrote to %q, want the dead-letter topic", m.Topic)
	}
	if e := hdrOf(t, m, hdrError); !strings.Contains(e, "timed out") {
		t.Errorf("error header = %q, want it to say the handler timed out", e)
	}
}

// A handler that finishes inside the timeout is unaffected by it.
func TestConsumerHandlerWithinTimeoutSucceeds(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.HandlerTimeout = time.Minute })
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		return ctx.Err()
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	h.reader(h.names.Default()).waitCommitted(t, 0, 0)
	if n := len(h.w.written()); n != 0 {
		t.Errorf("writer got %d messages for a successful task, want none", n)
	}
}

func TestConsumerPublishFailureRetriesUntilWritten(t *testing.T) {
	h := newHarness(t, 3, nil)
	h.p.gate = make(chan struct{}) // freeze the loop between attempts
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("boom")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	r := h.reader(h.names.Default())

	var pauses []time.Duration
	for range 3 {
		pauses = append(pauses, recv(t, h.p.paused, "a retry pause"))
		if got := r.commitCalls(); len(got) != 0 {
			t.Fatalf("committed %v while the retry copy was still unwritten", got)
		}
		if n := len(h.w.written()); n != 0 {
			t.Fatalf("writer holds %d messages before it accepted any", n)
		}
		h.p.gate <- struct{}{}
	}
	if want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}; !reflect.DeepEqual(pauses, want) {
		t.Errorf("pauses = %v, want %v", pauses, want)
	}
	if m := h.awaitWrite(); m.Topic != h.names.Delay() {
		t.Errorf("written to %q, want the delay topic", m.Topic)
	}
	r.waitCommitted(t, 0, 0)
}

func TestConsumerPublishPauseIsCapped(t *testing.T) {
	h := newHarness(t, 8, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("boom")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	h.awaitWrite()
	var last time.Duration
	for range 8 {
		last = recv(t, h.p.paused, "a retry pause")
	}
	if last != 5*time.Second {
		t.Errorf("eighth pause = %v, want the 5s cap", last)
	}
}

func TestConsumerDeadLetterPublishFailureRetries(t *testing.T) {
	h := newHarness(t, 2, nil)
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(fmt.Errorf("%w: no", types.ErrPermanent)))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 4, &engine.Task{ExecutionID: "e"}, "ns"))
	if m := h.awaitWrite(); m.Topic != h.names.DLQ() {
		t.Errorf("written to %q, want the DLQ", m.Topic)
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 4)
}

// A publish that never succeeds before the consumer stops must leave the
// message uncommitted so the group redelivers it.
func TestConsumerStopDuringFailingPublishDoesNotCommit(t *testing.T) {
	// The retry loop runs under the handlers' context, so stop waits out
	// ShutdownTimeout before cancelling it; keep that wait short.
	h := newHarness(t, math.MaxInt, func(c *Config) { c.ShutdownTimeout = 50 * time.Millisecond })
	h.p.gate = make(chan struct{})
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("boom")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	recv(t, h.p.paused, "the first retry pause")

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	recv(t, stopped, "stop to return")

	r := h.reader(h.names.Default())
	if got := r.commitCalls(); len(got) != 0 {
		t.Errorf("committed %v although the retry copy was never written", got)
	}
	if n := len(h.w.written()); n != 0 {
		t.Errorf("writer holds %d messages, want none", n)
	}
	if r.closeCount() != 1 {
		t.Errorf("reader closed %d times, want 1", r.closeCount())
	}
}

func TestConsumerPassesNamespaceToHandler(t *testing.T) {
	h := newHarness(t, 0, nil)
	got := make(chan namespace.Namespace, 2)
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		got <- namespace.FromContext(ctx)
		return nil
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "tenant-a"))
	h.feed(h.names.Batch(), taskMsg(t, h.names.Batch(), 0, 0, batchTask(), "tenant-b"))
	seen := map[namespace.Namespace]bool{recv(t, got, "first namespace"): true, recv(t, got, "second namespace"): true}
	if !seen["tenant-a"] || !seen["tenant-b"] {
		t.Errorf("handlers saw namespaces %v, want tenant-a and tenant-b", seen)
	}
}

func TestConsumerPassesDecodedTaskToHandler(t *testing.T) {
	h := newHarness(t, 0, nil)
	got := make(chan *engine.Task, 1)
	h.start(queue.ConsumerConfig{Concurrency: 2}, func(_ context.Context, task *engine.Task) error {
		got <- task
		return nil
	})
	want := sampleTask()
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, want, "ns"))
	task := recv(t, got, "the task")
	if task.ExecutionID != want.ExecutionID || task.NodeName != want.NodeName || task.ActivationID != want.ActivationID {
		t.Errorf("task = %+v, want %+v", task, want)
	}
}

// With MaxPending 4 the fetch loop must stop pulling messages once four are in
// flight. The fake reader counts how many messages it has handed out, so the
// test can tell a fifth fetch from a fifth message that merely sits in the feed.
func TestConsumerMaxPendingBoundsInFlight(t *testing.T) {
	const limit = 4
	h := newHarness(t, 0, func(c *Config) { c.MaxPending = limit })
	var mu sync.Mutex
	running, peak := 0, 0
	entered := make(chan struct{}, 64)
	release := make(chan struct{})
	h.start(queue.ConsumerConfig{Concurrency: 20}, func(context.Context, *engine.Task) error {
		mu.Lock()
		running++
		peak = max(peak, running)
		mu.Unlock()
		entered <- struct{}{}
		<-release
		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})
	const total = 12
	for i := range total {
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, int64(i), &engine.Task{ExecutionID: "e"}, "ns"))
	}
	for range limit {
		recv(t, entered, "a handler to start")
	}
	r := h.reader(h.names.Default())
	// The remaining messages are still queued in the feed, none was fetched.
	if got := len(r.in); got != total-limit {
		t.Fatalf("%d messages left in the feed, want %d: the loop fetched past MaxPending", got, total-limit)
	}
	close(release)
	r.waitCommitted(t, 0, total-1)
	mu.Lock()
	defer mu.Unlock()
	if peak > limit {
		t.Errorf("peak in-flight = %d, want at most %d", peak, limit)
	}
}

// Done work stuck behind a slow earlier message still counts against MaxPending,
// so a stuck head cannot let finished messages pile up without bound.
func TestConsumerMaxPendingCountsDoneBehindStuckHead(t *testing.T) {
	const limit = 3
	h := newHarness(t, 0, func(c *Config) { c.MaxPending = limit })
	stuck := make(chan struct{})
	finished := make(chan int64, 16)
	h.start(queue.ConsumerConfig{Concurrency: 20}, func(_ context.Context, task *engine.Task) error {
		if task.NodeName == "head" {
			<-stuck
			return nil
		}
		finished <- 1
		return nil
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e", NodeName: "head"}, "ns"))
	for i := int64(1); i <= 8; i++ {
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, i, &engine.Task{ExecutionID: "e", NodeName: "n"}, "ns"))
	}
	for range limit - 1 {
		recv(t, finished, "a finished message")
	}
	r := h.reader(h.names.Default())
	if got := len(r.in); got != 8-(limit-1) {
		t.Fatalf("%d messages left in the feed, want %d: finished messages did not count as pending", got, 8-(limit-1))
	}
	if got := r.commitCalls(); len(got) != 0 {
		t.Errorf("committed %v behind a stuck head", got)
	}
	close(stuck)
	r.waitCommitted(t, 0, 8)
}

func TestConsumerFetchErrorRetriesWithBackoff(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.onReader = func(topic string, r *fakeReader) {
		if topic == h.names.Default() {
			r.fetchErrs = []error{errors.New("coordinator moved"), errors.New("again"), errors.New("and again")}
		}
	}
	h.start(queue.ConsumerConfig{Concurrency: 2}, nilHandler)
	var pauses []time.Duration
	for range 3 {
		pauses = append(pauses, recv(t, h.p.paused, "a fetch retry pause"))
	}
	if want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}; !reflect.DeepEqual(pauses, want) {
		t.Errorf("pauses = %v, want %v", pauses, want)
	}
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	h.reader(h.names.Default()).waitCommitted(t, 0, 0)
}

func TestConsumerFetchEOFEndsTheLoop(t *testing.T) {
	h := newHarness(t, 0, nil)
	h.onReader = func(topic string, r *fakeReader) {
		if topic == h.names.Default() {
			r.fetchErrs = []error{io.EOF}
		}
	}
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, nilHandler)
	stop() // returns only if the loop already ended or ends now
	select {
	case d := <-h.p.paused:
		t.Errorf("paused %v after io.EOF, want the loop to exit without retrying", d)
	default:
	}
}

func TestConsumerStopIsIdempotent(t *testing.T) {
	h := newHarness(t, 0, nil)
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, nilHandler)
	stop()
	stop()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(stop)
	}
	wg.Wait()
	for _, topic := range h.names.All()[:3] {
		if got := h.reader(topic).closeCount(); got != 1 {
			t.Errorf("reader of %s closed %d times, want exactly once", topic, got)
		}
	}
}

func TestConsumerStopWaitsForInFlightHandler(t *testing.T) {
	h := newHarness(t, 0, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	var sawCancel atomic.Bool
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		close(entered)
		<-release
		sawCancel.Store(ctx.Err() != nil)
		return nil
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	recv(t, entered, "the handler to start")

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while a handler was still running")
	case <-h.reader(h.names.Default()).fetchEnded:
	}
	// Fetching has ended, and stop must still be waiting for the handler.
	select {
	case <-stopped:
		t.Fatal("stop returned while a handler was still running")
	default:
	}
	close(release)
	recv(t, stopped, "stop to return")

	if sawCancel.Load() {
		t.Error("the handler's context was cancelled although it finished within ShutdownTimeout")
	}
	// The handler finished before the committer's final flush, so its offset
	// is committed by stop.
	if got, ok := h.reader(h.names.Default()).committed()[0]; !ok || got != 0 {
		t.Errorf("committed = %v, want offset 0 flushed by stop", h.reader(h.names.Default()).committed())
	}
}

func TestConsumerStopCancelsHandlerAfterShutdownTimeout(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.ShutdownTimeout = 50 * time.Millisecond })
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	recv(t, entered, "the handler to start")
	stop()
	recv(t, cancelled, "the handler's context to be cancelled")
}

// A handler cancelled by stop returns an error, which must not be committed or
// turned into a retry: the group redelivers the message to another member.
func TestConsumerStopLeavesCancelledTaskUncommitted(t *testing.T) {
	h := newHarness(t, 0, func(c *Config) { c.ShutdownTimeout = 50 * time.Millisecond })
	entered := make(chan struct{})
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, func(ctx context.Context, _ *engine.Task) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	recv(t, entered, "the handler to start")
	stop()
	r := h.reader(h.names.Default())
	if got := r.commitCalls(); len(got) != 0 {
		t.Errorf("committed %v for a task that was cancelled mid-run", got)
	}
	if n := len(h.w.written()); n != 0 {
		t.Errorf("writer got %d messages for a cancelled task, want none", n)
	}
}

func TestConsumerStopWithoutMessagesReturnsPromptly(t *testing.T) {
	h := newHarness(t, 0, nil)
	stop := h.start(queue.ConsumerConfig{Concurrency: 9}, nilHandler)
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	recv(t, done, "stop to return")
	for _, topic := range h.names.All()[:3] {
		if got := h.reader(topic).closeCount(); got != 1 {
			t.Errorf("reader of %s closed %d times, want once", topic, got)
		}
	}
}

// Stop must not leave goroutines behind: fetch loops, workers, committers and
// the relay all return.
func TestConsumerStopLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	h := newHarness(t, 0, nil)
	stop := h.start(queue.ConsumerConfig{Concurrency: 20}, nilHandler)
	for i := range 5 {
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, int64(i), &engine.Task{ExecutionID: "e"}, "ns"))
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 4)
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

// sizeWriter is a messageWriter that refuses messages for a decided reason.
// reject is called with each message and returns the error to refuse it with,
// or nil to accept; accepted messages are recorded on wrote.
type sizeWriter struct {
	mu     sync.Mutex
	reject func(kafkago.Message) error
	msgs   []kafkago.Message
	tried  []kafkago.Message
	wrote  chan kafkago.Message
}

func newSizeWriter(reject func(kafkago.Message) error) *sizeWriter {
	return &sizeWriter{reject: reject, wrote: make(chan kafkago.Message, 64)}
}

func (w *sizeWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range msgs {
		w.tried = append(w.tried, m)
		if err := w.reject(m); err != nil {
			return err
		}
		w.msgs = append(w.msgs, m)
		w.wrote <- m
	}
	return ctx.Err()
}

func (w *sizeWriter) Close() error { return nil }

func (w *sizeWriter) attempts() []kafkago.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kafkago.Message(nil), w.tried...)
}

var errTooLarge = kafkago.MessageTooLargeError{}

// sizeHarness is a harness whose writer is a sizeWriter.
func sizeHarness(t *testing.T, reject func(kafkago.Message) error) (*harness, *sizeWriter) {
	t.Helper()
	h := newHarness(t, 0, nil)
	w := newSizeWriter(reject)
	h.tr.writer = w
	return h, w
}

// tooLargeFor refuses every message written to one of the given topics.
func tooLargeFor(topics ...string) func(kafkago.Message) error {
	return func(m kafkago.Message) error {
		if slices.Contains(topics, m.Topic) {
			return errTooLarge
		}
		return nil
	}
}

func recvWrite(t *testing.T, w *sizeWriter) kafkago.Message {
	t.Helper()
	return recv(t, w.wrote, "a message to be written")
}

// A retry copy the broker refuses as too large is dead-lettered instead, with
// the reason in the error header, and the original is committed.
func TestConsumerRetryCopyTooLargeGoesToDLQ(t *testing.T) {
	h, w := sizeHarness(t, nil)
	delay := h.names.Delay()
	w.reject = tooLargeFor(delay)
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("downstream 503")))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 1, 8, &engine.Task{ExecutionID: "e"}, "ns"))

	m := recvWrite(t, w)
	if m.Topic != h.names.DLQ() {
		t.Fatalf("written to %q, want the DLQ", m.Topic)
	}
	e := hdrOf(t, m, hdrError)
	if !strings.Contains(e, "retry copy too large") || !strings.Contains(e, "downstream 503") {
		t.Errorf("error header = %q, want the reason and the handler error", e)
	}
	if got := hdrOf(t, m, hdrSource); got != h.names.Default() {
		t.Errorf("source = %q, want %q", got, h.names.Default())
	}
	h.reader(h.names.Default()).waitCommitted(t, 1, 8)
	for _, a := range w.attempts() {
		if a.Topic == delay && len(h.p.paused) != 0 {
			t.Error("a too-large write was retried with a pause")
		}
	}
}

// The oversized DLQ copy is retried once with only key, value, source and a
// 128-byte error.
func TestConsumerDLQCopyTooLargeFallsBackToMinimalCopy(t *testing.T) {
	h, w := sizeHarness(t, nil)
	w.reject = func(m kafkago.Message) error {
		// The full copy carries the long error; the reduced one is shorter.
		if m.Topic == h.names.DLQ() && len(hdrValueOrEmpty(m, hdrError)) > minimalDLQBytes {
			return errTooLarge
		}
		return nil
	}
	long := strings.Repeat("e", 1000)
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(fmt.Errorf("%w: %s", types.ErrPermanent, long)))
	orig := taskMsg(t, h.names.Default(), 0, 3, &engine.Task{ExecutionID: "e"}, "ns", hdr("keep", "me"), hdr(hdrAttempt, "2"))
	h.feed(h.names.Default(), orig)

	m := recvWrite(t, w)
	if m.Topic != h.names.DLQ() {
		t.Fatalf("written to %q, want the DLQ", m.Topic)
	}
	if string(m.Key) != string(orig.Key) || string(m.Value) != string(orig.Value) {
		t.Error("the reduced copy lost its key or value")
	}
	if got := hdrOf(t, m, hdrSource); got != h.names.Default() {
		t.Errorf("source = %q, want %q", got, h.names.Default())
	}
	if e := hdrOf(t, m, hdrError); len(e) != minimalDLQBytes {
		t.Errorf("error header is %d bytes, want it cut to %d", len(e), minimalDLQBytes)
	}
	if len(m.Headers) != 2 {
		t.Errorf("headers = %+v, want only source and error", m.Headers)
	}
	h.reader(h.names.Default()).waitCommitted(t, 0, 3)
	if n := len(w.attempts()); n != 2 {
		t.Errorf("%d write attempts, want the full copy and the reduced copy", n)
	}
}

func hdrValueOrEmpty(m kafkago.Message, key string) string {
	v, _ := headerValue(m, key)
	return v
}

// When even the reduced copy is refused the message is abandoned: it is logged
// without its value and committed, so it does not hold the partition.
func TestConsumerDLQUnwritableIsLoggedAndAbandoned(t *testing.T) {
	h, w := sizeHarness(t, nil)
	w.reject = tooLargeFor(h.names.DLQ())
	log := &errorLogger{}
	h.tr.logger = log
	h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(fmt.Errorf("%w: no", types.ErrPermanent)))
	secret := &engine.Task{ExecutionID: "exec-secret-value", NodeName: "n"}
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 4, 21, secret, "ns"))

	h.reader(h.names.Default()).waitCommitted(t, 4, 21)
	if n := len(w.attempts()); n != 2 {
		t.Errorf("%d write attempts, want the full and the reduced copy", n)
	}
	errs := log.errored()
	if len(errs) != 1 {
		t.Fatalf("%d error log lines, want 1: %+v", len(errs), errs)
	}
	got := fmt.Sprint(errs[0].args...)
	for _, want := range []string{h.names.Default(), "4", "21", "exec-secret-value"} {
		if !strings.Contains(got, want) {
			t.Errorf("log %q does not mention %q", got, want)
		}
	}
	if strings.Contains(got, "NodeName") || strings.Contains(got, string(taskMsg(t, h.names.Default(), 0, 0, secret, "ns").Value)) {
		t.Errorf("log %q includes the message value", got)
	}
}

// errorLogger is an engine.Logger that keeps only the Error calls.
type errorLogger struct {
	mu   sync.Mutex
	errs []warnRecord
}

func (l *errorLogger) Debug(string, ...any)  {}
func (l *errorLogger) Debugf(string, ...any) {}
func (l *errorLogger) Info(string, ...any)   {}
func (l *errorLogger) Infof(string, ...any)  {}
func (l *errorLogger) Warn(string, ...any)   {}
func (l *errorLogger) Warnf(string, ...any)  {}
func (l *errorLogger) Errorf(string, ...any) {}
func (l *errorLogger) Panic(string, ...any)  {}
func (l *errorLogger) Panicf(string, ...any) {}
func (l *errorLogger) Error(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, warnRecord{msg: msg, args: args})
}

func (l *errorLogger) errored() []warnRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]warnRecord(nil), l.errs...)
}

var _ engine.Logger = (*errorLogger)(nil)

// The broker code and a WriteErrors entry count as too large, not only the
// writer's own error type.
func TestConsumerTooLargeRecognisedInEveryShape(t *testing.T) {
	shapes := map[string]error{
		"broker code":        kafkago.MessageSizeTooLarge,
		"write errors entry": kafkago.WriteErrors{nil, errTooLarge},
		"wrapped":            fmt.Errorf("produce: %w", errTooLarge),
	}
	for name, shape := range shapes {
		h, w := sizeHarness(t, nil)
		w.reject = func(m kafkago.Message) error {
			if m.Topic == h.names.Delay() {
				return shape
			}
			return nil
		}
		h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("boom")))
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
		if m := recvWrite(t, w); m.Topic != h.names.DLQ() {
			t.Errorf("%s: written to %q, want the DLQ", name, m.Topic)
		}
		h.reader(h.names.Default()).waitCommitted(t, 0, 0)
	}
}

// Anything but a size problem is a deployment problem: it is retried, never
// turned into a dead letter, and nothing is committed meanwhile.
func TestConsumerOtherPublishErrorsKeepRetrying(t *testing.T) {
	for name, cause := range map[string]error{
		"unknown topic": kafkago.UnknownTopicOrPartition,
		"auth":          kafkago.TopicAuthorizationFailed,
		"network":       timeoutNetErr{timeout: true},
	} {
		h, w := sizeHarness(t, nil)
		h.tr.cfg.ShutdownTimeout = 50 * time.Millisecond // the retry loop only ends when stop cancels it
		h.p.gate = make(chan struct{})
		w.reject = func(kafkago.Message) error { return cause }
		h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(errors.New("boom")))
		h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
		for range 3 {
			recv(t, h.p.paused, "a retry pause")
			h.p.gate <- struct{}{}
		}
		if got := h.reader(h.names.Default()).commitCalls(); len(got) != 0 {
			t.Errorf("%s: committed %v while the publish kept failing", name, got)
		}
		for _, a := range w.attempts() {
			if a.Topic != h.names.Delay() {
				t.Errorf("%s: wrote to %q, want only retries of the delay copy", name, a.Topic)
			}
		}
	}
}

// Stopping while the dead-letter write is still being retried must not commit.
func TestConsumerStopDuringDLQRetryDoesNotCommit(t *testing.T) {
	h, w := sizeHarness(t, func(kafkago.Message) error { return kafkago.LeaderNotAvailable })
	_ = w
	h.tr.cfg.ShutdownTimeout = 50 * time.Millisecond
	h.p.gate = make(chan struct{})
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, failWith(fmt.Errorf("%w: no", types.ErrPermanent)))
	h.feed(h.names.Default(), taskMsg(t, h.names.Default(), 0, 0, &engine.Task{ExecutionID: "e"}, "ns"))
	recv(t, h.p.paused, "the first DLQ retry pause")
	stop()
	if got := h.reader(h.names.Default()).commitCalls(); len(got) != 0 {
		t.Errorf("committed %v although the dead letter was never written", got)
	}
}
