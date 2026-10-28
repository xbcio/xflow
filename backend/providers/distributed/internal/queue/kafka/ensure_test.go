package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	"github.com/segmentio/kafka-go/protocol/metadata"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// fakeAdmin is a topicAdmin over an in-memory cluster. A topic it creates shows
// up as missing for lag more Topics calls, which models metadata propagation.
type fakeAdmin struct {
	mu    sync.Mutex
	state map[string]topicInfo

	createErrs map[string]error // per-topic create outcome
	createErr  error            // fails the whole create call
	topicsErr  error            // fails every Topics call
	lag        int
	block      bool          // Topics waits for ctx instead of answering
	entered    chan struct{} // receives one value when a blocked Topics call starts

	pending     map[string]int
	topicsCalls int
	creates     []kafkago.TopicConfig
	closes      int
	deadline    time.Time // deadline of the last Topics call's ctx
}

func newFakeAdmin(exist ...string) *fakeAdmin {
	a := &fakeAdmin{state: map[string]topicInfo{}, pending: map[string]int{}}
	for _, name := range exist {
		a.state[name] = topicInfo{Partitions: 3}
	}
	return a
}

func (a *fakeAdmin) Topics(ctx context.Context, names []string) (map[string]topicInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.topicsCalls++
	a.deadline, _ = ctx.Deadline()
	if a.block {
		a.mu.Unlock()
		if a.entered != nil {
			a.entered <- struct{}{}
		}
		<-ctx.Done()
		a.mu.Lock()
		return nil, ctx.Err()
	}
	if a.topicsErr != nil {
		return nil, a.topicsErr
	}
	out := map[string]topicInfo{}
	for _, name := range names {
		if info, ok := a.state[name]; ok && a.pending[name] == 0 {
			out[name] = info
		} else {
			out[name] = topicInfo{Err: kafkago.UnknownTopicOrPartition}
		}
	}
	for name, n := range a.pending {
		a.pending[name] = max(n-1, 0)
	}
	return out, nil
}

func (a *fakeAdmin) CreateTopics(_ context.Context, topics []kafkago.TopicConfig) (map[string]error, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.creates = append(a.creates, topics...)
	if a.createErr != nil {
		return nil, a.createErr
	}
	out := map[string]error{}
	for _, tc := range topics {
		err := a.createErrs[tc.Topic]
		out[tc.Topic] = err
		if err == nil || errors.Is(err, kafkago.TopicAlreadyExists) {
			if _, exists := a.state[tc.Topic]; !exists {
				a.state[tc.Topic] = topicInfo{Partitions: tc.NumPartitions}
				a.pending[tc.Topic] = a.lag
			}
		}
	}
	return out, nil
}

func (a *fakeAdmin) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closes++
}

func (a *fakeAdmin) created() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var names []string
	for _, tc := range a.creates {
		names = append(names, tc.Topic)
	}
	slices.Sort(names)
	return names
}

func (a *fakeAdmin) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.topicsCalls
}

// ensureHarness builds a Transport over a fake admin whose polling pause is
// recorded instead of slept.
type ensureHarness struct {
	t      *testing.T
	tr     *Transport
	admin  *fakeAdmin
	log    *recordingLogger
	names  []string
	pauses []time.Duration
	// onPause, if set, runs after each recorded pause with the pause count. A
	// test uses it to make time pass: cancelling the context ends the wait.
	onPause func(n int)
}

func newEnsureHarness(t *testing.T, cfg Config, admin *fakeAdmin) *ensureHarness {
	t.Helper()
	if cfg.Brokers == nil {
		cfg.Brokers = []string{"b:9092"}
	}
	h := &ensureHarness{t: t, admin: admin, log: &recordingLogger{}}
	tr, err := New(cfg, "", withWriter(&fakeWriter{}), withTopicAdmin(admin), WithLogger(h.log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tr.pause = func(ctx context.Context, d time.Duration) bool {
		h.pauses = append(h.pauses, d)
		if h.onPause != nil {
			h.onPause(len(h.pauses))
		}
		return ctx.Err() == nil
	}
	h.tr = tr
	h.names = tr.names.All()
	return h
}

func createCfg() Config { return Config{CreateTopics: true, Partitions: 3, ReplicationFactor: 1} }

func TestEnsureTopics_AllPresentDoesNothing(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	for _, n := range h.names {
		a.state[n] = topicInfo{Partitions: 3}
	}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	if got := a.created(); len(got) != 0 {
		t.Errorf("created %v, want nothing", got)
	}
	if len(h.pauses) != 0 || a.calls() != 1 {
		t.Errorf("pauses %v, describe calls %d; want one describe and no waiting", h.pauses, a.calls())
	}
	if a.closes == 0 {
		t.Error("EnsureTopics did not release the admin's connections")
	}
}

func TestEnsureTopics_MissingWithoutCreateReportsSortedNames(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	a.state[h.tr.names.Batch()] = topicInfo{Partitions: 3}
	err := h.tr.EnsureTopics(t.Context())
	if err == nil {
		t.Fatal("EnsureTopics = nil, want an error for the missing topics")
	}
	want := []string{h.tr.names.Default(), h.tr.names.Delay(), h.tr.names.DLQ()}
	slices.Sort(want)
	if !strings.Contains(err.Error(), strings.Join(want, ", ")) {
		t.Errorf("error %q does not list %v sorted", err, want)
	}
	if !strings.Contains(err.Error(), "CreateTopics") {
		t.Errorf("error %q does not point at CreateTopics", err)
	}
	if got := a.created(); len(got) != 0 {
		t.Errorf("created %v although CreateTopics is off", got)
	}
}

func TestEnsureTopics_CreatesOnlyTheMissingOnes(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, createCfg(), a)
	existing := []string{h.tr.names.Default(), h.tr.names.DLQ()}
	for _, n := range existing {
		a.state[n] = topicInfo{Partitions: 3}
	}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	want := []string{h.tr.names.Batch(), h.tr.names.Delay()}
	slices.Sort(want)
	if got := a.created(); !slices.Equal(got, want) {
		t.Errorf("created %v, want only the missing %v", got, want)
	}
	for _, tc := range a.creates {
		if tc.NumPartitions != 3 || tc.ReplicationFactor != 1 {
			t.Errorf("topic %s created with partitions %d replication %d, want the configured 3 and 1",
				tc.Topic, tc.NumPartitions, tc.ReplicationFactor)
		}
	}
}

func TestEnsureTopics_AlreadyExistsIsTolerated(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, createCfg(), a)
	// Another runner won the race for every topic.
	a.createErrs = map[string]error{}
	for _, n := range h.names {
		a.createErrs[n] = kafkago.TopicAlreadyExists
	}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics = %v, want TopicAlreadyExists treated as success", err)
	}
}

func TestEnsureTopics_CreateFailureNamesTheTopic(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, createCfg(), a)
	a.createErrs = map[string]error{
		h.tr.names.Delay(): kafkago.TopicAuthorizationFailed,
		h.tr.names.DLQ():   kafkago.InvalidReplicationFactor,
	}
	err := h.tr.EnsureTopics(t.Context())
	if err == nil {
		t.Fatal("EnsureTopics = nil, want the create failures")
	}
	for _, n := range []string{h.tr.names.Delay(), h.tr.names.DLQ()} {
		if !strings.Contains(err.Error(), n) {
			t.Errorf("error %q does not name %s", err, n)
		}
	}
	if !errors.Is(err, kafkago.TopicAuthorizationFailed) || !errors.Is(err, kafkago.InvalidReplicationFactor) {
		t.Errorf("error %v does not wrap both broker codes", err)
	}
	if strings.Contains(err.Error(), h.tr.names.Default()) {
		t.Errorf("error %q names a topic that was created fine", err)
	}
}

func TestEnsureTopics_WholeCreateCallFailing(t *testing.T) {
	a := newFakeAdmin()
	a.createErr = errors.New("connection refused")
	h := newEnsureHarness(t, createCfg(), a)
	err := h.tr.EnsureTopics(t.Context())
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("EnsureTopics = %v, want the call's error", err)
	}
}

// A topic that was just created is not visible at once. The wait polls with a
// growing pause and succeeds as soon as the cluster reports it.
func TestEnsureTopics_CreatedTopicAppearsLate(t *testing.T) {
	a := newFakeAdmin()
	a.lag = 3
	h := newEnsureHarness(t, createCfg(), a)
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	want := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	if !slices.Equal(h.pauses, want) {
		t.Errorf("pauses = %v, want %v", h.pauses, want)
	}
	if len(a.creates) != 4 {
		t.Errorf("created %d topics, want 4: a late topic must not be created again", len(a.creates))
	}
}

func TestEnsureTopics_PollPauseIsCapped(t *testing.T) {
	a := newFakeAdmin()
	a.lag = 8
	h := newEnsureHarness(t, createCfg(), a)
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	for _, d := range h.pauses {
		if d > 2*time.Second {
			t.Fatalf("pauses = %v, want none above 2s", h.pauses)
		}
	}
	if last := h.pauses[len(h.pauses)-1]; last != 2*time.Second {
		t.Errorf("pauses = %v, want them to settle at 2s", h.pauses)
	}
}

// A topic that exists but has no leader yet is waited for, not reported as
// missing and not created again.
func TestEnsureTopics_WaitsForLeaderlessTopic(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	for _, n := range h.names {
		a.state[n] = topicInfo{Partitions: 3}
	}
	a.state[h.tr.names.Batch()] = topicInfo{Partitions: 3, Err: kafkago.LeaderNotAvailable}
	h.onPause = func(n int) {
		if n == 2 {
			a.mu.Lock()
			a.state[h.tr.names.Batch()] = topicInfo{Partitions: 3}
			a.mu.Unlock()
		}
	}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}
	if len(h.pauses) != 2 || len(a.creates) != 0 {
		t.Errorf("pauses %v, creates %v; want two polls and no creation", h.pauses, a.created())
	}
}

// Cancelling the context ends the wait, and the error says which topics were
// still not ready.
func TestEnsureTopics_NeverAppearingTimesOut(t *testing.T) {
	a := newFakeAdmin()
	a.lag = 1 << 20
	h := newEnsureHarness(t, createCfg(), a)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.onPause = func(n int) {
		if n == 5 {
			cancel()
		}
	}
	err := h.tr.EnsureTopics(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureTopics = %v, want it to wrap context.Canceled", err)
	}
	for _, n := range h.names {
		if !strings.Contains(err.Error(), n) {
			t.Errorf("error %q does not name the missing topic %s", err, n)
		}
	}
}

func TestEnsureTopics_ContextCancelReturnsPromptly(t *testing.T) {
	a := newFakeAdmin()
	a.block = true
	a.entered = make(chan struct{}, 1)
	h := newEnsureHarness(t, Config{}, a)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.tr.EnsureTopics(ctx) }()
	recv(t, a.entered, "the describe call to start")
	cancel()
	if err := recv(t, done, "EnsureTopics to return after cancel"); !errors.Is(err, context.Canceled) {
		t.Errorf("EnsureTopics = %v, want it to wrap context.Canceled", err)
	}
}

func TestEnsureTopics_AlreadyCancelledContextTouchesNothing(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, createCfg(), a)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.tr.EnsureTopics(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("EnsureTopics = %v, want context.Canceled", err)
	}
	if a.calls() != 0 || len(a.creates) != 0 {
		t.Errorf("a cancelled context still reached the cluster: %d describes, %d creates", a.calls(), len(a.creates))
	}
}

// With no deadline of its own, the call bounds itself to 30 seconds; a deadline
// the caller set is left alone.
func TestEnsureTopics_DeadlineBounds(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	for _, n := range h.names {
		a.state[n] = topicInfo{Partitions: 3}
	}
	before := time.Now()
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	got := a.deadline
	a.mu.Unlock()
	if got.IsZero() || got.Sub(before) < 29*time.Second || got.Sub(before) > 31*time.Second {
		t.Errorf("deadline %v is not ~30s after the call", got.Sub(before))
	}

	own, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	wantDeadline, _ := own.Deadline()
	if err := h.tr.EnsureTopics(own); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	got = a.deadline
	a.mu.Unlock()
	if !got.Equal(wantDeadline) {
		t.Errorf("deadline = %v, want the caller's %v", got, wantDeadline)
	}
}

// A topic that already exists with another partition count is only warned
// about: never changed, never an error.
func TestEnsureTopics_PartitionDriftWarnsOnly(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{CreateTopics: true, Partitions: 6, ReplicationFactor: 1}, a)
	a.state[h.tr.names.Default()] = topicInfo{Partitions: 3}
	a.state[h.tr.names.Batch()] = topicInfo{Partitions: 6}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatalf("EnsureTopics = %v, want a partition mismatch to be no error", err)
	}
	warns := h.log.warned()
	if len(warns) != 1 {
		t.Fatalf("warnings = %+v, want exactly one, for the topic that differs", warns)
	}
	if !slices.Contains(warns[0].args, any(h.tr.names.Default())) {
		t.Errorf("warning args %v do not name %s", warns[0].args, h.tr.names.Default())
	}
	for _, tc := range a.creates {
		if tc.Topic == h.tr.names.Default() || tc.Topic == h.tr.names.Batch() {
			t.Errorf("existing topic %s was sent to CreateTopics", tc.Topic)
		}
	}
	if got := a.state[h.tr.names.Default()].Partitions; got != 3 {
		t.Errorf("existing topic now has %d partitions, want it untouched at 3", got)
	}
}

// Without CreateTopics there is no configured count to compare against.
func TestEnsureTopics_NoDriftWarningWithoutConfiguredCount(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	for _, n := range h.names {
		a.state[n] = topicInfo{Partitions: 9}
	}
	if err := h.tr.EnsureTopics(t.Context()); err != nil {
		t.Fatal(err)
	}
	if warns := h.log.warned(); len(warns) != 0 {
		t.Errorf("warnings = %+v, want none", warns)
	}
}

// A broker refusal that waiting cannot fix is reported at once, not polled.
func TestEnsureTopics_PermanentDescribeRefusalFailsFast(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, createCfg(), a)
	for _, n := range h.names {
		a.state[n] = topicInfo{Partitions: 3}
	}
	a.state[h.tr.names.Delay()] = topicInfo{Err: kafkago.TopicAuthorizationFailed}
	err := h.tr.EnsureTopics(t.Context())
	if !errors.Is(err, kafkago.TopicAuthorizationFailed) || !strings.Contains(err.Error(), h.tr.names.Delay()) {
		t.Errorf("EnsureTopics = %v, want the refusal naming %s", err, h.tr.names.Delay())
	}
	if len(h.pauses) != 0 {
		t.Errorf("polled %d times for a refusal that cannot clear", len(h.pauses))
	}
}

func TestEnsureTopics_DescribeFailureIsReturned(t *testing.T) {
	a := newFakeAdmin()
	a.topicsErr = errors.New("dial tcp: refused")
	h := newEnsureHarness(t, createCfg(), a)
	err := h.tr.EnsureTopics(t.Context())
	if err == nil || !strings.Contains(err.Error(), "dial tcp: refused") {
		t.Errorf("EnsureTopics = %v, want the describe error", err)
	}
	if len(a.creates) != 0 {
		t.Errorf("created %v after a describe failure", a.created())
	}
}

func TestTransportClose_ReleasesAdmin(t *testing.T) {
	a := newFakeAdmin()
	h := newEnsureHarness(t, Config{}, a)
	if err := h.tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.tr.Close(); err != nil {
		t.Fatal(err)
	}
	if a.closes != 1 {
		t.Errorf("admin closed %d times, want once: Close is idempotent", a.closes)
	}
}

// New and StartConsumer never contact the cluster for topic management; the
// caller decides when to run EnsureTopics.
func TestNewAndStartConsumerDoNotTouchTopics(t *testing.T) {
	a := newFakeAdmin()
	h := newHarness(t, 0, func(c *Config) {
		c.CreateTopics, c.Partitions, c.ReplicationFactor = true, 3, 1
	})
	h.tr.admin = a
	stop := h.start(queue.ConsumerConfig{Concurrency: 2}, func(context.Context, *engine.Task) error { return nil })
	stop()
	if a.calls() != 0 || len(a.creates) != 0 {
		t.Errorf("starting the consumer reached the cluster: %d describes, %d creates", a.calls(), len(a.creates))
	}
}

// stubRoundTripper answers admin requests and records what the admin sent.
type stubRoundTripper struct {
	mu       sync.Mutex
	requests []protocol.Message
	topics   []metadata.ResponseTopic
	created  []createtopics.ResponseTopic
	err      error
}

func (r *stubRoundTripper) RoundTrip(_ context.Context, _ net.Addr, req kafkago.Request) (kafkago.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	if r.err != nil {
		return nil, r.err
	}
	switch req.(type) {
	case *metadata.Request:
		return &metadata.Response{Topics: r.topics}, nil
	case *createtopics.Request:
		return &createtopics.Response{Topics: r.created}, nil
	}
	return nil, errors.New("unexpected request")
}

func newStubAdmin(rt *stubRoundTripper) *clientAdmin {
	return &clientAdmin{
		client:    &kafkago.Client{Addr: kafkago.TCP("b:9092"), Transport: rt},
		transport: &kafkago.Transport{},
	}
}

// Describing topics must not ask the broker to create them.
func TestClientAdmin_TopicsNeverAsksForAutoCreation(t *testing.T) {
	rt := &stubRoundTripper{topics: []metadata.ResponseTopic{{Name: "t"}}}
	a := newStubAdmin(rt)
	if _, err := a.Topics(t.Context(), []string{"t", "u"}); err != nil {
		t.Fatal(err)
	}
	if len(rt.requests) != 1 {
		t.Fatalf("sent %d requests, want one metadata request", len(rt.requests))
	}
	req, ok := rt.requests[0].(*metadata.Request)
	if !ok {
		t.Fatalf("request is %T, want a metadata request", rt.requests[0])
	}
	if req.AllowAutoTopicCreation {
		t.Error("metadata request has AllowAutoTopicCreation set; describing a topic would create it")
	}
	if !slices.Equal(req.TopicNames, []string{"t", "u"}) {
		t.Errorf("TopicNames = %v, want the two requested topics", req.TopicNames)
	}
}

func TestClientAdmin_TopicsReportsMissingLeaderlessAndReady(t *testing.T) {
	rt := &stubRoundTripper{topics: []metadata.ResponseTopic{
		{Name: "ready", Partitions: []metadata.ResponsePartition{{PartitionIndex: 0}, {PartitionIndex: 1}}},
		{Name: "missing", ErrorCode: int16(kafkago.UnknownTopicOrPartition)},
		{Name: "leaderless", Partitions: []metadata.ResponsePartition{
			{PartitionIndex: 0}, {PartitionIndex: 1, ErrorCode: int16(kafkago.LeaderNotAvailable)},
		}},
	}}
	got, err := newStubAdmin(rt).Topics(t.Context(), []string{"ready", "missing", "leaderless"})
	if err != nil {
		t.Fatal(err)
	}
	if got["ready"].Err != nil || got["ready"].Partitions != 2 {
		t.Errorf("ready = %+v, want 2 partitions and no error", got["ready"])
	}
	if !errors.Is(got["missing"].Err, kafkago.UnknownTopicOrPartition) {
		t.Errorf("missing = %+v, want UnknownTopicOrPartition", got["missing"])
	}
	if !errors.Is(got["leaderless"].Err, kafkago.LeaderNotAvailable) {
		t.Errorf("leaderless = %+v, want the partition's LeaderNotAvailable", got["leaderless"])
	}
}

func TestClientAdmin_CreateTopicsSendsConfigAndMapsErrors(t *testing.T) {
	rt := &stubRoundTripper{created: []createtopics.ResponseTopic{
		{Name: "a"},
		{Name: "b", ErrorCode: int16(kafkago.TopicAlreadyExists)},
	}}
	got, err := newStubAdmin(rt).CreateTopics(t.Context(), []kafkago.TopicConfig{
		{Topic: "a", NumPartitions: 4, ReplicationFactor: 2},
		{Topic: "b", NumPartitions: 4, ReplicationFactor: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != nil || !errors.Is(got["b"], kafkago.TopicAlreadyExists) {
		t.Errorf("results = %v, want a created and b already existing", got)
	}
	req := rt.requests[0].(*createtopics.Request)
	if len(req.Topics) != 2 || req.Topics[0].NumPartitions != 4 || req.Topics[0].ReplicationFactor != 2 || req.ValidateOnly {
		t.Errorf("create request = %+v, want both topics with 4 partitions and 2 replicas", req)
	}
}

func TestClientAdmin_RoundTripErrorIsReturned(t *testing.T) {
	a := newStubAdmin(&stubRoundTripper{err: errors.New("broker down")})
	if _, err := a.Topics(t.Context(), []string{"t"}); err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Errorf("Topics = %v, want the transport error", err)
	}
	if _, err := a.CreateTopics(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Errorf("CreateTopics = %v, want the transport error", err)
	}
}

func TestNewClientAdmin_CarriesSecurityAndTimeout(t *testing.T) {
	a, err := newClientAdmin(Config{
		Brokers: []string{"b:9092"}, SASLMechanism: "plain", SASLUsername: "u", SASLPassword: "p",
		TLS: &tls.Config{MinVersion: tls.VersionTLS12}, WriteTimeout: 7 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.transport.TLS == nil || a.transport.SASL == nil {
		t.Errorf("transport TLS=%v SASL=%v, want both carried over", a.transport.TLS, a.transport.SASL)
	}
	if a.client.Timeout != 7*time.Second || a.client.Transport != a.transport {
		t.Errorf("client timeout %v, transport shared %v", a.client.Timeout, a.client.Transport == a.transport)
	}
	if def, _ := newClientAdmin(Config{Brokers: []string{"b:9092"}}); def.client.Timeout != 10*time.Second {
		t.Errorf("default admin timeout = %v, want the 10s write timeout", def.client.Timeout)
	}
}
