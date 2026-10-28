package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
)

// fakeBroker is an in-memory stand-in for the three broker calls the lag
// sampler makes. logs holds each topic's partitions, commits the offsets each
// group has committed; the err fields make a call, or one topic of it, fail.
type fakeBroker struct {
	mu      sync.Mutex
	logs    map[string][]partitionRange
	commits map[string]map[string]map[int]int64 // group -> topic -> partition -> offset

	metaErr  error
	listErr  error
	fetchErr error
	// topicErr fails the topic inside the Metadata response; partErr fails one
	// partition inside the ListOffsets response; fetchTopicErr and fetchPartErr do
	// the same for OffsetFetch; unanswered leaves the first n partitions of a
	// topic out of the ListOffsets response; blank answers them with -1, which is
	// what kafka-go leaves where the broker gave no offset.
	topicErr      map[string]error
	partErr       map[string]map[int]error
	fetchTopicErr map[string]error
	fetchPartErr  map[string]map[int]error
	unanswered    map[string]int
	blank         map[string]int

	// metaHook, if set, runs at the top of Metadata.
	metaHook  func(ctx context.Context)
	metaCalls atomic.Int32
	closes    atomic.Int32
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		logs:          map[string][]partitionRange{},
		commits:       map[string]map[string]map[int]int64{},
		topicErr:      map[string]error{},
		partErr:       map[string]map[int]error{},
		fetchTopicErr: map[string]error{},
		fetchPartErr:  map[string]map[int]error{},
		unanswered:    map[string]int{},
		blank:         map[string]int{},
	}
}

// commit records that group has committed offset on a partition of topic.
func (b *fakeBroker) commit(group, topic string, partition int, offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.commits[group] == nil {
		b.commits[group] = map[string]map[int]int64{}
	}
	if b.commits[group][topic] == nil {
		b.commits[group][topic] = map[int]int64{}
	}
	b.commits[group][topic][partition] = offset
}

func (b *fakeBroker) Metadata(ctx context.Context, req *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
	b.metaCalls.Add(1)
	if b.metaHook != nil {
		b.metaHook(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.metaErr != nil {
		return nil, b.metaErr
	}
	res := &kafkago.MetadataResponse{}
	for _, name := range req.Topics {
		tp := kafkago.Topic{Name: name, Error: b.topicErr[name]}
		if parts, ok := b.logs[name]; ok {
			for id := range parts {
				tp.Partitions = append(tp.Partitions, kafkago.Partition{Topic: name, ID: id})
			}
		} else if tp.Error == nil {
			tp.Error = kafkago.UnknownTopicOrPartition
		}
		res.Topics = append(res.Topics, tp)
	}
	return res, nil
}

func (b *fakeBroker) ListOffsets(_ context.Context, req *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listErr != nil {
		return nil, b.listErr
	}
	res := &kafkago.ListOffsetsResponse{Topics: map[string][]kafkago.PartitionOffsets{}}
	for topic, reqs := range req.Topics {
		seen := map[int]bool{}
		for _, r := range reqs {
			if seen[r.Partition] {
				continue
			}
			seen[r.Partition] = true
			if r.Partition < b.unanswered[topic] {
				continue
			}
			po := kafkago.PartitionOffsets{Partition: r.Partition, Error: b.partErr[topic][r.Partition]}
			if r.Partition < b.blank[topic] {
				po.FirstOffset, po.LastOffset = -1, -1
			} else {
				po.FirstOffset, po.LastOffset = b.logs[topic][r.Partition].first, b.logs[topic][r.Partition].end
			}
			res.Topics[topic] = append(res.Topics[topic], po)
		}
	}
	return res, nil
}

func (b *fakeBroker) OffsetFetch(_ context.Context, req *kafkago.OffsetFetchRequest) (*kafkago.OffsetFetchResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fetchErr != nil {
		return nil, b.fetchErr
	}
	res := &kafkago.OffsetFetchResponse{Topics: map[string][]kafkago.OffsetFetchPartition{}}
	for topic, parts := range req.Topics {
		if err := b.fetchTopicErr[topic]; err != nil {
			res.Error = err
			continue
		}
		for _, p := range parts {
			// The broker answers -1 for a partition the group has not committed on.
			off, ok := b.commits[req.GroupID][topic][p]
			if !ok {
				off = -1
			}
			res.Topics[topic] = append(res.Topics[topic], kafkago.OffsetFetchPartition{
				Partition: p, CommittedOffset: off, Error: b.fetchPartErr[topic][p]})
		}
	}
	return res, nil
}

func (b *fakeBroker) Close() { b.closes.Add(1) }

var _ offsetReader = (*fakeBroker)(nil)

// lagObserver records what the sampler reports, in order.
type lagObserver struct {
	mu   sync.Mutex
	lags []lagReport
	dlq  []int64
}

type lagReport struct {
	role string
	lag  int64
}

// OnEnqueue makes lagObserver a queue.Observer, which is what a transport takes.
func (*lagObserver) OnEnqueue(string, time.Duration, error) {}

func (o *lagObserver) OnKafkaLag(role string, lag int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lags = append(o.lags, lagReport{role, lag})
}

func (o *lagObserver) OnKafkaDLQDepth(depth int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.dlq = append(o.dlq, depth)
}

// reports returns the lag reports and DLQ depths so far, as copies.
func (o *lagObserver) reports() ([]lagReport, []int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.lags), slices.Clone(o.dlq)
}

// byRole is the last lag reported per role.
func (o *lagObserver) byRole() map[string]int64 {
	lags, _ := o.reports()
	out := map[string]int64{}
	for _, r := range lags {
		out[r.role] = r.lag
	}
	return out
}

// Mutation: using 0 instead of the first retained offset as the position of a
// partition the group never committed on turns the "uncommitted" rows red.
func TestGroupLag(t *testing.T) {
	cases := []struct {
		name       string
		ranges     map[int]partitionRange
		committed  map[int]int64
		fromLatest bool
		want       int64
	}{
		{"caught up", map[int]partitionRange{0: {0, 10}}, map[int]int64{0: 10}, false, 0},
		{"behind", map[int]partitionRange{0: {0, 10}, 1: {0, 7}}, map[int]int64{0: 4, 1: 7}, false, 6},
		{"uncommitted counts from the first retained offset", map[int]partitionRange{0: {100, 130}}, map[int]int64{}, false, 30},
		{"uncommitted negative is the same", map[int]partitionRange{0: {100, 130}}, map[int]int64{0: -1}, false, 30},
		{"uncommitted from latest has no backlog", map[int]partitionRange{0: {100, 130}}, map[int]int64{}, true, 0},
		{"committed wins over from latest", map[int]partitionRange{0: {100, 130}}, map[int]int64{0: 110}, true, 20},
		{"committed behind retention is taken as is", map[int]partitionRange{0: {100, 130}}, map[int]int64{0: 40}, false, 90},
		{"committed past the end never goes negative", map[int]partitionRange{0: {0, 10}}, map[int]int64{0: 12}, false, 0},
		{"empty topic", map[int]partitionRange{0: {0, 0}}, map[int]int64{}, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := groupLag(tc.ranges, tc.committed, tc.fromLatest); got != tc.want {
				t.Fatalf("groupLag = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRetained(t *testing.T) {
	got := retained(map[int]partitionRange{0: {5, 15}, 1: {0, 3}, 2: {9, 9}})
	if got != 13 {
		t.Fatalf("retained = %d, want 13", got)
	}
}

// samplerFixture is a sampler over a fake broker that holds, for the default
// deployment's topics, three partitions each: default 0..10 with the group
// committed at 4/0/-, batch empty, delay 5..9 uncommitted, dlq 2..5 on each.
type samplerFixture struct {
	b     *fakeBroker
	obs   *lagObserver
	s     *lagSampler
	names topicNames
	warns *recordingLogger
}

func newSamplerFixture(t *testing.T) *samplerFixture {
	t.Helper()
	names, err := newTopicNames("")
	if err != nil {
		t.Fatalf("topic names: %v", err)
	}
	f := &samplerFixture{b: newFakeBroker(), obs: &lagObserver{}, names: names, warns: &recordingLogger{}}
	f.b.logs[names.Default()] = []partitionRange{{0, 10}, {0, 10}, {0, 10}}
	f.b.logs[names.Batch()] = []partitionRange{{0, 0}, {0, 0}, {0, 0}}
	f.b.logs[names.Delay()] = []partitionRange{{5, 9}, {5, 9}, {5, 9}}
	f.b.logs[names.DLQ()] = []partitionRange{{2, 5}, {2, 5}, {2, 5}}
	f.b.commit(names.Group(groupDefault), names.Default(), 0, 4)
	f.b.commit(names.Group(groupDefault), names.Default(), 1, 0)
	f.s = &lagSampler{client: f.b, names: names, obs: f.obs, warn: f.warns.Warn}
	return f
}

// Sampling reports each consumed topic's group lag and the DLQ depth, and
// nothing else. Mutation: any change to how a partition's position is chosen
// (groupLag) or to which topics are sampled turns the numbers red.
func TestLagSamplerReportsLagAndDLQDepth(t *testing.T) {
	f := newSamplerFixture(t)
	f.s.sample(t.Context())

	lags, dlq := f.obs.reports()
	want := []lagReport{
		{groupDefault, (10 - 4) + (10 - 0) + (10 - 0)},
		{groupBatch, 0},
		{groupDelay, 3 * 4},
	}
	if !slices.Equal(lags, want) {
		t.Fatalf("lag reports = %v, want %v", lags, want)
	}
	if !slices.Equal(dlq, []int64{9}) {
		t.Fatalf("dlq reports = %v, want [9]", dlq)
	}
	if got := f.warns.warned(); len(got) != 0 {
		t.Fatalf("a clean round logged %v", got)
	}
}

// A topic the group has no offsets on counts from where its reader would start.
// Mutation: using 0 instead of the first retained offset turns the delay row red.
func TestLagSamplerUncommittedStartsAtFirstRetained(t *testing.T) {
	f := newSamplerFixture(t)
	f.s.sample(t.Context())
	if got := f.obs.byRole()[groupDelay]; got != 12 {
		t.Fatalf("delay lag = %d, want 12 (end 9 minus first retained 5, three partitions)", got)
	}
	f.s.fromLatest = true
	f.s.sample(t.Context())
	if got := f.obs.byRole()[groupDelay]; got != 0 {
		t.Fatalf("delay lag with StartFromLatest = %d, want 0", got)
	}
}

// A read that fails reports nothing for what it covers: a missing sample means
// "not measured", never zero. Mutation: reporting 0 (or any value) where a read
// failed turns every row red.
func TestLagSamplerFailedReadReportsNothing(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name string
		fail func(f *samplerFixture)
		// wantRoles are the lag roles still reported; wantDLQ whether the DLQ depth is.
		wantRoles []string
		wantDLQ   bool
	}{
		{"metadata call fails", func(f *samplerFixture) { f.b.metaErr = boom }, nil, false},
		{"list offsets call fails", func(f *samplerFixture) { f.b.listErr = boom }, nil, false},
		{"offset fetch call fails", func(f *samplerFixture) { f.b.fetchErr = boom }, nil, true},
		{"one topic's metadata errors", func(f *samplerFixture) { f.b.topicErr[f.names.Batch()] = boom },
			[]string{groupDefault, groupDelay}, true},
		{"the dlq topic's metadata errors", func(f *samplerFixture) { f.b.topicErr[f.names.DLQ()] = boom },
			[]string{groupDefault, groupBatch, groupDelay}, false},
		{"a topic does not exist", func(f *samplerFixture) { delete(f.b.logs, f.names.Delay()) },
			[]string{groupDefault, groupBatch}, true},
		{"one partition's offsets error", func(f *samplerFixture) { f.b.partErr[f.names.Default()] = map[int]error{1: boom} },
			[]string{groupBatch, groupDelay}, true},
		{"a partition's offsets are unanswered", func(f *samplerFixture) { f.b.unanswered[f.names.Delay()] = 1 },
			[]string{groupDefault, groupBatch}, true},
		{"a partition's offsets are blank", func(f *samplerFixture) { f.b.blank[f.names.DLQ()] = 1 },
			[]string{groupDefault, groupBatch, groupDelay}, false},
		{"one group's offset fetch is refused", func(f *samplerFixture) {
			f.b.fetchTopicErr[f.names.Batch()] = boom
		}, []string{groupDefault, groupDelay}, true},
		{"one group's partition fetch errors", func(f *samplerFixture) {
			f.b.fetchPartErr[f.names.Default()] = map[int]error{2: boom}
		}, []string{groupBatch, groupDelay}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSamplerFixture(t)
			tc.fail(f)
			f.s.sample(t.Context())

			lags, dlq := f.obs.reports()
			var roles []string
			for _, r := range lags {
				roles = append(roles, r.role)
			}
			if !slices.Equal(roles, tc.wantRoles) {
				t.Errorf("roles reported = %v, want %v (reports %v)", roles, tc.wantRoles, lags)
			}
			if (len(dlq) == 1) != tc.wantDLQ || len(dlq) > 1 {
				t.Errorf("dlq reports = %v, want reported=%v", dlq, tc.wantDLQ)
			}
			if len(f.warns.warned()) == 0 {
				t.Error("a failed read was not logged")
			}
		})
	}
}

// A stop that cancels the sampler mid-call is not a broker fault, so it is not
// logged as one. Mutation: dropping the ctx check in fail turns this red.
func TestLagSamplerCancelledReadIsNotLogged(t *testing.T) {
	f := newSamplerFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	f.b.metaErr = context.Canceled
	cancel()
	f.s.sample(ctx)
	if got := f.warns.warned(); len(got) != 0 {
		t.Fatalf("a cancelled sample logged %v", got)
	}
	if lags, dlq := f.obs.reports(); len(lags) != 0 || len(dlq) != 0 {
		t.Fatalf("a cancelled sample reported %v %v", lags, dlq)
	}
}

// The sampler runs on its own goroutine, where a panicking observer would end
// the process. Mutation: calling the observer without safely turns this red.
func TestLagSamplerSurvivesAPanickingObserver(t *testing.T) {
	f := newSamplerFixture(t)
	f.s.obs = panicObserver{}
	f.s.sample(t.Context())
}

type panicObserver struct{}

func (panicObserver) OnKafkaLag(string, int64) { panic("observer bug") }
func (panicObserver) OnKafkaDLQDepth(int64)    { panic("observer bug") }

// run samples at once, then on every tick, and returns when its context ends.
// Mutation: sampling only after the first tick turns the first wait red; not
// returning on ctx.Done turns the wait for done red.
func TestLagSamplerRunSamplesImmediatelyThenPeriodically(t *testing.T) {
	f := newSamplerFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.s.run(ctx, 10*time.Millisecond)
	}()
	waitSamples := func(n int32) {
		t.Helper()
		deadline := time.Now().Add(consumerGuard)
		for f.b.metaCalls.Load() < n {
			if time.Now().After(deadline) {
				t.Fatalf("only %d rounds ran, want at least %d", f.b.metaCalls.Load(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitSamples(3)
	cancel()
	select {
	case <-done:
	case <-time.After(consumerGuard):
		t.Fatal("run did not return after its context ended")
	}
}

// newLagHarness is a harness whose transport reports to a lagObserver and reads
// offsets from b instead of a broker.
func newLagHarness(t *testing.T, b offsetReader) (*harness, *lagObserver) {
	t.Helper()
	h := newHarness(t, 0, nil)
	obs := &lagObserver{}
	h.tr.observer = obs
	h.tr.offsets = b
	return h, obs
}

// waitReports waits until obs has a lag report for every role and a DLQ depth.
func waitReports(t *testing.T, obs *lagObserver) {
	t.Helper()
	deadline := time.Now().Add(consumerGuard)
	for {
		if _, dlq := obs.reports(); len(obs.byRole()) == len(LagRoles()) && len(dlq) > 0 {
			return
		}
		if time.Now().After(deadline) {
			lags, dlq := obs.reports()
			t.Fatalf("the sampler did not report in time: lags %v, dlq %v", lags, dlq)
		}
		time.Sleep(time.Millisecond)
	}
}

// A consumer whose observer implements queue.KafkaStatsObserver samples at once,
// and again on cfg.StatsInterval. Mutation: not starting the sampler turns it
// red; ignoring cfg.StatsInterval turns the second wait red.
func TestConsumerReportsLagToAStatsObserver(t *testing.T) {
	f := newSamplerFixture(t)
	h, obs := newLagHarness(t, f.b)
	h.start(queue.ConsumerConfig{Concurrency: 1, StatsInterval: 10 * time.Millisecond}, nilHandler)

	waitReports(t, obs)
	want := map[string]int64{groupDefault: 26, groupBatch: 0, groupDelay: 12}
	for role, lag := range want {
		if got := obs.byRole()[role]; got != lag {
			t.Errorf("%s lag = %d, want %d", role, got, lag)
		}
	}
	deadline := time.Now().Add(consumerGuard)
	for f.b.metaCalls.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d rounds ran with a 10ms interval", f.b.metaCalls.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// Without a StatsInterval the sampler uses queue.DefaultStatsInterval, far longer
// than this test: one round, then quiet. Mutation: a zero interval ticking
// continuously turns the call count red (time.NewTicker would also panic).
func TestConsumerLagSamplerDefaultsItsInterval(t *testing.T) {
	f := newSamplerFixture(t)
	h, obs := newLagHarness(t, f.b)
	h.start(queue.ConsumerConfig{Concurrency: 1}, nilHandler)

	waitReports(t, obs)
	time.Sleep(100 * time.Millisecond)
	if got := f.b.metaCalls.Load(); got != 1 {
		t.Fatalf("%d rounds ran within 100ms of the default %v interval, want 1", got, queue.DefaultStatsInterval)
	}
}

// An observer that is not a queue.KafkaStatsObserver (the Asynq-style default)
// starts nothing: the broker is never asked for offsets. Mutation: starting the
// sampler regardless turns the call count red.
func TestConsumerWithoutKafkaStatsObserverDoesNotSample(t *testing.T) {
	b := newFakeBroker()
	h := newHarness(t, 0, nil)
	h.tr.offsets = b
	stop := h.start(queue.ConsumerConfig{Concurrency: 1, StatsInterval: time.Millisecond}, nilHandler)
	time.Sleep(50 * time.Millisecond)
	stop()
	if got := b.metaCalls.Load(); got != 0 {
		t.Fatalf("the broker was asked %d times though the observer takes no Kafka stats", got)
	}
}

// stop waits for the sampler, even while it is inside a broker call that does
// not return. Mutation: removing the sampler.Wait from stop turns this red,
// because stop would return while the call is still blocked.
func TestConsumerStopWaitsForTheLagSampler(t *testing.T) {
	f := newSamplerFixture(t)
	h, _ := newLagHarness(t, f.b)
	inHook := make(chan struct{})
	release := make(chan struct{})
	f.b.metaHook = func(context.Context) {
		close(inHook)
		<-release
	}
	stop := h.start(queue.ConsumerConfig{Concurrency: 1}, nilHandler)
	recv(t, inHook, "the sampler's first broker call")

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while the sampler was still inside a broker call")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	recv(t, stopped, "stop to return once the sampler's call did")
	stop()
}

// Stop cancels the sampler's context, so a call that honours it ends at once and
// nothing it was waiting on is reported. Mutation: not cancelling the sampler's
// context in stop leaves stop waiting for ever and turns this red.
func TestConsumerStopCancelsTheLagSampler(t *testing.T) {
	f := newSamplerFixture(t)
	h, obs := newLagHarness(t, f.b)
	inHook := make(chan struct{})
	f.b.metaHook = func(ctx context.Context) {
		close(inHook)
		<-ctx.Done()
	}
	stop := h.start(queue.ConsumerConfig{Concurrency: 1}, nilHandler)
	recv(t, inHook, "the sampler's first broker call")

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	recv(t, stopped, "stop to cancel the sampler and return")
	if lags, dlq := obs.reports(); len(lags) != 0 || len(dlq) != 0 {
		t.Fatalf("a sample cancelled by stop was reported: %v %v", lags, dlq)
	}
	calls := f.b.metaCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if got := f.b.metaCalls.Load(); got != calls {
		t.Fatalf("the broker was asked %d more times after stop returned", got-calls)
	}
}

// Closing the transport releases the sampler's client, once however often it is
// called. Mutation: dropping offsets.Close from Close turns this red.
func TestTransportCloseReleasesTheOffsetClient(t *testing.T) {
	b := newFakeBroker()
	h := newHarness(t, 0, nil)
	h.tr.offsets = b
	for range 2 {
		if err := h.tr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if got := b.closes.Load(); got != 1 {
		t.Fatalf("offset client closed %d times, want once", got)
	}
}
