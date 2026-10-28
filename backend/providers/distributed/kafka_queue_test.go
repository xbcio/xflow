package distributed

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	asynqtransport "github.com/xbcio/xflow/backend/providers/distributed/internal/queue/asynq"
	kafkaqueue "github.com/xbcio/xflow/backend/providers/distributed/internal/queue/kafka"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
)

// unreachableKafka is a broker address nothing listens on, so a connection
// attempt is refused at once.
const unreachableKafka = "127.0.0.1:1"

func newMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	return mr
}

// requireNoRedisClient fails unless every client New opened against mr has been
// closed. The server notices a closed client a moment later, so it polls.
func requireNoRedisClient(t *testing.T, mr *miniredis.Miniredis) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for mr.CurrentConnectionCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d redis connection(s) still open after New failed: the client leaked", mr.CurrentConnectionCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Mutation note: making WithKafkaQueue win over WithTransport (or the reverse)
// without an error must turn this red. The check runs before any connection, so
// an unreachable Redis address must not change the error.
func TestKafkaQueueAndTransportAreMutuallyExclusive(t *testing.T) {
	_, err := New(unreachableKafka, nil,
		WithKafkaQueue(KafkaConfig{Brokers: []string{unreachableKafka}}),
		WithTransport(&stubTransport{}),
	)
	if err == nil {
		t.Fatal("New() = nil error, want a conflict between WithKafkaQueue and WithTransport")
	}
	for _, want := range []string{"WithKafkaQueue", "WithTransport", "mutually exclusive"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "redis ping") {
		t.Fatalf("New tried to connect before rejecting the conflict: %v", err)
	}
}

// Mutation note: dropping validateKafkaSelection from New must turn the rows red
// (the error would come from a later stage, after Redis was already open), and
// so must reordering it below the Redis ping: the miniredis server would see a
// connection.
func TestKafkaQueueRejectsInvalidConfigBeforeConnecting(t *testing.T) {
	cases := []struct {
		name string
		cfg  KafkaConfig
		want string
	}{
		{"no brokers", KafkaConfig{}, "Brokers"},
		{"malformed broker", KafkaConfig{Brokers: []string{"no-port"}}, "Brokers[0]"},
		{"create topics without partitions", KafkaConfig{
			Brokers: []string{unreachableKafka}, CreateTopics: true, ReplicationFactor: 1,
		}, "Partitions"},
		{"create topics without replication factor", KafkaConfig{
			Brokers: []string{unreachableKafka}, CreateTopics: true, Partitions: 3,
		}, "ReplicationFactor"},
		{"partitions without create topics", KafkaConfig{
			Brokers: []string{unreachableKafka}, Partitions: 3,
		}, "CreateTopics"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr := newMiniredis(t)
			_, err := New(mr.Addr(), nil, WithKafkaQueue(tc.cfg))
			if err == nil {
				t.Fatal("New() = nil error, want the invalid Kafka configuration rejected")
			}
			if !strings.HasPrefix(err.Error(), "kafka queue: ") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want a %q-prefixed error naming %q", err, "kafka queue: ", tc.want)
			}
			if got := mr.TotalConnectionCount(); got != 0 {
				t.Fatalf("redis saw %d connection(s), want 0: the config must be rejected before connecting", got)
			}
		})
	}
}

// Mutation note: removing the _ = rdb.Close() from the Kafka failure branch of
// New must turn this red (the miniredis client count never drops to zero).
// Dropping the EnsureTopics call must turn it red too: building the transport
// contacts no broker, so New would succeed against a dead address. The
// API-only row proves instances that never consume still check the topics,
// because they produce tasks.
func TestKafkaQueueUnreachableBrokerFailsNewAndReleasesRedis(t *testing.T) {
	cfg := KafkaConfig{
		Brokers:      []string{unreachableKafka},
		WriteTimeout: 500 * time.Millisecond,
	}
	cases := []struct {
		name string
		opts []Option
	}{
		{"consumer", nil},
		{"api only", []Option{WithConsumer(false)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr := newMiniredis(t)
			start := time.Now()
			b, err := New(mr.Addr(), nil, append([]Option{WithKafkaQueue(cfg)}, tc.opts...)...)
			if err == nil {
				_ = b.rdb.Close()
				t.Fatal("New() = nil error, want the unreachable broker to fail startup")
			}
			if !strings.HasPrefix(err.Error(), "kafka queue: ") {
				t.Fatalf("error = %q, want the %q prefix", err, "kafka queue: ")
			}
			if elapsed := time.Since(start); elapsed > 10*time.Second {
				t.Fatalf("New took %v to fail, want a few seconds at most", elapsed)
			}
			requireNoRedisClient(t, mr)
		})
	}
}

// Mutation note: when no option is given New must keep building the Asynq
// transport. Letting any Kafka code run by default turns this red.
func TestDefaultTransportStaysAsynq(t *testing.T) {
	mr := newMiniredis(t)
	b, err := New(mr.Addr(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.transport.Close(); _ = b.rdb.Close() })

	if _, ok := b.transport.(*asynqtransport.Transport); !ok {
		t.Fatalf("default transport is %T, want *asynq.Transport", b.transport)
	}
}

// useKafkaStub replaces the Kafka transport builder with one that returns stub
// and records what New passed it, so the selection path runs without a broker.
func useKafkaStub(t *testing.T, stub queue.Transport) *kafkaStubCall {
	t.Helper()
	call := &kafkaStubCall{}
	prev := buildKafkaTransport
	buildKafkaTransport = func(cfg *config, deploymentSuffix string) (queue.Transport, error) {
		call.calls++
		call.cfg = *cfg.kafkaQueue
		call.suffix = deploymentSuffix
		call.observer = cfg.queueObserver
		return stub, nil
	}
	t.Cleanup(func() { buildKafkaTransport = prev })
	return call
}

type kafkaStubCall struct {
	calls    int
	cfg      KafkaConfig
	suffix   string
	observer queue.Observer
}

type recordingObserver struct{}

func (recordingObserver) OnEnqueue(string, time.Duration, error) {}

// Mutation note: skipping the Kafka branch (leaving the Asynq default in place)
// must turn the first assertions red; passing a different suffix than the one
// derived from the owned set would split topics from the leader key and sweep
// scope and must turn the suffix assertion red.
func TestKafkaQueueSelectsKafkaTransport(t *testing.T) {
	mr := newMiniredis(t)
	stub := &stubTransport{}
	call := useKafkaStub(t, stub)
	obs := recordingObserver{}

	brokers := []string{"broker-a:9092", "broker-b:9092"}
	b, err := New(mr.Addr(), nil,
		WithKafkaQueue(KafkaConfig{Brokers: brokers, MaxRetries: 7}),
		WithQueueObserver(obs),
		WithOwnedNamespaces([]namespace.Namespace{"local-jack"}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.rdb.Close() })

	if call.calls != 1 {
		t.Fatalf("Kafka transport built %d times, want 1", call.calls)
	}
	if b.Queue() != queue.Transport(stub) || b.transport != queue.Transport(stub) {
		t.Fatalf("backend transport = %T, want the Kafka transport", b.transport)
	}
	if call.suffix != "local-jack" {
		t.Fatalf("deployment suffix passed to Kafka = %q, want %q", call.suffix, "local-jack")
	}
	if call.cfg.MaxRetries != 7 || len(call.cfg.Brokers) != 2 || call.cfg.Brokers[1] != "broker-b:9092" {
		t.Fatalf("KafkaConfig passed through = %+v", call.cfg)
	}
	if call.observer != queue.Observer(obs) {
		t.Fatalf("queue observer passed to Kafka = %v, want the one given to WithQueueObserver", call.observer)
	}
}

// Mutation note: copying the broker slice in WithKafkaQueue is what lets a
// caller reuse its slice afterwards; removing the clone must turn this red.
func TestWithKafkaQueueCopiesBrokerList(t *testing.T) {
	brokers := []string{"broker-a:9092"}
	c := &config{}
	WithKafkaQueue(KafkaConfig{Brokers: brokers})(c)
	brokers[0] = "changed:1"
	if got := c.kafkaQueue.Brokers[0]; got != "broker-a:9092" {
		t.Fatalf("stored broker = %q, want the value at option time", got)
	}
}

// Mutation note: restoring the unconditional "transport != nil" condition must
// turn the kafka rows red (the Kafka transport names its topics from the same
// suffix, so it does not bypass queue separation); dropping the warning for an
// injected transport must turn the injected row red.
func TestQueueSeparationWarningIsOnlyForInjectedTransports(t *testing.T) {
	cases := []struct {
		name      string
		useKafka  bool
		owned     []namespace.Namespace
		wantWarns int
	}{
		{"kafka dedicated", true, []namespace.Namespace{"local-jack"}, 0},
		{"kafka default", true, []namespace.Namespace{namespace.Default}, 0},
		{"injected dedicated", false, []namespace.Namespace{"local-jack"}, 1},
		{"injected default", false, []namespace.Namespace{namespace.Default}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr := newMiniredis(t)
			log := &warnRecordingLogger{}
			opts := []Option{WithStateLogger(log), WithOwnedNamespaces(tc.owned)}
			if tc.useKafka {
				useKafkaStub(t, &stubTransport{})
				opts = append(opts, WithKafkaQueue(KafkaConfig{Brokers: []string{unreachableKafka}}))
			} else {
				opts = append(opts, WithTransport(&stubTransport{}))
			}
			b, err := New(mr.Addr(), nil, opts...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = b.rdb.Close() })

			if got := log.count(); got != tc.wantWarns {
				t.Fatalf("warnings = %d (%v), want %d", got, log.warns, tc.wantWarns)
			}
		})
	}
}

// Mutation note: adding a kind to the transport without teaching the metrics
// adapter (or the reverse) must turn this red. Otherwise the new kind would be
// exported as "unknown" and an alert written for it would never fire.
func TestKafkaProduceKindVocabularyMatchesMetrics(t *testing.T) {
	transport := kafkaqueue.ProduceKinds()
	exported := metrics.KafkaProduceKinds()
	for _, kind := range transport {
		if !slices.Contains(exported, kind) {
			t.Errorf("transport reports kind %q but the metrics adapter does not export it (has %v)", kind, exported)
		}
	}
	for _, kind := range exported {
		if kind != "unknown" && !slices.Contains(transport, kind) {
			t.Errorf("metrics adapter exports kind %q that the transport never reports (has %v)", kind, transport)
		}
	}
}

// Mutation note: adding a lag role to the transport without teaching the metrics
// adapter (or the reverse) must turn this red. Otherwise the new role would be
// exported as "unknown" and an alert written for it would never fire.
func TestKafkaLagRoleVocabularyMatchesMetrics(t *testing.T) {
	transport := kafkaqueue.LagRoles()
	exported := metrics.KafkaLagRoles()
	for _, role := range transport {
		if !slices.Contains(exported, role) {
			t.Errorf("transport reports lag role %q but the metrics adapter does not export it (has %v)", role, exported)
		}
	}
	for _, role := range exported {
		if role != "unknown" && !slices.Contains(transport, role) {
			t.Errorf("metrics adapter exports lag role %q that the transport never reports (has %v)", role, transport)
		}
	}
}

// methodSet lists "Name signature" for each method v has, sorted. A method value
// carries no receiver, so a concrete type and an interface compare directly.
func methodSet(v reflect.Value) []string {
	var out []string
	for i := range v.NumMethod() {
		out = append(out, v.Type().Method(i).Name+" "+v.Method(i).Type().String())
	}
	slices.Sort(out)
	return out
}

// The metrics package cannot import this one, so it mirrors the observer
// interfaces with primitive parameters. KafkaQueueMetrics is installed through
// WithQueueObserver and then found by type assertion, so a method that differs in
// name or signature from the interface here silently turns that observer off.
// Mutation note: renaming a method or changing a parameter type on either side
// must turn this red, as must adding a method to the mirror that no interface
// here asks for.
func TestKafkaQueueMetricsMethodsMatchTheObserverInterfaces(t *testing.T) {
	var (
		_ QueueObserver             = metrics.KafkaQueueMetrics{}
		_ QueueProduceErrorObserver = metrics.KafkaQueueMetrics{}
		_ QueueKafkaStatsObserver   = metrics.KafkaQueueMetrics{}
	)
	var want []string
	for _, iface := range []reflect.Type{
		reflect.TypeFor[QueueObserver](),
		reflect.TypeFor[QueueProduceErrorObserver](),
		reflect.TypeFor[QueueKafkaStatsObserver](),
	} {
		for i := range iface.NumMethod() {
			m := iface.Method(i)
			want = append(want, m.Name+" "+m.Type.String())
		}
	}
	slices.Sort(want)
	got := methodSet(reflect.ValueOf(metrics.KafkaQueueMetrics{}))
	if !slices.Equal(got, want) {
		t.Fatalf("KafkaQueueMetrics methods:\n  %v\nobserver interfaces:\n  %v", got, want)
	}
}

// Mutation note: redeclaring the alias as an interface of its own (instead of
// naming the transport's) turns this red, even if the method set is identical.
func TestQueueKafkaStatsObserverIsTheTransportInterface(t *testing.T) {
	if reflect.TypeFor[QueueKafkaStatsObserver]() != reflect.TypeFor[queue.KafkaStatsObserver]() {
		t.Fatal("QueueKafkaStatsObserver is not an alias of queue.KafkaStatsObserver")
	}
}
