package apiserver

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/observability/metrics"
)

// TestKafkaQueueRequiresRedis pins the loud failure: the in-memory backend has
// no task transport to replace, so a Kafka selection without Redis must stop
// startup instead of being ignored.
func TestKafkaQueueRequiresRedis(t *testing.T) {
	kc := &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}
	if _, err := New(Config{KafkaQueue: kc}); !errors.Is(err, errKafkaQueueNoRedis) {
		t.Fatalf("New with KafkaQueue and no Redis: err = %v, want errKafkaQueueNoRedis", err)
	}
}

// TestKafkaQueueInvalidConfigFailsNewWithRedis proves the setting reaches
// distributed.New once Redis is configured: an unusable configuration is
// rejected there, before any broker is contacted, and the error is returned
// rather than swallowed. Brokers is empty, which Validate refuses at once.
func TestKafkaQueueInvalidConfigFailsNewWithRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	_, err := New(Config{RedisAddr: mr.Addr(), KafkaQueue: &distributed.KafkaConfig{}})
	if err == nil {
		t.Fatal("New accepted a Kafka configuration with no brokers")
	}
	if errors.Is(err, errKafkaQueueNoRedis) {
		t.Fatalf("err = %v: Redis is configured, so the failure must come from the Kafka configuration", err)
	}
	if !strings.Contains(err.Error(), "kafka queue") || !strings.Contains(err.Error(), "Brokers") {
		t.Fatalf("err = %q, want it to name the kafka queue and the Brokers field", err)
	}
}

// TestNoKafkaQueueKeepsTheDefaultTransport is the negative twin: with the field
// nil, a Redis-backed server builds exactly as before.
func TestNoKafkaQueueKeepsTheDefaultTransport(t *testing.T) {
	mr := miniredis.RunT(t)
	srv, err := New(Config{RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatalf("New without KafkaQueue: %v", err)
	}
	if srv.ctrl == nil {
		t.Fatal("server has no control module")
	}
}

// kafkaProduceSeries counts the series of xflow_kafka_produce_errors_total in m
// and returns the family's total, so "never registered" and "registered but
// zero" stay distinguishable.
func kafkaProduceSeries(t *testing.T, m *metrics.Metrics) (series int, total float64) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() != "xflow_kafka_produce_errors_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			series++
			total += metric.GetCounter().GetValue()
		}
	}
	return series, total
}

// TestKafkaQueueObserverOnlyWhenKafkaAndMetrics pins when the Kafka produce
// error counter is wired: only for a Kafka selection with metrics on. The
// default (Asynq) deployment and a Kafka deployment without metrics get no
// observer, so nothing is registered. Mutation: returning the observer
// unconditionally turns the nil rows red.
func TestKafkaQueueObserverOnlyWhenKafkaAndMetrics(t *testing.T) {
	kc := &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}
	cases := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"default asynq with metrics", Config{Metrics: metrics.New()}, false},
		{"kafka without metrics", Config{KafkaQueue: kc}, false},
		{"neither", Config{}, false},
		{"kafka with metrics", Config{KafkaQueue: kc, Metrics: metrics.New()}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := kafkaQueueObserver(tc.cfg)
			if (obs != nil) != tc.want {
				t.Fatalf("kafkaQueueObserver() = %v, want non-nil=%v", obs, tc.want)
			}
			if tc.cfg.Metrics != nil {
				if series, _ := kafkaProduceSeries(t, tc.cfg.Metrics); series != 0 {
					t.Fatalf("%d xflow_kafka_produce_errors_total series exist before any failure", series)
				}
			}
		})
	}
}

// TestKafkaQueueObserverCountsThroughTheRegistry runs the observer the server
// would install against the registry cfg.Metrics owns: a failure kind shows up
// as one series, a success does not.
func TestKafkaQueueObserverCountsThroughTheRegistry(t *testing.T) {
	m := metrics.New()
	obs := kafkaQueueObserver(Config{
		KafkaQueue: &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}},
		Metrics:    m,
	})
	if obs == nil {
		t.Fatal("no observer for a Kafka selection with metrics")
	}
	pe, ok := obs.(distributed.QueueProduceErrorObserver)
	if !ok {
		t.Fatalf("observer %T does not implement QueueProduceErrorObserver", obs)
	}

	obs.OnEnqueue("enqueue", time.Millisecond, nil)
	if series, _ := kafkaProduceSeries(t, m); series != 0 {
		t.Fatalf("a successful enqueue created %d series", series)
	}
	pe.OnProduceError("queue_full")
	pe.OnProduceError("queue_full")
	pe.OnProduceError("permanent")
	if series, total := kafkaProduceSeries(t, m); series != 2 || total != 3 {
		t.Fatalf("series = %d total = %v, want 2 series totalling 3", series, total)
	}
}

// kafkaGaugeSeries returns the values of the series of gauge family name in m,
// keyed by the given label ("" for none), so "never registered" and "zero" stay
// distinguishable.
func kafkaGaugeSeries(t *testing.T, m *metrics.Metrics, name, label string) map[string]float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			key := ""
			for _, l := range metric.GetLabel() {
				if l.GetName() == label {
					key = l.GetValue()
				}
			}
			out[key] = metric.GetGauge().GetValue()
		}
	}
	return out
}

// TestKafkaQueueObserverExportsLagAndDLQDepth runs the observer the server would
// install: it must be told the lag and dead-letter depth the Kafka consumer
// samples, and they must show up in the registry cfg.Metrics owns. Before the
// first sample neither series exists. Mutation: wiring a wrapper that hides the
// stats methods (or the adapter dropping them) turns the type assertion red;
// dropping the Set turns the values red.
func TestKafkaQueueObserverExportsLagAndDLQDepth(t *testing.T) {
	m := metrics.New()
	obs := kafkaQueueObserver(Config{
		KafkaQueue: &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}},
		Metrics:    m,
	})
	if obs == nil {
		t.Fatal("no observer for a Kafka selection with metrics")
	}
	ks, ok := obs.(distributed.QueueKafkaStatsObserver)
	if !ok {
		t.Fatalf("observer %T does not implement QueueKafkaStatsObserver", obs)
	}
	for _, name := range []string{"xflow_kafka_lag", "xflow_kafka_dlq_depth"} {
		if got := kafkaGaugeSeries(t, m, name, "role"); len(got) != 0 {
			t.Fatalf("%s has series %v before any sample", name, got)
		}
	}

	ks.OnKafkaLag("default", 42)
	ks.OnKafkaLag("delay", 0)
	ks.OnKafkaDLQDepth(3)

	lag := kafkaGaugeSeries(t, m, "xflow_kafka_lag", "role")
	if len(lag) != 2 || lag["default"] != 42 || lag["delay"] != 0 {
		t.Fatalf("xflow_kafka_lag = %v, want default=42 delay=0", lag)
	}
	if _, ok := lag["batch"]; ok {
		t.Fatalf("xflow_kafka_lag has a batch series though none was sampled: %v", lag)
	}
	dlq := kafkaGaugeSeries(t, m, "xflow_kafka_dlq_depth", "")
	if len(dlq) != 1 || dlq[""] != 3 {
		t.Fatalf("xflow_kafka_dlq_depth = %v, want 3", dlq)
	}
}

// TestKafkaQueueGaugesOnlyWithKafkaSelection pins that the two new gauges follow
// the observer's rule: a default (Asynq) deployment, or Kafka without metrics,
// gets no observer to call and so registers neither. Mutation: registering the
// gauges at construction, or returning the observer unconditionally, turns it red.
func TestKafkaQueueGaugesOnlyWithKafkaSelection(t *testing.T) {
	kc := &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}
	for name, cfg := range map[string]Config{
		"default asynq with metrics": {Metrics: metrics.New()},
		"kafka without metrics":      {KafkaQueue: kc},
	} {
		t.Run(name, func(t *testing.T) {
			if obs := kafkaQueueObserver(cfg); obs != nil {
				t.Fatalf("kafkaQueueObserver() = %T, want nil", obs)
			}
			if cfg.Metrics != nil {
				for _, n := range []string{"xflow_kafka_lag", "xflow_kafka_dlq_depth"} {
					if got := kafkaGaugeSeries(t, cfg.Metrics, n, "role"); len(got) != 0 {
						t.Fatalf("%s exported %v with no Kafka selection", n, got)
					}
				}
			}
		})
	}
}

// TestDefaultServerRegistersNoKafkaMetric is the end-to-end twin: a Redis-backed
// server without Kafka, given a Metrics registry, must not register the
// Kafka-only counter. Mutation: registering it at construction turns this red.
func TestDefaultServerRegistersNoKafkaMetric(t *testing.T) {
	mr := miniredis.RunT(t)
	m := metrics.New()
	if _, err := New(Config{RedisAddr: mr.Addr(), Metrics: m}); err != nil {
		t.Fatalf("New: %v", err)
	}
	if series, _ := kafkaProduceSeries(t, m); series != 0 {
		t.Fatalf("a default deployment exports %d xflow_kafka_produce_errors_total series", series)
	}
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if strings.Contains(f.GetName(), "kafka") {
			t.Errorf("default deployment registered Kafka metric %q", f.GetName())
		}
	}
}
