package metrics

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// kafkaProduceCounts reads xflow_kafka_produce_errors_total back from the
// registry as kind -> count. Gathering is the only view that also shows which
// series exist, so a test can tell "counted zero" from "never created".
func kafkaProduceCounts(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != metricKafkaProduceErrors {
			continue
		}
		if family.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("%s type = %s, want COUNTER", metricKafkaProduceErrors, family.GetType())
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != 1 {
				t.Errorf("series has labels %v, want only kind", metric.GetLabel())
			}
			out[labelValue(metric, "kind")] = metric.GetCounter().GetValue()
		}
	}
	return out
}

// Mutation: replacing the kind label with the error text, or counting every
// OnEnqueue call, turns the counts below red.
func TestKafkaQueueMetricsCountsEachKind(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	for range 3 {
		k.OnProduceError("queue_full")
	}
	k.OnProduceError("retryable")
	k.OnProduceError("retryable")
	k.OnProduceError("permanent")
	k.OnProduceError("caller")
	k.OnProduceError("other")

	got := kafkaProduceCounts(t, m)
	want := map[string]float64{"queue_full": 3, "retryable": 2, "permanent": 1, "caller": 1, "other": 1}
	if len(got) != len(want) {
		t.Fatalf("series = %v, want %v", got, want)
	}
	for kind, n := range want {
		if got[kind] != n {
			t.Errorf("kind %q = %v, want %v (all: %v)", kind, got[kind], n, got)
		}
	}
}

// OnEnqueue is the Asynq-shared hook and carries the raw error. Neither a
// success nor a failure may be counted through it: the kind comes from the
// transport's own classification via OnProduceError, and a healthy transport
// exports no series at all.
func TestKafkaQueueMetricsOnEnqueueDoesNotCount(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	k.OnEnqueue("enqueue", time.Millisecond, nil)
	k.OnEnqueue("enqueue_delayed", time.Millisecond, nil)
	k.OnEnqueue("enqueue", time.Millisecond, errors.New("boom"))

	if got := kafkaProduceCounts(t, m); len(got) != 0 {
		t.Fatalf("series after successes and an uncounted OnEnqueue = %v, want none", got)
	}
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), metricKafkaProduceErrors) {
		t.Errorf("/metrics exports %s though nothing failed:\n%s", metricKafkaProduceErrors, rec.Body.String())
	}
}

// The kind label's cardinality is fixed by the table, whatever the transport
// sends. Anything outside it collapses into one "unknown" series, so a hostile
// or future value cannot mint new ones, and error text can never become a label.
func TestKafkaQueueMetricsLabelCardinalityIsBounded(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	for _, kind := range []string{"", "Queue_Full", "kafka: unexpected EOF", "connection reset by peer", "x"} {
		k.OnProduceError(kind)
	}
	for i := range 200 {
		k.OnProduceError(strings.Repeat("e", i+1))
	}

	got := kafkaProduceCounts(t, m)
	if len(got) != 1 || got["unknown"] != 205 {
		t.Fatalf("series = %v, want a single unknown series at 205", got)
	}
	allowed := map[string]bool{}
	for _, kind := range KafkaProduceKinds() {
		allowed[kind] = true
	}
	for kind := range got {
		if !allowed[kind] {
			t.Errorf("series kind %q is not in KafkaProduceKinds() %v", kind, KafkaProduceKinds())
		}
	}
	if len(KafkaProduceKinds()) != 6 {
		t.Errorf("KafkaProduceKinds() = %v, want 5 transport kinds plus unknown", KafkaProduceKinds())
	}
}

// A nil *Metrics is the "metrics disabled" spelling every adapter here accepts.
func TestKafkaQueueMetricsNilMetricsIsInert(t *testing.T) {
	k := NewKafkaQueueMetrics(nil)
	k.OnProduceError("permanent")
	k.OnEnqueue("enqueue", time.Millisecond, errors.New("boom"))
}

// The help text is what tells an operator which kinds to alert on; it has to
// name every one. Mutation: dropping a kind from the text turns this red.
func TestKafkaProduceErrorsHelpNamesEveryKind(t *testing.T) {
	help, ok := metricHelp[metricKafkaProduceErrors]
	if !ok {
		t.Fatalf("no metricHelp entry for %s", metricKafkaProduceErrors)
	}
	for _, kind := range KafkaProduceKinds() {
		if !strings.Contains(help, kind) {
			t.Errorf("%s help does not mention kind %q", metricKafkaProduceErrors, kind)
		}
	}
}

// kafkaGauges reads a gauge family back from the registry as label value ->
// value, keyed by the given label ("" for an unlabelled family). Like
// kafkaProduceCounts, gathering is what shows which series exist.
func kafkaGauges(t *testing.T, m *Metrics, name, label string) map[string]float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if family.GetType() != dto.MetricType_GAUGE {
			t.Fatalf("%s type = %s, want GAUGE", name, family.GetType())
		}
		for _, metric := range family.GetMetric() {
			want := 0
			if label != "" {
				want = 1
			}
			if len(metric.GetLabel()) != want {
				t.Errorf("%s series has labels %v, want %d", name, metric.GetLabel(), want)
			}
			out[labelValue(metric, label)] = metric.GetGauge().GetValue()
		}
	}
	return out
}

// Mutation: swapping the role label for the topic name, or adding a counter
// instead of setting a gauge, turns the values below red.
func TestKafkaQueueMetricsSetsLagPerRole(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	k.OnKafkaLag("default", 12)
	k.OnKafkaLag("batch", 0)
	k.OnKafkaLag("delay", 3)
	k.OnKafkaLag("default", 7) // a later sample replaces the earlier one

	got := kafkaGauges(t, m, metricKafkaLag, "role")
	want := map[string]float64{"default": 7, "batch": 0, "delay": 3}
	if len(got) != len(want) {
		t.Fatalf("series = %v, want %v", got, want)
	}
	for role, v := range want {
		if got[role] != v {
			t.Errorf("role %q = %v, want %v (all: %v)", role, got[role], v, got)
		}
	}
}

// Mutation: dropping the Set (or writing a counter) turns the values red; a
// label would turn the label-count check inside kafkaGauges red.
func TestKafkaQueueMetricsSetsDLQDepth(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	k.OnKafkaDLQDepth(4)
	k.OnKafkaDLQDepth(1) // retention removed records

	got := kafkaGauges(t, m, metricKafkaDLQDepth, "")
	if len(got) != 1 || got[""] != 1 {
		t.Fatalf("series = %v, want a single unlabelled series at 1", got)
	}
}

// Like the produce counter, neither gauge exists until a sample arrives: a
// healthy start that has not sampled yet must not read as zero lag. Mutation:
// registering either at construction turns this red.
func TestKafkaQueueMetricsGaugesAreLazy(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)
	k.OnEnqueue("enqueue", time.Millisecond, nil)
	k.OnProduceError("permanent")

	for _, name := range []string{metricKafkaLag, metricKafkaDLQDepth} {
		if got := kafkaGauges(t, m, name, ""); len(got) != 0 {
			t.Errorf("%s exists before any sample: %v", name, got)
		}
	}
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	for _, name := range []string{metricKafkaLag, metricKafkaDLQDepth} {
		if strings.Contains(rec.Body.String(), name) {
			t.Errorf("/metrics exports %s though nothing was sampled", name)
		}
	}
}

// The role label's cardinality is fixed by the table, whatever the transport
// sends: anything outside it collapses into one "unknown" series. Mutation:
// using the raw role as the label turns the series count red.
func TestKafkaQueueMetricsLagRoleCardinalityIsBounded(t *testing.T) {
	m := New()
	k := NewKafkaQueueMetrics(m)

	for _, role := range []string{"", "Default", "xflow.task.default", "x"} {
		k.OnKafkaLag(role, 1)
	}
	for i := range 200 {
		k.OnKafkaLag(strings.Repeat("r", i+1), 1)
	}

	got := kafkaGauges(t, m, metricKafkaLag, "role")
	if len(got) != 1 || got["unknown"] != 1 {
		t.Fatalf("series = %v, want a single unknown series", got)
	}
	if roles := KafkaLagRoles(); len(roles) != 4 || roles[3] != "unknown" {
		t.Errorf("KafkaLagRoles() = %v, want 3 transport roles plus unknown", roles)
	}
}

// A nil *Metrics is the "metrics disabled" spelling every adapter accepts.
func TestKafkaQueueMetricsNilMetricsIgnoresSamples(t *testing.T) {
	k := NewKafkaQueueMetrics(nil)
	k.OnKafkaLag("default", 1)
	k.OnKafkaDLQDepth(1)
}

// The help text is what tells an operator how to read the gauges; the lag help
// has to name every role and both have to say they only exist with Kafka.
// Mutation: dropping a role or the registration note turns this red.
func TestKafkaLagHelpNamesEveryRole(t *testing.T) {
	for _, name := range []string{metricKafkaLag, metricKafkaDLQDepth} {
		help, ok := metricHelp[name]
		if !ok {
			t.Fatalf("no metricHelp entry for %s", name)
		}
		if !strings.Contains(help, "Only registered when the Kafka transport is selected") {
			t.Errorf("%s help does not say it is Kafka-only", name)
		}
	}
	for _, role := range KafkaLagRoles() {
		if !strings.Contains(metricHelp[metricKafkaLag], role) {
			t.Errorf("%s help does not mention role %q", metricKafkaLag, role)
		}
	}
}
