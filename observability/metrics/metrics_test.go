package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestUsesPrometheusRegistry(t *testing.T) {
	metrics := New()
	metrics.Inc("xflow_audit_write_total", map[string]string{"result": "failed", "op": "upsert_node"})

	family := gatherMetricFamily(t, metrics, "xflow_audit_write_total")
	if family.GetType() != dto.MetricType_COUNTER {
		t.Fatalf("metric type = %s, want COUNTER", family.GetType())
	}
	if got := family.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Fatalf("counter value = %v, want 1", got)
	}
	if got := labelValue(family.GetMetric()[0], "op"); got != "upsert_node" {
		t.Fatalf("op label = %q, want upsert_node", got)
	}
}

func TestHandlerExportsCountersWithStableLabels(t *testing.T) {
	metrics := New()
	metrics.Inc("xflow_audit_write_total", map[string]string{"result": "failed", "op": "upsert_node"})

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)

	got := rec.Body.String()
	want := `xflow_audit_write_total{op="upsert_node",result="failed"} 1`
	if !strings.Contains(got, want) {
		t.Fatalf("metrics body = %q, want %q", got, want)
	}
}

func TestDurationExportsCountAndSum(t *testing.T) {
	metrics := New()
	metrics.Observe("xflow_node_duration_seconds", map[string]string{"node": "n"}, 2*time.Second)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `xflow_node_duration_seconds_count{node="n"} 1`) {
		t.Fatalf("duration count missing: %s", body)
	}
	if !strings.Contains(body, `xflow_node_duration_seconds_sum{node="n"} 2`) {
		t.Fatalf("duration sum missing: %s", body)
	}

	family := gatherMetricFamily(t, metrics, "xflow_node_duration_seconds")
	if family.GetType() != dto.MetricType_HISTOGRAM {
		t.Fatalf("duration metric type = %s, want HISTOGRAM", family.GetType())
	}
	histogram := family.GetMetric()[0].GetHistogram()
	if histogram.GetSampleCount() != 1 {
		t.Fatalf("histogram sample count = %d, want 1", histogram.GetSampleCount())
	}
	if histogram.GetSampleSum() != 2 {
		t.Fatalf("histogram sample sum = %v, want 2", histogram.GetSampleSum())
	}
}

func gatherMetricFamily(t *testing.T, metrics *Metrics, name string) *dto.MetricFamily {
	t.Helper()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found in %#v", name, families)
	return nil
}

func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
