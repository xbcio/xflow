package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"

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

func TestMetricsHooksDoNotExportExecutionIDAsLabel(t *testing.T) {
	metrics := New()
	hooks := NewMetricsHooks(metrics)

	hooks.OnNodeStart(context.Background(), types.ExecutionID("exec-123"), "send_email")
	hooks.OnNodeComplete(context.Background(), types.ExecutionID("exec-123"), "send_email", types.NodeStatusSuccess)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "exec-123") {
		t.Fatalf("metrics body leaked execution id: %s", body)
	}
	if !strings.Contains(body, `xflow_node_completed_total{node="send_email",status="success"} 1`) {
		t.Fatalf("node completed metric missing from body: %s", body)
	}
}

func TestObserverAdaptersIncrementExpectedMetrics(t *testing.T) {
	metrics := New()

	NewAuditMetrics(metrics).OnAuditFailed("save_signal", assertErr{})
	NewSweepMetrics(metrics).OnSweepReclaim("exec-1", "node-1", 1500)
	NewSweepMetrics(metrics).OnSweepReclaimResult("reclaimed", time.Millisecond)
	NewDispatcherMetrics(metrics).OnDispatchTransient("no_capacity")
	NewAuthMetrics(metrics).OnAuthDecision("register", "deny", "enforcing")
	NewCommitMetrics(metrics).OnCommitOutcome(context.Background(), engine.CommitOutcomeAccepted)
	outbox := NewOutboxMetrics(metrics)
	outbox.OnOutboxRetry(context.Background(), 1)
	outbox.OnOutboxDeadLetter(context.Background())
	outbox.OnOutboxPending(context.Background(), 2, 1, time.Second)
	NewLeaseMetrics(metrics).OnLeaseAcquire("acquired", time.Millisecond)
	NewRunnerClaimMetrics(metrics).OnRunnerClaimReclaimed(2)
	NewRunnerClaimMetrics(metrics).OnRunnerLeaseReplayed()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`xflow_audit_write_total{op="save_signal",result="failed"} 1`,
		`xflow_lease_sweep_reclaimed_total{result="reclaimed"} 1`,
		`xflow_dispatch_transient_total{reason="no_capacity"} 1`,
		`xflow_runner_auth_decisions_total{auth_mode="enforcing",result="deny"} 1`,
		`xflow_lease_age_seconds_count{result="reclaimed"} 1`,
		`xflow_commit_outcomes_total{outcome="accepted"} 1`,
		`xflow_outbox_retries_total 1`,
		`xflow_outbox_dead_letters_total 1`,
		`xflow_outbox_pending 2`,
		`xflow_outbox_dead_letters 1`,
		`xflow_lease_acquire_total{result="acquired"} 1`,
		`xflow_runner_claim_reclaimed_total 2`,
		`xflow_runner_lease_replayed_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }
