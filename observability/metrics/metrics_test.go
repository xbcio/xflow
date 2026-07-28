package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
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
	if !strings.Contains(body, `xflow_node_completed_total{namespace="default",node="send_email",status="success"} 1`) {
		t.Fatalf("node completed metric missing from body: %s", body)
	}
}

func TestObserverAdaptersIncrementExpectedMetrics(t *testing.T) {
	metrics := New()
	ctx := context.Background()

	NewAuditMetrics(metrics).OnAuditFailed(ctx, "save_signal", assertErr{})
	NewSweepMetrics(metrics).OnSweepReclaim(ctx, "exec-1", "node-1", 1500)
	NewSweepMetrics(metrics).OnSweepReclaimResult(ctx, "reclaimed", time.Millisecond)
	NewDispatcherMetrics(metrics).OnDispatchTransient(ctx, "no_capacity")
	NewAuthMetrics(metrics).OnAuthDecision(ctx, "register", "deny", "enforcing")
	NewCommitMetrics(metrics).OnCommitOutcome(ctx, engine.CommitOutcomeAccepted)
	outbox := NewOutboxMetrics(metrics)
	outbox.OnOutboxRetry(ctx, 1)
	outbox.OnOutboxDeadLetter(ctx)
	outbox.OnOutboxPending(ctx, 2, 1, time.Second)
	NewLeaseMetrics(metrics).OnLeaseAcquire(ctx, "acquired", time.Millisecond)
	NewRunnerClaimMetrics(metrics).OnRunnerClaimReclaimed(ctx, 2)
	NewRunnerClaimMetrics(metrics).OnRunnerLeaseReplayed(ctx)
	NewScriptMetrics(metrics).OnScriptExecute(ctx, "js", "goja", "main", 5*time.Millisecond)
	NewScriptMetrics(metrics).OnScriptOutputBytes(ctx, "js", "goja", 2048)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`xflow_audit_write_total{namespace="default",op="save_signal",result="failed"} 1`,
		`xflow_lease_sweep_reclaimed_total{namespace="default",result="reclaimed"} 1`,
		`xflow_dispatch_transient_total{namespace="default",reason="no_capacity"} 1`,
		`xflow_runner_auth_decisions_total{auth_mode="enforcing",namespace="default",result="deny"} 1`,
		`xflow_lease_age_seconds_count{namespace="default",result="reclaimed"} 1`,
		`xflow_commit_outcomes_total{namespace="default",outcome="accepted"} 1`,
		`xflow_outbox_retries_total{namespace="default"} 1`,
		`xflow_outbox_dead_letters_total{namespace="default"} 1`,
		`xflow_outbox_pending{namespace="default"} 2`,
		`xflow_outbox_dead_letters{namespace="default"} 1`,
		`xflow_lease_acquire_total{namespace="default",result="acquired"} 1`,
		`xflow_runner_claim_reclaimed_total{namespace="default"} 2`,
		`xflow_runner_lease_replayed_total{namespace="default"} 1`,
		`xflow_script_execute_total{language="js",namespace="default",outcome="main",runtime="goja"} 1`,
		`xflow_script_output_bytes_count{language="js",namespace="default",runtime="goja"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsHooksCarryNamespaceLabel(t *testing.T) {
	metrics := New()
	hooks := NewMetricsHooks(metrics)

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	hooks.OnNodeStart(ctx, types.ExecutionID("exec-123"), "send_email")
	hooks.OnNodeComplete(ctx, types.ExecutionID("exec-123"), "send_email", types.NodeStatusSuccess)
	hooks.OnExecutionComplete(ctx, types.ExecutionID("exec-123"), types.ExecutionStatusSuccess)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`xflow_node_started_total{namespace="namespace-a",node="send_email"} 1`,
		`xflow_node_completed_total{namespace="namespace-a",node="send_email",status="success"} 1`,
		`xflow_execution_completed_total{namespace="namespace-a",status="success"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "exec-123") {
		t.Fatalf("metrics body leaked execution id: %s", body)
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }
