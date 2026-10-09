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
	NewSweepMetrics(metrics).OnAssignmentQueueDepth(ctx, "legacy", 4)
	dispatcher := NewDispatcherMetrics(metrics)
	dispatcher.OnDispatchTransient(ctx, "no_capacity")
	dispatcher.OnDispatchDropped(ctx, "execution_gone")
	dispatcher.OnTaskDeliveryLag(ctx, 2*time.Second)
	NewQueueStatsMetrics(metrics).OnQueueStats("xflow:default", 7, 3, 1, 90*time.Second)
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
		`xflow_assignment_queue_depth{lane="legacy"} 4`,
		`xflow_dispatch_transient_total{namespace="default",reason="no_capacity"} 1`,
		`xflow_dispatch_dropped_total{namespace="default",reason="execution_gone"} 1`,
		`xflow_task_delivery_lag_seconds_count{namespace="default"} 1`,
		// The 2s observation must land in a real bucket, not +Inf: the default
		// Observe buckets stop at 10s, which is exactly why delivery lag uses
		// the dedicated second-to-hour bucket set.
		`xflow_task_delivery_lag_seconds_bucket{namespace="default",le="5"} 1`,
		`xflow_queue_depth{queue="xflow:default",state="pending"} 7`,
		`xflow_queue_depth{queue="xflow:default",state="active"} 3`,
		`xflow_queue_depth{queue="xflow:default",state="retry"} 1`,
		`xflow_queue_oldest_pending_age_seconds{queue="xflow:default"} 90`,
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

// TestAssignmentQueueDepthIsALevelWithoutNamespace pins the two deliberate
// choices in this series: a gauge (the level must be able to fall when the
// backlog drains — a counter could only ever rise) and no namespace label
// (the assignment queues are cluster-wide, so attributing a depth to a tenant
// would be a fabricated dimension). An idle lane is reported as a zero, so
// "configured and drained" is distinguishable from "never read".
func TestAssignmentQueueDepthIsALevelWithoutNamespace(t *testing.T) {
	metrics := New()
	observer := NewSweepMetrics(metrics)
	ctx := context.Background()
	observer.OnAssignmentQueueDepth(ctx, "legacy", 4)
	observer.OnAssignmentQueueDepth(ctx, "xflow.sas.webscan-sink", 0)

	body := func() string {
		req := httptest.NewRequest("GET", "/metrics", nil)
		rec := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(rec, req)
		return rec.Body.String()
	}()

	for _, want := range []string{
		`xflow_assignment_queue_depth{lane="legacy"} 4`,
		`xflow_assignment_queue_depth{lane="xflow.sas.webscan-sink"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `xflow_assignment_queue_depth{namespace=`) ||
		strings.Contains(body, `xflow_assignment_queue_depth{lane="legacy",namespace=`) {
		t.Fatalf("depth series carries a namespace label:\n%s", body)
	}

	// The next read must move the same series down: that is what Set is for,
	// and it is the movement a counter cannot express.
	observer.OnAssignmentQueueDepth(ctx, "legacy", 1)
	body = func() string {
		req := httptest.NewRequest("GET", "/metrics", nil)
		rec := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(rec, req)
		return rec.Body.String()
	}()
	if !strings.Contains(body, `xflow_assignment_queue_depth{lane="legacy"} 1`) {
		t.Fatalf("depth did not fall with the backlog:\n%s", body)
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

func TestRunnerMetricsProxyHelpTextsRegistered(t *testing.T) {
	names := []string{
		"xflow_runner_up",
		"xflow_runner_metrics_last_report_age_seconds",
		"xflow_runner_metrics_received_total",
		"xflow_runner_metrics_rejected_total",
		"xflow_runner_metrics_inbox_size",
		"xflow_runner_metrics_gather_errors_total",
		"xflow_runner_metrics_reports_total",
		"xflow_runner_metrics_report_bytes",
	}
	for _, name := range names {
		help, ok := metricHelp[name]
		if !ok {
			t.Errorf("metricHelp missing %q; it would fall back to the generic description", name)
			continue
		}
		if help == "" {
			t.Errorf("metricHelp[%q] is empty", name)
		}
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }

// TestDeliveryLagHelpDoesNotClaimLoss pins the corrected reading of the lag
// series: a sample above the retention TTL proves the execution's state was
// exposed to expiry while the task waited, NOT that the work was lost, because
// commits and advances re-EXPIRE the status key and the task can still route
// to the live execution. The old text called those samples "direct proof of
// the loss" and was falsified by exactly that path. It must also not claim to
// be the exclusive pre-loss signal: xflow_queue_oldest_pending_age_seconds
// reports the same condition from the broker side. And it must carry the same
// falsifiable reading as the drop counter it points at: the gone verdict is a
// claim, not proof, so no "confirmed"/"confirms" wording may survive here.
func TestDeliveryLagHelpDoesNotClaimLoss(t *testing.T) {
	help, ok := metricHelp["xflow_task_delivery_lag_seconds"]
	if !ok || help == "" {
		t.Fatal("metricHelp[xflow_task_delivery_lag_seconds] is missing or empty")
	}
	for _, forbidden := range []string{"proof of the loss", "the only signal", "confirm"} {
		if strings.Contains(help, forbidden) {
			t.Fatalf("delivery-lag help overclaims %q:\n%s", forbidden, help)
		}
	}
	for _, want := range []string{
		"re-EXPIRE",
		"exposed to expiry",
		"a claim, not proof",
		`reason="execution_unattributed"`,
		"xflow_queue_oldest_pending_age_seconds",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("delivery-lag help missing %q:\n%s", want, help)
		}
	}
}

// TestDroppedHelpDocumentsTheTriState pins the drop counter's help as a
// falsifiable claim: it names the unattributed middle, bounds the gone verdict
// to the retention window, counts system-task deliveries, and — since false
// positives and unprovable losses both exist — must NOT present execution_gone
// as a lower bound on loss. The previous revision pinned the overclaim
// ("LOWER BOUND") as required text, which is what locked it in.
func TestDroppedHelpDocumentsTheTriState(t *testing.T) {
	help, ok := metricHelp["xflow_dispatch_dropped_total"]
	if !ok || help == "" {
		t.Fatal("metricHelp[xflow_dispatch_dropped_total] is missing or empty")
	}
	for _, want := range []string{
		"execution_unattributed",
		"inside the execution's retention window",
		"upper bound",
		"System-task (advance/skip) deliveries for an inactive execution are counted under the same reasons",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("dropped-total help missing %q:\n%s", want, help)
		}
	}
	for _, forbidden := range []string{"LOWER BOUND", "provable lost work", "not counted here"} {
		if strings.Contains(help, forbidden) {
			t.Fatalf("dropped-total help still carries the falsified claim %q:\n%s", forbidden, help)
		}
	}
}
