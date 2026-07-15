package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	backendasynq "github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// NodeTypeFunc resolves a node type for metrics labels. Return an empty string
// when the type is unknown.
type NodeTypeFunc func(types.ExecutionID, string) string

// HookChain fans engine lifecycle hooks out to multiple receivers.
type HookChain []engine.Hooks

func (c HookChain) OnNodeStart(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeStart(ctx, id, name) })
}
func (c HookChain) OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeComplete(ctx, id, name, status) })
}
func (c HookChain) OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeSuspended(ctx, id, name) })
}
func (c HookChain) OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnExecutionComplete(ctx, id, status) })
}
func (c HookChain) OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalDelivered(ctx, id, signalName, data) })
}
func (c HookChain) OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalRevoked(ctx, id, signalName) })
}
func (c HookChain) OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeTimeout(ctx, id, nodeName) })
}
func (c HookChain) OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, attempt int, delay time.Duration) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeRetry(ctx, id, name, attempt, delay) })
}

func (c HookChain) each(ctx context.Context, fn func(engine.Hooks, context.Context)) {
	for _, h := range c {
		if h == nil {
			continue
		}
		engine.SafeHook(ctx, nil, func(ctx context.Context) { fn(h, ctx) })
	}
}

// MetricsHooks turns engine lifecycle hooks into xflow_ Prometheus counters.
type MetricsHooks struct {
	Metrics  *metrics.Metrics
	NodeType NodeTypeFunc

	started sync.Map // "exec\x00node" -> time.Time
}

func NewMetricsHooks(metrics *metrics.Metrics) *MetricsHooks {
	return &MetricsHooks{Metrics: metrics}
}

func (h *MetricsHooks) OnNodeStart(_ context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc("xflow_node_started_total", h.nodeLabels(id, name, ""))
	h.started.Store(h.nodeKey(id, name), time.Now())
}

func (h *MetricsHooks) OnNodeComplete(_ context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	labels := h.nodeLabels(id, name, string(status))
	h.Metrics.Inc("xflow_node_completed_total", labels)
	if started, ok := h.started.LoadAndDelete(h.nodeKey(id, name)); ok {
		h.Metrics.Observe("xflow_node_duration_seconds", labels, time.Since(started.(time.Time)))
	}
}

func (h *MetricsHooks) OnNodeSuspended(_ context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc("xflow_node_suspended_total", h.nodeLabels(id, name, ""))
}

func (h *MetricsHooks) OnExecutionComplete(_ context.Context, _ types.ExecutionID, status types.ExecutionStatus) {
	h.Metrics.Inc("xflow_execution_completed_total", map[string]string{"status": string(status)})
}

func (h *MetricsHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *MetricsHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}

func (h *MetricsHooks) OnNodeTimeout(_ context.Context, id types.ExecutionID, nodeName string) {
	h.Metrics.Inc("xflow_node_timed_out_total", h.nodeLabels(id, nodeName, ""))
}

func (h *MetricsHooks) OnNodeRetry(_ context.Context, id types.ExecutionID, name string, _ int, _ time.Duration) {
	h.Metrics.Inc("xflow_node_retried_total", h.nodeLabels(id, name, ""))
}

func (h *MetricsHooks) nodeLabels(id types.ExecutionID, name string, status string) map[string]string {
	labels := map[string]string{"node": name}
	nodeType := ""
	if h.NodeType != nil {
		nodeType = h.NodeType(id, name)
	}
	if nodeType == "" {
		nodeType = "unknown"
	}
	labels["node_type"] = nodeType
	if status != "" {
		labels["status"] = status
	}
	return labels
}

func (*MetricsHooks) nodeKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("%s\x00%s", id, name)
}

// AuditMetrics observes backend/asynq audit-store dual-write outcomes.
type AuditMetrics struct {
	Metrics *metrics.Metrics
}

func NewAuditMetrics(metrics *metrics.Metrics) AuditMetrics {
	return AuditMetrics{Metrics: metrics}
}

func (a AuditMetrics) OnAuditOK(op string) {
	a.Metrics.Inc("xflow_audit_write_total", map[string]string{"op": op, "result": "ok"})
}

func (a AuditMetrics) OnAuditFailed(op string, _ error) {
	a.Metrics.Inc("xflow_audit_write_total", map[string]string{"op": op, "result": "failed"})
}

// SweepMetrics observes lease sweeper outcomes.
type SweepMetrics struct {
	Metrics *metrics.Metrics
}

func NewSweepMetrics(metrics *metrics.Metrics) SweepMetrics {
	return SweepMetrics{Metrics: metrics}
}

func (s SweepMetrics) OnSweepReclaim(_, _ string, ageMs int64) {
	s.Metrics.Inc("xflow_lease_sweep_reclaimed_total", map[string]string{"result": "reclaimed"})
	s.Metrics.Observe("xflow_lease_age_seconds", map[string]string{"result": "reclaimed"}, time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepRace(_, _ string) {
	s.Metrics.Inc("xflow_lease_sweep_reclaimed_total", map[string]string{"result": "race"})
}

func (s SweepMetrics) OnSweepError(_, _ string, _ error) {
	s.Metrics.Inc("xflow_lease_sweep_errors_total", map[string]string{"reason": "reclaim_error"})
}

// DispatcherMetrics observes retryable dispatcher placement failures.
type DispatcherMetrics struct {
	Metrics *metrics.Metrics
}

func NewDispatcherMetrics(metrics *metrics.Metrics) DispatcherMetrics {
	return DispatcherMetrics{Metrics: metrics}
}

func (d DispatcherMetrics) OnDispatchTransient(reason string) {
	d.Metrics.Inc("xflow_dispatch_transient_total", map[string]string{"reason": reason})
}

// AuthMetrics observes runner-protocol auth decisions.
type AuthMetrics struct {
	Metrics *metrics.Metrics
}

func NewAuthMetrics(metrics *metrics.Metrics) AuthMetrics {
	return AuthMetrics{Metrics: metrics}
}

func (a AuthMetrics) OnAuthDecision(_, result, authMode string) {
	a.Metrics.Inc("xflow_runner_auth_decisions_total", map[string]string{"result": result, "auth_mode": authMode})
}

// CommitMetrics observes structured runner result-commit outcomes.
type CommitMetrics struct {
	Metrics *metrics.Metrics
}

// NewCommitMetrics creates a commit outcome observer backed by Metrics.
func NewCommitMetrics(metrics *metrics.Metrics) CommitMetrics {
	return CommitMetrics{Metrics: metrics}
}

// OnCommitOutcome records one stable low-cardinality commit classification.
func (c CommitMetrics) OnCommitOutcome(_ context.Context, outcome engine.CommitOutcome) {
	c.Metrics.Inc("xflow_commit_outcomes_total", map[string]string{"outcome": string(outcome)})
}

// OutboxMetrics observes durable outbox delivery failures and periodic backlog
// snapshots. It intentionally does not expose entry, execution, or error IDs.
type OutboxMetrics struct {
	Metrics *metrics.Metrics
}

// NewOutboxMetrics creates a durable outbox observer backed by Metrics.
func NewOutboxMetrics(metrics *metrics.Metrics) OutboxMetrics {
	return OutboxMetrics{Metrics: metrics}
}

// OnOutboxRetry records a retryable task-queue handoff failure.
func (o OutboxMetrics) OnOutboxRetry(context.Context, int) {
	o.Metrics.Inc("xflow_outbox_retries_total", nil)
}

// OnOutboxDeadLetter records an entry moved to durable dead-letter storage.
func (o OutboxMetrics) OnOutboxDeadLetter(context.Context) {
	o.Metrics.Inc("xflow_outbox_dead_letters_total", nil)
}

// OnOutboxPending records the current pending and dead-letter backlog gauges
// and observes the age of the oldest pending entry when one exists.
func (o OutboxMetrics) OnOutboxPending(_ context.Context, pending int, deadLettered int, oldestAge time.Duration) {
	o.Metrics.Set("xflow_outbox_pending", nil, float64(pending))
	o.Metrics.Set("xflow_outbox_dead_letters", nil, float64(deadLettered))
	if pending > 0 {
		o.Metrics.Observe("xflow_outbox_oldest_pending_age_seconds", nil, oldestAge)
	}
}

// OnOutboxError records an outbox operation failure without using error text
// as a metric label.
func (o OutboxMetrics) OnOutboxError(_ context.Context, operation string, _ error) {
	o.Metrics.Inc("xflow_outbox_errors_total", map[string]string{"op": operation})
}

// LeaseMetrics observes backend/asynq lease lifecycle operations.
type LeaseMetrics struct {
	Metrics *metrics.Metrics
}

// NewLeaseMetrics creates a Redis lease observer backed by Metrics.
func NewLeaseMetrics(metrics *metrics.Metrics) LeaseMetrics {
	return LeaseMetrics{Metrics: metrics}
}

// OnLeaseAcquire records a lease acquisition attempt and its storage latency.
func (l LeaseMetrics) OnLeaseAcquire(result string, elapsed time.Duration) {
	labels := map[string]string{"result": result}
	l.Metrics.Inc("xflow_lease_acquire_total", labels)
	l.Metrics.Observe("xflow_lease_acquire_duration_seconds", labels, elapsed)
}

// OnLeaseExpiryScan records an expiry-index scan and its candidate count.
func (l LeaseMetrics) OnLeaseExpiryScan(candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	l.Metrics.Inc("xflow_lease_expiry_scan_total", labels)
	l.Metrics.Observe("xflow_lease_expiry_scan_duration_seconds", labels, elapsed)
	l.Metrics.Set("xflow_lease_expiry_candidates", nil, float64(candidates))
}

// OnLeaseRepair records a bounded lease-index reconciliation pass.
func (l LeaseMetrics) OnLeaseRepair(reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	l.Metrics.Inc("xflow_lease_repair_runs_total", labels)
	l.Metrics.Observe("xflow_lease_repair_duration_seconds", labels, elapsed)
	l.Metrics.Set("xflow_lease_repair_reconciled", labels, float64(reconciled))
}

// OnSweepListExpired records lease-sweeper scan latency when SweepMetrics is
// installed as the optional SweepTimingObserver extension.
func (s SweepMetrics) OnSweepListExpired(candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	s.Metrics.Inc("xflow_lease_sweep_scan_total", labels)
	s.Metrics.Observe("xflow_lease_sweep_scan_duration_seconds", labels, elapsed)
	s.Metrics.Set("xflow_lease_sweep_candidates", nil, float64(candidates))
}

// OnSweepReclaimResult records the duration and outcome of one fenced reclaim.
func (s SweepMetrics) OnSweepReclaimResult(result string, elapsed time.Duration) {
	labels := map[string]string{"result": result}
	s.Metrics.Inc("xflow_lease_reclaim_total", labels)
	s.Metrics.Observe("xflow_lease_reclaim_duration_seconds", labels, elapsed)
}

// OnSweepRepair records the duration and reconciliation count of a sweeper
// initiated lease-index repair pass.
func (s SweepMetrics) OnSweepRepair(reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	s.Metrics.Inc("xflow_lease_sweep_repair_total", labels)
	s.Metrics.Observe("xflow_lease_sweep_repair_duration_seconds", labels, elapsed)
	s.Metrics.Set("xflow_lease_sweep_repair_reconciled", labels, float64(reconciled))
}

// RunnerClaimMetrics observes durable runner-directory claim recovery and
// finalized lease replay events.
type RunnerClaimMetrics struct {
	Metrics *metrics.Metrics
}

// NewRunnerClaimMetrics creates a runner-directory observer backed by Metrics.
func NewRunnerClaimMetrics(metrics *metrics.Metrics) RunnerClaimMetrics {
	return RunnerClaimMetrics{Metrics: metrics}
}

// OnRunnerClaimReclaimed records each expired claim returned to the durable queue.
func (r RunnerClaimMetrics) OnRunnerClaimReclaimed(count int) {
	for i := 0; i < count; i++ {
		r.Metrics.Inc("xflow_runner_claim_reclaimed_total", nil)
	}
}

// OnRunnerLeaseReplayed records one durable lease replay to a runner session.
func (r RunnerClaimMetrics) OnRunnerLeaseReplayed() {
	r.Metrics.Inc("xflow_runner_lease_replayed_total", nil)
}

var _ engine.CommitObserver = CommitMetrics{}
var _ engine.OutboxObserver = OutboxMetrics{}
var _ backendasynq.LeaseObserver = LeaseMetrics{}
