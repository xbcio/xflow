package metrics

import (
	"context"
	"time"
)

// Reconcile metric names (T9 audit reconcile worker).
const (
	metricAuditReconcileScan            = "xflow_audit_reconcile_scan_total"
	metricAuditReconcileScanDuration    = "xflow_audit_reconcile_scan_duration_seconds"
	metricAuditReconcileSettled         = "xflow_audit_reconcile_settled_total"
	metricAuditReconcileSkipped         = "xflow_audit_reconcile_skipped_total"
	metricAuditReconcileErrors         = "xflow_audit_reconcile_errors_total"
	metricAuditReconcileBacklogAge     = "xflow_audit_reconcile_backlog_age_seconds"
	metricAuditReconcilePending        = "xflow_audit_reconcile_pending"
)

// reconcileObserver is the local mirror of control.ReconcileObserver, kept
// here to avoid an import cycle with service/control (which depends on
// observability/metrics). The method set must match control.ReconcileObserver
// exactly.
type reconcileObserver interface {
	OnReconcileScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnReconcileSettled(ctx context.Context, outcome string, appended bool, ageMs int64)
	OnReconcileSkipped(ctx context.Context, reason string)
	OnReconcileError(ctx context.Context, requestID string, err error)
	OnReconcileBacklog(ctx context.Context, oldestAge time.Duration, pending int)
}

// ReconcileMetrics observes the T9 audit reconcile worker. It emits per-sweep
// scan counters/durations, per-admission settled/skipped/error counters, and
// a backlog-age gauge + pending gauge so a persistent audit backlog (e.g. a
// sustained authority or SQL outage) is observable as an alarm.
type ReconcileMetrics struct {
	Metrics *Metrics
}

// NewReconcileMetrics wires the audit reconcile worker's observer into the
// Prometheus registry.
func NewReconcileMetrics(metrics *Metrics) ReconcileMetrics {
	return ReconcileMetrics{Metrics: metrics}
}

func (r ReconcileMetrics) OnReconcileScan(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withTenant(ctx, map[string]string{"result": result})
	r.Metrics.Inc(metricAuditReconcileScan, labels)
	r.Metrics.Observe(metricAuditReconcileScanDuration, labels, elapsed)
}

func (r ReconcileMetrics) OnReconcileSettled(ctx context.Context, outcome string, appended bool, ageMs int64) {
	result := "appended"
	if !appended {
		result = "duplicate"
	}
	labels := withTenant(ctx, map[string]string{"outcome": outcome, "result": result})
	r.Metrics.Inc(metricAuditReconcileSettled, labels)
}

func (r ReconcileMetrics) OnReconcileSkipped(ctx context.Context, reason string) {
	r.Metrics.Inc(metricAuditReconcileSkipped, withTenant(ctx, map[string]string{"reason": reason}))
}

func (r ReconcileMetrics) OnReconcileError(ctx context.Context, _ string, _ error) {
	r.Metrics.Inc(metricAuditReconcileErrors, withTenant(ctx, nil))
}

func (r ReconcileMetrics) OnReconcileBacklog(ctx context.Context, oldestAge time.Duration, pending int) {
	r.Metrics.Set(metricAuditReconcileBacklogAge, withTenant(ctx, nil), oldestAge.Seconds())
	r.Metrics.Set(metricAuditReconcilePending, withTenant(ctx, nil), float64(pending))
}

var _ reconcileObserver = ReconcileMetrics{}
