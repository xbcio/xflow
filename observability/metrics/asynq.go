package metrics

import (
	"context"
	"time"
)

// Asynq backend metric names.
const (
	metricAuditWrite              = "xflow_audit_write_total"
	metricLeaseAcquire            = "xflow_lease_acquire_total"
	metricLeaseAcquireDuration    = "xflow_lease_acquire_duration_seconds"
	metricLeaseExpiryScan         = "xflow_lease_expiry_scan_total"
	metricLeaseExpiryScanDuration = "xflow_lease_expiry_scan_duration_seconds"
	metricLeaseExpiryCandidates   = "xflow_lease_expiry_candidates"
	metricLeaseRepairRuns         = "xflow_lease_repair_runs_total"
	metricLeaseRepairDuration     = "xflow_lease_repair_duration_seconds"
	metricLeaseRepairReconciled   = "xflow_lease_repair_reconciled"
)

// Local mirrors keep the observability package below concrete backend
// providers. The service-layer wiring passes these adapters to distributed's
// structurally identical observer contracts, so that call site also provides a
// compile-time compatibility check without reversing the dependency direction.
type auditObserver interface {
	OnAuditOK(ctx context.Context, op string)
	OnAuditFailed(ctx context.Context, op string, err error)
}

type leaseObserver interface {
	OnLeaseAcquire(ctx context.Context, result string, elapsed time.Duration)
	OnLeaseExpiryScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnLeaseRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error)
}

// AuditMetrics observes backend/providers/distributed audit-store dual-write outcomes.
type AuditMetrics struct {
	Metrics *Metrics
}

func NewAuditMetrics(metrics *Metrics) AuditMetrics {
	return AuditMetrics{Metrics: metrics}
}

func (a AuditMetrics) OnAuditOK(ctx context.Context, op string) {
	a.Metrics.Inc(metricAuditWrite, withNamespace(ctx, map[string]string{"op": op, "result": "ok"}))
}

func (a AuditMetrics) OnAuditFailed(ctx context.Context, op string, _ error) {
	a.Metrics.Inc(metricAuditWrite, withNamespace(ctx, map[string]string{"op": op, "result": "failed"}))
}

var _ auditObserver = AuditMetrics{}

// LeaseMetrics observes backend/providers/distributed lease lifecycle operations.
type LeaseMetrics struct {
	Metrics *Metrics
}

// NewLeaseMetrics creates a Redis lease observer backed by Metrics.
func NewLeaseMetrics(metrics *Metrics) LeaseMetrics {
	return LeaseMetrics{Metrics: metrics}
}

// OnLeaseAcquire records a lease acquisition attempt and its storage latency.
func (l LeaseMetrics) OnLeaseAcquire(ctx context.Context, result string, elapsed time.Duration) {
	labels := withNamespace(ctx, map[string]string{"result": result})
	l.Metrics.Inc(metricLeaseAcquire, labels)
	l.Metrics.Observe(metricLeaseAcquireDuration, labels, elapsed)
}

// OnLeaseExpiryScan records an expiry-index scan and its candidate count.
func (l LeaseMetrics) OnLeaseExpiryScan(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withNamespace(ctx, map[string]string{"result": result})
	l.Metrics.Inc(metricLeaseExpiryScan, labels)
	l.Metrics.Observe(metricLeaseExpiryScanDuration, labels, elapsed)
	l.Metrics.Set(metricLeaseExpiryCandidates, withNamespace(ctx, nil), float64(candidates))
}

// OnLeaseRepair records a bounded lease-index reconciliation pass.
func (l LeaseMetrics) OnLeaseRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withNamespace(ctx, map[string]string{"result": result})
	l.Metrics.Inc(metricLeaseRepairRuns, labels)
	l.Metrics.Observe(metricLeaseRepairDuration, labels, elapsed)
	l.Metrics.Set(metricLeaseRepairReconciled, labels, float64(reconciled))
}

var _ leaseObserver = LeaseMetrics{}
