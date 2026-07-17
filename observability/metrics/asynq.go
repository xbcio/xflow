package metrics

import (
	"time"

	"github.com/xbcio/xflow/backend/distributed"
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

// AuditMetrics observes backend/distributed audit-store dual-write outcomes.
type AuditMetrics struct {
	Metrics *Metrics
}

func NewAuditMetrics(metrics *Metrics) AuditMetrics {
	return AuditMetrics{Metrics: metrics}
}

func (a AuditMetrics) OnAuditOK(op string) {
	a.Metrics.Inc(metricAuditWrite, map[string]string{"op": op, "result": "ok"})
}

func (a AuditMetrics) OnAuditFailed(op string, _ error) {
	a.Metrics.Inc(metricAuditWrite, map[string]string{"op": op, "result": "failed"})
}

var _ distributed.AuditObserver = AuditMetrics{}

// LeaseMetrics observes backend/distributed lease lifecycle operations.
type LeaseMetrics struct {
	Metrics *Metrics
}

// NewLeaseMetrics creates a Redis lease observer backed by Metrics.
func NewLeaseMetrics(metrics *Metrics) LeaseMetrics {
	return LeaseMetrics{Metrics: metrics}
}

// OnLeaseAcquire records a lease acquisition attempt and its storage latency.
func (l LeaseMetrics) OnLeaseAcquire(result string, elapsed time.Duration) {
	labels := map[string]string{"result": result}
	l.Metrics.Inc(metricLeaseAcquire, labels)
	l.Metrics.Observe(metricLeaseAcquireDuration, labels, elapsed)
}

// OnLeaseExpiryScan records an expiry-index scan and its candidate count.
func (l LeaseMetrics) OnLeaseExpiryScan(candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	l.Metrics.Inc(metricLeaseExpiryScan, labels)
	l.Metrics.Observe(metricLeaseExpiryScanDuration, labels, elapsed)
	l.Metrics.Set(metricLeaseExpiryCandidates, nil, float64(candidates))
}

// OnLeaseRepair records a bounded lease-index reconciliation pass.
func (l LeaseMetrics) OnLeaseRepair(reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	l.Metrics.Inc(metricLeaseRepairRuns, labels)
	l.Metrics.Observe(metricLeaseRepairDuration, labels, elapsed)
	l.Metrics.Set(metricLeaseRepairReconciled, labels, float64(reconciled))
}

var _ distributed.LeaseObserver = LeaseMetrics{}
