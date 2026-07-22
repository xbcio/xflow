package metrics

import (
	"context"
	"time"
)

// Control-plane metric names.
const (
	metricLeaseSweepReclaimed        = "xflow_lease_sweep_reclaimed_total"
	metricLeaseAge                   = "xflow_lease_age_seconds"
	metricLeaseSweepErrors           = "xflow_lease_sweep_errors_total"
	metricLeaseSweepScan             = "xflow_lease_sweep_scan_total"
	metricLeaseSweepScanDuration     = "xflow_lease_sweep_scan_duration_seconds"
	metricLeaseSweepCandidates       = "xflow_lease_sweep_candidates"
	metricLeaseReclaim               = "xflow_lease_reclaim_total"
	metricLeaseReclaimDuration       = "xflow_lease_reclaim_duration_seconds"
	metricLeaseSweepRepair           = "xflow_lease_sweep_repair_total"
	metricLeaseSweepRepairDuration   = "xflow_lease_sweep_repair_duration_seconds"
	metricLeaseSweepRepairReconciled = "xflow_lease_sweep_repair_reconciled"
	metricRunnerAuthDecisions        = "xflow_runner_auth_decisions_total"
	metricRunnerClaimReclaimed       = "xflow_runner_claim_reclaimed_total"
	metricRunnerLeaseReplayed        = "xflow_runner_lease_replayed_total"
	metricDispatchTransient          = "xflow_dispatch_transient_total"
)

// Local mirror interfaces avoid an import cycle with service/control.
type authObserver interface {
	OnAuthDecision(ctx context.Context, op, result, authMode string)
}

type sweepObserver interface {
	OnSweepReclaim(ctx context.Context, execID, nodeName string, ageMs int64)
	OnSweepRace(ctx context.Context, execID, nodeName string)
	OnSweepError(ctx context.Context, execID, nodeName string, err error)
}

type sweepTimingObserver interface {
	OnSweepListExpired(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnSweepReclaimResult(ctx context.Context, result string, elapsed time.Duration)
	OnSweepRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error)
}

type runnerClaimObserver interface {
	OnRunnerClaimReclaimed(ctx context.Context, count int)
	OnRunnerLeaseReplayed(ctx context.Context)
}

type dispatcherObserver interface {
	OnDispatchTransient(ctx context.Context, reason string)
}

// AuthMetrics observes runner-protocol auth decisions.
type AuthMetrics struct {
	Metrics *Metrics
}

func NewAuthMetrics(metrics *Metrics) AuthMetrics {
	return AuthMetrics{Metrics: metrics}
}

func (a AuthMetrics) OnAuthDecision(ctx context.Context, _, result, authMode string) {
	a.Metrics.Inc(metricRunnerAuthDecisions, withTenant(ctx, map[string]string{"result": result, "auth_mode": authMode}))
}

var _ authObserver = AuthMetrics{}

// SweepMetrics observes lease sweeper outcomes.
// It satisfies both sweepObserver and sweepTimingObserver.
type SweepMetrics struct {
	Metrics *Metrics
}

func NewSweepMetrics(metrics *Metrics) SweepMetrics {
	return SweepMetrics{Metrics: metrics}
}

func (s SweepMetrics) OnSweepReclaim(ctx context.Context, _, _ string, ageMs int64) {
	s.Metrics.Inc(metricLeaseSweepReclaimed, withTenant(ctx, map[string]string{"result": "reclaimed"}))
	s.Metrics.Observe(metricLeaseAge, withTenant(ctx, map[string]string{"result": "reclaimed"}), time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepRace(ctx context.Context, _, _ string) {
	s.Metrics.Inc(metricLeaseSweepReclaimed, withTenant(ctx, map[string]string{"result": "race"}))
}

func (s SweepMetrics) OnSweepError(ctx context.Context, _, _ string, _ error) {
	s.Metrics.Inc(metricLeaseSweepErrors, withTenant(ctx, map[string]string{"reason": "reclaim_error"}))
}

func (s SweepMetrics) OnSweepReclaimApplied(ctx context.Context, _, _ string, ageMs int64) {
	// Reclaim state was applied but the synchronous outbox flush failed. The
	// durable OutboxDispatcher will retry delivery; count it as applied-pending
	// rather than a completed delivery.
	s.Metrics.Inc(metricLeaseSweepReclaimed, withTenant(ctx, map[string]string{"result": "applied_pending"}))
	s.Metrics.Observe(metricLeaseAge, withTenant(ctx, map[string]string{"result": "applied_pending"}), time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepListExpired(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withTenant(ctx, map[string]string{"result": result})
	s.Metrics.Inc(metricLeaseSweepScan, labels)
	s.Metrics.Observe(metricLeaseSweepScanDuration, labels, elapsed)
	s.Metrics.Set(metricLeaseSweepCandidates, withTenant(ctx, nil), float64(candidates))
}

func (s SweepMetrics) OnSweepReclaimResult(ctx context.Context, result string, elapsed time.Duration) {
	labels := withTenant(ctx, map[string]string{"result": result})
	s.Metrics.Inc(metricLeaseReclaim, labels)
	s.Metrics.Observe(metricLeaseReclaimDuration, labels, elapsed)
}

func (s SweepMetrics) OnSweepRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withTenant(ctx, map[string]string{"result": result})
	s.Metrics.Inc(metricLeaseSweepRepair, labels)
	s.Metrics.Observe(metricLeaseSweepRepairDuration, labels, elapsed)
	s.Metrics.Set(metricLeaseSweepRepairReconciled, labels, float64(reconciled))
}

var (
	_ sweepObserver       = SweepMetrics{}
	_ sweepTimingObserver = SweepMetrics{}
)

// RunnerClaimMetrics observes durable runner-directory claim recovery and
// finalized lease replay events.
type RunnerClaimMetrics struct {
	Metrics *Metrics
}

func NewRunnerClaimMetrics(metrics *Metrics) RunnerClaimMetrics {
	return RunnerClaimMetrics{Metrics: metrics}
}

func (r RunnerClaimMetrics) OnRunnerClaimReclaimed(ctx context.Context, count int) {
	for i := 0; i < count; i++ {
		r.Metrics.Inc(metricRunnerClaimReclaimed, withTenant(ctx, nil))
	}
}

func (r RunnerClaimMetrics) OnRunnerLeaseReplayed(ctx context.Context) {
	r.Metrics.Inc(metricRunnerLeaseReplayed, withTenant(ctx, nil))
}

var _ runnerClaimObserver = RunnerClaimMetrics{}

// DispatcherMetrics observes retryable dispatcher placement failures.
type DispatcherMetrics struct {
	Metrics *Metrics
}

func NewDispatcherMetrics(metrics *Metrics) DispatcherMetrics {
	return DispatcherMetrics{Metrics: metrics}
}

func (d DispatcherMetrics) OnDispatchTransient(ctx context.Context, reason string) {
	d.Metrics.Inc(metricDispatchTransient, withTenant(ctx, map[string]string{"reason": reason}))
}

var _ dispatcherObserver = DispatcherMetrics{}
