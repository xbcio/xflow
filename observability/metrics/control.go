package metrics

import "time"

// Control-plane metric names.
const (
	metricLeaseSweepReclaimed      = "xflow_lease_sweep_reclaimed_total"
	metricLeaseAge                 = "xflow_lease_age_seconds"
	metricLeaseSweepErrors         = "xflow_lease_sweep_errors_total"
	metricLeaseSweepScan           = "xflow_lease_sweep_scan_total"
	metricLeaseSweepScanDuration   = "xflow_lease_sweep_scan_duration_seconds"
	metricLeaseSweepCandidates     = "xflow_lease_sweep_candidates"
	metricLeaseReclaim             = "xflow_lease_reclaim_total"
	metricLeaseReclaimDuration     = "xflow_lease_reclaim_duration_seconds"
	metricLeaseSweepRepair         = "xflow_lease_sweep_repair_total"
	metricLeaseSweepRepairDuration = "xflow_lease_sweep_repair_duration_seconds"
	metricLeaseSweepRepairReconciled = "xflow_lease_sweep_repair_reconciled"
	metricRunnerAuthDecisions      = "xflow_runner_auth_decisions_total"
	metricRunnerClaimReclaimed     = "xflow_runner_claim_reclaimed_total"
	metricRunnerLeaseReplayed      = "xflow_runner_lease_replayed_total"
	metricDispatchTransient        = "xflow_dispatch_transient_total"
)

// Local mirror interfaces avoid an import cycle with service/control.
type authObserver interface {
	OnAuthDecision(op, result, authMode string)
}

type sweepObserver interface {
	OnSweepReclaim(execID, nodeName string, ageMs int64)
	OnSweepRace(execID, nodeName string)
	OnSweepError(execID, nodeName string, err error)
}

type sweepTimingObserver interface {
	OnSweepListExpired(candidates int, elapsed time.Duration, err error)
	OnSweepReclaimResult(result string, elapsed time.Duration)
	OnSweepRepair(reconciled int, elapsed time.Duration, err error)
}

type runnerClaimObserver interface {
	OnRunnerClaimReclaimed(count int)
	OnRunnerLeaseReplayed()
}

type dispatcherObserver interface {
	OnDispatchTransient(reason string)
}

// AuthMetrics observes runner-protocol auth decisions.
type AuthMetrics struct {
	Metrics *Metrics
}

func NewAuthMetrics(metrics *Metrics) AuthMetrics {
	return AuthMetrics{Metrics: metrics}
}

func (a AuthMetrics) OnAuthDecision(_, result, authMode string) {
	a.Metrics.Inc(metricRunnerAuthDecisions, map[string]string{"result": result, "auth_mode": authMode})
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

func (s SweepMetrics) OnSweepReclaim(_, _ string, ageMs int64) {
	s.Metrics.Inc(metricLeaseSweepReclaimed, map[string]string{"result": "reclaimed"})
	s.Metrics.Observe(metricLeaseAge, map[string]string{"result": "reclaimed"}, time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepRace(_, _ string) {
	s.Metrics.Inc(metricLeaseSweepReclaimed, map[string]string{"result": "race"})
}

func (s SweepMetrics) OnSweepError(_, _ string, _ error) {
	s.Metrics.Inc(metricLeaseSweepErrors, map[string]string{"reason": "reclaim_error"})
}

func (s SweepMetrics) OnSweepListExpired(candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
	s.Metrics.Inc(metricLeaseSweepScan, labels)
	s.Metrics.Observe(metricLeaseSweepScanDuration, labels, elapsed)
	s.Metrics.Set(metricLeaseSweepCandidates, nil, float64(candidates))
}

func (s SweepMetrics) OnSweepReclaimResult(result string, elapsed time.Duration) {
	labels := map[string]string{"result": result}
	s.Metrics.Inc(metricLeaseReclaim, labels)
	s.Metrics.Observe(metricLeaseReclaimDuration, labels, elapsed)
}

func (s SweepMetrics) OnSweepRepair(reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := map[string]string{"result": result}
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

func (r RunnerClaimMetrics) OnRunnerClaimReclaimed(count int) {
	for i := 0; i < count; i++ {
		r.Metrics.Inc(metricRunnerClaimReclaimed, nil)
	}
}

func (r RunnerClaimMetrics) OnRunnerLeaseReplayed() {
	r.Metrics.Inc(metricRunnerLeaseReplayed, nil)
}

var _ runnerClaimObserver = RunnerClaimMetrics{}

// DispatcherMetrics observes retryable dispatcher placement failures.
type DispatcherMetrics struct {
	Metrics *Metrics
}

func NewDispatcherMetrics(metrics *Metrics) DispatcherMetrics {
	return DispatcherMetrics{Metrics: metrics}
}

func (d DispatcherMetrics) OnDispatchTransient(reason string) {
	d.Metrics.Inc(metricDispatchTransient, map[string]string{"reason": reason})
}

var _ dispatcherObserver = DispatcherMetrics{}
