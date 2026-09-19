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
	metricSupplyHintReadErrors       = "xflow_supply_hint_read_errors_total"
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

type supplyHintObserver interface {
	OnSupplyHintReadError(ctx context.Context, namespace, cause string)
}

// SupplyHintMetrics observes failures reading supply content while the control
// plane computes heartbeat hints.
//
// This exists because that path used to be entirely silent: a decrypt failure
// (the shape a changed at-rest KEK produces) and the expected "not written yet"
// case were folded into one `continue`, so an unreadable supply produced no log
// and no metric on every heartbeat, while the runner kept serving last-good
// content and /readyz stayed green — even though the HTTP GET for that same row
// answered 500. The log half is now in service/control; this is the half that
// can page someone who is not watching logs.
type SupplyHintMetrics struct {
	Metrics *Metrics
}

func NewSupplyHintMetrics(metrics *Metrics) SupplyHintMetrics {
	return SupplyHintMetrics{Metrics: metrics}
}

// OnSupplyHintReadError counts one failed supply read.
//
// namespace is the parameter, deliberately NOT withNamespace(ctx, ...) as every
// other method in this file does. HintsForRunner iterates its own namespace
// list, so the namespace that actually failed is not necessarily the one on
// ctx — that ctx carries whatever the heartbeat request scoped to, typically
// nothing. Reading it from ctx here would attribute every failure to that
// namespace instead of the real one, which is worse than having no label at
// all: an operator would chase the wrong tenant.
func (s SupplyHintMetrics) OnSupplyHintReadError(_ context.Context, namespace, cause string) {
	s.Metrics.Inc(metricSupplyHintReadErrors, map[string]string{
		"namespace": namespace,
		"cause":     cause,
	})
}

var _ supplyHintObserver = SupplyHintMetrics{}

// AuthMetrics observes runner-protocol auth decisions.
type AuthMetrics struct {
	Metrics *Metrics
}

func NewAuthMetrics(metrics *Metrics) AuthMetrics {
	return AuthMetrics{Metrics: metrics}
}

func (a AuthMetrics) OnAuthDecision(ctx context.Context, _, result, authMode string) {
	a.Metrics.Inc(metricRunnerAuthDecisions, withNamespace(ctx, map[string]string{"result": result, "auth_mode": authMode}))
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
	s.Metrics.Inc(metricLeaseSweepReclaimed, withNamespace(ctx, map[string]string{"result": "reclaimed"}))
	s.Metrics.Observe(metricLeaseAge, withNamespace(ctx, map[string]string{"result": "reclaimed"}), time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepRace(ctx context.Context, _, _ string) {
	s.Metrics.Inc(metricLeaseSweepReclaimed, withNamespace(ctx, map[string]string{"result": "race"}))
}

func (s SweepMetrics) OnSweepError(ctx context.Context, _, _ string, _ error) {
	s.Metrics.Inc(metricLeaseSweepErrors, withNamespace(ctx, map[string]string{"reason": "reclaim_error"}))
}

func (s SweepMetrics) OnSweepReclaimApplied(ctx context.Context, _, _ string, ageMs int64) {
	// Reclaim state was applied but the synchronous outbox flush failed. The
	// durable OutboxDispatcher will retry delivery; count it as applied-pending
	// rather than a completed delivery.
	s.Metrics.Inc(metricLeaseSweepReclaimed, withNamespace(ctx, map[string]string{"result": "applied_pending"}))
	s.Metrics.Observe(metricLeaseAge, withNamespace(ctx, map[string]string{"result": "applied_pending"}), time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepListExpired(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withNamespace(ctx, map[string]string{"result": result})
	s.Metrics.Inc(metricLeaseSweepScan, labels)
	s.Metrics.Observe(metricLeaseSweepScanDuration, labels, elapsed)
	s.Metrics.Set(metricLeaseSweepCandidates, withNamespace(ctx, nil), float64(candidates))
}

func (s SweepMetrics) OnSweepReclaimResult(ctx context.Context, result string, elapsed time.Duration) {
	labels := withNamespace(ctx, map[string]string{"result": result})
	s.Metrics.Inc(metricLeaseReclaim, labels)
	s.Metrics.Observe(metricLeaseReclaimDuration, labels, elapsed)
}

func (s SweepMetrics) OnSweepRepair(ctx context.Context, reconciled int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withNamespace(ctx, map[string]string{"result": result})
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
		r.Metrics.Inc(metricRunnerClaimReclaimed, withNamespace(ctx, nil))
	}
}

func (r RunnerClaimMetrics) OnRunnerLeaseReplayed(ctx context.Context) {
	r.Metrics.Inc(metricRunnerLeaseReplayed, withNamespace(ctx, nil))
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
	d.Metrics.Inc(metricDispatchTransient, withNamespace(ctx, map[string]string{"reason": reason}))
}

var _ dispatcherObserver = DispatcherMetrics{}
