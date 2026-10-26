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
	metricLeaseMaintenancePass       = "xflow_lease_maintenance_pass_total"
	metricLeaseMaintenanceReleased   = "xflow_lease_maintenance_released_total"
	metricLeaseMaintenanceCandidates = "xflow_lease_maintenance_candidates_total"
	metricExecutionDeadlineExpired   = "xflow_execution_deadline_expired_total"
	metricRunnerAuthDecisions        = "xflow_runner_auth_decisions_total"
	metricRunnerClaimReclaimed       = "xflow_runner_claim_reclaimed_total"
	metricRunnerLeaseReplayed        = "xflow_runner_lease_replayed_total"
	metricDispatchTransient          = "xflow_dispatch_transient_total"
	metricDispatchDropped            = "xflow_dispatch_dropped_total"
	metricTaskDeliveryLag            = "xflow_task_delivery_lag_seconds"
	metricAssignmentQueueDepth       = "xflow_assignment_queue_depth"
	metricAssignmentQueueOldestAge   = "xflow_assignment_queue_oldest_age_seconds"
	metricRunnerClaimPollTargets     = "xflow_runner_claim_poll_targets"
	metricRunnerClaimPollRoundTrips  = "xflow_runner_claim_poll_round_trips"
	metricRunnerSelectorRejected     = "xflow_runner_selector_rejected_total"
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

type sweepPassObserver interface {
	OnSweepPass(ctx context.Context, pass, outcome string, released int)
}

// sweepPassCandidateObserver mirrors service/control's SweepPassCandidateObserver,
// including its embedding of the pass observer: the candidate count is only
// emitted by an adapter that also emits the pass and released counts.
type sweepPassCandidateObserver interface {
	sweepPassObserver
	OnSweepPassCandidates(ctx context.Context, pass, outcome string, inspected int)
}

// sweepQueueDepthObserver mirrors service/control's optional
// SweepQueueDepthObserver extension: the standing depth of each assignment
// queue, as opposed to what a maintenance pass did with it.
type sweepQueueDepthObserver interface {
	OnAssignmentQueueDepth(ctx context.Context, lane string, depth int64)
}

type sweepQueueAgeObserver interface {
	OnAssignmentQueueOldestAge(ctx context.Context, lane string, age time.Duration)
}

// executionDeadlineObserver mirrors service/control's optional
// ExecutionDeadlineObserver extension.
type executionDeadlineObserver interface {
	OnExecutionDeadlineExpired(ctx context.Context, source string)
}

type runnerClaimObserver interface {
	OnRunnerClaimReclaimed(ctx context.Context, count int)
	OnRunnerLeaseReplayed(ctx context.Context)
}

type dispatcherObserver interface {
	OnDispatchTransient(ctx context.Context, reason string)
}

// dispatcherDropObserver mirrors service/control's optional
// DispatcherDropObserver extension: dispatched tasks dropped without an
// assignment, partitioned by bounded reason.
type dispatcherDropObserver interface {
	OnDispatchDropped(ctx context.Context, reason string)
}

// deliveryLagObserver mirrors service/control's optional DeliveryLagObserver
// extension: how long a consumed task waited after becoming deliverable.
type deliveryLagObserver interface {
	OnTaskDeliveryLag(ctx context.Context, lag time.Duration)
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

// OnSweepPass records one background maintenance pass and how much it released.
//
// pass and outcome are compared and forwarded as literals rather than through
// service/control's SweepPass* constants because this package must not import
// service/control — the local mirror interfaces above exist for that reason.
// service/control's tests assert the literals the sweeper sends, and
// TestSweepPassMetricsSeparateRunsFromSkips pins the two the branch below
// depends on.
//
// The released counter is incremented for a pass that ran, including one that
// released nothing, so "ran and released 0" is a visible zero rather than a
// missing series. A skipped pass reports no released sample at all: a zero
// there would be indistinguishable from the zero above, which is the exact
// confusion this family exists to remove.
func (s SweepMetrics) OnSweepPass(ctx context.Context, pass, outcome string, released int) {
	s.Metrics.Inc(metricLeaseMaintenancePass, withNamespace(ctx, map[string]string{
		"pass":    pass,
		"outcome": outcome,
	}))
	if outcome != "ran" && outcome != "error" {
		return
	}
	s.Metrics.Add(metricLeaseMaintenanceReleased, withNamespace(ctx, map[string]string{"pass": pass}), float64(released))
}

// OnSweepPassCandidates records how many candidates a pass inspected, beside how
// many of them it released.
//
// Gated exactly like the released count: a pass that ran reports its candidate
// count even when it is zero, and a skipped pass reports none, so a zero never
// has to be read as either "inspected nothing" or "did not run".
//
// It exists because the released count alone cannot tell a pass that found
// nothing to do from one whose scope is far wider than the shape it drains: the
// ratio of the two is what answers whether a pass releases what it inspects.
func (s SweepMetrics) OnSweepPassCandidates(ctx context.Context, pass, outcome string, inspected int) {
	if outcome != "ran" && outcome != "error" {
		return
	}
	s.Metrics.Add(metricLeaseMaintenanceCandidates, withNamespace(ctx, map[string]string{"pass": pass}), float64(inspected))
}

// OnExecutionDeadlineExpired counts one execution driven into the timeout
// terminal state by its settings.timeout deadline.
//
// source names the observer that made the transition (service/control sends
// "sweeper" for its deadline pass); it is forwarded as a literal for the same
// reason the pass labels are. Unlike the pass counters, the namespace label is
// the execution's own tenant: the sweeper runs each expiry under the namespace
// its index entry names, so the context passed here is already scoped.
func (s SweepMetrics) OnExecutionDeadlineExpired(ctx context.Context, source string) {
	s.Metrics.Inc(metricExecutionDeadlineExpired, withNamespace(ctx, map[string]string{"source": source}))
}

// OnAssignmentQueueDepth records one assignment queue's depth.
//
// lane is forwarded as-is; service/control resolves the shared legacy queue to
// the literal "legacy" before calling, so the label never carries the empty
// string the directory uses internally, and the set of label values is the
// configured lanes plus that one — bounded by configuration, not by traffic.
//
// Deliberately no namespace label, unlike almost every method in this file:
// the assignment queues are cluster-wide (one per lane node type plus the
// shared legacy queue), so a depth is a property of the queue itself and there
// is no tenant to attribute it to. A namespace label here would be a
// fabricated dimension.
//
// Set is used rather than Inc because a depth is a level, not a count of
// events: it must go down when the backlog drains, which is exactly the
// movement a counter cannot show. A missing series means the reporter did not
// read (not leader, no capability, read failed) — it is never zeroed on
// failure, because a zeroed gauge reads as "the queue drained" and would hide
// the very backlog this series exists to show.
func (s SweepMetrics) OnAssignmentQueueDepth(_ context.Context, lane string, depth int64) {
	s.Metrics.Set(metricAssignmentQueueDepth, map[string]string{"lane": lane}, float64(depth))
}

// OnAssignmentQueueOldestAge records the age, in seconds, of the earliest
// queued-at stamp among the first entries at the head (oldest end) of one
// assignment queue. The sample is the head few entries, not the whole queue,
// so the value is "the oldest wait at the front", which is where the claim
// walk starts. An empty queue is reported as zero so a drained queue does not
// freeze at its last age. Entries an older writer queued carry no stamp and
// are skipped; a non-empty queue with no stamped entry in its sample is not
// reported. A failed read leaves the last value. The labels match
// OnAssignmentQueueDepth.
func (s SweepMetrics) OnAssignmentQueueOldestAge(_ context.Context, lane string, age time.Duration) {
	s.Metrics.Set(metricAssignmentQueueOldestAge, map[string]string{"lane": lane}, age.Seconds())
}

var (
	_ sweepObserver              = SweepMetrics{}
	_ sweepTimingObserver        = SweepMetrics{}
	_ sweepPassObserver          = SweepMetrics{}
	_ sweepPassCandidateObserver = SweepMetrics{}
	_ sweepQueueDepthObserver    = SweepMetrics{}
	_ sweepQueueAgeObserver      = SweepMetrics{}
	_ executionDeadlineObserver  = SweepMetrics{}
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

// OnRunnerClaimPoll records one claim poll's walk cost as two count
// histograms: queue targets walked and Redis round trips issued. No labels —
// the distribution across the fleet is what bounds the poll-wide cost.
func (r RunnerClaimMetrics) OnRunnerClaimPoll(_ context.Context, targets, roundTrips int) {
	r.Metrics.ObserveCount(metricRunnerClaimPollTargets, nil, targets)
	r.Metrics.ObserveCount(metricRunnerClaimPollRoundTrips, nil, roundTrips)
}

// OnRunnerSelectorRejected counts one claim-path selector rejection, by
// reason.
//
// reason is one of service/control's SelectorReject* values — label_pending,
// label_required, capability, policy, namespace — so the label set is bounded
// by the code, not by traffic. It answers the question the queue depth and
// oldest-age series cannot on their own: a queue that is not draining because
// no live runner satisfies its placement (label_*) looks nothing like one that
// is not draining for want of capacity (capability/policy), and before this
// counter the two were distinguished only by reading runner registrations by
// hand. label_pending is the count of entries inside the default-mode grace
// window, so a sustained high label_pending with a low claim rate is the
// signal that the placement is waiting on a runner that is not there.
//
// Deliberately no namespace label, matching OnAssignmentQueueDepth: the
// queues are cluster-wide, and the rejection is a property of the pairing
// between a queued entry and the live runner fleet rather than of a tenant.
func (r RunnerClaimMetrics) OnRunnerSelectorRejected(_ context.Context, reason string) {
	r.Metrics.Inc(metricRunnerSelectorRejected, map[string]string{"reason": reason})
}

var _ runnerClaimObserver = RunnerClaimMetrics{}

type runnerClaimPollObserver interface {
	OnRunnerClaimPoll(ctx context.Context, targets, roundTrips int)
}

var _ runnerClaimPollObserver = RunnerClaimMetrics{}

// runnerSelectorRejectObserver mirrors service/control's optional
// RunnerClaimObserver extension of the same name: claim-path selector
// rejections, partitioned by bounded reason.
type runnerSelectorRejectObserver interface {
	OnRunnerSelectorRejected(ctx context.Context, reason string)
}

var _ runnerSelectorRejectObserver = RunnerClaimMetrics{}

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

// OnDispatchDropped counts one task dropped without an assignment, by reason.
// See metricHelp for what each reason means: execution_gone is a window-bounded
// loss claim (not proof — see metricHelp), execution_unattributed may hold late
// duplicates beside loss, and the rest are benign.
func (d DispatcherMetrics) OnDispatchDropped(ctx context.Context, reason string) {
	d.Metrics.Inc(metricDispatchDropped, withNamespace(ctx, map[string]string{"reason": reason}))
}

// OnTaskDeliveryLag records how long a consumed task waited between becoming
// deliverable and being consumed. Read its tail against the execution TTL: it
// is the pre-loss warning for queue residency.
func (d DispatcherMetrics) OnTaskDeliveryLag(ctx context.Context, lag time.Duration) {
	d.Metrics.ObserveLag(metricTaskDeliveryLag, withNamespace(ctx, nil), lag)
}

var (
	_ dispatcherObserver     = DispatcherMetrics{}
	_ dispatcherDropObserver = DispatcherMetrics{}
	_ deliveryLagObserver    = DispatcherMetrics{}
)
