package metrics

import (
	"context"
	"time"
)

// Runner-control metric names. These are fleet-wide control-plane metrics:
// they intentionally carry neither a runner identity nor a tenant namespace.
const (
	metricRunnerControlTransitions = "xflow_runner_control_transitions_total"
	metricRunnerDrainingCount      = "xflow_runner_draining_count"
	metricRunnerDrainDuration      = "xflow_runner_drain_duration_seconds"
	metricRunnerDrainBlockers      = "xflow_runner_drain_blockers"
)

// Runner-control actions are a closed label set. Callers should use these
// constants instead of passing route fragments or user-provided values.
const (
	RunnerControlActionDrain  = "drain"
	RunnerControlActionResume = "resume"
	RunnerControlActionOther  = "other"
)

// Runner-control results are a closed label set. They describe the durable
// result of a drain/resume request, not a free-form error message.
const (
	RunnerControlResultTransitioned = "transitioned"
	RunnerControlResultUnchanged    = "unchanged"
	RunnerControlResultReplayed     = "replayed"
	RunnerControlResultConflict     = "conflict"
	RunnerControlResultNotFound     = "not_found"
	RunnerControlResultInvalid      = "invalid"
	RunnerControlResultUnsupported  = "unsupported"
	RunnerControlResultError        = "error"
	RunnerControlResultOther        = "other"
)

const (
	runnerDrainBlockerHandoffDebt       = "handoff_debt"
	runnerDrainBlockerReplayDebt        = "replay_debt"
	runnerDrainBlockerActivationReceipt = "activation_receipt"
	runnerDrainBlockerRunnerAck         = "runner_ack"
	runnerDrainBlockerDeadline          = "deadline"
)

// RunnerControlFleetSnapshot is the aggregate drain projection that a control
// caller reports after one complete fleet observation. It deliberately has no
// runner ID, request ID, actor, reason, or other unbounded value: management
// APIs are the surface for per-runner diagnosis.
//
// DrainingCount counts every runner whose desired state is draining, including
// quiescing, complete, and timed-out drain phases. OverdueDrainCount is the
// count whose current drain phase is timed_out (or whose deadline has elapsed
// while still draining). The remaining fields are summed blockers across that
// same fleet observation.
type RunnerControlFleetSnapshot struct {
	DrainingCount                      int
	HandoffDebtCount                   int
	ReplayDebtCount                    int
	PendingActivationReceiptCount      int
	AwaitingRunnerAcknowledgementCount int
	OverdueDrainCount                  int
}

// RunnerControlObserver is the narrow, reusable observability boundary for
// runner operation control. service/control can implement its own collection
// pass and call this interface without observability/metrics importing the
// service package.
//
// OnRunnerControlTransition is called once a drain/resume operation has a
// final, bounded result. OnRunnerDrainCompleted is called once when a drain
// reaches its complete phase; elapsed is the time since that drain's requested
// time. ObserveRunnerControlSnapshot receives a fleet aggregate, never one
// per-runner snapshot, because these gauges represent fleet-wide state.
type RunnerControlObserver interface {
	OnRunnerControlTransition(ctx context.Context, action, result string)
	OnRunnerDrainCompleted(ctx context.Context, elapsed time.Duration)
	ObserveRunnerControlSnapshot(ctx context.Context, snapshot RunnerControlFleetSnapshot)
}

// NoopRunnerControlObserver keeps runner-control instrumentation optional for
// callers that do not install a metrics registry.
type NoopRunnerControlObserver struct{}

func (NoopRunnerControlObserver) OnRunnerControlTransition(context.Context, string, string) {}

func (NoopRunnerControlObserver) OnRunnerDrainCompleted(context.Context, time.Duration) {}

func (NoopRunnerControlObserver) ObserveRunnerControlSnapshot(context.Context, RunnerControlFleetSnapshot) {
}

var _ RunnerControlObserver = NoopRunnerControlObserver{}

// RunnerControlMetrics adapts runner-control observations to the shared
// Prometheus Metrics API. A nil Metrics pointer is safe and behaves as a noop.
type RunnerControlMetrics struct {
	Metrics *Metrics
}

// NewRunnerControlMetrics creates a runner-control observer backed by metrics.
func NewRunnerControlMetrics(metrics *Metrics) RunnerControlMetrics {
	return RunnerControlMetrics{Metrics: metrics}
}

// OnRunnerControlTransition records a bounded action/result pair for one
// completed drain or resume operation. Unknown values collapse to "other" so
// caller mistakes cannot create a high-cardinality Prometheus label.
func (r RunnerControlMetrics) OnRunnerControlTransition(_ context.Context, action, result string) {
	r.Metrics.Inc(metricRunnerControlTransitions, map[string]string{
		"action": normalizeRunnerControlAction(action),
		"result": normalizeRunnerControlResult(result),
	})
}

// OnRunnerDrainCompleted records the elapsed time for one drain that reached
// the complete phase. A negative duration is invalid and deliberately dropped
// rather than corrupting the duration distribution.
func (r RunnerControlMetrics) OnRunnerDrainCompleted(_ context.Context, elapsed time.Duration) {
	if elapsed < 0 {
		return
	}
	r.Metrics.Observe(metricRunnerDrainDuration, nil, elapsed)
}

// ObserveRunnerControlSnapshot replaces fleet gauges with one aggregate
// control-plane observation. Negative counts are treated as zero: a malformed
// caller snapshot must not publish impossible negative debt or runner counts.
func (r RunnerControlMetrics) ObserveRunnerControlSnapshot(_ context.Context, snapshot RunnerControlFleetSnapshot) {
	r.Metrics.Set(metricRunnerDrainingCount, nil, nonNegative(snapshot.DrainingCount))
	r.Metrics.Set(metricRunnerDrainBlockers, map[string]string{
		"kind": runnerDrainBlockerHandoffDebt,
	}, nonNegative(snapshot.HandoffDebtCount))
	r.Metrics.Set(metricRunnerDrainBlockers, map[string]string{
		"kind": runnerDrainBlockerReplayDebt,
	}, nonNegative(snapshot.ReplayDebtCount))
	r.Metrics.Set(metricRunnerDrainBlockers, map[string]string{
		"kind": runnerDrainBlockerActivationReceipt,
	}, nonNegative(snapshot.PendingActivationReceiptCount))
	r.Metrics.Set(metricRunnerDrainBlockers, map[string]string{
		"kind": runnerDrainBlockerRunnerAck,
	}, nonNegative(snapshot.AwaitingRunnerAcknowledgementCount))
	r.Metrics.Set(metricRunnerDrainBlockers, map[string]string{
		"kind": runnerDrainBlockerDeadline,
	}, nonNegative(snapshot.OverdueDrainCount))
}

var _ RunnerControlObserver = RunnerControlMetrics{}

func normalizeRunnerControlAction(action string) string {
	switch action {
	case RunnerControlActionDrain, RunnerControlActionResume:
		return action
	default:
		return RunnerControlActionOther
	}
}

func normalizeRunnerControlResult(result string) string {
	switch result {
	case RunnerControlResultTransitioned,
		RunnerControlResultUnchanged,
		RunnerControlResultReplayed,
		RunnerControlResultConflict,
		RunnerControlResultNotFound,
		RunnerControlResultInvalid,
		RunnerControlResultUnsupported,
		RunnerControlResultError:
		return result
	default:
		return RunnerControlResultOther
	}
}

func nonNegative(value int) float64 {
	if value < 0 {
		return 0
	}
	return float64(value)
}
