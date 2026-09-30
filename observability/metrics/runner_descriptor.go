package metrics

import (
	"context"
)

// Runner-reported node descriptor metric names.
//
// A runner's descriptor payload never fails its registration: an over-limit,
// malformed, or unentitled entry is dropped with a warning. This counter is
// what makes such a silent drop visible without reading logs.
const (
	metricRunnerDescriptorRejected  = "xflow_runner_descriptor_rejected_total"
	metricRunnerDescriptorConflicts = "xflow_runner_descriptor_conflicts"
)

// runnerDescriptorObserver mirrors service/control.RunnerDescriptorObserver
// locally so the mirror fails to compile here rather than silently detaching
// from the producer.
type runnerDescriptorObserver interface {
	OnRunnerDescriptorRejected(ctx context.Context, reason string)
	OnRunnerDescriptorConflicts(ctx context.Context, nodeType string, conflicts int)
}

var _ runnerDescriptorObserver = RunnerDescriptorMetrics{}

// RunnerDescriptorMetrics counts runner-reported descriptors dropped by control.
type RunnerDescriptorMetrics struct {
	Metrics *Metrics
}

func NewRunnerDescriptorMetrics(metrics *Metrics) RunnerDescriptorMetrics {
	return RunnerDescriptorMetrics{Metrics: metrics}
}

// OnRunnerDescriptorRejected counts one dropped envelope or entry. reason is
// one of the RunnerDescriptorRejected* constants in service/control: a small
// closed set, never a runner identifier or node type.
func (r RunnerDescriptorMetrics) OnRunnerDescriptorRejected(ctx context.Context, reason string) {
	r.Metrics.Inc(metricRunnerDescriptorRejected, withNamespace(ctx, map[string]string{"reason": reason}))
}

// OnRunnerDescriptorConflicts records how many versions of nodeType the live
// fleet reports with disagreeing descriptors. It is a gauge per node type,
// set on every aggregation (0 when consistent). Node types are bounded by
// what runners are entitled to register, never by runner or execution.
func (r RunnerDescriptorMetrics) OnRunnerDescriptorConflicts(_ context.Context, nodeType string, conflicts int) {
	r.Metrics.Set(metricRunnerDescriptorConflicts, map[string]string{"node_type": nodeType}, float64(conflicts))
}
