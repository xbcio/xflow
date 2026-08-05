package metrics

import "github.com/xbcio/xflow/engine"

// Subgraph metric names.
const (
	metricSubgraphItemFailures = "xflow_subgraph_item_failures_total"
)

// SubgraphMetrics observes per-item outcomes inside a map node's body.
//
// It exists for one reason: continue_on_error is otherwise a switch that drops
// data silently. With it set, a failed item leaves a {_error, _index}
// placeholder that the downstream filter removes, the batch commits, and the
// execution reports Success — so a node steadily losing records is
// indistinguishable from a healthy one. Nothing else in the system carries that
// signal: the trigger counters cover only the Kafka boundary, and a node's own
// "failed" count lands in its execution output, which is a value to read once,
// not a time series to alert on.
type SubgraphMetrics struct {
	Metrics *Metrics
}

// NewSubgraphMetrics creates a per-item outcome observer backed by Metrics.
func NewSubgraphMetrics(m *Metrics) SubgraphMetrics { return SubgraphMetrics{Metrics: m} }

// OnItemFailures counts the items of one batch whose body execution failed.
//
// Labels are the workflow and the map node — the two an operator needs to find
// the node — and nothing else. The item's index is deliberately absent: it is
// unbounded, so a large map node would mint a series per item. The item's
// content is absent for the same reason it is absent from the trigger counters:
// it is not an operator's to read from a metric.
//
// A batch with no failures emits nothing. A zero-valued series reads the same
// as an absent one on a dashboard while still costing cardinality on every
// batch of every healthy map node.
func (s SubgraphMetrics) OnItemFailures(workflow, node string, failed int) {
	if failed <= 0 {
		return
	}
	s.Metrics.Add(metricSubgraphItemFailures, map[string]string{
		"workflow": workflow,
		"node":     node,
	}, float64(failed))
}

// ObserveItemFailures implements engine.ItemFailureObserver. The engine reports
// the batch's Total alongside Failed; only Failed becomes a counter, because a
// "total items" counter is already derivable from the map node's own output and
// would double the series for no new signal.
func (s SubgraphMetrics) ObserveItemFailures(f engine.ObservedItemFailures) {
	s.OnItemFailures(f.Workflow, f.NodeName, f.Failed)
}

var _ engine.ItemFailureObserver = SubgraphMetrics{}
