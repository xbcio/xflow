package metrics

import (
	"context"

	"github.com/xbcio/xflow/engine"
)

// NodeSkip metric names.
const (
	metricNodeSkipped = "xflow_node_skipped_total"
)

// NodeSkipMetrics observes the units a scheduling transition resolved as skip
// rather than execute.
//
// It exists because a skip is the one losing branch with no other series behind
// it. A skip does leave a durable record — an engine.TaskTypeNodeSkip outbox
// intent, which is more than a silent drop — but a record is not a rate: an
// operator watching a host that treats "no data loss" as a hard requirement had
// no way to tell "everything is flowing" from "everything is being skipped".
// Worse, the signal that normally exposes a stalled consumer points the wrong
// way here: the skip consumes the batch and advances the scheduling position
// past it, so downstream lag DECREASES while data stops arriving. This counter
// is the only thing that moves.
//
// Labels are the namespace, the flow that made the decision, and the downstream
// unit's node:
//
//   - namespace comes from context, like every other namespace-scoped series
//     here (see withNamespace). It is low-cardinality by construction — tens to
//     low hundreds of tenants.
//   - flow is a closed enum naming the decision point: "entry", "advance",
//     "group". It is the dimension that makes the counter actionable, because
//     the three flows fail for different reasons and are fixed by different
//     things — an admission whose downstream never activated, an ordinary
//     fan-in whose alternate route was taken, a group whose commit resolved a
//     member's branch as unreached.
//   - node is the downstream unit's representative node name, bounded by the
//     compiled graph and shared across every execution of that workflow. It is
//     the same dimension xflow_subgraph_item_failures_total already carries for
//     the same reason: "workflow A keeps losing node X" is an answer, and "some
//     node somewhere was skipped" is not.
//
// Deliberately absent: the execution ID and the admission key (unbounded, one
// series per execution), the unit index (an implementation detail of the
// compiled graph, already implied by node), and the reason text — the skip
// branch has exactly one cause, which flow already carries.
type NodeSkipMetrics struct {
	Metrics *Metrics
}

// NewNodeSkipMetrics creates a skip observer backed by Metrics.
func NewNodeSkipMetrics(m *Metrics) NodeSkipMetrics { return NodeSkipMetrics{Metrics: m} }

// OnNodeSkip implements engine.NodeSkipObserver, the interface the engine calls
// through engine.WithNodeSkipObserver (service/control/controlplane.go).
//
// It is deliberate that the structural method carries the counter and the
// convenience method below delegates to it, rather than the other way round:
// the interface method is the one that cannot be renamed or dropped without a
// compile error, so it is the one an operator's series must depend on.
func (s NodeSkipMetrics) OnNodeSkip(ctx context.Context, flow, node string, count int) {
	if count <= 0 {
		return
	}
	s.Metrics.Add(metricNodeSkipped, withNamespace(ctx, map[string]string{
		"flow": flow, "node": node,
	}), float64(count))
}

// OnNodeSkipped counts one node's skip without constructing an observed-item
// struct, mirroring SubgraphMetrics.OnItemFailures: a caller that already holds
// plain values should not have to wrap them to be counted.
//
// count is added rather than incremented once per call because a caller that
// already knows it skipped N units of the same node should not pay N label
// lookups for one series.
func (s NodeSkipMetrics) OnNodeSkipped(ctx context.Context, flow, node string, count int) {
	s.OnNodeSkip(ctx, flow, node, count)
}

var _ engine.NodeSkipObserver = NodeSkipMetrics{}
