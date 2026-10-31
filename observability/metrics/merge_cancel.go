package metrics

import (
	"context"

	"github.com/xbcio/xflow/engine"
)

// metricMergeCancelIncomplete counts the nodes a fired merge's on_others:
// cancel scan could not retire.
const metricMergeCancelIncomplete = "xflow_merge_cancel_incomplete_total"

// MergeCancelMetrics observes the residue a merge-cancel scan leaves behind:
// the Waiting rows hanging on a sub-workflow or an expansion, and the nodes
// under a group unit — the v1-excluded cases the scan deliberately does not
// attempt.
//
// It exists because those cases are the only way an execution can still hang
// after its merge fired. Every other closure node is settled either by the
// scan's own fenced commit or by the fence's refusal (already terminal, never
// scheduled, mid-skip — all "nothing to do"), and the skip cascade carries the
// rest of the graph. A host that treats "no stuck executions" as a requirement
// alerts on this counter being nonzero: a persistent residue means either a
// sub-workflow child outlived the merge that abandoned it, or a group unit was
// the blocking branch — both need the coordinated teardown a later design
// owns, and neither is visible anywhere else.
//
// Labels are the namespace and the unresolved node:
//
//   - namespace comes from context, like every other namespace-scoped series
//     here (see withNamespace).
//   - node is bounded by the compiled graph and shared across every execution
//     of that workflow — the same dimension xflow_node_skipped_total carries,
//     because "workflow A keeps stranding node X" is an answer and "something
//     somewhere is incomplete" is not.
//
// Deliberately absent: the execution ID (unbounded, one series per execution)
// and the merge node's own name (derivable from the execution's graph; one
// scan of one merge already attributes its residue per node).
type MergeCancelMetrics struct {
	Metrics *Metrics
}

// NewMergeCancelMetrics creates a merge-cancel observer backed by Metrics.
func NewMergeCancelMetrics(m *Metrics) MergeCancelMetrics {
	return MergeCancelMetrics{Metrics: m}
}

// OnMergeCancelIncomplete implements engine.MergeCancelObserver, the interface
// the engine calls through engine.WithMergeCancelObserver
// (service/control/controlplane.go).
func (s MergeCancelMetrics) OnMergeCancelIncomplete(ctx context.Context, node string, count int) {
	if count <= 0 {
		return
	}
	s.Metrics.Add(metricMergeCancelIncomplete, withNamespace(ctx, map[string]string{
		"node": node,
	}), float64(count))
}

var _ engine.MergeCancelObserver = MergeCancelMetrics{}
