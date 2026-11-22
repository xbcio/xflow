package metrics

import (
	"context"

	"github.com/xbcio/xflow/engine"
)

// metricOrphanReclaimed counts applied concurrency-orphan convergences.
const metricOrphanReclaimed = "xflow_orphan_reclaimed_total"

// OrphanReclaimMetrics publishes xflow_orphan_reclaimed_total, the counter of
// executions a reclaim pass converged, partitioned by the status they were
// reclaimed from (kind) and the path that asked for it (reason).
//
// It is the observer implementation for engine.OrphanReclaimObserver: both
// state-store backends accept that narrow interface, so this adapter is the
// only place the repair touches an exporter.
type OrphanReclaimMetrics struct {
	Metrics *Metrics
}

// NewOrphanReclaimMetrics wires the concurrency-orphan observer into the
// Prometheus registry.
func NewOrphanReclaimMetrics(metrics *Metrics) OrphanReclaimMetrics {
	return OrphanReclaimMetrics{Metrics: metrics}
}

// OnConcurrencyOrphanReclaimed records one applied convergence. Dry runs never
// reach this method: a preview answers candidates, and counting it would make
// the series a count of requests rather than of reclaimed executions.
func (o OrphanReclaimMetrics) OnConcurrencyOrphanReclaimed(_ context.Context, kind, reason string) {
	o.Metrics.Inc(metricOrphanReclaimed, map[string]string{
		"kind":   kind,
		"reason": reason,
	})
}

var _ engine.OrphanReclaimObserver = OrphanReclaimMetrics{}
