package metrics

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// TestOrphanReclaimMetrics_PublishesKindAndReason pins the two label dimensions
// of xflow_orphan_reclaimed_total. It is a counter of reclaimed executions, not
// of requests: one call is one increment, and the kind/reason pair is the
// series identity, so an operator reset can be read apart from a sweep.
func TestOrphanReclaimMetrics_PublishesKindAndReason(t *testing.T) {
	m := New()
	obs := NewOrphanReclaimMetrics(m)

	obs.OnConcurrencyOrphanReclaimed(context.Background(), engine.OrphanKindRunning, engine.OrphanTriggerReset)
	obs.OnConcurrencyOrphanReclaimed(context.Background(), engine.OrphanKindRunning, engine.OrphanTriggerReset)
	obs.OnConcurrencyOrphanReclaimed(context.Background(), engine.OrphanKindQueued, engine.OrphanTriggerCancel)
	obs.OnConcurrencyOrphanReclaimed(context.Background(), engine.OrphanKindMissing, engine.OrphanTriggerSweep)

	family := gatherMetricFamily(t, m, "xflow_orphan_reclaimed_total")
	got := make(map[string]float64, len(family.GetMetric()))
	for _, metric := range family.GetMetric() {
		key := labelValue(metric, "kind") + "/" + labelValue(metric, "reason")
		got[key] = metric.GetCounter().GetValue()
	}
	want := map[string]float64{
		"running/operator_reset": 2,
		"queued/cancel":          1,
		"missing/sweep":          1,
	}
	if len(got) != len(want) {
		t.Fatalf("series count = %d (%v), want %d", len(got), got, len(want))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("series %q = %v, want %v (all series %v)", key, got[key], value, got)
		}
	}
}

// TestOrphanReclaimMetrics_NilMetricsIsInert proves the adapter is safe to
// construct without a registry: deployments that never configured metrics must
// not crash when the store reports a convergence.
func TestOrphanReclaimMetrics_NilMetricsIsInert(t *testing.T) {
	obs := NewOrphanReclaimMetrics(nil)
	obs.OnConcurrencyOrphanReclaimed(context.Background(), engine.OrphanKindRunning, engine.OrphanTriggerSweep)
}
