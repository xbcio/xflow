package metrics

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// TestCommitMetrics_DiscardedPublishesKindAndReason pins the three label
// dimensions of xflow_commit_discarded_total. It counts discarded submissions,
// not reclaimed executions: one call is one increment, and kind/reason are the
// series identity that separates the node entry point from the group one.
func TestCommitMetrics_DiscardedPublishesKindAndReason(t *testing.T) {
	m := New()
	obs := NewCommitMetrics(m)
	ctx := context.Background()

	obs.OnCommitDiscarded(ctx, engine.CommitDiscardKindNode, engine.CommitOutcomeExecutionInactive)
	obs.OnCommitDiscarded(ctx, engine.CommitDiscardKindNode, engine.CommitOutcomeExecutionInactive)
	obs.OnCommitDiscarded(ctx, engine.CommitDiscardKindGroup, engine.CommitOutcomeExecutionInactive)

	family := gatherMetricFamily(t, m, "xflow_commit_discarded_total")
	got := make(map[string]float64, len(family.GetMetric()))
	for _, metric := range family.GetMetric() {
		key := labelValue(metric, "kind") + "/" + labelValue(metric, "reason") + "/" + labelValue(metric, "namespace")
		got[key] = metric.GetCounter().GetValue()
	}
	want := map[string]float64{
		"node/execution_inactive/default":  2,
		"group/execution_inactive/default": 1,
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

// TestCommitMetrics_DiscardedNilMetricsIsInert proves the adapter is safe to
// construct without a registry: a deployment that never configured metrics
// must not crash when an engine discards a late commit.
func TestCommitMetrics_DiscardedNilMetricsIsInert(t *testing.T) {
	obs := NewCommitMetrics(nil)
	obs.OnCommitDiscarded(context.Background(), engine.CommitDiscardKindNode, engine.CommitOutcomeExecutionInactive)
}

// TestCommitMetrics_DiscardedHasHelpText pins the operator-facing description.
// The source-scan guard would catch a missing entry too; this states the
// requirement where the adapter lives.
func TestCommitMetrics_DiscardedHasHelpText(t *testing.T) {
	if _, ok := metricHelp["xflow_commit_discarded_total"]; !ok {
		t.Fatal("no metricHelp entry for xflow_commit_discarded_total")
	}
}
