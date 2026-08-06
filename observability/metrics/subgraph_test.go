package metrics

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// continue_on_error: true means a failed item is dropped from the batch's
// usable results and the execution still reports Success. Without a counter,
// that switch drops data silently: a node losing 3% of its records and a
// healthy node look identical — both Success, both handing downstream clean
// data. This counter is the only signal that distinguishes them.
func TestSubgraphMetricsCountsPerItemFailures(t *testing.T) {
	m := New()
	sg := NewSubgraphMetrics(m)

	// One batch of a 10-item map node where three items failed.
	sg.OnItemFailures("ingest", "enrich", 3)

	body := gatherMetricsBody(t, m)
	want := `xflow_subgraph_item_failures_total{node="enrich",workflow="ingest"} 3`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics body missing %q; without it a node silently dropping "+
			"data is indistinguishable from a healthy one:\n%s", want, body)
	}
}

// A batch with no failed items must not emit the counter at all. A counter
// present at zero and a counter absent read the same on a dashboard, but the
// series itself is what costs: emitting one per (workflow, node) on every batch
// of every healthy map node is cardinality bought for nothing.
func TestSubgraphMetricsSkipsHealthyBatches(t *testing.T) {
	m := New()
	NewSubgraphMetrics(m).OnItemFailures("ingest", "enrich", 0)

	if body := gatherMetricsBody(t, m); strings.Contains(body, "xflow_subgraph_item_failures_total") {
		t.Errorf("a batch with no failures emitted the failure counter:\n%s", body)
	}
}

// The label set is exactly {workflow, node}. Item content is not an operator's
// to read from a metric, and the item's index is unbounded — a 100k-item map
// node would mint 100k series.
func TestSubgraphMetricsLabelsCarryNoItemIdentity(t *testing.T) {
	m := New()
	NewSubgraphMetrics(m).OnItemFailures("ingest", "enrich", 1)

	body := gatherMetricsBody(t, m)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "xflow_subgraph_item_failures_total{") {
			continue
		}
		for _, banned := range []string{"index", "item", "batch"} {
			if strings.Contains(line, banned+`="`) {
				t.Errorf("failure counter carries an unbounded %q label: %s", banned, line)
			}
		}
	}
}

// The whole chain, not just the type: the engine hands an observation to
// whatever satisfies engine.ItemFailureObserver, and this is the type the
// control plane installs there. If SubgraphMetrics stopped satisfying that
// interface — or satisfied it without emitting — the counter would exist and
// still never appear on /metrics.
func TestSubgraphMetricsSatisfiesTheEnginesObserver(t *testing.T) {
	m := New()
	var obs engine.ItemFailureObserver = NewSubgraphMetrics(m)

	obs.ObserveItemFailures(engine.ObservedItemFailures{
		Workflow: "ingest", NodeName: "enrich", Failed: 2, Total: 10,
	})

	want := `xflow_subgraph_item_failures_total{node="enrich",workflow="ingest"} 2`
	if body := gatherMetricsBody(t, m); !strings.Contains(body, want) {
		t.Fatalf("metrics body missing %q after an engine observation:\n%s", want, body)
	}
}
