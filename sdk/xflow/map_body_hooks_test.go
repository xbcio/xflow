package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// A map body item runs on its own inner engine, built inside
// execution/subgraph.Executor. That engine was constructed with no Hooks at
// all, so the nodes doing the per-item work — the whole point of a map — emitted
// nothing. The outer engine's hooks do not reach them: they observe a different
// engine.
//
// This asserts both halves of the wiring, because either one alone is wrong:
//
//   - the inner nodes must be counted somewhere (the gap this closes), and
//   - they must NOT be counted in the outer family. Threading the outer hooks
//     into the inner engine would "fix" the first half while making
//     xflow_execution_completed_total mean something else — three items would
//     read as four workflow runs.
func TestLocalEngineObservesMapBodyNodes(t *testing.T) {
	m := metrics.New()
	eng, err := NewLocal(
		WithHooks(metrics.NewMetricsHooks(m)),
		WithSubgraphHooks(metrics.NewSubgraphMetricsHooks(m)),
	)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	recorder := &mapBodyRecorder{}
	body := Workflow("observed-body")
	body.LocalNode("double", recorder)

	wf := Workflow("map-body-observed")
	start := wf.Node("start", node.Start())
	mapNode := wf.Node("m", node.Map("$input.ids", 2))
	mapNode.Body(body)
	wf.Connect(start, mapNode)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	execID, err := eng.Invoke(ctx, wfID, Start(), map[string]any{"ids": []any{1, 2, 3}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	res, err := eng.Wait(ctx, execID)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}
	// Guard the premise: if the body never ran, every count below is legitimately
	// zero and the assertions would be measuring nothing.
	if ran := recorder.items(); len(ran) != 3 {
		t.Fatalf("body ran %d time(s) (%v), want 3 — the metric assertions below "+
			"would pass vacuously", len(ran), ran)
	}

	// The body's own node, counted once per item.
	if got := counterFor(t, m, "xflow_subgraph_node_started_total", "node", "double"); got != 3 {
		t.Errorf("xflow_subgraph_node_started_total{node=\"double\"} = %v, want 3 "+
			"(one per item) — the inner engine's nodes are unobserved", got)
	}
	if got := counterFor(t, m, "xflow_subgraph_node_completed_total", "node", "double"); got != 3 {
		t.Errorf("xflow_subgraph_node_completed_total{node=\"double\"} = %v, want 3", got)
	}
	// One inner execution per item, in its own series.
	if got := counterSum(t, m, "xflow_subgraph_execution_completed_total"); got != 3 {
		t.Errorf("xflow_subgraph_execution_completed_total = %v, want 3 (items)", got)
	}

	// The separation. One workflow run finished, whatever the fan-out width.
	if got := counterSum(t, m, "xflow_execution_completed_total"); got != 1 {
		t.Errorf("xflow_execution_completed_total = %v, want 1 — inner executions "+
			"leaked into the outer family, so a map over N items now reads as N+1 "+
			"workflow runs", got)
	}
	// The body's node name must not appear in the outer node family either.
	if got := counterFor(t, m, "xflow_node_started_total", "node", "double"); got != 0 {
		t.Errorf("xflow_node_started_total{node=\"double\"} = %v, want 0 — a body "+
			"member is not a node of the outer graph", got)
	}
	// ...while the outer graph's own nodes still are, so this is not passing
	// because the outer hooks stopped working.
	if got := counterFor(t, m, "xflow_node_started_total", "node", "start"); got != 1 {
		t.Errorf("xflow_node_started_total{node=\"start\"} = %v, want 1 — the outer "+
			"hooks are not observing anything, which would make the checks above "+
			"pass for the wrong reason", got)
	}
}

// counterFor sums the samples of one counter family whose label `label` equals
// `value`. Summing rather than requiring a single series keeps the assertion
// independent of which other labels (namespace, status) the hook attaches.
func counterFor(t *testing.T, m *metrics.Metrics, name, label, value string) float64 {
	t.Helper()
	total := 0.0
	forEachSample(t, m, name, func(labels map[string]string, v float64) {
		if labels[label] == value {
			total += v
		}
	})
	return total
}

// counterSum sums every sample of a counter family.
func counterSum(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	total := 0.0
	forEachSample(t, m, name, func(_ map[string]string, v float64) { total += v })
	return total
}

func forEachSample(t *testing.T, m *metrics.Metrics, name string, fn func(map[string]string, float64)) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, metric := range fam.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			fn(labels, metric.GetCounter().GetValue())
		}
	}
}
