package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// TestNodeSkipMetrics_CountsWithFlowNodeAndNamespace asserts the exported
// series carries the labels the metric is partitioned on, read back through the
// registry rather than by inspecting the observer — gathering is what proves a
// real Prometheus collector was created rather than a method that merely
// compiled.
func TestNodeSkipMetrics_CountsWithFlowNodeAndNamespace(t *testing.T) {
	m := New()
	obs := NewNodeSkipMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")

	obs.OnNodeSkipped(ctx, "advance", "join", 1)

	family := gatherMetricFamily(t, m, "xflow_node_skipped_total")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("series count = %d, want 1", len(family.GetMetric()))
	}
	metric := family.GetMetric()[0]
	for name, want := range map[string]string{
		"namespace": "tenant-a",
		"flow":      "advance",
		"node":      "join",
	} {
		if got := labelValue(metric, name); got != want {
			t.Fatalf("label %q = %q, want %q", name, got, want)
		}
	}
	if got := metric.GetCounter().GetValue(); got != 1 {
		t.Fatalf("value = %v, want 1", got)
	}
}

// TestNodeSkipMetrics_ZeroCountIsNotExported pins the gate on the producer side.
//
// The engine already declines to call the observer for an empty skip list, so
// this is the second line of defense: a backend or a future caller that reports
// a zero must not mint a series that reads as "a skip happened here" on a
// dashboard while costing cardinality on every healthy transition.
func TestNodeSkipMetrics_ZeroCountIsNotExported(t *testing.T) {
	m := New()
	obs := NewNodeSkipMetrics(m)

	obs.OnNodeSkipped(context.Background(), "advance", "join", 0)
	obs.OnNodeSkipped(context.Background(), "advance", "join", -1)

	if _, found := findMetricFamily(m, "xflow_node_skipped_total"); found {
		t.Fatal("a zero or negative count created an xflow_node_skipped_total " +
			"series; the counter must stay absent so it keeps meaning " +
			"\"work was lost\"")
	}
}

// TestNodeSkipMetrics_AddsRatherThanIncrementsOnce asserts the count argument is
// the value, not a presence flag: a caller that already knows it skipped three
// units of one node must land as 3, and two calls must accumulate rather than
// overwrite.
func TestNodeSkipMetrics_AddsRatherThanIncrementsOnce(t *testing.T) {
	m := New()
	obs := NewNodeSkipMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")

	obs.OnNodeSkipped(ctx, "group", "out", 3)
	obs.OnNodeSkipped(ctx, "group", "out", 2)

	family := gatherMetricFamily(t, m, "xflow_node_skipped_total")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("series count = %d, want 1 — the two calls share every label", len(family.GetMetric()))
	}
	if got := family.GetMetric()[0].GetCounter().GetValue(); got != 5 {
		t.Fatalf("value = %v, want 5 (3+2)", got)
	}
}

// TestNodeSkipMetrics_DistinctFlowsAreDistinctSeries is the cardinality
// assertion the flow label exists for: the three decision points have to be
// separable, because they are fixed by different things.
func TestNodeSkipMetrics_DistinctFlowsAreDistinctSeries(t *testing.T) {
	m := New()
	obs := NewNodeSkipMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")

	for _, flow := range []string{"entry", "advance", "group"} {
		obs.OnNodeSkipped(ctx, flow, "n", 1)
	}

	family := gatherMetricFamily(t, m, "xflow_node_skipped_total")
	if len(family.GetMetric()) != 3 {
		t.Fatalf("series count = %d, want 3 — one per flow", len(family.GetMetric()))
	}
	seen := map[string]bool{}
	for _, metric := range family.GetMetric() {
		seen[labelValue(metric, "flow")] = true
	}
	for _, flow := range []string{"entry", "advance", "group"} {
		if !seen[flow] {
			t.Fatalf("flow %q missing from %v", flow, seen)
		}
	}
}

// TestNodeSkipMetrics_SatisfiesTheEngineInterface is the wiring assertion that
// matters most, and the one a compile error alone does not make: production
// installs NodeSkipMetrics through engine.WithNodeSkipObserver
// (service/control/controlplane.go), so if the type stopped satisfying
// engine.NodeSkipObserver the counter would exist, have help text, and never
// move.
//
// It calls through the interface rather than the concrete type for exactly that
// reason — a direct call would compile even if the method set drifted.
func TestNodeSkipMetrics_SatisfiesTheEngineInterface(t *testing.T) {
	m := New()
	var observer engine.NodeSkipObserver = NewNodeSkipMetrics(m)
	ctx := namespace.WithNamespace(context.Background(), "tenant-b")

	observer.OnNodeSkip(ctx, "entry", "sink", 4)

	family := gatherMetricFamily(t, m, "xflow_node_skipped_total")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("series count = %d, want 1", len(family.GetMetric()))
	}
	metric := family.GetMetric()[0]
	if got := labelValue(metric, "namespace"); got != "tenant-b" {
		t.Fatalf("namespace label = %q, want %q", got, "tenant-b")
	}
	if got := labelValue(metric, "flow"); got != "entry" {
		t.Fatalf("flow label = %q, want %q", got, "entry")
	}
	if got := labelValue(metric, "node"); got != "sink" {
		t.Fatalf("node label = %q, want %q", got, "sink")
	}
	if got := metric.GetCounter().GetValue(); got != 4 {
		t.Fatalf("value = %v, want 4", got)
	}
}

// TestNodeSkippedMetricHasHelpText asserts the metric ships with a real
// description. TestEveryMetricNameInThisPackageHasHelpText covers the same
// ground by scanning source; this pins the CONTENT, because that test would
// still pass on a one-word placeholder.
func TestNodeSkippedMetricHasHelpText(t *testing.T) {
	help, ok := metricHelp["xflow_node_skipped_total"]
	if !ok {
		t.Fatal("xflow_node_skipped_total has no metricHelp entry, so it exports " +
			"the useless fallback description")
	}
	for _, want := range []string{"skip", "entry", "advance", "group"} {
		if !strings.Contains(strings.ToLower(help), want) {
			t.Fatalf("help text %q does not mention %q, so an operator cannot tell "+
				"what the labels mean", help, want)
		}
	}
}

// findMetricFamily is gatherMetricFamily's non-fatal twin: the zero-count test
// has to assert absence, and a helper that calls t.Fatalf on a missing family
// cannot express that.
func findMetricFamily(m *Metrics, name string) (any, bool) {
	families, err := m.Registry().Gather()
	if err != nil {
		return nil, false
	}
	for _, family := range families {
		if family.GetName() == name {
			return family, true
		}
	}
	return nil, false
}
