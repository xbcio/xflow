package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// sweptByNamespace gathers a swept-starts family into namespace -> count and
// fails if any series carries a label other than namespace.
func sweptByNamespace(t *testing.T, m *Metrics, family string) map[string]float64 {
	t.Helper()
	got, shapes := adapterSeries(gatherMetricFamily(t, m, family), counterValue)
	if len(shapes) != 1 || shapes[0] != "namespace" {
		t.Fatalf("%s label shapes = %q, want exactly [namespace]", family, shapes)
	}
	return got
}

// The age sweep runs from an arbitrary caller's OnNodeStart and drops entries
// belonging to any namespace, so what it counts has to be attributed to the
// namespace each dropped start ran in. The starts go in through OnNodeStart with
// a namespaced ctx — the only way production records one — and the sweep is
// handed a clock far enough ahead to age all of them out. Counts differ per
// namespace so a collapsed or swapped bucket cannot hide behind equal totals.
// Mutation: counting every dropped start under one fixed namespace (the old
// hard-coded "") or under the trigger's namespace collapses the series;
// recording the namespace from the sweep's ctx-less caller instead of from the
// start moves everything to "default".
func TestSweptNodeStartsAreAttributedToTheirNamespace(t *testing.T) {
	tests := []struct {
		name   string
		hooks  func(*Metrics) *MetricsHooks
		family string
	}{
		{"top-level engine", NewMetricsHooks, "xflow_node_starts_swept_total"},
		{"subgraph engine", NewSubgraphMetricsHooks, "xflow_subgraph_node_starts_swept_total"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := New()
			hooks := tc.hooks(m)
			a := namespace.WithNamespace(context.Background(), "ns-a")
			b := namespace.WithNamespace(context.Background(), "ns-b")
			bare := context.Background()

			// ns-a: two executions, three starts. ns-b: one execution, two starts.
			// bare ctx: one start, which must land on "default". 3/2/1 keeps every
			// pair of buckets distinguishable if two of them were swapped.
			hooks.OnNodeStart(a, types.ExecutionID("a-1"), "n1")
			hooks.OnNodeStart(a, types.ExecutionID("a-1"), "n2")
			hooks.OnNodeStart(a, types.ExecutionID("a-2"), "n1")
			hooks.OnNodeStart(b, types.ExecutionID("b-1"), "n1")
			hooks.OnNodeStart(b, types.ExecutionID("b-1"), "n2")
			hooks.OnNodeStart(bare, types.ExecutionID("d-1"), "n1")
			if got := hooks.liveNodeStarts(); got != 6 {
				t.Fatalf("seeded starts = %d, want 6", got)
			}

			hooks.sweepStaleNodeStarts(time.Now().Add(2 * nodeStartMaxAge))

			if got := hooks.liveNodeStarts(); got != 0 {
				t.Fatalf("starts after the sweep = %d, want 0", got)
			}
			got := sweptByNamespace(t, m, tc.family)
			want := map[string]float64{"ns-a": 3, "ns-b": 2, "default": 1}
			if len(got) != len(want) {
				t.Fatalf("swept series = %v, want %v", got, want)
			}
			for ns, n := range want {
				if got[ns] != n {
					t.Errorf("swept[%q] = %v, want %v; all series: %v", ns, got[ns], n, got)
				}
			}
		})
	}
}

// Only the stale starts are counted, and only under the namespace they ran in: a
// fresh start in another namespace must not produce a series at all, and a sweep
// that finds nothing must not create one.
// Mutation: emitting a zero-valued series per known namespace, or counting
// fresh starts, makes the ns-fresh series appear.
func TestSweptNodeStartsCountOnlyTheStaleOnes(t *testing.T) {
	m := New()
	hooks := NewMetricsHooks(m)
	now := time.Now()

	hooks.storeNodeStartAt(types.ExecutionID("stale"), "ns-stale", "n", now.Add(-2*nodeStartMaxAge))
	hooks.storeNodeStartAt(types.ExecutionID("fresh"), "ns-fresh", "n", now)

	hooks.sweepStaleNodeStarts(now)

	got := sweptByNamespace(t, m, "xflow_node_starts_swept_total")
	if len(got) != 1 || got["ns-stale"] != 1 {
		t.Fatalf("swept series = %v, want exactly {ns-stale: 1}", got)
	}

	// Nothing left to drop: a second sweep adds nothing and creates no series.
	hooks.sweepStaleNodeStarts(now)
	again := sweptByNamespace(t, m, "xflow_node_starts_swept_total")
	if len(again) != 1 || again["ns-stale"] != 1 {
		t.Fatalf("swept series after a no-op sweep = %v, want unchanged {ns-stale: 1}", again)
	}
}

// An execution lives in exactly one namespace, so the namespace recorded with its
// first start is the one its later starts are attributed to. A re-lease fires
// OnNodeStart again for the same node; that must not re-home the execution.
// Mutation: overwriting e.ns on every start (dropping the e.ns == "" guard)
// attributes the swept starts to ns-b.
func TestSweptNodeStartsKeepTheFirstRecordedNamespace(t *testing.T) {
	m := New()
	hooks := NewMetricsHooks(m)
	id := types.ExecutionID("exec-1")

	hooks.OnNodeStart(namespace.WithNamespace(context.Background(), "ns-a"), id, "n1")
	hooks.OnNodeStart(namespace.WithNamespace(context.Background(), "ns-b"), id, "n2")

	hooks.sweepStaleNodeStarts(time.Now().Add(2 * nodeStartMaxAge))

	got := sweptByNamespace(t, m, "xflow_node_starts_swept_total")
	if len(got) != 1 || got["ns-a"] != 2 {
		t.Fatalf("swept series = %v, want exactly {ns-a: 2}", got)
	}
}

// A tombstone carries no namespace and holds no starts, so sweeping around one
// must neither count anything (the family is never registered, which keeps a
// series that is expected to stay at zero out of the exposition) nor delete it:
// the ring owns its lifetime, and a swept tombstone would let a start that lost
// the race with the execution's end land afterwards and leak.
// Mutation: dropping the e.ended branch of the sweep deletes the tombstone, so
// the tracked entry disappears and the late start is recorded.
func TestSweepIgnoresTombstonesWhenAttributing(t *testing.T) {
	m := New()
	hooks := NewMetricsHooks(m)
	ctx := namespace.WithNamespace(context.Background(), "ns-a")
	id := types.ExecutionID("done")

	hooks.OnNodeStart(ctx, id, "n")
	hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusSuccess)
	hooks.sweepStaleNodeStarts(time.Now().Add(2 * nodeStartMaxAge))

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == "xflow_node_starts_swept_total" {
			t.Fatalf("swept family exported after sweeping only a tombstone: %v", family.GetMetric())
		}
	}
	if got := hooks.trackedExecutions(); got != 1 {
		t.Fatalf("tracked entries after the sweep = %d, want 1 (the tombstone must survive)", got)
	}
	hooks.OnNodeStart(ctx, id, "late")
	if got := hooks.liveNodeStarts(); got != 0 {
		t.Errorf("in-flight starts after a late start on a swept tombstone = %d, want 0", got)
	}
}

// Mutation: dropping the sentence from either entry turns this red.
func TestSweptNodeStartsHelpDescribesNamespaceLabel(t *testing.T) {
	for _, family := range []string{"xflow_node_starts_swept_total", "xflow_subgraph_node_starts_swept_total"} {
		help, ok := metricHelp[family]
		if !ok {
			t.Fatalf("no metricHelp entry for %s", family)
		}
		if !strings.Contains(help, "namespace label") {
			t.Errorf("%s help does not describe the namespace label: %q", family, help)
		}
	}
}
