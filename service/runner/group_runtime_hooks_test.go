package runner

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// A group's members run on an inner engine that GroupRuntime builds per attempt,
// not on the runner's own. Nothing observed them: the executor was constructed
// with no Hooks, so a group of ten members contributed nothing to any node
// series while its outer task looked like one ordinary node.
//
// WithGroupHooks is the only way to reach that engine — the runner's own hook
// receiver is attached to a different engine and cannot see through the group
// boundary — so this asserts the option is actually plumbed, not merely stored.
func TestGroupRuntimeHooksObserveMemberNodes(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})

	m := metrics.New()
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	rt := NewGroupRuntime(reg, cache,
		WithSuspendDisabled(),
		WithGroupHooks(metrics.NewSubgraphMetricsHooks(m)))

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "chain",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "chain",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.echo", Version: 1},
				{Name: "b", Type: "test.echo", Version: 1},
				{Name: "__collector_b_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "b"}}}},
				"b": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_b_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_b_main", SrcNode: "b", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.echo", NodeVersion: 1},
		},
	}

	lease := buildTestLease(t, pkg, &types.Input{Data: map[string]any{"x": 1}})
	result, err := rt.Execute(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	// Guard the premise: a group that did not run leaves every counter at zero
	// for reasons that have nothing to do with the hooks.
	if result.Outcome != "success" {
		t.Fatalf("outcome = %s (error %q); the counts below would be zero for the "+
			"wrong reason", result.Outcome, result.Error)
	}

	for _, member := range []string{"a", "b"} {
		if got := subgraphCounter(t, m, "xflow_subgraph_node_started_total", member); got != 1 {
			t.Errorf("xflow_subgraph_node_started_total{node=%q} = %v, want 1 — the "+
				"group's inner engine is unobserved", member, got)
		}
	}
	// The group's members must not land in the outer node family, which counts
	// the group task itself.
	if got := subgraphCounter(t, m, "xflow_node_started_total", "b"); got != 0 {
		t.Errorf("xflow_node_started_total{node=\"b\"} = %v, want 0 — a group member "+
			"is not a node of the outer graph", got)
	}
}

// subgraphCounter sums the samples of one counter family carrying node=name.
func subgraphCounter(t *testing.T, m *metrics.Metrics, family, name string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	total := 0.0
	for _, fam := range families {
		if fam.GetName() != family {
			continue
		}
		for _, metric := range fam.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "node" && pair.GetValue() == name {
					total += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// The map counterpart. SubgraphRuntime carries its own option struct, so the
// group test above says nothing about this path: WithSubgraphHooks writing one
// field while NewSubgraphRuntime reads another would compile, pass every
// existing test, and leave every body item uncounted.
func TestSubgraphRuntimeHooksObserveBodyNodes(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.body_item", &bodyItemHandler{})

	m := metrics.New()
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSubgraphHooks(metrics.NewSubgraphMetricsHooks(m)))

	result, err := rt.Execute(context.Background(), batchBodyLease(t, bodyPackageForTest()))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Error != nil {
		t.Fatalf("batch failed: %v; the count below would be zero for the wrong reason", result.Error)
	}

	// The lease carries two items, and the body runs once per item.
	if got := subgraphCounter(t, m, "xflow_subgraph_node_started_total", "step"); got != 2 {
		t.Errorf("xflow_subgraph_node_started_total{node=%q} = %v, want 2 (one per "+
			"item in the batch) — a map body's nodes are unobserved", "step", got)
	}
}
