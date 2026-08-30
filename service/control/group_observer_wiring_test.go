package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"

	dto "github.com/prometheus/client_model/go"
)

// TestNewControlPlaneWiresGroupObserver is the group-metrics counterpart of
// TestNewControlPlaneWiresMetricsIntoAuthAndSweeper (controlplane_test.go):
// it proves Config.Metrics reaches the internal engine's GroupObserver.
//
// It deliberately does not read a private field off cp.eng to check for
// non-nil — engine.Engine's groupObserver field belongs to package engine,
// not to this package, so there is nothing to peek at even with same-package
// access to *ControlPlane. Instead it drives the real assembly path: build a
// ControlPlane over a real backendlocal.Backend (which implements
// engine.GroupStateStore), Submit a grouped workflow through cp's own
// engine, then call BuildGroupLease — the exact call executeGroup and
// service/control/group_control_loop.go's dispatchGroupLease make in
// production — and read the outcome back off the metrics registry's own
// Gather() output. That is the strongest assertion available from this
// package: proof the wiring is load-bearing, not merely present.
func TestNewControlPlaneWiresGroupObserver(t *testing.T) {
	m := metrics.New()
	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), Metrics: m})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	def := &types.WorkflowDef{
		Name:    "group-observer-wiring",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "B", Type: "test.action", Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}

	ctx := context.Background()
	execID, err := cp.eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("cp.eng.Submit: %v", err)
	}

	gm := g.Groups()[0]
	task := &engine.Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        engine.TaskTypeGroupExec,
	}

	if _, _, err := cp.eng.BuildGroupLease(ctx, task); err != nil {
		t.Fatalf("cp.eng.BuildGroupLease: %v", err)
	}

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}
	family := findMetricFamily(families, "xflow_group_lease_acquired_total")
	if family == nil {
		t.Fatal("xflow_group_lease_acquired_total not found — NewControlPlane does not wire " +
			"Config.Metrics into the engine's GroupObserver")
	}
	if len(family.GetMetric()) != 1 || family.GetMetric()[0].GetCounter().GetValue() != 1 {
		t.Fatalf("xflow_group_lease_acquired_total metrics = %+v, want exactly one series with value 1", family.GetMetric())
	}
}

func findMetricFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}
