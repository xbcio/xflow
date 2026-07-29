package control

import (
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// entrySeedTopologyGraph compiles a two-node workflow: a standalone trigger
// node "trig" feeding a downstream action "down". No groups, so each node is
// its own scheduling unit and "trig" is the entry unit.
func entrySeedTopologyGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "trig-down",
		Nodes: []types.NodeDef{
			{Name: "trig", Kind: types.NodeKindTrigger},
			{Name: "down", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": {{Node: "down", Input: "main"}}}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestDeriveEntrySeedTopology(t *testing.T) {
	g := entrySeedTopologyGraph(t)

	trigIdx, ok := g.NodeIndex("trig")
	if !ok {
		t.Fatal("trig node not found")
	}
	wantEntryUnit := g.UnitIndexForNode(trigIdx)

	downIdx, ok := g.NodeIndex("down")
	if !ok {
		t.Fatal("down node not found")
	}
	wantDownUnit := g.UnitIndexForNode(downIdx)

	// The entry unit fired its "main" port, so its single boundary edge is active.
	exits := []engine.BoundaryExit{{NodeName: "trig", Port: "main", Data: map[string]any{"x": 1}}}
	entryUnitIdx, downstream, err := deriveEntrySeedTopology(g, "trig", exits)
	if err != nil {
		t.Fatalf("deriveEntrySeedTopology: %v", err)
	}
	if entryUnitIdx != wantEntryUnit {
		t.Fatalf("entryUnitIdx = %d, want %d", entryUnitIdx, wantEntryUnit)
	}
	if len(downstream) != 1 {
		t.Fatalf("downstream len = %d, want 1: %+v", len(downstream), downstream)
	}
	arr := downstream[0]
	if arr.NodeName != "down" {
		t.Fatalf("downstream NodeName = %q, want down", arr.NodeName)
	}
	if arr.NodeIdx != downIdx {
		t.Fatalf("downstream NodeIdx = %d, want %d", arr.NodeIdx, downIdx)
	}
	if arr.UnitIdx != wantDownUnit {
		t.Fatalf("downstream UnitIdx = %d, want %d", arr.UnitIdx, wantDownUnit)
	}
	if arr.ArrivalCount != 1 {
		t.Fatalf("downstream ArrivalCount = %d, want 1", arr.ArrivalCount)
	}
	// The entry unit fired the port that feeds "down", so the downstream is
	// scheduled to execute (active).
	if arr.ActiveCount != 1 {
		t.Fatalf("downstream ActiveCount = %d, want 1", arr.ActiveCount)
	}
	if arr.ExecTaskType != engine.TaskTypeNodeExec {
		t.Fatalf("downstream ExecTaskType = %v, want NodeExec", arr.ExecTaskType)
	}
}

// entrySeedMultiPortGraph compiles a router entry node "route" with two output
// ports ("hit" and "miss") feeding two distinct downstream nodes. It lets a test
// fire only a subset of the entry's ports and assert active/inactive fan-out.
func entrySeedMultiPortGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "route-fanout",
		Nodes: []types.NodeDef{
			{Name: "route", Kind: types.NodeKindTrigger},
			{Name: "on_hit", Kind: types.NodeKindAction},
			{Name: "on_miss", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"route": {
				"hit":  {{Node: "on_hit", Input: "main"}},
				"miss": {{Node: "on_miss", Input: "main"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestDeriveEntrySeedTopology_SubsetOfPortsFired verifies faithful mirroring of
// engine.downstreamUnitArrivals: when the entry unit fires only a subset of its
// output ports, only the fired branch's downstream edge is active; the unfired
// branch is counted as an arrival but NOT active (so it is skip-propagated, not
// executed).
func TestDeriveEntrySeedTopology_SubsetOfPortsFired(t *testing.T) {
	g := entrySeedMultiPortGraph(t)

	hitIdx, _ := g.NodeIndex("on_hit")
	missIdx, _ := g.NodeIndex("on_miss")

	// Only the "hit" port fired.
	exits := []engine.BoundaryExit{{NodeName: "route", Port: "hit", Data: map[string]any{"ok": true}}}
	_, downstream, err := deriveEntrySeedTopology(g, "route", exits)
	if err != nil {
		t.Fatalf("deriveEntrySeedTopology: %v", err)
	}
	if len(downstream) != 2 {
		t.Fatalf("downstream len = %d, want 2 (both branches counted): %+v", len(downstream), downstream)
	}

	byNode := make(map[string]engine.DownstreamArrival, len(downstream))
	for _, a := range downstream {
		byNode[a.NodeName] = a
	}

	hit, ok := byNode["on_hit"]
	if !ok {
		t.Fatalf("missing arrival for on_hit: %+v", downstream)
	}
	if hit.NodeIdx != hitIdx {
		t.Fatalf("on_hit NodeIdx = %d, want %d", hit.NodeIdx, hitIdx)
	}
	if hit.ArrivalCount != 1 || hit.ActiveCount != 1 {
		t.Fatalf("on_hit arrival = %+v, want ArrivalCount=1 ActiveCount=1 (fired branch)", hit)
	}

	miss, ok := byNode["on_miss"]
	if !ok {
		t.Fatalf("missing arrival for on_miss: %+v", downstream)
	}
	if miss.NodeIdx != missIdx {
		t.Fatalf("on_miss NodeIdx = %d, want %d", miss.NodeIdx, missIdx)
	}
	if miss.ArrivalCount != 1 || miss.ActiveCount != 0 {
		t.Fatalf("on_miss arrival = %+v, want ArrivalCount=1 ActiveCount=0 (unfired branch → skip-propagated)", miss)
	}
}

func TestDeriveEntrySeedTopology_EntryUnitNotFound(t *testing.T) {
	g := entrySeedTopologyGraph(t)
	if _, _, err := deriveEntrySeedTopology(g, "nonexistent", nil); err == nil {
		t.Fatal("expected error for unknown entry unit, got nil")
	}
}

func TestDeriveEntrySeedTopology_NilGraph(t *testing.T) {
	if _, _, err := deriveEntrySeedTopology(nil, "trig", nil); err == nil {
		t.Fatal("expected error for nil graph, got nil")
	}
}
