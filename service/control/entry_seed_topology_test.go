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

	entryUnitIdx, downstream, err := deriveEntrySeedTopology(g, "trig")
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
	// The entry unit is the seed source that succeeded: all its boundary-output
	// edges are active so the downstream is scheduled to execute (not skip).
	if arr.ActiveCount != 1 {
		t.Fatalf("downstream ActiveCount = %d, want 1", arr.ActiveCount)
	}
	if arr.ExecTaskType != engine.TaskTypeNodeExec {
		t.Fatalf("downstream ExecTaskType = %v, want NodeExec", arr.ExecTaskType)
	}
}

func TestDeriveEntrySeedTopology_EntryUnitNotFound(t *testing.T) {
	g := entrySeedTopologyGraph(t)
	if _, _, err := deriveEntrySeedTopology(g, "nonexistent"); err == nil {
		t.Fatal("expected error for unknown entry unit, got nil")
	}
}

func TestDeriveEntrySeedTopology_NilGraph(t *testing.T) {
	if _, _, err := deriveEntrySeedTopology(nil, "trig"); err == nil {
		t.Fatal("expected error for nil graph, got nil")
	}
}
