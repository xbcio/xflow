package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestUnitGraphWithGroup(t *testing.T) {
	// nodes ingest,analyze,store; group {ingest,analyze} => units: 1 group + store = 2
	g, err := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount = %d, want 2", g.UnitCount())
	}
	gm := g.Groups()[0]
	if gm.UnitIdx < 0 {
		t.Fatal("group UnitIdx not assigned")
	}
	if len(gm.BoundaryOutputs) != 1 {
		t.Fatalf("boundary outputs = %d, want 1", len(gm.BoundaryOutputs))
	}
	bo := gm.BoundaryOutputs[0]
	ni := func(name string) int { idx, _ := g.NodeIndex(name); return idx }
	if bo.Src.NodeIdx != ni("analyze") || bo.Dst.NodeIdx != ni("store") {
		t.Fatalf("boundary endpoints wrong: %+v", bo)
	}
	storeUnit := g.UnitIndexForNode(ni("store"))
	if g.UnitInDegreeAt(storeUnit) != 1 {
		t.Fatalf("store unit in-degree = %d, want 1", g.UnitInDegreeAt(storeUnit))
	}
	if g.UnitIndexForNode(ni("ingest")) != gm.UnitIdx {
		t.Fatal("ingest must map to the group unit")
	}
}

// TestUnitMergeMode_GroupReturnsEmpty is a probe that documents the current
// behaviour: UnitMergeMode for a UnitGroup always returns "" (wait_all
// default). GroupDef has no MergeMode field, so no other value is possible.
// If GroupDef.MergeMode is introduced, update UnitMergeMode to read it and
// update this test accordingly.
func TestUnitMergeMode_GroupReturnsEmpty(t *testing.T) {
	g, err := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Find the UnitGroup unit.
	var groupUnitIdx = -1
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == UnitGroup {
			groupUnitIdx = i
			break
		}
	}
	if groupUnitIdx < 0 {
		t.Fatal("expected a UnitGroup unit")
	}
	got := g.UnitMergeMode(groupUnitIdx)
	if got != "" {
		t.Fatalf("UnitMergeMode for UnitGroup = %q, want %q (wait_all default)", got, "")
	}
}

func TestUnitGraphMultiExit(t *testing.T) {
	// Single-member group {a}, a has two output ports ok->x, err->y.
	def := &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "a", Kind: types.NodeKindAction},
			{Name: "x", Kind: types.NodeKindAction},
			{Name: "y", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"a": {"ok": {Targets: []types.Connection{{Node: "x", Input: "main"}}}, "err": {Targets: []types.Connection{{Node: "y", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a"}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	gm := g.Groups()[0]
	if len(gm.BoundaryOutputs) != 2 {
		t.Fatalf("boundary outputs = %d, want 2", len(gm.BoundaryOutputs))
	}
	// Stable sort: by Src.Port, "err" < "ok"
	if gm.BoundaryOutputs[0].Src.Port != "err" || gm.BoundaryOutputs[1].Src.Port != "ok" {
		t.Fatalf("boundary outputs not stably sorted: %+v", gm.BoundaryOutputs)
	}
}
