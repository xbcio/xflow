package graph

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestSupplyNodeExcludedFromUnitLayer(t *testing.T) {
	g, err := Compile(supplyDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if g.NodeCount() != 3 {
		t.Fatalf("NodeCount = %d, want 3", g.NodeCount())
	}
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount = %d, want 2", g.UnitCount())
	}
	supplyIdx, _ := g.NodeIndex("rules")
	if got := g.UnitIndexForNode(supplyIdx); got != -1 {
		t.Fatalf("UnitIndexForNode(rules) = %d, want -1", got)
	}
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitAt(i).Name == "rules" {
			t.Fatalf("unit %d is the supply node", i)
		}
	}
}

// A supply node must not shift the unit index of any other node, otherwise
// every persisted execution's unit-indexed state would be misaligned.
func TestSupplyDoesNotShiftUnitIndexes(t *testing.T) {
	withSupply, err := Compile(supplyDef())
	if err != nil {
		t.Fatalf("compile with supply: %v", err)
	}
	plain := supplyDef()
	plain.Nodes = []types.NodeDef{plain.Nodes[0], plain.Nodes[2]}
	plain.DependencyEdges = nil
	without, err := Compile(plain)
	if err != nil {
		t.Fatalf("compile without supply: %v", err)
	}
	if withSupply.UnitCount() != without.UnitCount() {
		t.Fatalf("UnitCount %d vs %d", withSupply.UnitCount(), without.UnitCount())
	}
	for _, name := range []string{"start", "clean"} {
		a, _ := withSupply.NodeIndex(name)
		b, _ := without.NodeIndex(name)
		if withSupply.UnitIndexForNode(a) != without.UnitIndexForNode(b) {
			t.Fatalf("node %q unit index %d vs %d", name,
				withSupply.UnitIndexForNode(a), without.UnitIndexForNode(b))
		}
	}
}

// buildUnits runs again on UnmarshalJSON; boundary edges must not accumulate.
func TestBuildUnitsIsIdempotentForBoundaryEdges(t *testing.T) {
	g, err := Compile(groupedDefForBoundary())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	boundaryCount := func(gr *Graph) int {
		gm := gr.Groups()[0]
		return len(gm.BoundaryInputs) + len(gm.BoundaryOutputs)
	}
	before := boundaryCount(g)
	if before == 0 {
		t.Fatal("fixture must produce boundary edges")
	}
	if err := buildUnits(g); err != nil {
		t.Fatalf("rebuild units: %v", err)
	}
	if after := boundaryCount(g); after != before {
		t.Fatalf("boundary edges accumulated: %d -> %d", before, after)
	}

	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round Graph
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := boundaryCount(&round); got != before {
		t.Fatalf("round-trip boundary edges = %d, want %d", got, before)
	}
}

func groupedDefForBoundary() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "grouped",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "a", Type: "xflow.transform.set"},
			{Name: "b", Type: "xflow.transform.set"},
			{Name: "tail", Type: "xflow.transform.set"},
		},
		Groups: []types.GroupDef{{Name: "grp", Members: []string{"a", "b"}}},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "a", Input: "main"}}},
			"a":     {"main": []types.Connection{{Node: "b", Input: "main"}}},
			"b":     {"main": []types.Connection{{Node: "tail", Input: "main"}}},
		},
	}
}
