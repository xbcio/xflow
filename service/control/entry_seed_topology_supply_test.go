package control

import (
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestEntryUnitIndexRejectsSupplyNode(t *testing.T) {
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "clean", Type: "xflow.code.script"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "clean", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "clean", Supply: "rules"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if idx, ok := entryUnitIndex(g, "rules"); ok {
		t.Fatalf("entryUnitIndex(rules) = (%d, true), want (_, false)", idx)
	}
	if idx, ok := entryUnitIndex(g, "clean"); !ok || idx < 0 {
		t.Fatalf("entryUnitIndex(clean) = (%d, %v), want valid", idx, ok)
	}
	if _, ok := entryUnitIndex(g, "missing"); ok {
		t.Fatal("entryUnitIndex(missing) must be false")
	}
}
