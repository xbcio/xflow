package engine

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func supplyGraphForSignal(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "clean", Type: "xflow.code.script"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "clean", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "clean", Supply: "rules"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// Resuming at a supply node must be rejected, not turned into UnitIdx -1.
func TestResumeIntentRejectsSupplyNode(t *testing.T) {
	g := supplyGraphForSignal(t)
	_, err := resolveResumeIntent(g, "rules")
	if err == nil || !strings.Contains(err.Error(), "supply node") {
		t.Fatalf("err = %v, want supply node rejection", err)
	}
	intent, err := resolveResumeIntent(g, "clean")
	if err != nil {
		t.Fatalf("resolveResumeIntent(clean): %v", err)
	}
	if intent.UnitIdx < 0 {
		t.Fatalf("UnitIdx = %d, want >= 0", intent.UnitIdx)
	}
}
