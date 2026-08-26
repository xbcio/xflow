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
	// UnitIdx must name clean's own unit, not merely be non-negative.
	// "UnitIdx >= 0" was a guard behind a guard: resolveResumeIntent already
	// returns an error whenever UnitIndexForNode is negative, so the only way
	// to reach this line is with a non-negative value. Resuming at the WRONG
	// unit — start's, say — passed it, and a resume that silently restarts a
	// different node is worse than one that fails.
	//
	// Resolved forward through the unit table rather than by re-calling
	// UnitIndexForNode, which would just recompute the value under test.
	if intent.NodeName != "clean" {
		t.Errorf("NodeName = %q, want %q", intent.NodeName, "clean")
	}
	if got := g.NodeName(g.UnitNodeIndex(intent.UnitIdx)); got != "clean" {
		t.Errorf("UnitIdx %d is the unit for node %q, want clean's own unit; "+
			"a resume signal would restart the wrong node", intent.UnitIdx, got)
	}
}
