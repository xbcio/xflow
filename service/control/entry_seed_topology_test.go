package control

import (
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestEntryTriggerKind pins the entry-trigger-kind derivation the execution
// statistics read back for trigger_kind. It reads the authoritative compiled
// graph rather than anything in the seed request, so the three cases below are
// about what the graph can and cannot answer: a recognised trigger node type
// gives its kind, a non-trigger entry gives "" (UNKNOWN, not a guess), and an
// out-of-range or absent graph gives "" instead of indexing off the end.
func TestEntryTriggerKind(t *testing.T) {
	g := mustCompile(t, &types.WorkflowDef{
		Name:    "timer-down",
		Version: "v1",
		Nodes: []types.NodeDef{
			// The timer node type, not a real handler: compile validates the
			// declared type/kind pair, and the kind table is the production
			// types.TriggerKindFromNodeType. interval is the parameter the real
			// node/trigger/timer requires, kept so the fixture stays a shape the
			// runtime registry would accept.
			{Name: "tick", Type: "xflow.trigger.timer", Kind: types.NodeKindTrigger, Parameters: map[string]any{"interval": "1m"}},
			{Name: "down", Type: "xflow.function", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"tick": {"main": types.PortConnections{Targets: []types.Connection{{Node: "down", Input: "main"}}}},
		},
	})

	tickIdx, ok := g.NodeIndex("tick")
	if !ok {
		t.Fatal("fixture mistake: trigger node tick not in the compiled graph")
	}
	tickUnitIdx := g.UnitIndexForNode(tickIdx)
	if tickUnitIdx < 0 {
		t.Fatal("fixture mistake: trigger node has no scheduling unit")
	}
	if got := g.UnitKindAt(tickUnitIdx); got != graph.UnitNode {
		t.Fatalf("fixture mistake: trigger unit kind = %v, want UnitNode", got)
	}
	if got := entryTriggerKind(g, tickUnitIdx); got != types.TriggerKindTimer {
		t.Fatalf("entryTriggerKind(timer unit) = %q, want %q", got, types.TriggerKindTimer)
	}

	downIdx, ok := g.NodeIndex("down")
	if !ok {
		t.Fatal("fixture mistake: node down not in the compiled graph")
	}
	if got := entryTriggerKind(g, g.UnitIndexForNode(downIdx)); got != "" {
		t.Fatalf("entryTriggerKind(non-trigger unit) = %q, want \"\" (UNKNOWN)", got)
	}

	if got := entryTriggerKind(g, -1); got != "" {
		t.Fatalf("entryTriggerKind(-1) = %q, want \"\"", got)
	}
	if got := entryTriggerKind(g, g.UnitCount()); got != "" {
		t.Fatalf("entryTriggerKind(UnitCount()) = %q, want \"\"", got)
	}
	if got := entryTriggerKind(nil, 0); got != "" {
		t.Fatalf("entryTriggerKind(nil graph) = %q, want \"\"", got)
	}
}
