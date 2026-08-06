package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// mkGroupDef builds a three-node chain ingest(trigger)->analyze->store with the
// given members assigned to a single group "edge".
func mkGroupDef(members []string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "ingest", Type: "kafka", Kind: types.NodeKindTrigger},
			{Name: "analyze", Type: "wasm", Kind: types.NodeKindAction},
			{Name: "store", Type: "db", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: members}},
	}
}

func TestCompileGroupBasic(t *testing.T) {
	g, err := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	groups := g.Groups()
	if len(groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(groups))
	}
	gm := groups[0]
	if !gm.Trigger {
		t.Fatal("group containing a trigger member must be Trigger")
	}
	ni := func(name string) int { idx, _ := g.NodeIndex(name); return idx }
	if gm.EntryIdx != ni("ingest") {
		t.Fatalf("entry = %d, want ingest", gm.EntryIdx)
	}
	if g.NodeAt(ni("analyze")).GroupIdx != 0 {
		t.Fatal("analyze GroupIdx must be 0")
	}
	if g.NodeAt(ni("store")).GroupIdx != -1 {
		t.Fatal("store must stay ungrouped (-1)")
	}
}

func TestCompileGroupRejects(t *testing.T) {
	// Multi-entry: src1->a, src2->b, a->b, group {a,b} has two external entries.
	multiEntry := &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "src1", Kind: types.NodeKindAction},
			{Name: "src2", Kind: types.NodeKindAction},
			{Name: "a", Kind: types.NodeKindAction},
			{Name: "b", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"src1": {"main": {Targets: []types.Connection{{Node: "a", Input: "main"}}}},
			"src2": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"a":    {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}},
	}
	// Unreachable member: entry a cannot reach c (c only connected from outside).
	unreachable := &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "a", Kind: types.NodeKindAction},
			{Name: "c", Kind: types.NodeKindAction},
			{Name: "src", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"src": {"main": {Targets: []types.Connection{{Node: "c", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "g", Members: []string{"a", "c"}}},
	}
	memberSelector := mkGroupDef([]string{"ingest", "analyze"})
	memberSelector.Nodes[1].RunnerSelector = &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired}

	overlap := mkGroupDef([]string{"ingest", "analyze"})
	overlap.Groups = append(overlap.Groups, types.GroupDef{Name: "g2", Members: []string{"analyze", "store"}})

	cases := map[string]*types.WorkflowDef{
		"unknown member":  mkGroupDef([]string{"ingest", "ghost"}),
		"member selector": memberSelector,
		"multi entry":     multiEntry,
		"unreachable":     unreachable,
		"overlap":         overlap,
	}
	for name, def := range cases {
		if _, err := Compile(def); err == nil {
			t.Fatalf("%s: expected compile error, got nil", name)
		}
	}
}
