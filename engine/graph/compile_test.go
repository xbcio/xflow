package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestCompile_LinearChain(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "linear",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "B", Type: "test.b"},
			{Name: "C", Type: "test.c"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "C", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	if len(g.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(g.Nodes))
	}
	if g.InDegree[g.Index["A"]] != 0 {
		t.Error("A should have in-degree 0")
	}
	if g.InDegree[g.Index["B"]] != 1 {
		t.Error("B should have in-degree 1")
	}
	if g.InDegree[g.Index["C"]] != 1 {
		t.Error("C should have in-degree 1")
	}
}

func TestCompile_CycleDetection(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "cycle",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "B", Type: "test.b"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "A", Input: "main"}}},
		},
	}

	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestCompile_AllowCyclesAllowsCycleWithStart(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cycle",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 7},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if !g.AllowCycles {
		t.Fatal("expected cyclic graph")
	}
	if g.StartIdx != g.Index["start"] {
		t.Fatalf("StartIdx = %d, want %d", g.StartIdx, g.Index["start"])
	}
	if g.MaxAutoDepth != 7 {
		t.Fatalf("MaxAutoDepth = %d, want 7", g.MaxAutoDepth)
	}
}

func TestCompileCollectsStartAndTriggerEntries(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "entries",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "cron", Type: "xflow.trigger.cron", Kind: types.NodeKindTrigger},
			{Name: "work", Type: "test.work"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "work", Input: "main"}}},
			"cron":  {"main": []types.Connection{{Node: "work", Input: "main"}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.EntryIndexes) != 2 {
		t.Fatalf("EntryIndexes len = %d, want 2", len(g.EntryIndexes))
	}
	if g.EntryIndexes["start"] != 0 || g.EntryIndexes["cron"] != 1 {
		t.Fatalf("EntryIndexes = %+v", g.EntryIndexes)
	}
}

func TestCompile_AllowCyclesDefaultsMaxAutoDepth(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cycle",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if g.MaxAutoDepth != DefaultMaxAutoDepth {
		t.Fatalf("MaxAutoDepth = %d, want %d", g.MaxAutoDepth, DefaultMaxAutoDepth)
	}
}

func TestCompile_AllowCyclesRequiresExactlyOneStart(t *testing.T) {
	tests := []struct {
		name  string
		nodes []types.NodeDef
	}{
		{
			name:  "missing",
			nodes: []types.NodeDef{{Name: "review", Type: "test.review"}},
		},
		{
			name: "multiple",
			nodes: []types.NodeDef{
				{Name: "start1", Type: "xflow.start"},
				{Name: "start2", Type: "xflow.start"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := &types.WorkflowDef{
				Name:    tt.name,
				Options: &types.WorkflowOptions{AllowCycles: true},
				Nodes:   tt.nodes,
			}

			if _, err := Compile(def); err == nil {
				t.Fatal("expected start validation error")
			}
		})
	}
}

func TestCompile_AllowCyclesDoesNotRejectTriggerEntry(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "trigger",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "trigger", Type: "xflow.trigger", Kind: types.NodeKindTrigger},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if g.EntryIndexes["trigger"] != 1 {
		t.Fatalf("trigger entry index = %d, want 1", g.EntryIndexes["trigger"])
	}
}

func TestCompile_AllowCyclesRejectsWaitAllMerge(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "merge",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "join", Type: "xflow.merge", Parameters: map[string]any{"mode": "wait_all"}},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"join":  {"main": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}

	if _, err := Compile(def); err == nil {
		t.Fatal("expected wait_all merge validation error")
	}
}

func TestCompile_FanOutFanIn(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "left", Type: "test.left"},
			{Name: "right", Type: "test.right"},
			{Name: "join", Type: "test.join"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{
				{Node: "left", Input: "main"},
				{Node: "right", Input: "main"},
			}},
			"left":  {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"right": {"main": []types.Connection{{Node: "join", Input: "main"}}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	joinIdx := g.Index["join"]
	if g.InDegree[joinIdx] != 2 {
		t.Errorf("join should have in-degree 2, got %d", g.InDegree[joinIdx])
	}
}

func TestCompile_PortRouting(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "ports",
		Nodes: []types.NodeDef{
			{Name: "check", Type: "test.check"},
			{Name: "ok", Type: "test.ok"},
			{Name: "fail", Type: "test.fail"},
		},
		Connections: types.Connections{
			"check": {
				"main":  []types.Connection{{Node: "ok", Input: "main"}},
				"error": []types.Connection{{Node: "fail", Input: "main"}},
			},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	checkIdx := g.Index["check"]
	if len(g.OutEdges[checkIdx]) != 2 {
		t.Errorf("check should have 2 out-edges, got %d", len(g.OutEdges[checkIdx]))
	}

	for _, e := range g.OutEdges[checkIdx] {
		if e.SrcPort != "main" && e.SrcPort != "error" {
			t.Errorf("unexpected port: %s", e.SrcPort)
		}
	}
}

func TestCompile_DuplicateNodeName(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "dup",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "A", Type: "test.b"},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected duplicate node name error")
	}
}

func TestCompile_UnknownConnectionNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "unknown",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "Z", Input: "main"}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected unknown destination node error")
	}
}
