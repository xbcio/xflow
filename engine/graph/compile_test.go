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
