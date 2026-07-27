package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestUnitGraphNoGroupsMirrorsNodes(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "w",
		Nodes: []types.NodeDef{
			{Name: "a", Type: "noop", Kind: types.NodeKindAction},
			{Name: "b", Type: "noop", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"a": {"main": {{Node: "b", Input: "main"}}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if g.UnitCount() != g.NodeCount() {
		t.Fatalf("UnitCount %d != NodeCount %d", g.UnitCount(), g.NodeCount())
	}
	for i := 0; i < g.UnitCount(); i++ {
		if u := g.UnitAt(i); u.Kind != UnitNode || u.NodeIdx != i {
			t.Fatalf("unit %d: kind=%d nodeIdx=%d", i, u.Kind, u.NodeIdx)
		}
	}
	if b, _ := g.NodeIndex("b"); g.UnitInDegreeAt(b) != 1 {
		t.Fatalf("b unit in-degree = %d, want 1", g.UnitInDegreeAt(b))
	}
}
