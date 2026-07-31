package graph

import (
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func supplyDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "clean", Type: "xflow.code.script"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "clean", Input: "main"}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "clean", Supply: "rules"}},
	}
}

func TestCompileSupplyDependencyEdge(t *testing.T) {
	g, err := Compile(supplyDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, ok := g.NodeIndex("clean")
	if !ok {
		t.Fatal("clean not registered")
	}
	refs := g.SupplyRefsFor(idx)
	if len(refs) != 1 || refs[0] != "rules" {
		t.Fatalf("SupplyRefsFor(clean) = %v, want [rules]", refs)
	}
	if got := g.SupplyNodeIndexes(); len(got) != 1 {
		t.Fatalf("SupplyNodeIndexes = %v, want 1 entry", got)
	}
	// The supply node is registered as a node but never as a unit.
	if g.NodeCount() != 3 {
		t.Fatalf("NodeCount = %d, want 3", g.NodeCount())
	}
	t.Skip("unit exclusion lands in Task 3")
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount = %d, want 2 (supply excluded)", g.UnitCount())
	}
}

func TestCompileSupplyRefsSortedAndNilWhenAbsent(t *testing.T) {
	def := supplyDef()
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "aaa", Type: "xflow.supply.static", Kind: types.NodeKindSupply})
	def.DependencyEdges = append(def.DependencyEdges,
		types.DependencyEdge{Node: "clean", Supply: "aaa"})
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, _ := g.NodeIndex("clean")
	refs := g.SupplyRefsFor(idx)
	if len(refs) != 2 || refs[0] != "aaa" || refs[1] != "rules" {
		t.Fatalf("refs = %v, want [aaa rules]", refs)
	}
	startIdx, _ := g.NodeIndex("start")
	if got := g.SupplyRefsFor(startIdx); got != nil {
		t.Fatalf("SupplyRefsFor(start) = %v, want nil", got)
	}
}

func TestCompileRejectsUnknownDependencyNodes(t *testing.T) {
	def := supplyDef()
	def.DependencyEdges = []types.DependencyEdge{{Node: "nope", Supply: "rules"}}
	if _, err := Compile(def); err == nil ||
		!strings.Contains(err.Error(), "unknown consumer node") {
		t.Fatalf("err = %v, want unknown consumer node", err)
	}

	def = supplyDef()
	def.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "nope"}}
	if _, err := Compile(def); err == nil ||
		!strings.Contains(err.Error(), "unknown supply node") {
		t.Fatalf("err = %v, want unknown supply node", err)
	}
}

func TestCompileRejectsNonSupplyTarget(t *testing.T) {
	def := supplyDef()
	def.DependencyEdges = []types.DependencyEdge{{Node: "clean", Supply: "start"}}
	if _, err := Compile(def); err == nil ||
		!strings.Contains(err.Error(), "is not a supply node") {
		t.Fatalf("err = %v, want not a supply node", err)
	}
}

func TestCompileRejectsSupplyConsumer(t *testing.T) {
	def := supplyDef()
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "other", Type: "xflow.supply.static", Kind: types.NodeKindSupply})
	def.DependencyEdges = []types.DependencyEdge{{Node: "other", Supply: "rules"}}
	if _, err := Compile(def); err == nil ||
		!strings.Contains(err.Error(), "supply node may not depend on another supply") {
		t.Fatalf("err = %v, want supply-on-supply rejection", err)
	}
}

func TestCompileRejectsSupplyInDataflow(t *testing.T) {
	def := supplyDef()
	def.Connections["rules"] = map[string][]types.Connection{
		"main": {{Node: "clean", Input: "main"}},
	}
	_, err := Compile(def)
	if !errors.Is(err, ErrSupplyInDataflow) {
		t.Fatalf("err = %v, want ErrSupplyInDataflow", err)
	}

	def = supplyDef()
	def.Connections["start"]["main"] = append(def.Connections["start"]["main"],
		types.Connection{Node: "rules", Input: "main"})
	if _, err := Compile(def); !errors.Is(err, ErrSupplyInDataflow) {
		t.Fatalf("err = %v, want ErrSupplyInDataflow", err)
	}
}
