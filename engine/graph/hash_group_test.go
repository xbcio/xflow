package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestGraphHashChangesWithGroup(t *testing.T) {
	baseDef := mkGroupDef([]string{"ingest", "analyze"})
	baseDef.Groups = nil
	base, err := Compile(baseDef)
	if err != nil {
		t.Fatal(err)
	}
	grouped, err := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if err != nil {
		t.Fatal(err)
	}
	if base.Hash() == grouped.Hash() {
		t.Fatal("introducing a group must change graph hash")
	}
}

func TestGraphHashDeterministic(t *testing.T) {
	g1, _ := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	g2, _ := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if g1.Hash() != g2.Hash() {
		t.Fatal("identical definition must hash identically")
	}
}

func TestGraphHashChangesWithGroupSelector(t *testing.T) {
	d2 := mkGroupDef([]string{"ingest", "analyze"})
	d2.Groups[0].RunnerSelector = &types.RunnerSelector{
		Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"cloud": "tencent"}}
	g1, _ := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	g2, _ := Compile(d2)
	if g1.Hash() == g2.Hash() {
		t.Fatal("group selector change must change graph hash")
	}
}

func TestGraphHashChangesWithActivationReplicas(t *testing.T) {
	groupBase, err := Compile(mkGroupDef([]string{"ingest", "analyze"}))
	if err != nil {
		t.Fatalf("compile group base: %v", err)
	}
	groupReplicatedDef := mkGroupDef([]string{"ingest", "analyze"})
	groupReplicatedDef.Groups[0].ActivationReplicas = 3
	groupReplicated, err := Compile(groupReplicatedDef)
	if err != nil {
		t.Fatalf("compile replicated group: %v", err)
	}
	if groupBase.Hash() == groupReplicated.Hash() {
		t.Fatal("group activation cardinality must change graph hash")
	}

	nodeBaseDef := mkGroupDef([]string{"ingest", "analyze"})
	nodeBaseDef.Groups = nil
	nodeBase, err := Compile(nodeBaseDef)
	if err != nil {
		t.Fatalf("compile node base: %v", err)
	}
	nodeReplicatedDef := mkGroupDef([]string{"ingest", "analyze"})
	nodeReplicatedDef.Groups = nil
	nodeReplicatedDef.Nodes[0].ActivationReplicas = 3
	nodeReplicated, err := Compile(nodeReplicatedDef)
	if err != nil {
		t.Fatalf("compile replicated node: %v", err)
	}
	if nodeBase.Hash() == nodeReplicated.Hash() {
		t.Fatal("standalone node activation cardinality must change graph hash")
	}
}

func TestCompilerVersionV3(t *testing.T) {
	if compilerVersion != "v3" {
		t.Fatalf("compilerVersion = %q, want %q", compilerVersion, "v3")
	}
}
