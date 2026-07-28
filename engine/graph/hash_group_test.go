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

func TestCompilerVersionV3(t *testing.T) {
	if compilerVersion != "v3" {
		t.Fatalf("compilerVersion = %q, want %q", compilerVersion, "v3")
	}
}
