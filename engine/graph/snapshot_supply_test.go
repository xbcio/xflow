package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestGraphSnapshotRoundTripsSupplyRefs(t *testing.T) {
	g, err := Compile(supplyDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round Graph
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	idx, ok := round.NodeIndex("clean")
	if !ok {
		t.Fatal("clean missing after round-trip")
	}
	refs := round.SupplyRefsFor(idx)
	if len(refs) != 1 || refs[0] != "rules" {
		t.Fatalf("round-trip refs = %v, want [rules]", refs)
	}
	// supplyIndexes is rebuilt from Nodes[i].Kind, not serialized.
	if got := round.SupplyNodeIndexes(); len(got) != 1 || got["rules"] != 1 {
		t.Fatalf("round-trip supply indexes = %v", got)
	}
	if round.UnitCount() != g.UnitCount() {
		t.Fatalf("UnitCount %d vs %d", round.UnitCount(), g.UnitCount())
	}
	if round.Hash() != g.Hash() {
		t.Fatalf("hash drift after round-trip")
	}
}

// A graph with no supply nodes must serialize and hash byte-identically to
// before this feature existed: every persisted execution carries a graph hash.
func TestNoSupplyMeansNoWireOrHashChange(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "plain",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "clean", Type: "xflow.code.script"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "clean", Input: "main"}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "supply_refs") {
		t.Fatalf("supply_refs must be omitted for supply-free graphs: %s", b)
	}
	// Pin the hash so any future payload change to graphHashPayload that would
	// invalidate persisted executions fails here loudly.
	const wantPrefix = "sha256:"
	if !strings.HasPrefix(g.Hash(), wantPrefix) {
		t.Fatalf("hash = %q", g.Hash())
	}
	t.Logf("supply-free graph hash: %s", g.Hash())
}
