package graph

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

// C0: a compiled graph's map body must survive a JSON round-trip. Production
// reloads a graph from Redis via json.Unmarshal (rstate.Store.LoadGraph) any
// time the in-memory per-process cache has been evicted (terminal state) or
// simply does not exist (a second server replica, or the same server after a
// restart). BuildSubgraphLease reads MapBodyAt straight off the reloaded
// Graph, so if the body does not round-trip, every batch of every map-with-
// body workflow fails the moment it is served from a reloaded graph.
func TestGraphSnapshotRoundTripsMapBody(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "map-body-roundtrip",
		Nodes: []types.NodeDef{
			mapNode(map[string]any{
				"items": "$input.rows",
				"body":  subgraphBody(),
			}),
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}
	if body := g.MapBodyAt(idx); body == nil {
		t.Fatal("compiled graph carries no body package for \"m\" before round-trip")
	}

	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var round Graph
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	roundIdx, ok := round.NodeIndex("m")
	if !ok {
		t.Fatal("round-tripped graph has no node \"m\"")
	}
	body := round.MapBodyAt(roundIdx)
	if body == nil {
		t.Fatal("round-tripped graph lost its map body package: " +
			"BuildSubgraphLease would leave Package/PackageHash empty on every reload, " +
			"and the runner would reject the batch with ErrPackageMissing")
	}
	if body.Hash == "" {
		t.Error("round-tripped body package has no hash")
	}
	if body.Package == nil || body.Package.EntryNode != "inner" {
		t.Errorf("round-tripped body package entry = %+v, want \"inner\"", body.Package)
	}
}

// A graph with no map bodies at all must serialize and hash byte-identically
// to before this feature existed: every persisted execution carries a graph
// hash, and adding an omitempty-tagged field must not move it for a graph
// that never uses the feature. This pins the exact pre-change hash value so a
// tag typo (missing omitempty, wrong json name) fails loudly here rather than
// silently invalidating every already-persisted execution's graph hash.
func TestNoMapBodyMeansNoWireOrHashChange(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "plain",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "clean", Type: "xflow.code.script"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "clean", Input: "main"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	const wantHash = "sha256:4dc4775e3c3deee2a745a7951255de9176128030059e62a45f75b368bc888e71"
	if g.Hash() != wantHash {
		t.Fatalf("hash = %q, want %q (pinned before map bodies entered the hash payload); "+
			"a map-body-less graph must hash byte-identically to before", g.Hash(), wantHash)
	}
}
