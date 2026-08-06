package graph

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

// C0 shipped as a graph-level map[int]*NodeBodyPackage, which meant the body
// had its own encode step, its own decode step, and its own hash-payload
// entry — three places to forget. One of them WAS forgotten, and a graph
// reloaded from Redis lost every body silently: BuildSubgraphLease shipped an
// empty Package and every batch of every map-with-body workflow failed at the
// runner with ErrPackageMissing, identically on every retry.
//
// Moving the body onto NodeMeta removes the possibility rather than fixing the
// instance: the node and its body are ONE wire object, so there is no separate
// step that can be skipped. This test pins that property structurally — it
// reads the encoded JSON and asserts the body lives inside the node object,
// not beside it. A future refactor that lifts bodies back out to a graph-level
// field fails here, with the reason attached.
func TestBodyIsEncodedInsideItsNodeNotBesideIt(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "wf",
		Nodes: []types.NodeDef{mapNode(map[string]any{"items": "$input.rows", "body": subgraphBody()})},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal to raw: %v", err)
	}
	// No graph-level body container may exist. If one reappears, a body can be
	// dropped independently of its node again.
	for _, key := range []string{"map_bodies", "bodies", "node_bodies"} {
		if _, found := raw[key]; found {
			t.Errorf("graph JSON carries a top-level %q container; bodies must ride inside "+
				"their node so they cannot be serialized apart from it", key)
		}
	}

	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(raw["nodes"], &nodes); err != nil {
		t.Fatalf("unmarshal nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if _, ok := nodes[0]["body"]; !ok {
		t.Fatal("the map node's JSON object carries no \"body\" key: the body is not " +
			"travelling with its node, which is the entire point of storing it on NodeMeta")
	}
}

// A body-bearing node type other than xflow.map must need no new wire, hash,
// or decode plumbing — that is what makes this placement worth the refactor,
// since transform nodes (filter, reduce) grow bodies as soon as their logic
// outgrows a single expression.
//
// The discriminating property is not "a non-map node CAN carry a body" — the
// old graph-level map keyed on node index and was equally type-agnostic, so
// that assertion passed there too and proved nothing. It is that the body
// needs no plumbing of its own: here the body is set through the ordinary
// NodeMeta path, and the assertion is that this ALONE suffices to get it onto
// the wire and back. Under the old design the same code produced a graph whose
// BodyAt returned nil, because the body also had to be written into a separate
// container that the node knew nothing about.
func TestABodySetOnANodeNeedsNoSeparatePlumbing(t *testing.T) {
	body, err := ProjectNodeBodyPackage("t", map[string]any{"body": subgraphBody()}, nil)
	if err != nil {
		t.Fatalf("project body: %v", err)
	}

	// A hand-built graph standing in for a future body-bearing node type. The
	// body is attached ONLY to the node — no other field is touched.
	withBody := &Graph{
		name:  "wf",
		nodes: []NodeMeta{{Name: "t", Type: "xflow.transform.reduce", GroupIdx: -1, Body: body}},
		index: map[string]int{"t": 0},
	}
	data, err := json.Marshal(withBody)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round Graph
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := round.BodyAt(0)
	if got == nil {
		t.Fatal("a body attached to a node did not survive the round-trip: setting " +
			"NodeMeta.Body must be sufficient on its own, or every future body-bearing " +
			"node type has to remember a second, separate step — the exact failure this " +
			"placement exists to make impossible")
	}
	if got.Hash != body.Hash {
		t.Errorf("round-tripped body hash = %q, want %q", got.Hash, body.Hash)
	}
	if got.Package == nil || got.Package.EntryNode != "inner" {
		t.Errorf("round-tripped package = %+v, want entry \"inner\"", got.Package)
	}
}
