package workflowreg

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// A record persisted before map bodies travelled on the wire has the map
// node's "body" parameter but no map_bodies entry. Graph.UnmarshalJSON fails
// closed on that (ErrBodySnapshotMissingPackage) rather than handing back a
// graph whose every batch would fail at the runner with ErrPackageMissing.
//
// This test is the reason that fail-closed choice is the right one: the record
// decoder treats a Graph decode failure as "recompile from the stored
// Definition", and the recompile runs projectNodeBodies, so the body comes
// back. The condition is self-healing, not merely loud. Had UnmarshalJSON
// returned a bodyless graph instead, this fallback would never engage and the
// workflow would be permanently broken with no signal.
func TestUnmarshalWorkflowRecordRecompilesAMapBodyLostFromTheSnapshot(t *testing.T) {
	def := &types.WorkflowDef{
		Namespace: "test",
		Name:      "map-body-migration",
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.rows",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "inner", "type": "xflow.noop"},
						},
					},
				},
			}},
		},
	}

	// Build the corrupt payload from a real compiled graph so every other field
	// is exactly what this version writes, then strip only the node's body.
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	full, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(full, &fields); err != nil {
		t.Fatalf("unmarshal to fields: %v", err)
	}
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(fields["nodes"], &nodes); err != nil {
		t.Fatalf("unmarshal nodes: %v", err)
	}
	if _, ok := nodes[0]["body"]; !ok {
		t.Fatal("fixture node has no body to drop; this test would prove nothing")
	}
	delete(nodes[0], "body")
	patchedNodes, err := json.Marshal(nodes)
	if err != nil {
		t.Fatalf("re-marshal nodes: %v", err)
	}
	fields["nodes"] = patchedNodes
	legacyGraph, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("re-marshal graph: %v", err)
	}

	raw, err := json.Marshal(storedWorkflowRecord{
		ID:             types.WorkflowID(uuid.NewString()),
		Key:            "test/map-body-migration@v1",
		Namespace:      "test",
		Name:           "map-body-migration",
		Version:        "v1",
		DefinitionHash: "irrelevant",
		Definition:     def,
		Graph:          json.RawMessage(legacyGraph),
	})
	if err != nil {
		t.Fatalf("marshal stored record fixture: %v", err)
	}

	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		t.Fatalf("unmarshalWorkflowRecord() error = %v; a snapshot that lost its map "+
			"body must fall back to recompiling from the Definition", err)
	}
	if record.Graph == nil {
		t.Fatal("record.Graph is nil, want the recompiled graph")
	}
	// The point of the whole exercise: the body is back, so BuildSubgraphLease
	// will ship a real Package and the runner will accept the batch.
	idx, ok := record.Graph.NodeIndex("m")
	if !ok {
		t.Fatal("recompiled graph has no node \"m\"")
	}
	body := record.Graph.BodyAt(idx)
	if body == nil {
		t.Fatal("recompiled graph still has no map body; the fallback recompile did not " +
			"restore it, so every batch would fail at the runner with ErrPackageMissing")
	}
	if body.Hash == "" {
		t.Fatal("restored body has an empty hash; the runner's package cache keys on it")
	}
	// "Not empty" was the whole check, and it is green for any constant string.
	// The cache key has to be right, not merely present, and there are two ways
	// for it to be wrong that this test can actually see.
	//
	// First, the scheme prefix. It is written out literally here rather than
	// read from graph.packageHashPrefix on purpose: it is a wire-format
	// constant that a runner from another build compares against, so a test
	// that imports it would follow a breaking change instead of reporting one.
	if !strings.HasPrefix(body.Hash, "pkg-sha256:v1:") {
		t.Errorf("restored body hash = %q, want the pkg-sha256:v1: scheme prefix; "+
			"without it a runner cannot tell two hash schemes apart in its cache",
			body.Hash)
	}
	// Second, it must be the hash the ordinary compile produces. The fallback
	// recompiles from stored.Definition, which reached it through a JSON round
	// trip -- a NodeDef field that does not survive that trip yields a body that
	// is present, hashes fine, and keys the runner's cache differently from
	// every workflow registered the normal way.
	refIdx, ok := compiled.NodeIndex("m")
	if !ok {
		t.Fatal("reference graph has no node \"m\"")
	}
	refBody := compiled.BodyAt(refIdx)
	if refBody == nil {
		t.Fatal("reference graph has no map body; the fixture is not what this test assumes")
	}
	if body.Hash != refBody.Hash {
		t.Errorf("recompiled body hash = %q, want %q (the directly compiled graph's): "+
			"the fallback and the normal path must agree or a migrated workflow misses "+
			"the runner's package cache forever", body.Hash, refBody.Hash)
	}
}
