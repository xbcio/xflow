package workflowreg

import (
	"encoding/json"
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
		t.Error("restored body has an empty hash; the runner's package cache keys on it")
	}
}
