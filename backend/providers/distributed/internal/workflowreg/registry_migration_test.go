package workflowreg

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestUnmarshalWorkflowRecordFallsBackToDefinitionWhenGraphFailsClosed covers
// T1's migration-reachability requirement: a stored record whose persisted
// Graph payload is a legacy/inconsistent snapshot that Graph.UnmarshalJSON
// fails closed on (e.g. grouped nodes with no matching Groups entries) must
// not abort decoding the whole workflow record. Before the fix,
// storedWorkflowRecord embedded *graph.Graph directly, so a Graph decode
// error from json.Unmarshal propagated straight out of unmarshalWorkflowRecord
// and the record.Graph==nil && record.Definition!=nil recompile fallback was
// unreachable for any such record.
func TestUnmarshalWorkflowRecordFallsBackToDefinitionWhenGraphFailsClosed(t *testing.T) {
	def := &types.WorkflowDef{
		Namespace: "test",
		Name:      "recompile-fallback",
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "next", Type: "test.next"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	}

	// A grouped-but-no-Groups snapshot: nodes explicitly declare group_idx=0
	// but the top-level groups array is empty. Graph.UnmarshalJSON must fail
	// closed on this (T1), which is exactly the failure mode this test
	// exercises against the record-level fallback.
	brokenGraphJSON := `{
		"graph_hash": "sha256:broken",
		"name": "recompile-fallback",
		"nodes": [
			{"Name":"start","Type":"xflow.start","group_idx":0},
			{"Name":"next","Type":"test.next","group_idx":0}
		],
		"index": {"start":0,"next":1},
		"entry_indexes": {"start":0},
		"out_edges": [[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}],[]],
		"in_edges": [[],[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}]],
		"in_degree": [0,1]
	}`

	raw, err := json.Marshal(storedWorkflowRecord{
		ID:             types.WorkflowID(uuid.NewString()),
		Key:            "test/recompile-fallback@v1",
		Namespace:      "test",
		Name:           "recompile-fallback",
		Version:        "v1",
		DefinitionHash: "irrelevant",
		Definition:     def,
		Graph:          json.RawMessage(brokenGraphJSON),
	})
	if err != nil {
		t.Fatalf("marshal stored record fixture: %v", err)
	}

	record, err := unmarshalWorkflowRecord(raw)
	if err != nil {
		t.Fatalf("unmarshalWorkflowRecord() error = %v, want fallback recompile from Definition to succeed", err)
	}
	if record.Graph == nil {
		t.Fatal("record.Graph is nil, want recompiled graph from Definition")
	}
	if record.Graph.NodeCount() != 2 {
		t.Fatalf("recompiled graph NodeCount() = %d, want 2", record.Graph.NodeCount())
	}
	if record.Graph.UnitCount() != 2 {
		t.Fatalf("recompiled graph UnitCount() = %d, want 2 (Definition has no groups, unlike the broken persisted Graph)", record.Graph.UnitCount())
	}
}

// TestUnmarshalWorkflowRecordGraphRoundTrips is the non-broken companion: a
// normally-persisted record's Graph must decode without needing the
// Definition-recompile fallback, proving the fallback path added above only
// engages when needed.
func TestUnmarshalWorkflowRecordGraphRoundTrips(t *testing.T) {
	def := &types.WorkflowDef{
		Namespace: "test",
		Name:      "normal",
		Version:   "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rec := backend.WorkflowRecord{
		ID:             types.WorkflowID(uuid.NewString()),
		Key:            "test/normal@v1",
		Namespace:      "test",
		Name:           "normal",
		Version:        "v1",
		DefinitionHash: "h1",
		Definition:     def,
		Graph:          g,
	}
	payload, err := marshalWorkflowRecordPayload(rec)
	if err != nil {
		t.Fatalf("marshalWorkflowRecordPayload: %v", err)
	}
	got, err := unmarshalWorkflowRecord(payload)
	if err != nil {
		t.Fatalf("unmarshalWorkflowRecord: %v", err)
	}
	if got.Graph == nil || got.Graph.Hash() != g.Hash() {
		t.Fatalf("decoded graph hash = %v, want %q", got.Graph, g.Hash())
	}
}
