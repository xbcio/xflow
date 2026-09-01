package graph

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func groupedRoundTripDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "grouped",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
			"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
	}
}

// TestGraphSnapshotGroupedRoundTrip covers T1: a modern grouped graph must
// round-trip through JSON with its unit topology and GroupMeta intact — this
// is the regression that motivated the two-layer unit IR rebuild
// (UnmarshalJSON -> buildUnits). It must keep passing after the T1 legacy
// disambiguation change on top.
func TestGraphSnapshotGroupedRoundTrip(t *testing.T) {
	def := groupedRoundTripDef()
	def.Groups[0].ActivationReplicas = 3
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount() = %d, want 2 (grouped {ingest,analyze} + store)", g.UnitCount())
	}

	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var g2 Graph
	if err := json.Unmarshal(data, &g2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if g2.UnitCount() != g.UnitCount() {
		t.Fatalf("UnitCount() after round-trip = %d, want %d", g2.UnitCount(), g.UnitCount())
	}
	if len(g2.Groups()) != 1 {
		t.Fatalf("Groups() after round-trip = %d, want 1", len(g2.Groups()))
	}
	if got := g2.Groups()[0].ActivationReplicas; got != 3 {
		t.Fatalf("group ActivationReplicas after round-trip = %d, want 3", got)
	}
	if g2.Hash() != g.Hash() {
		t.Fatalf("Hash() after round-trip = %q, want %q", g2.Hash(), g.Hash())
	}
	storeIdx, ok := g2.NodeIndex("store")
	if !ok {
		t.Fatal("store node missing after round-trip")
	}
	storeUnit := g2.UnitIndexForNode(storeIdx)
	if g2.UnitInDegreeAt(storeUnit) != 1 {
		t.Fatalf("store unit in-degree = %d, want 1", g2.UnitInDegreeAt(storeUnit))
	}
}

// TestGraphSnapshotLegacyNoGroupIdxFieldDegradesToUngrouped covers T1: a
// snapshot with no group_idx/GroupIdx field on any node at all (as if
// compiled and serialized before that field existed) must be recognized as
// legacy and degrade to a 1:1 unit mapping with every node's GroupIdx
// normalized to -1. It must NOT be misjudged as grouped: before the fix, Go's
// zero value (0) for a missing int field was indistinguishable from an
// explicit group index of 0, and buildUnits interpreted it as "node 0 belongs
// to group 0", producing a broken/zeroed unit topology (UnitCount 0) even
// though this snapshot has no groups at all.
func TestGraphSnapshotLegacyNoGroupIdxFieldDegradesToUngrouped(t *testing.T) {
	legacyJSON := `{
		"graph_hash": "sha256:legacy",
		"name": "legacy",
		"workflow_version": "1",
		"compiler_version": "v1",
		"nodes": [
			{"Name":"start","Type":"test.start","PortOuts":["main"]},
			{"Name":"wait","Type":"test.wait"}
		],
		"index": {"start":0,"wait":1},
		"entry_indexes": {"start":0},
		"out_edges": [[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}],[]],
		"in_edges": [[],[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}]],
		"in_degree": [0,1],
		"allow_cycles": false,
		"start_idx": -1,
		"max_auto_depth": 0
	}`
	var g Graph
	if err := json.Unmarshal([]byte(legacyJSON), &g); err != nil {
		t.Fatalf("unmarshal legacy snapshot: %v", err)
	}
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount() = %d, want 2 (legacy snapshot must degrade to 1:1, not be misjudged as grouped)", g.UnitCount())
	}
	for i := 0; i < g.NodeCount(); i++ {
		if g.NodeAt(i).GroupIdx != -1 {
			t.Fatalf("node %d GroupIdx = %d, want -1 (legacy must normalize, not inherit Go zero value)", i, g.NodeAt(i).GroupIdx)
		}
	}
	if g.UnitIndexForNode(0) == g.UnitIndexForNode(1) {
		t.Fatal("legacy ungrouped nodes must map to distinct units")
	}
}

// TestGraphSnapshotLegacyAllExplicitNegativeOneDegradesToUngrouped covers T1:
// a snapshot where every node explicitly carries group_idx/GroupIdx = -1 (the
// modern "explicitly ungrouped" representation, as opposed to the field being
// absent) must also degrade to 1:1, exercising the "modern, present" branch
// rather than the "legacy, absent" branch.
func TestGraphSnapshotLegacyAllExplicitNegativeOneDegradesToUngrouped(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "ungrouped",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "wait", Type: "test.wait"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "wait", Input: "main"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Every node's GroupIdx is -1 by construction (Compile always sets it),
	// so the wire form already exercises the "present, all -1" branch.
	if !strings.Contains(string(data), `"group_idx":-1`) {
		t.Fatalf("expected explicit group_idx:-1 in wire payload, got: %s", data)
	}
	var g2 Graph
	if err := json.Unmarshal(data, &g2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if g2.UnitCount() != 2 {
		t.Fatalf("UnitCount() = %d, want 2", g2.UnitCount())
	}
}

// TestGraphSnapshotPartialGroupIdxPresenceFailsClosed covers T1: a snapshot
// where some nodes carry group_idx/GroupIdx and others do not is internally
// inconsistent (neither cleanly legacy nor cleanly modern) and must fail
// closed rather than guess.
func TestGraphSnapshotPartialGroupIdxPresenceFailsClosed(t *testing.T) {
	mixedJSON := `{
		"graph_hash": "sha256:mixed",
		"name": "mixed",
		"nodes": [
			{"Name":"start","Type":"test.start","group_idx":-1},
			{"Name":"wait","Type":"test.wait"}
		],
		"index": {"start":0,"wait":1},
		"entry_indexes": {"start":0},
		"out_edges": [[],[]],
		"in_edges": [[],[]],
		"in_degree": [0,0]
	}`
	var g Graph
	err := json.Unmarshal([]byte(mixedJSON), &g)
	if err == nil {
		t.Fatal("expected error for partial group_idx presence, got nil")
	}
}

// TestGraphSnapshotGroupedNodesWithoutGroupsFailsClosed covers T1: a
// snapshot whose nodes explicitly declare a group (GroupIdx >= 0) but whose
// top-level "groups" array is empty/missing must fail closed. Silently
// normalizing this to ungrouped would be exactly the P0-2/F9 regression this
// task exists to prevent: the durable remaining/in-degree counters would be
// seeded from the wrong (larger, ungrouped) unit count.
func TestGraphSnapshotGroupedNodesWithoutGroupsFailsClosed(t *testing.T) {
	brokenJSON := `{
		"graph_hash": "sha256:broken",
		"name": "broken",
		"nodes": [
			{"Name":"ingest","Type":"test.ingest","group_idx":0},
			{"Name":"analyze","Type":"test.analyze","group_idx":0}
		],
		"index": {"ingest":0,"analyze":1},
		"entry_indexes": {"ingest":0},
		"out_edges": [[],[]],
		"in_edges": [[],[]],
		"in_degree": [0,0]
	}`
	var g Graph
	err := json.Unmarshal([]byte(brokenJSON), &g)
	if err == nil {
		t.Fatal("expected error for grouped nodes with no group definitions, got nil")
	}
	if !errors.Is(err, ErrGroupedSnapshotMissingUnitIR) {
		t.Fatalf("error = %v, want wrapping ErrGroupedSnapshotMissingUnitIR", err)
	}
}

// TestGraphSnapshotLegacyNodesWithGroupsFailsClosed covers T1: a snapshot
// with no per-node group_idx field (legacy shape) but a non-empty top-level
// "groups" array is internally inconsistent — the Groups entries reference
// node indexes that decodeWireNodes just normalized to ungrouped. This must
// fail closed rather than silently pick one side as authoritative.
func TestGraphSnapshotLegacyNodesWithGroupsFailsClosed(t *testing.T) {
	inconsistentJSON := `{
		"graph_hash": "sha256:inconsistent",
		"name": "inconsistent",
		"nodes": [
			{"Name":"ingest","Type":"test.ingest"},
			{"Name":"analyze","Type":"test.analyze"}
		],
		"index": {"ingest":0,"analyze":1},
		"entry_indexes": {"ingest":0},
		"out_edges": [[],[]],
		"in_edges": [[],[]],
		"in_degree": [0,0],
		"groups": [{"Name":"edge","Members":[0,1],"EntryIdx":0,"UnitIdx":0}]
	}`
	var g Graph
	err := json.Unmarshal([]byte(inconsistentJSON), &g)
	if err == nil {
		t.Fatal("expected error for legacy nodes with non-empty groups, got nil")
	}
}

// TestGraphSnapshotLegacyBigEndianGroupIdxKeyRoundTrips covers a real,
// already-persisted wire shape: this repository's Graph.MarshalJSON emitted
// bare Go-default field name "GroupIdx" (no json tag) before the T1 wire
// format introduced canonical snake_case "group_idx". A grouped snapshot
// already written under the old key must NOT be misjudged as a legacy
// (ungrouped) snapshot just because the canonical key is absent — that would
// silently drop real group membership and corrupt the durable unit topology
// for any such already-persisted snapshot. Only the presence of the field
// (under either key) matters, not which key spells it.
func TestGraphSnapshotLegacyBigEndianGroupIdxKeyRoundTrips(t *testing.T) {
	// Simulates a grouped snapshot ("ingest","analyze" in group 0, "store"
	// ungrouped) written with only the historical bare "GroupIdx" key.
	oldKeyJSON := `{
		"graph_hash": "sha256:oldkey",
		"name": "oldkey",
		"nodes": [
			{"Name":"ingest","Type":"test.ingest","GroupIdx":0},
			{"Name":"analyze","Type":"test.analyze","GroupIdx":0},
			{"Name":"store","Type":"test.store","GroupIdx":-1}
		],
		"index": {"ingest":0,"analyze":1,"store":2},
		"entry_indexes": {"ingest":0},
		"out_edges": [
			[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}],
			[{"SrcIdx":1,"DstIdx":2,"SrcPort":"main","DstPort":"main"}],
			[]
		],
		"in_edges": [
			[],
			[{"SrcIdx":0,"DstIdx":1,"SrcPort":"main","DstPort":"main"}],
			[{"SrcIdx":1,"DstIdx":2,"SrcPort":"main","DstPort":"main"}]
		],
		"in_degree": [0,1,1],
		"groups": [{"Name":"edge","Members":[0,1],"EntryIdx":0,"UnitIdx":0}]
	}`
	var g Graph
	if err := json.Unmarshal([]byte(oldKeyJSON), &g); err != nil {
		t.Fatalf("unmarshal old-key grouped snapshot: %v", err)
	}
	if g.UnitCount() != 2 {
		t.Fatalf("UnitCount() = %d, want 2 (grouped {ingest,analyze} + store); old GroupIdx key must not be treated as legacy-absent", g.UnitCount())
	}
	if got := g.Groups()[0].ActivationReplicas; got != 0 {
		t.Fatalf("legacy group ActivationReplicas = %d, want zero", got)
	}
	ingestIdx, _ := g.NodeIndex("ingest")
	storeIdx, _ := g.NodeIndex("store")
	if g.UnitIndexForNode(ingestIdx) == g.UnitIndexForNode(storeIdx) {
		t.Fatal("ingest (grouped) and store (ungrouped) must map to distinct units")
	}
}

func TestGraphSnapshotStandaloneActivationReplicasRoundTrip(t *testing.T) {
	def := groupedRoundTripDef()
	def.Groups = nil
	def.Nodes[0].ActivationReplicas = 4
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Graph
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	idx, ok := decoded.NodeIndex("ingest")
	if !ok {
		t.Fatal("ingest missing after round-trip")
	}
	if got := decoded.NodeAt(idx).ActivationReplicas; got != 4 {
		t.Fatalf("node ActivationReplicas after round-trip = %d, want 4", got)
	}
	if decoded.Hash() != g.Hash() {
		t.Fatalf("hash after round-trip = %q, want %q", decoded.Hash(), g.Hash())
	}
}

// TestGraphSnapshotConflictingGroupIdxKeysFailsClosed covers T1: a snapshot
// where a node carries both the canonical "group_idx" and the legacy
// "GroupIdx" keys with different values is untrusted and must fail closed
// rather than silently preferring one key over the other.
func TestGraphSnapshotConflictingGroupIdxKeysFailsClosed(t *testing.T) {
	conflictJSON := `{
		"graph_hash": "sha256:conflict",
		"name": "conflict",
		"nodes": [
			{"Name":"ingest","Type":"test.ingest","group_idx":0,"GroupIdx":1}
		],
		"index": {"ingest":0},
		"entry_indexes": {"ingest":0},
		"out_edges": [[]],
		"in_edges": [[]],
		"in_degree": [0],
		"groups": [{"Name":"a","Members":[0],"EntryIdx":0,"UnitIdx":0},{"Name":"b","Members":[],"EntryIdx":0,"UnitIdx":1}]
	}`
	var g Graph
	if err := json.Unmarshal([]byte(conflictJSON), &g); err == nil {
		t.Fatal("expected error for conflicting group_idx/GroupIdx keys, got nil")
	}
}
