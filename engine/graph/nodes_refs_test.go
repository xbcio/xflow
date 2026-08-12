package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Compile-time $nodes reference validation
// ---------------------------------------------------------------------------

// TestNodesRef_UnknownName asserts that referencing a node name not present in
// the workflow definition is a compile-time error.
func TestNodesRef_UnknownName(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "unknown-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop", Parameters: map[string]any{
				"url": "${{ $nodes['ghost'].value }}",
			}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile error for unknown $nodes reference, got nil")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should mention the unknown node name \"ghost\", got: %s", err.Error())
	}
}

// TestNodesRef_ForwardReference asserts that referencing a downstream node
// (forward reference) is a compile-time error.
func TestNodesRef_ForwardReference(t *testing.T) {
	// A -> B -> C; B references $nodes['C'] which is downstream.
	def := &types.WorkflowDef{
		Name: "forward-ref",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop", Parameters: map[string]any{
				"url": "${{ $nodes['C'].result }}",
			}},
			{Name: "C", Type: "test.noop"},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "C", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile error for forward $nodes reference, got nil")
	}
	if !strings.Contains(err.Error(), "forward") {
		t.Fatalf("error should mention \"forward\", got: %s", err.Error())
	}
}

// TestNodesRef_CrossBranchWarning asserts that referencing a node on a
// different branch (not a deterministic ancestor) produces a compile warning
// advising use of ??.
func TestNodesRef_CrossBranchWarning(t *testing.T) {
	// start -> router(switch) -> branch_a, branch_b
	// branch_b references $nodes['branch_a'] -- cross-branch, not deterministic ancestor.
	def := &types.WorkflowDef{
		Name: "cross-branch",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.noop"},
			{Name: "router", Type: "xflow.switch", Parameters: map[string]any{
				"mode": "rules",
				"rules": []any{
					map[string]any{"condition": "true", "output": "left"},
					map[string]any{"condition": "false", "output": "right"},
				},
				"default_output": "right",
			}},
			{Name: "branch_a", Type: "test.noop"},
			{Name: "branch_b", Type: "test.noop", Parameters: map[string]any{
				"msg": "${{ $nodes['branch_a'].result }}",
			}},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "router", Input: "main"}}}},
			"router": {
				"left":  {Targets: []types.Connection{{Node: "branch_a", Input: "main"}}},
				"right": {Targets: []types.Connection{{Node: "branch_b", Input: "main"}}},
			},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("expected successful compile with cross-branch warning, got error: %v", err)
	}
	warnings := g.Warnings()
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "branch_a") && strings.Contains(w, "??") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warning mentioning \"branch_a\" and \"??\", got warnings: %v", warnings)
	}
}

// TestNodesRef_DeterministicAncestor asserts that referencing a deterministic
// ancestor (all paths to the referencing node pass through the referenced one)
// produces zero warnings. This is the positive control for TestNodesRef_CrossBranchWarning.
func TestNodesRef_DeterministicAncestor(t *testing.T) {
	// A -> B -> C; C references $nodes['A'] -- A is a deterministic ancestor.
	def := &types.WorkflowDef{
		Name: "ancestor",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "C", Type: "test.noop", Parameters: map[string]any{
				"url": "${{ $nodes['A'].value }}",
			}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "C", Input: "main"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("expected successful compile for deterministic ancestor ref, got error: %v", err)
	}
	if len(g.Warnings()) != 0 {
		t.Fatalf("deterministic ancestor reference should produce zero warnings, got: %v", g.Warnings())
	}
}

// TestNodesRef_MapBodyDoesNotLeakToOuter asserts that $nodes references inside
// a map node's body sub-graph are NOT attributed to the outer map node. This
// prevents a legitimate workflow (body members referencing each other) from
// being falsely rejected because the outer graph has no node with that name.
//
// The test also includes a positive control: the map node's own "items"
// parameter (not body) references $nodes['S'], which MUST appear in the outer
// node's refs.
func TestNodesRef_MapBodyDoesNotLeakToOuter(t *testing.T) {
	// Outer: S -> M -> T
	// M is xflow.map with a body containing innerA -> innerB,
	// where innerB references $nodes['innerA'].
	// M's own "items" param references $nodes['S'].list.
	def := &types.WorkflowDef{
		Name: "body-leak",
		Nodes: []types.NodeDef{
			{Name: "S", Type: "test.noop"},
			{Name: "M", Type: "xflow.map", Parameters: map[string]any{
				"items": "${{ $nodes['S'].list }}",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "innerA", "type": "test.noop"},
							map[string]any{
								"name": "innerB", "type": "test.noop",
								"parameters": map[string]any{
									"data": "${{ $nodes['innerA'].result }}",
								},
							},
						},
						"connections": map[string]any{
							"innerA": map[string]any{
								"main": map[string]any{
									"targets": []any{
										map[string]any{"node": "innerB", "input": "main"},
									},
								},
							},
						},
					},
				},
			}},
			{Name: "T", Type: "test.noop"},
		},
		Connections: types.Connections{
			"S": {"main": {Targets: []types.Connection{{Node: "M", Input: "main"}}}},
			"M": {"main": {Targets: []types.Connection{{Node: "T", Input: "main"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("expected successful compile (body refs should not leak), got error: %v", err)
	}
	mIdx, ok := g.NodeIndex("M")
	if !ok {
		t.Fatal("node M not found in compiled graph")
	}
	refs := g.NodesRefsFor(mIdx)

	// Positive control: M's "items" uses $nodes['S'], so S must be in refs.
	foundS := false
	for _, r := range refs {
		if r == "S" {
			foundS = true
		}
		if r == "innerA" {
			t.Fatal("NodesRefsFor(M) must NOT contain \"innerA\" -- that ref lives " +
				"inside the body sub-graph and should not leak to the outer node")
		}
	}
	if !foundS {
		t.Fatalf("NodesRefsFor(M) should contain \"S\" (from items param), got: %v", refs)
	}
}

// TestNodesRef_RoundTrip asserts that NodesRefsFor survives a JSON
// Marshal/Unmarshal cycle -- proving the wire form carries the field.
func TestNodesRef_RoundTrip(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "roundtrip",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop", Parameters: map[string]any{
				"url": "${{ $nodes['A'].value }}",
			}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	bIdx, _ := g.NodeIndex("B")
	before := g.NodesRefsFor(bIdx)
	if len(before) == 0 {
		t.Fatal("NodesRefsFor(B) is empty before round-trip")
	}

	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var g2 Graph
	if err := json.Unmarshal(data, &g2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	bIdx2, ok := g2.NodeIndex("B")
	if !ok {
		t.Fatal("node B not found after unmarshal")
	}
	after := g2.NodesRefsFor(bIdx2)
	if len(after) != len(before) {
		t.Fatalf("NodesRefsFor(B) after round-trip = %v, want %v", after, before)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("NodesRefsFor(B)[%d] after round-trip = %q, want %q", i, after[i], before[i])
		}
	}
}
