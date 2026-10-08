package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// disabledDef builds the minimal two-node chain a->b with the flag on a.
func disabledDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "a", Type: "test.echo", Kind: types.NodeKindAction, Disabled: true},
			{Name: "b", Type: "test.echo", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
		},
	}
}

func TestAssignDisabledNodes_MarksAndSurvivesRoundTrip(t *testing.T) {
	g, err := Compile(disabledDef())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	for _, c := range []struct {
		idx      int
		disabled bool
	}{{0, true}, {1, false}, {-1, false}, {99, false}} {
		if got := g.NodeDisabled(c.idx); got != c.disabled {
			t.Fatalf("NodeDisabled(%d) = %v, want %v", c.idx, got, c.disabled)
		}
	}
	if !g.HasDisabledNodes() {
		t.Fatal("HasDisabledNodes() = false, want true")
	}
	if !g.NodeAt(0).Disabled {
		t.Fatal("NodeAt(0).Disabled = false, want true")
	}

	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back Graph
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// A snapshot that dropped the flag would silently run the node for real on
	// every cluster dispatch after a cache miss -- the exact failure the wire
	// field exists to prevent.
	if !back.HasDisabledNodes() || !back.NodeDisabled(0) || back.NodeDisabled(1) {
		t.Fatalf("round-tripped disabled state lost: has=%v n0=%v n1=%v",
			back.HasDisabledNodes(), back.NodeDisabled(0), back.NodeDisabled(1))
	}
}

func TestAssignDisabledNodes_HashAndWireOnlyForDisabledGraphs(t *testing.T) {
	plain := disabledDef()
	plain.Nodes[0].Disabled = false
	plainG, err := Compile(plain)
	if err != nil {
		t.Fatalf("Compile(plain): %v", err)
	}
	disabledG, err := Compile(disabledDef())
	if err != nil {
		t.Fatalf("Compile(disabled): %v", err)
	}
	if plainG.HasDisabledNodes() {
		t.Fatal("plain graph reports disabled nodes")
	}
	// The flag is part of the graph's identity: two definitions that differ
	// only in disabled must not share a graph hash.
	if plainG.Hash() == disabledG.Hash() {
		t.Fatal("disabled graph hashes like the same graph without the flag")
	}
	// omitempty is load-bearing: a graph without disabled nodes must not gain a
	// "disabled" key on the wire, or every persisted graph's hash would move.
	raw, err := json.Marshal(plainG)
	if err != nil {
		t.Fatalf("Marshal(plain): %v", err)
	}
	if strings.Contains(string(raw), "disabled") {
		t.Fatalf("plain graph wire carries a disabled key: %s", raw)
	}
	raw, err = json.Marshal(disabledG)
	if err != nil {
		t.Fatalf("Marshal(disabled): %v", err)
	}
	if !strings.Contains(string(raw), `"disabled":true`) {
		t.Fatalf("disabled graph wire lacks the flag: %s", raw)
	}
}

func TestAssignDisabledNodes_UnsupportedShapesAreRejected(t *testing.T) {
	cyclic := &types.WorkflowDef{
		Name:    "cycle",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start", Disabled: true},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	}

	faf := validFAFDef()
	faf.Nodes[0].Disabled = true

	trigger := &types.WorkflowDef{
		Name:  "t",
		Nodes: []types.NodeDef{{Name: "in", Type: "xflow.timer", Kind: types.NodeKindTrigger, Disabled: true}},
	}

	supply := supplyDef()
	supply.Nodes[1].Disabled = true // rules (Kind supply)

	group := mkGroupDef([]string{"ingest", "analyze"})
	for i := range group.Nodes {
		if group.Nodes[i].Name == "analyze" {
			group.Nodes[i].Disabled = true
		}
	}

	body := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
		mapNode(map[string]any{"items": "$input.rows", "body": map[string]any{
			"type": "xflow.subgraph",
			"parameters": map[string]any{
				"nodes": []any{
					map[string]any{"name": "inner", "type": "xflow.noop", "disabled": true},
				},
			},
		}}),
	}}

	cases := []struct {
		name    string
		def     *types.WorkflowDef
		wantErr string
	}{
		{"allow_cycles", cyclic, "allow_cycles"},
		{"faf", faf, "faf"},
		{"trigger node", trigger, "trigger nodes"},
		{"supply node", supply, "supply nodes"},
		{"group member", group, "group members"},
		{"body member", body, "body member"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Every unsupported shape must fail the compile: silently ignoring
			// the flag would run the node for real, which is the definition of
			// the gap this pass closes.
			_, err := Compile(tc.def)
			if err == nil {
				t.Fatalf("Compile accepted disabled on %s, want explicit rejection", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q lacks %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestCompileProjectedPackage_RejectsDisabledNode(t *testing.T) {
	pkg := &SubgraphPackage{
		Def: &types.WorkflowDef{
			Name: "p",
			Nodes: []types.NodeDef{
				{Name: "a", Type: ReservedNodeTypePrefix + "group_exit", Disabled: true},
			},
		},
	}
	_, err := CompileProjectedPackage(pkg)
	if err == nil || !strings.Contains(err.Error(), "disabled is not supported in projected packages") {
		t.Fatalf("CompileProjectedPackage = %v, want projected-package disabled rejection", err)
	}
}
