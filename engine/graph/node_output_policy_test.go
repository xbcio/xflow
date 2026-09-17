package graph

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

func outputPolicyProbeDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "output-policy",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name:   "worker",
				Type:   "test.worker",
				Output: &types.NodeOutputPolicy{Private: true},
			},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
		},
	}
}

func TestCompileCarriesNodeOutputPolicyAndNodeAtCopies(t *testing.T) {
	def := outputPolicyProbeDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	worker, ok := g.NodeIndex("worker")
	if !ok {
		t.Fatal("worker node not found")
	}

	// Compile must snapshot the definition's policy rather than retaining a
	// caller-owned pointer.
	def.Nodes[1].Output.Private = false
	if output := g.NodeAt(worker).Output; output == nil || !output.Private {
		t.Fatalf("compiled Output = %+v, want independent private policy", output)
	}

	// NodeAt must not leak the graph's mutable policy pointer.
	first := g.NodeAt(worker)
	first.Output.Private = false
	if output := g.NodeAt(worker).Output; output == nil || !output.Private {
		t.Fatalf("NodeAt leaked Output mutation: %+v", output)
	}
}

func TestSnapshotRoundTripPreservesNodeOutputPolicy(t *testing.T) {
	g, err := Compile(outputPolicyProbeDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round Graph
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	worker, ok := round.NodeIndex("worker")
	if !ok {
		t.Fatal("round-tripped worker node not found")
	}
	if output := round.NodeAt(worker).Output; output == nil || !output.Private {
		t.Fatalf("round-tripped Output = %+v, want private policy", output)
	}
}

func TestGroupPackageAndTrustedCompilePreserveNodeOutputPolicy(t *testing.T) {
	def := makeGroupedDef()
	def.Nodes[1].Output = &types.NodeOutputPolicy{Private: true} // B
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("project package: %v", err)
	}
	var projected *types.NodeDef
	for i := range pkg.Def.Nodes {
		if pkg.Def.Nodes[i].Name == "B" {
			projected = &pkg.Def.Nodes[i]
			break
		}
	}
	if projected == nil || projected.Output == nil || !projected.Output.Private {
		t.Fatalf("projected B Output = %+v, want private policy", projected)
	}

	trusted, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("trusted compile: %v", err)
	}
	idx, ok := trusted.NodeIndex("B")
	if !ok {
		t.Fatal("trusted graph has no B node")
	}
	if output := trusted.NodeAt(idx).Output; output == nil || !output.Private {
		t.Fatalf("trusted graph B Output = %+v, want private policy", output)
	}
}

func TestMapBodyJSONAndTrustedCompilePreserveNodeOutputPolicy(t *testing.T) {
	body := map[string]any{
		"type": types.SubgraphNodeType,
		"parameters": map[string]any{
			"nodes": []types.NodeDef{{
				Name:   "inner",
				Type:   "test.inner",
				Output: &types.NodeOutputPolicy{Private: true},
			}},
		},
	}
	// This is the same JSON boundary used by DSL input and builder body
	// assembly: nested NodeDefs become map[string]any before graph compilation.
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	var jsonBody map[string]any
	if err := json.Unmarshal(data, &jsonBody); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	g, err := Compile(&types.WorkflowDef{
		Name: "map-body-output-policy",
		Nodes: []types.NodeDef{{
			Name: "m",
			Type: "xflow.map",
			Parameters: map[string]any{
				"items": "$input.rows",
				"body":  jsonBody,
			},
		}},
	})
	if err != nil {
		t.Fatalf("compile map body: %v", err)
	}
	mapIdx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("map node not found")
	}
	bodyPackage := g.BodyAt(mapIdx)
	if bodyPackage == nil || bodyPackage.Package == nil {
		t.Fatal("compiled map body package missing")
	}
	var inner *types.NodeDef
	for i := range bodyPackage.Package.Def.Nodes {
		if bodyPackage.Package.Def.Nodes[i].Name == "inner" {
			inner = &bodyPackage.Package.Def.Nodes[i]
			break
		}
	}
	if inner == nil || inner.Output == nil || !inner.Output.Private {
		t.Fatalf("projected map body Output = %+v, want private policy", inner)
	}

	trusted, err := CompileProjectedPackage(bodyPackage.Package)
	if err != nil {
		t.Fatalf("trusted compile map body: %v", err)
	}
	innerIdx, ok := trusted.NodeIndex("inner")
	if !ok {
		t.Fatal("trusted map body graph has no inner node")
	}
	if output := trusted.NodeAt(innerIdx).Output; output == nil || !output.Private {
		t.Fatalf("trusted map body Output = %+v, want private policy", output)
	}
}

func TestNilOutputPolicyKeepsLegacyGraphHashAndSnapshot(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "t",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "slow", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "slow"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	const wantHash = "sha256:9a205c7fa88aa3b46788936f5aae62d1e526dae04e26344bfd3a68a9bd9dedab"
	if got := g.Hash(); got != wantHash {
		t.Fatalf("hash = %q, want %q; nil Output must not change legacy graph hashes", got, wantHash)
	}

	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire struct {
		Nodes []map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode wire: %v", err)
	}
	for _, node := range wire.Nodes {
		if _, ok := node["output"]; ok {
			t.Fatalf("nil Output must be omitted from legacy snapshot node: %s", node["output"])
		}
	}
}
