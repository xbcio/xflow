package control

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// mapBodyConsumerDef builds the SAS cross-env collection topology: a kafka
// trigger feeding a map node whose BODY holds the wasm script, with the supply
// dependency edge on the map node (a body-local edge is dropped by
// decodeSubgraphMembers, so the outer node is where it must live).
//
// The digest is the same shape as taggerDigest but a distinct value, so a
// binding derived from this definition cannot be confused with one derived from
// wasmConsumerDef.
const bodyDigest = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

func mapBodyConsumerDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "collect", Type: "xflow.map", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"items": "$input.messages",
					"body": map[string]any{
						"type": "xflow.subgraph",
						"parameters": map[string]any{
							"nodes": []any{
								map[string]any{
									"name": "decode",
									"type": "xflow.script",
									"parameters": map[string]any{
										"language":        "wasm",
										"runtime":         "wazero-reactor",
										"artifact_digest": bodyDigest,
									},
								},
							},
						},
					},
				}},
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{"resource": "shared-rules"}},
		},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "collect", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "collect", Supply: "rules"}},
	}
}

// TestSupplyConsumerBindingsReachMapBodyScript pins whether a wasm module that
// lives inside a map BODY is bound to the supply its parent map node depends on.
//
// This matters because the binding is what makes the runner register the module
// as a supply consumer at activation time, which is in turn what compiles it and
// configures its instance pool. Without a binding the module stays on the legacy
// globals path with no rules at all: every record passes through unfiltered,
// with no error anywhere. walkEntryUnitNodes walks the compiled Graph's flow
// edges, and a body member is not a Graph node — so this test is the question
// "does the derivation see through a body?" asked directly.
func TestSupplyConsumerBindingsReachMapBodyScript(t *testing.T) {
	g, err := graph.Compile(mapBodyConsumerDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1 (the trigger)", len(units))
	}
	t.Logf("SupplyConsumers = %+v", units[0].SupplyConsumers)
	t.Logf("Supplies = %+v", units[0].Supplies)
	if len(units[0].SupplyConsumers) == 0 {
		t.Fatalf("no supply consumer binding derived for the map body's wasm script: "+
			"the module would run with no rules. Supplies = %+v", units[0].Supplies)
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "collect", NodeName: "decode", SupplyNode: "rules", DigestExpr: bodyDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want %+v", units[0].SupplyConsumers, want)
	}
}

// TestMapBodyMembersInheritEveryParentSupply pins the semantics a body member
// gets, which is NOT the one an author reading the topology would assume: a
// body member has no dependency edges of its own, so it inherits ALL of its
// parent map node's supplies — every member is bound to every supply.
//
// For a wasm module that is an overwrite, not a merge: swapConfig replaces the
// whole config with the notifying supply's content. Two members that each need
// their OWN rule set must therefore share ONE supply whose content carries both
// sections, with each guest decoding only the keys it knows. Two separate
// supplies would have each guest reconfigured by the other's content, and a
// config whose keys a guest does not recognise decodes into an EMPTY rule set
// that configures successfully and then discards nothing.
func TestMapBodyMembersInheritEveryParentSupply(t *testing.T) {
	const secondDigest = "sha256:0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
	def := mapBodyConsumerDef()
	body := def.Nodes[1].Parameters["body"].(map[string]any)
	params := body["parameters"].(map[string]any)
	params["nodes"] = append(params["nodes"].([]any), map[string]any{
		"name": "clean",
		"type": "xflow.script",
		"parameters": map[string]any{
			"language":        "wasm",
			"runtime":         "wazero-reactor",
			"artifact_digest": secondDigest,
		},
	})
	params["connections"] = map[string]any{
		"decode": map[string]any{"main": []any{map[string]any{"node": "clean", "input": "main"}}},
	}
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name: "hints", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
		Parameters: map[string]any{"resource": "shared-hints"},
	})
	def.DependencyEdges = append(def.DependencyEdges, types.DependencyEdge{Node: "collect", Supply: "hints"})

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "collect", NodeName: "clean", SupplyNode: "hints", DigestExpr: secondDigest},
		{WorkflowName: "collect", NodeName: "clean", SupplyNode: "rules", DigestExpr: secondDigest},
		{WorkflowName: "collect", NodeName: "decode", SupplyNode: "hints", DigestExpr: bodyDigest},
		{WorkflowName: "collect", NodeName: "decode", SupplyNode: "rules", DigestExpr: bodyDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want the full cross product %+v",
			units[0].SupplyConsumers, want)
	}
}
