package control

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// supplyPullDef builds a workflow whose trigger reaches one pull supply and one
// store-backed supply, so the derivation is exercised with both shapes at once:
// the pull requirement must carry its parsed source and the store-backed one
// must not grow a pull_source key.
func supplyPullDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf-pull",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "clean", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"code":     "$supplies.rules.threshold",
					"language": "wasm",
					"runtime":  "wazero-reactor",
				}},
			{Name: "rules", Type: types.SupplyPullNodeType, Kind: types.NodeKindSupply,
				Parameters: map[string]any{
					types.SupplyPullParamURL:            "https://config.example.com/rules.json",
					types.SupplyPullParamAuthentication: "rules_token",
					types.SupplyPullParamPollInterval:   "30s",
				}},
			{Name: "hints", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{types.SupplyParamResource: "shared-hints"}},
		},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "clean", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: "clean", Supply: "rules"},
			{Node: "clean", Supply: "hints"},
		},
	}
}

// TestDeriveEntryActivationsCarriesPullSource pins the derived requirement: the
// runner gets the fetch plan from the directive, not by looking the definition
// up again, and the store-backed sibling stays byte-identical to what previous
// versions emitted.
func TestDeriveEntryActivationsCarriesPullSource(t *testing.T) {
	g, err := graph.Compile(supplyPullDef())
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
	want := []engine.SupplyRequirement{
		{Node: "hints", Resource: "shared-hints", RequireReady: true},
		{
			Node: "rules", Resource: "rules", RequireReady: true,
			PullSource: &types.SupplyPullSource{
				URL:            "https://config.example.com/rules.json",
				Authentication: "rules_token",
				PollInterval:   30 * time.Second,
			},
		},
	}
	if !reflect.DeepEqual(units[0].Supplies, want) {
		t.Fatalf("supplies = %+v, want %+v", units[0].Supplies, want)
	}
}

// TestSuppliesFailClosedOnMalformedPullSource drives the derivation with a
// graph the compiler would have rejected, which is the shape a snapshot from an
// older or foreign control plane has. The derivation must refuse, not fall back
// to a store-backed requirement: with require_ready false, a silent fallback
// would have the runner serve empty content as if the source said so.
//
// The tamper is done on a serialized graph because a graph that cannot be
// compiled cannot be built any other way from outside the package.
func TestSuppliesFailClosedOnMalformedPullSource(t *testing.T) {
	g, err := graph.Compile(supplyPullDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Control: the untampered snapshot decodes and derives cleanly, so the
	// failure below is the tampered URL and not the decode round trip.
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	var clean graph.Graph
	if err := json.Unmarshal(raw, &clean); err != nil {
		t.Fatalf("decode clean snapshot: %v", err)
	}
	if _, err := DeriveEntryActivations(&clean); err != nil {
		t.Fatalf("clean snapshot: derive: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	tampered := false
	for _, n := range doc["nodes"].([]any) {
		m := n.(map[string]any)
		if m["Name"] != "rules" {
			continue
		}
		m["Parameters"].(map[string]any)[types.SupplyPullParamURL] = "http://config.example.com/rules.json"
		tampered = true
	}
	if !tampered {
		t.Fatal("did not find the pull node in the snapshot")
	}
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var bad graph.Graph
	if err := json.Unmarshal(raw, &bad); err != nil {
		t.Fatalf("decode tampered snapshot: %v", err)
	}

	_, err = DeriveEntryActivations(&bad)
	if err == nil {
		t.Fatal("a malformed pull source derived without error; the runner would have " +
			"been told to fetch from the store and, with require_ready false, would " +
			"have served empty content")
	}
	if !strings.Contains(err.Error(), `"rules"`) || !strings.Contains(err.Error(), "https") {
		t.Errorf("error %q does not both name the supply node and explain the rule", err)
	}
}
