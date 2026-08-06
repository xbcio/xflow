package control

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// supplyGatedDef builds a workflow whose kafka trigger feeds a script node that
// depends on two supplies with different readiness policies.
func supplyGatedDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "clean", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"code":     "$supplies.rules.threshold",
					"language": "wasm",
					"runtime":  "wazero-reactor",
				}},
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{"resource": "shared-rules"}},
			{Name: "hints", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{"require_ready": false}},
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

func TestDeriveEntryActivationsCarriesSupplies(t *testing.T) {
	g, err := graph.Compile(supplyGatedDef())
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
		// resource falls back to the node name for "hints"; require_ready
		// defaults to true for "rules" and is explicitly false for "hints".
		{Node: "hints", Resource: "hints", RequireReady: false},
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}
	if !reflect.DeepEqual(units[0].Supplies, want) {
		t.Fatalf("supplies = %+v, want %+v", units[0].Supplies, want)
	}
}

// The requirement set must reach the durable record, or the reconciler has
// nothing to put on the directive.
func TestAddOrUpdateWorkflowPersistsSupplies(t *testing.T) {
	st := NewMemoryEntryActivationStore()
	m := NewEntryActivationManager(st)
	g, err := graph.Compile(supplyGatedDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	if err := m.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	got, ok, err := st.Get(ctx, engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "src",
	})
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if len(got.Supplies) != 2 {
		t.Fatalf("persisted supplies = %+v", got.Supplies)
	}
}

// A workflow with no supply node must produce a nil slice, not an empty one:
// omitempty on the wire depends on it, and a non-nil empty slice would change
// existing directives' JSON.
func TestNoSupplyMeansNilSlice(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf-nosupply",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "clean", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"code":     "plain code no supplies",
					"language": "js",
					"runtime":  "goja",
				}},
		},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "clean", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if units[0].Supplies != nil {
		t.Fatalf("supplies = %#v, want nil", units[0].Supplies)
	}
}

// A supply reached only through a node in a DIFFERENT entry unit must not be
// attributed to this one: the gate would then block on content this entry never
// uses.
func TestSuppliesScopedToTheEntryUnit(t *testing.T) {
	// Build two independent defs each with their own trigger and supplies.
	// A single def with two independent triggers may trip an existing multi-entry
	// validation in graph.Compile, so we compile separately.

	// Def 1: trigger "src" depends on "rules" and "hints"
	def1 := supplyGatedDef()
	g1, err := graph.Compile(def1)
	if err != nil {
		t.Fatalf("compile def1: %v", err)
	}
	units1, err := DeriveEntryActivations(g1)
	if err != nil {
		t.Fatalf("derive def1: %v", err)
	}
	if len(units1) != 1 {
		t.Fatalf("def1 units = %d, want 1", len(units1))
	}
	for _, s := range units1[0].Supplies {
		if s.Node == "other" {
			t.Fatalf("def1 entry unit src must not require supply %q from the other branch", s.Node)
		}
	}

	// Def 2: trigger "src2" depends on "other" only
	def2 := &types.WorkflowDef{
		Name: "wf2",
		Nodes: []types.NodeDef{
			{Name: "src2", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "clean2", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{"code": "x", "language": "js", "runtime": "goja"}},
			{Name: "other", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
		},
		Connections: map[string]map[string]types.PortConnections{
			"src2": {"main": types.PortConnections{Targets: []types.Connection{{Node: "clean2", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: "clean2", Supply: "other"},
		},
	}
	g2, err := graph.Compile(def2)
	if err != nil {
		t.Fatalf("compile def2: %v", err)
	}
	units2, err := DeriveEntryActivations(g2)
	if err != nil {
		t.Fatalf("derive def2: %v", err)
	}
	if len(units2) != 1 {
		t.Fatalf("def2 units = %d, want 1", len(units2))
	}
	for _, s := range units2[0].Supplies {
		if s.Node != "other" {
			t.Fatalf("def2 entry unit src2 requires unexpected supply %q", s.Node)
		}
	}
}

// RemoveWorkflow must preserve Supplies on the record when marking it non-desired.
func TestRemoveWorkflowPreservesSupplies(t *testing.T) {
	st := NewMemoryEntryActivationStore()
	m := NewEntryActivationManager(st)
	g, err := graph.Compile(supplyGatedDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	if err := m.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	if err := m.RemoveWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	got, ok, err := st.Get(ctx, engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "src",
	})
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.Desired {
		t.Fatal("expected Desired=false after RemoveWorkflow")
	}
	if len(got.Supplies) != 2 {
		t.Fatalf("RemoveWorkflow wiped Supplies: got %+v", got.Supplies)
	}
}
