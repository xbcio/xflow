package control

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// groupTriggerGraph compiles a workflow with a co-location group whose entry is
// a trigger node and which carries a RunnerSelector — i.e. a remote-hosted
// group entry unit. The group members are http.request nodes so the projected
// package requirements are non-empty and predictable.
func groupTriggerGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "grp-trigger",
		Nodes: []types.NodeDef{
			{Name: "trig", Type: "http.request", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "worker", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
			{Name: "down", Type: "db.query", Version: 1, Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{
			{
				Name:           "grp1",
				Members:        []string{"trig", "worker"},
				RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
			},
		},
		Connections: types.Connections{
			"trig":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
			"worker": {"main": types.PortConnections{Targets: []types.Connection{{Node: "down", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestDeriveEntryActivations_GroupRequirementsIncludeGroupExec asserts a fresh
// group entry unit derives NON-EMPTY requirements that include the group-exec
// capability. This locks the fail-closed contract: a group activation must
// carry the requirements that the reconciler enforces against runner
// capabilities.
func TestDeriveEntryActivations_GroupRequirementsIncludeGroupExec(t *testing.T) {
	g := groupTriggerGraph(t)

	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("DeriveEntryActivations: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("expected 1 group entry unit, got %d: %+v", len(units), units)
	}
	eu := units[0]
	if eu.EntryUnitID != "grp1" {
		t.Fatalf("EntryUnitID = %q, want grp1", eu.EntryUnitID)
	}
	if len(eu.Requirements) == 0 {
		t.Fatal("fresh group activation must carry non-empty Requirements (fail-closed)")
	}
	var hasGroupExec bool
	for _, r := range eu.Requirements {
		if r.Feature == engine.FeatureGroupExecV1 {
			hasGroupExec = true
		}
	}
	if !hasGroupExec {
		t.Fatalf("group requirements must include the %q feature, got %+v", engine.FeatureGroupExecV1, eu.Requirements)
	}
}

// TestDeriveEntryActivations_ProjectionFailurePropagates asserts that when the
// group package projection fails, the error propagates instead of being
// swallowed into an empty-requirements (selector-only) activation.
func TestDeriveEntryActivations_ProjectionFailurePropagates(t *testing.T) {
	g := groupTriggerGraph(t)

	sentinel := errors.New("boom: cannot project package")
	orig := projectGroupPackage
	projectGroupPackage = func(*graph.Graph, int) (*graph.GroupPackage, string, error) {
		return nil, "", sentinel
	}
	defer func() { projectGroupPackage = orig }()

	units, err := DeriveEntryActivations(g)
	if err == nil {
		t.Fatalf("expected projection error to propagate, got units=%+v", units)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error does not wrap projection failure: %v", err)
	}
	if units != nil {
		t.Fatalf("no units must be returned on projection failure, got %+v", units)
	}
}

// TestAddOrUpdateWorkflow_ProjectionFailureStoresNothing asserts the fail-closed
// contract end-to-end: a projection failure surfaces from AddOrUpdateWorkflow
// and NO selector-only (empty-requirements) activation is persisted.
func TestAddOrUpdateWorkflow_ProjectionFailureStoresNothing(t *testing.T) {
	ctx := context.Background()
	g := groupTriggerGraph(t)
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	sentinel := errors.New("boom: cannot project package")
	orig := projectGroupPackage
	projectGroupPackage = func(*graph.Graph, int) (*graph.GroupPackage, string, error) {
		return nil, "", sentinel
	}
	defer func() { projectGroupPackage = orig }()

	err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", g)
	if err == nil {
		t.Fatal("AddOrUpdateWorkflow must fail when derivation fails")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error does not wrap projection failure: %v", err)
	}
	// No activation may have been stored (fail-closed: never a selector-only record).
	list, lerr := store.List(ctx, namespace.Default)
	if lerr != nil {
		t.Fatalf("List: %v", lerr)
	}
	if len(list) != 0 {
		t.Fatalf("no activation must be stored on failure, got %+v", list)
	}
}

// TestAddOrUpdateWorkflow_GroupPersistsRequirements asserts the happy path
// stores the derived group requirements (non-empty, includes group-exec).
func TestAddOrUpdateWorkflow_GroupPersistsRequirements(t *testing.T) {
	ctx := context.Background()
	g := groupTriggerGraph(t)
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	got, ok, err := store.Get(ctx, engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-x",
		WorkflowVersion: "v1",
		EntryUnitID:     "grp1",
	})
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if len(got.Requirements) == 0 {
		t.Fatal("stored group activation must carry non-empty Requirements")
	}
}
