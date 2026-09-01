package control

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

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
	return groupTriggerGraphWithReplicas(t, 0)
}

func groupTriggerGraphWithReplicas(t *testing.T, replicas uint32) *graph.Graph {
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
				Name:               "grp1",
				Members:            []string{"trig", "worker"},
				RunnerSelector:     &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
				ActivationReplicas: replicas,
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

func workflowActivations(t *testing.T, store engine.EntryActivationStore, workflowID types.WorkflowID, workflowVersion string) []engine.EntryActivation {
	t.Helper()
	acts, err := store.List(context.Background(), namespace.Default)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	filtered := acts[:0]
	for _, act := range acts {
		if act.WorkflowID == workflowID && act.WorkflowVersion == workflowVersion {
			filtered = append(filtered, act)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ReplicaIndex < filtered[j].ReplicaIndex
	})
	return filtered
}

func hasRequirementFeature(requirements []engine.CapabilityRequirement, feature string) bool {
	for _, requirement := range requirements {
		if requirement.Feature == feature {
			return true
		}
	}
	return false
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
	projectGroupPackage = func(*graph.Graph, int) (*graph.SubgraphPackage, string, error) {
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
	projectGroupPackage = func(*graph.Graph, int) (*graph.SubgraphPackage, string, error) {
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

func TestAddOrUpdateWorkflow_CreatesReplicaSiblingsWithCapabilityGate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", groupTriggerGraphWithReplicas(t, 3)); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}

	acts := workflowActivations(t, store, "wf-x", "v1")
	if len(acts) != 3 {
		t.Fatalf("activation count = %d, want 3: %+v", len(acts), acts)
	}
	for replica, act := range acts {
		if act.ReplicaIndex != uint32(replica) {
			t.Fatalf("activation[%d].ReplicaIndex = %d, want %d", replica, act.ReplicaIndex, replica)
		}
		if !act.Desired {
			t.Fatalf("activation[%d].Desired = false, want true", replica)
		}
		hasReplicaFeature := hasRequirementFeature(act.Requirements, engine.FeatureEntryActivationReplicaV1)
		if replica == 0 && hasReplicaFeature {
			t.Fatalf("replica zero must remain eligible for legacy runners: %+v", act.Requirements)
		}
		if replica > 0 && !hasReplicaFeature {
			t.Fatalf("replica %d requirements lack %q: %+v", replica, engine.FeatureEntryActivationReplicaV1, act.Requirements)
		}
	}
}

func TestAddOrUpdateWorkflow_ScaleDownPreservesOwnersForDeactivate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", groupTriggerGraphWithReplicas(t, 3)); err != nil {
		t.Fatalf("create replicas: %v", err)
	}
	for replica := uint32(0); replica < 3; replica++ {
		key := engine.EntryActivationKey{
			Namespace:       namespace.Default,
			WorkflowID:      "wf-x",
			WorkflowVersion: "v1",
			EntryUnitID:     "grp1",
			ReplicaIndex:    replica,
		}
		ok, err := store.Assign(ctx, key, "runner-"+string(rune('a'+replica)), "session", uint64(replica+1), time.Now().Add(time.Minute))
		if err != nil || !ok {
			t.Fatalf("Assign replica %d: ok=%v err=%v", replica, ok, err)
		}
	}

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", groupTriggerGraphWithReplicas(t, 1)); err != nil {
		t.Fatalf("scale down: %v", err)
	}

	acts := workflowActivations(t, store, "wf-x", "v1")
	if len(acts) != 3 {
		t.Fatalf("activation count after scale-down = %d, want historical 3: %+v", len(acts), acts)
	}
	for replica, act := range acts {
		wantDesired := replica == 0
		if act.Desired != wantDesired {
			t.Fatalf("replica %d Desired = %v, want %v", replica, act.Desired, wantDesired)
		}
		if act.RunnerID == "" || act.SessionID == "" {
			t.Fatalf("replica %d owner was cleared before reconciler deactivate: %+v", replica, act)
		}
	}
}

func TestAddOrUpdateWorkflow_ScaleUpPreservesExistingAssignment(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", groupTriggerGraphWithReplicas(t, 1)); err != nil {
		t.Fatalf("create initial activation: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-x",
		WorkflowVersion: "v1",
		EntryUnitID:     "grp1",
	}
	deadline := time.Now().Add(time.Minute).Round(time.Millisecond)
	ok, err := store.Assign(ctx, key, "runner-a", "session-a", 7, deadline)
	if err != nil || !ok {
		t.Fatalf("Assign initial activation: ok=%v err=%v", ok, err)
	}

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", groupTriggerGraphWithReplicas(t, 3)); err != nil {
		t.Fatalf("scale up: %v", err)
	}

	acts := workflowActivations(t, store, "wf-x", "v1")
	if len(acts) != 3 {
		t.Fatalf("activation count after scale-up = %d, want 3: %+v", len(acts), acts)
	}
	if acts[0].RunnerID != "runner-a" || acts[0].SessionID != "session-a" || acts[0].Generation != 7 || !acts[0].LeaseDeadline.Equal(deadline) {
		t.Fatalf("replica zero assignment was overwritten: %+v", acts[0])
	}
	for replica := 1; replica < 3; replica++ {
		if acts[replica].RunnerID != "" || acts[replica].SessionID != "" || acts[replica].Generation != 0 {
			t.Fatalf("new replica %d unexpectedly inherited an assignment: %+v", replica, acts[replica])
		}
	}
}

func TestRemoveWorkflow_MarksEverySiblingWithoutCrossVersionLeak(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	for _, tc := range []struct {
		workflowID types.WorkflowID
		version    string
		replicas   uint32
	}{
		{workflowID: "wf-x", version: "v1", replicas: 3},
		{workflowID: "wf-x", version: "v2", replicas: 2},
		{workflowID: "wf-y", version: "v1", replicas: 2},
	} {
		if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, tc.workflowID, tc.version, groupTriggerGraphWithReplicas(t, tc.replicas)); err != nil {
			t.Fatalf("create %s/%s: %v", tc.workflowID, tc.version, err)
		}
	}

	// RemoveWorkflow must not need the graph: durable records are the complete
	// source of truth for historical siblings, including replicas removed by a
	// prior scale-down.
	if err := mgr.RemoveWorkflow(ctx, namespace.Default, "wf-x", "v1", nil); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}

	for _, tc := range []struct {
		workflowID  types.WorkflowID
		version     string
		wantCount   int
		wantDesired bool
	}{
		{workflowID: "wf-x", version: "v1", wantCount: 3, wantDesired: false},
		{workflowID: "wf-x", version: "v2", wantCount: 2, wantDesired: true},
		{workflowID: "wf-y", version: "v1", wantCount: 2, wantDesired: true},
	} {
		acts := workflowActivations(t, store, tc.workflowID, tc.version)
		if len(acts) != tc.wantCount {
			t.Fatalf("%s/%s activation count = %d, want %d", tc.workflowID, tc.version, len(acts), tc.wantCount)
		}
		for _, act := range acts {
			if act.Desired != tc.wantDesired {
				t.Fatalf("%s/%s replica %d Desired = %v, want %v", tc.workflowID, tc.version, act.ReplicaIndex, act.Desired, tc.wantDesired)
			}
		}
	}
}
