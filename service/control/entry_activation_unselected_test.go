package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// unselectedTriggerGraph compiles the shape a deployment produces when it never
// labels its runners: a trigger and a group, neither carrying a RunnerSelector.
//
// The compiler accepts this — validateNodeRunnerSelector returns nil for an
// absent selector and resolveRunnerSelector yields nil when neither the
// workflow nor the node sets one — so it reaches DeriveEntryActivations as a
// legitimate definition, not as something a caller was warned about.
func unselectedTriggerGraph(t *testing.T, withGroup bool) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "unselected-trigger",
		Nodes: []types.NodeDef{
			{Name: "trig", Type: "http.request", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "worker", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
			{Name: "down", Type: "db.query", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"trig":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
			"worker": {"main": types.PortConnections{Targets: []types.Connection{{Node: "down", Input: "main"}}}},
		},
	}
	if withGroup {
		def.Groups = []types.GroupDef{{Name: "grp1", Members: []string{"trig", "worker"}}}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestDeriveEntryActivations_UnselectedTriggerIsDerived pins that a trigger
// without a RunnerSelector still produces an activation.
//
// Skipping it was a silent total failure. The only production caller of
// DeriveEntryActivations is apiserver's registerWorkflow, and that path hosts
// nothing inline — every unit it derives is dispatched to a remote runner. A
// skipped trigger therefore has no executor at all: it is never activated, no
// runner ever subscribes, and nothing anywhere reports it. The definition
// compiles, registration returns success with no warnings, and the workflow
// simply never fires.
//
// nil already means "any runner" one layer down: the reconciler's
// selectorMatches returns true for a nil selector. Deriving these makes the two
// layers agree instead of one erasing what the other would have placed.
func TestDeriveEntryActivations_UnselectedTriggerIsDerived(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withGroup bool
		wantUnit  string
	}{
		{"standalone trigger", false, "trig"},
		{"trigger group", true, "grp1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			units, err := DeriveEntryActivations(unselectedTriggerGraph(t, tc.withGroup))
			if err != nil {
				t.Fatalf("DeriveEntryActivations: %v", err)
			}
			if len(units) != 1 {
				t.Fatalf("derived %d entry units, want 1 (%q) — a trigger with no runner "+
					"selector was dropped, so nothing will ever activate it: %+v",
					len(units), tc.wantUnit, units)
			}
			if units[0].EntryUnitID != tc.wantUnit {
				t.Fatalf("EntryUnitID = %q, want %q", units[0].EntryUnitID, tc.wantUnit)
			}
			if units[0].Selector != nil {
				t.Fatalf("Selector = %+v, want nil preserved — the reconciler reads nil as "+
					"\"any runner\"; substituting a selector here would narrow placement",
					units[0].Selector)
			}
			if len(units[0].Requirements) == 0 {
				t.Fatal("activation carries no Requirements; the reconciler would place it " +
					"on a runner that cannot host it")
			}
		})
	}
}

// TestUnselectedTriggerIsPlacedOnAnyRunner runs the derived activation through
// the reconciler, which is what makes the test above worth having: deriving a
// unit nothing can place would be the same silent failure one layer over.
//
// It goes through AddOrUpdateWorkflow rather than hand-assembling an
// EntryActivation, so the fields the manager copies out of the derived unit —
// Requirements above all — are the ones the reconciler then matches against
// runner capabilities. A hand-built activation would let a manager that drops
// Requirements pass.
//
// It also pins the placement rule. An unselected activation must land on a
// runner that has labels of its own — nil means the deployment expressed no
// constraint, not that it requires an unlabelled runner.
func TestUnselectedTriggerIsPlacedOnAnyRunner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	g := unselectedTriggerGraph(t, false)
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "trig",
	}
	if _, ok, err := store.Get(ctx, key); err != nil || !ok {
		t.Fatalf("no activation stored for the unselected trigger: ok=%v err=%v", ok, err)
	}

	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-labelled",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
		Capabilities:  []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}},
	}}}

	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after reconcile: ok=%v err=%v", ok, err)
	}
	if got.RunnerID != "runner-labelled" {
		t.Fatalf("RunnerID = %q, want runner-labelled — a nil selector means the "+
			"deployment set no constraint, so any capable live runner may host it",
			got.RunnerID)
	}
}
