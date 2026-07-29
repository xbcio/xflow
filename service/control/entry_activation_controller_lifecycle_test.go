package control

import (
	"context"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestEntryActivationLifecycle exercises the full node-generic activation
// lifecycle that Task 5 wires up: a ControlPlane constructed with a memory
// EntryActivationStore exposes an EntryActivationManager; registering a workflow
// whose trigger node carries a RunnerSelector derives a desired EntryActivation
// (desired-state only, unassigned). Driving one reconcile pass assigns a capable
// live runner at generation 1 and delivers an Activate directive to it. Then
// deregistering the workflow marks the activation non-desired; the next reconcile
// pass fences the owner, delivers a Deactivate directive, and leaves the record
// !Desired with no owner.
func TestEntryActivationLifecycle(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryEntryActivationStore()
	cp, err := NewControlPlane(Config{
		Backend:              backendlocal.New(),
		EntryActivationStore: store,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	mgr := cp.EntryActivationManager()
	if mgr == nil {
		t.Fatal("EntryActivationManager() = nil, want non-nil when EntryActivationStore is set")
	}

	// A workflow whose entry is a single remote-hosted trigger node carrying a
	// node-level RunnerSelector {zone: a}.
	g := singleTriggerGraph(t, map[string]string{"zone": "a"})
	const (
		wfID = types.WorkflowID("wf-lifecycle")
		wfV  = "v1"
	)

	// Registering derives the desired activation (desired-state only, unassigned).
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, wfID, wfV, g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}

	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      wfID,
		WorkflowVersion: wfV,
		EntryUnitID:     "trig",
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get desired activation: ok=%v err=%v", ok, err)
	}
	if !got.Desired {
		t.Fatal("derived activation must be Desired")
	}
	if got.RunnerID != "" {
		t.Fatalf("derived activation must be unassigned, got runner %q", got.RunnerID)
	}

	// A reconciler over the same store with one capable, selector-matching live
	// runner. This mirrors the leader-gated loop the ControlPlane launches; the
	// loop is exercised via a direct Reconcile so the test is deterministic.
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	runner := RunnerSnapshot{
		RunnerID:      "runner-1",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.source"}},
		LastHeartbeat: now,
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	sel := DefaultRunnerSelector()
	rec := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	if err := rec.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}

	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "runner-1" || got.Generation != 1 {
		t.Fatalf("after reconcile: runner=%q gen=%d, want runner-1 @ gen1", got.RunnerID, got.Generation)
	}

	dir := rec.DirectivesForRunner("runner-1")
	if dir == nil || len(dir.Activate) != 1 {
		t.Fatalf("expected 1 activate directive for runner-1, got %+v", dir)
	}
	if dir.Activate[0].EntryUnitID != "trig" {
		t.Fatalf("Activate.EntryUnitID = %q, want trig", dir.Activate[0].EntryUnitID)
	}

	// Deregister → desired-state only flip to !Desired (owner still set so the
	// reconciler can deliver the Deactivate).
	if err := mgr.RemoveWorkflow(ctx, namespace.Default, wfID, wfV, g); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.Desired {
		t.Fatal("after RemoveWorkflow, activation must be !Desired")
	}

	// Next reconcile fences the owner and delivers a Deactivate to runner-1.
	later := now.Add(time.Second)
	runner.LastHeartbeat = later
	lister.runners = []RunnerSnapshot{runner}
	if err := rec.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile (deactivate): %v", err)
	}

	dir = rec.DirectivesForRunner("runner-1")
	if dir == nil || len(dir.Deactivate) != 1 {
		t.Fatalf("expected 1 deactivate directive for runner-1, got %+v", dir)
	}

	got, _, _ = store.Get(ctx, key)
	if got.Desired {
		t.Fatal("activation must remain !Desired after deactivate reconcile")
	}
	if got.RunnerID != "" {
		t.Fatalf("activation owner must be cleared after fence, got %q", got.RunnerID)
	}
}
