package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

func TestEntryActivationReconcilerDrainingOwnerIsFencedAndReassigned(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	activation := testEntryActivation()
	key := keyOfActivation(activation)
	if err := store.Upsert(ctx, activation); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	// First establish an owner while both runners are ACTIVE.
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "runner-a", Capacity: 1, Labels: map[string]string{"zone": "a"}, LastHeartbeat: now},
		{RunnerID: "runner-b", Capacity: 1, Labels: map[string]string{"zone": "a"}, LastHeartbeat: now},
	}}
	sel := DefaultRunnerSelector()
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	assigned, ok, err := store.Get(ctx, key)
	if err != nil || !ok || assigned.RunnerID != "runner-a" || assigned.Generation != 1 {
		t.Fatalf("initial assignment = %+v, ok=%v, err=%v; want runner-a generation 1", assigned, ok, err)
	}

	// Drain the current owner. It remains heartbeating/visible, but must be
	// treated as invalid for its existing activation too, so Fence emits a
	// deactivation directive before a healthy replacement takes generation 2.
	lister.runners[0].Control = &RunnerControlSnapshot{
		DesiredState: RunnerDesiredStateDraining,
		Generation:   1,
	}
	lister.runners[0].LastHeartbeat = now.Add(time.Second)
	lister.runners[1].LastHeartbeat = now.Add(time.Second)
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("drain reconcile: %v", err)
	}
	migrated, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after drain: ok=%v err=%v", ok, err)
	}
	if migrated.RunnerID != "runner-b" || migrated.Generation != 2 {
		t.Fatalf("migrated activation = %+v, want runner-b generation 2", migrated)
	}
	stop := reconciler.DirectivesForRunner("runner-a")
	if stop == nil || len(stop.Deactivate) != 1 || stop.Deactivate[0].Generation != 1 {
		t.Fatalf("draining owner deactivate = %+v, want generation 1", stop)
	}
	start := reconciler.DirectivesForRunner("runner-b")
	if start == nil || len(start.Activate) != 1 || start.Activate[0].Generation != 2 {
		t.Fatalf("replacement activate = %+v, want generation 2", start)
	}
}

func TestDrainingRunnerInventoryDoesNotRenewOldActivation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	activation := testEntryActivation()
	key := keyOfActivation(activation)
	if err := store.Upsert(ctx, activation); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 15, 13, 0, 0, 0, time.UTC)
	if ok, err := store.Assign(ctx, key, "runner-a", "old-session", 1, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", LastHeartbeat: now,
		Control: &RunnerControlSnapshot{DesiredState: RunnerDesiredStateDraining, Generation: 1},
	}}}
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.ReconcileRunnerInventory(ctx, "runner-a", []protocol.ActivationInventoryItem{{
		WorkflowID: string(activation.WorkflowID), EntryUnitID: activation.EntryUnitID, Generation: 1,
	}}, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}
	got, _, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunnerID != "" || got.Generation != 1 || !got.LeaseDeadline.IsZero() {
		t.Fatalf("draining inventory renewed activation: %+v; want fenced/unassigned generation floor 1", got)
	}
	stop := reconciler.DirectivesForRunner("runner-a")
	if stop == nil || len(stop.Deactivate) != 1 || stop.Deactivate[0].Generation != 1 {
		t.Fatalf("deactivation after draining inventory = %+v", stop)
	}
}
