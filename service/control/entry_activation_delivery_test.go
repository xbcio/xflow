package control

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// TestEntryActivationDelivery verifies the reconciler enqueues an Activate
// directive after a successful Assign and delivers it (drain-once) via
// DirectivesForRunner, and that clearing the desired state enqueues a
// Deactivate directive to the previously-hosting runner.
func TestEntryActivationDelivery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Selector: required, {zone: a}, EntryUnitID "tg"
	act.NodeType = "kafka.source"
	act.Params = map[string]any{"topic": "orders", "group": "g1"}
	act.Requirements = []engine.CapabilityRequirement{{NodeType: "kafka.source"}}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

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

	// The activation must now be assigned at generation 1.
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after reconcile: ok=%v err=%v", ok, err)
	}
	if got.RunnerID != "runner-1" || got.Generation != 1 {
		t.Fatalf("expected runner-1 @ gen1, got runner=%q gen=%d", got.RunnerID, got.Generation)
	}

	// An Activate directive must be delivered to runner-1 with the node-generic
	// identity + trigger params.
	dir := r.DirectivesForRunner("runner-1")
	if dir == nil {
		t.Fatal("DirectivesForRunner(runner-1): expected directives, got nil")
	}
	if len(dir.Activate) != 1 {
		t.Fatalf("expected 1 activate directive, got %d", len(dir.Activate))
	}
	a := dir.Activate[0]
	if a.EntryUnitID != "tg" {
		t.Errorf("Activate.EntryUnitID = %q, want tg", a.EntryUnitID)
	}
	if a.NodeType != "kafka.source" {
		t.Errorf("Activate.NodeType = %q, want kafka.source", a.NodeType)
	}
	if a.Generation != 1 {
		t.Errorf("Activate.Generation = %d, want 1", a.Generation)
	}
	if !reflect.DeepEqual(a.Params, map[string]any{"topic": "orders", "group": "g1"}) {
		t.Errorf("Activate.Params = %+v, want {topic:orders group:g1}", a.Params)
	}
	if len(dir.Deactivate) != 0 {
		t.Errorf("unexpected deactivate directives on assign: %+v", dir.Deactivate)
	}

	// Drain-once: a second call returns nil.
	if again := r.DirectivesForRunner("runner-1"); again != nil {
		t.Fatalf("DirectivesForRunner second call must drain to nil, got %+v", again)
	}

	// Clear the desired state (workflow removed). The reconciler must fence the
	// owner and enqueue a Deactivate to the previously-hosting runner.
	act.Desired = false
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert (clear desired): %v", err)
	}
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile after clear: %v", err)
	}
	dir = r.DirectivesForRunner("runner-1")
	if dir == nil {
		t.Fatal("DirectivesForRunner after clear: expected a deactivate, got nil")
	}
	if len(dir.Deactivate) != 1 {
		t.Fatalf("expected 1 deactivate directive, got %d (%+v)", len(dir.Deactivate), dir)
	}
	d := dir.Deactivate[0]
	if d.EntryUnitID != "tg" {
		t.Errorf("Deactivate.EntryUnitID = %q, want tg", d.EntryUnitID)
	}
	if d.Generation != 1 {
		t.Errorf("Deactivate.Generation = %d, want 1", d.Generation)
	}
	if len(dir.Activate) != 0 {
		t.Errorf("unexpected activate directives on clear: %+v", dir.Activate)
	}

	// Drain-once again.
	if again := r.DirectivesForRunner("runner-1"); again != nil {
		t.Fatalf("DirectivesForRunner after clear second call must be nil, got %+v", again)
	}
}

// TestEntryActivationDelivery_ConcurrentDrain exercises the drain-once mutex
// under concurrent reconciles (enqueue) and heartbeat drains (DirectivesForRunner)
// so the -race detector can prove the directive queue is data-race free.
func TestEntryActivationDelivery_ConcurrentDrain(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation()
	act.NodeType = "kafka.source"
	act.Requirements = []engine.CapabilityRequirement{{NodeType: "kafka.source"}}
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-1",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.source"}},
		LastHeartbeat: now,
	}}}

	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = r.Reconcile(ctx, now)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = r.DirectivesForRunner("runner-1")
			}
		}()
	}
	wg.Wait()
}
