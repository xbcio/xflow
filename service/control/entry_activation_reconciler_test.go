package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// TestEntryActivationReconciler_AssignsMatchingRunner verifies the reconciler
// assigns a desired-but-unassigned activation to a capable, selector-matching
// live runner and never to a non-matching one (fail-closed), and that an
// expired lease is fenced and reassigned with a fresh generation.
func TestEntryActivationReconciler_AssignsMatchingRunner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Selector: required, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	matching := RunnerSnapshot{
		RunnerID:      "runner-match",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}
	nonMatching := RunnerSnapshot{
		RunnerID:      "runner-nomatch",
		Capacity:      4,
		Labels:        map[string]string{"zone": "b"},
		LastHeartbeat: now,
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{nonMatching, matching}}

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
	if got.RunnerID != "runner-match" {
		t.Fatalf("expected runner-match assigned, got %q", got.RunnerID)
	}
	if got.RunnerID == "runner-nomatch" {
		t.Fatal("non-matching runner must never be assigned (fail-closed)")
	}
	if got.Generation != 1 {
		t.Fatalf("first assignment generation = %d, want 1", got.Generation)
	}
	firstGen := got.Generation
	wantDeadline := now.Add(60 * time.Second)
	if !got.LeaseDeadline.Equal(wantDeadline) {
		t.Fatalf("lease deadline = %v, want %v", got.LeaseDeadline, wantDeadline)
	}

	// A second reconcile at the same time is a no-op (already assigned, not
	// expired) — owner and generation unchanged.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (idempotent): %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.Generation != firstGen || got.RunnerID != "runner-match" {
		t.Fatalf("idempotent reconcile changed assignment: %+v", got)
	}

	// Expire the lease → reconcile fences the old generation and reassigns with a
	// strictly higher generation. Refresh runner heartbeats so they remain live
	// at the later clock.
	later := got.LeaseDeadline.Add(time.Second)
	matching.LastHeartbeat = later
	nonMatching.LastHeartbeat = later
	lister.runners = []RunnerSnapshot{nonMatching, matching}
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile after expiry: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.Generation <= firstGen {
		t.Fatalf("expected generation to advance past %d after expiry, got %d", firstGen, got.Generation)
	}
	if got.RunnerID != "runner-match" {
		t.Fatalf("reassignment must still be a matching runner, got %q", got.RunnerID)
	}
}

// TestReconcilerCapabilityMatch verifies the reconciler only assigns an
// activation to a runner that advertises the required node-type capability. Two
// live, label-matching runners are offered: one advertises the required
// http.request capability, one does not. Only the capable one may be assigned.
// When no capable runner exists the activation stays UNASSIGNED (fail-closed).
func TestReconcilerCapabilityMatch(t *testing.T) {
	ctx := context.Background()

	newDesired := func(store *MemoryEntryActivationStore) engine.EntryActivation {
		act := testEntryActivation() // Selector: required, {zone: a}
		act.Requirements = []engine.CapabilityRequirement{{NodeType: "http.request"}}
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		return act
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	t.Run("assigns only the capable runner", func(t *testing.T) {
		store := NewMemoryEntryActivationStore()
		act := newDesired(store)
		key := keyOfActivation(act)

		capable := RunnerSnapshot{
			RunnerID:      "runner-capable",
			Capacity:      4,
			Labels:        map[string]string{"zone": "a"},
			Capabilities:  []protocol.Capability{{NodeType: "http.request"}},
			LastHeartbeat: now,
		}
		incapable := RunnerSnapshot{
			RunnerID:      "runner-incapable",
			Capacity:      4,
			Labels:        map[string]string{"zone": "a"},
			Capabilities:  []protocol.Capability{{NodeType: "kafka.consume"}},
			LastHeartbeat: now,
		}
		// Offer the incapable runner first so a naive first-match assigns it.
		lister := &mockRunnerLister{runners: []RunnerSnapshot{incapable, capable}}

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
		got, _, _ := store.Get(ctx, key)
		if got.RunnerID != "runner-capable" {
			t.Fatalf("expected runner-capable assigned, got %q", got.RunnerID)
		}
	})

	t.Run("no capable runner leaves it unassigned", func(t *testing.T) {
		store := NewMemoryEntryActivationStore()
		act := newDesired(store)
		key := keyOfActivation(act)

		incapable := RunnerSnapshot{
			RunnerID:      "runner-incapable",
			Capacity:      4,
			Labels:        map[string]string{"zone": "a"},
			Capabilities:  []protocol.Capability{{NodeType: "kafka.consume"}},
			LastHeartbeat: now,
		}
		lister := &mockRunnerLister{runners: []RunnerSnapshot{incapable}}

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
		got, _, _ := store.Get(ctx, key)
		if got.RunnerID != "" {
			t.Fatalf("no capable runner: expected unassigned, got %q", got.RunnerID)
		}
		if got.Generation != 0 {
			t.Fatalf("no capable runner: generation must stay 0, got %d", got.Generation)
		}
	})
}

// TestEntryActivationReconciler_NoMatchingRunner verifies fail-closed behavior:
// when no live runner matches a required selector, nothing is assigned.
func TestEntryActivationReconciler_NoMatchingRunner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Selector: required, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "r-b", Capacity: 4, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now},
	}}

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
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("no matching runner: expected unassigned, got %q", got.RunnerID)
	}
	if got.Generation != 0 {
		t.Fatalf("no matching runner: generation must stay 0, got %d", got.Generation)
	}
}
