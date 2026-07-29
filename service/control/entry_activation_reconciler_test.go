package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// mockRunnerLister implements ActivationRunnerLister for tests.
type mockRunnerLister struct {
	runners []RunnerSnapshot
}

func (m *mockRunnerLister) ListLiveRunners(_ context.Context) []RunnerSnapshot {
	return m.runners
}

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

// TestInventoryReconcile_ReportedRenewsLeaseGenerationUnchanged verifies that
// when a runner re-registers and reports (in its inventory) an activation it
// already owns, the reconciler RENEWS the lease deadline WITHOUT advancing the
// generation. Bumping the generation here would fence the runner's own in-flight
// seeds (see the RENEW SEMANTICS constraint), so it must stay stable.
func TestInventoryReconcile_ReportedRenewsLeaseGenerationUnchanged(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Selector: required, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	// Assign the activation to runner-match at generation 1 with a soon-to-expire
	// lease so the renew is observable.
	shortDeadline := now.Add(5 * time.Second)
	ok, err := store.Assign(ctx, key, "runner-match", "sess-1", 1, shortDeadline)
	if err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}

	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	// The runner re-registers, reporting it still hosts this activation at gen 1.
	reported := []protocol.ActivationInventoryItem{{
		WorkflowID:  string(act.WorkflowID),
		EntryUnitID: act.EntryUnitID,
		Generation:  1,
	}}
	if err := r.ReconcileRunnerInventory(ctx, "runner-match", reported, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.Generation != 1 {
		t.Fatalf("generation must stay 1 on renew, got %d", got.Generation)
	}
	if got.RunnerID != "runner-match" {
		t.Fatalf("owner must be retained on renew, got %q", got.RunnerID)
	}
	wantDeadline := now.Add(60 * time.Second)
	if !got.LeaseDeadline.Equal(wantDeadline) {
		t.Fatalf("lease deadline must be renewed to %v, got %v", wantDeadline, got.LeaseDeadline)
	}
}

// TestInventoryReconcile_UnreportedRevokesForReassign verifies that when a
// runner re-registers on a NEW session but does NOT report an activation still
// assigned to it (it lost the subscription on reconnect), the reconciler REVOKES
// the assignment (fences + unassigns) so a later reconcile can reassign it to a
// live runner instead of orphaning it.
func TestInventoryReconcile_UnreportedRevokesForReassign(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Selector: required, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	ok, err := store.Assign(ctx, key, "runner-match", "sess-1", 1, now.Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}

	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	// The runner re-registers with an EMPTY inventory: on the new session it is
	// no longer hosting anything, so the still-assigned activation must be revoked.
	if err := r.ReconcileRunnerInventory(ctx, "runner-match", nil, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("unreported activation must be revoked/unassigned, got owner %q", got.RunnerID)
	}
	if !got.Desired {
		t.Fatalf("revoked activation must stay desired (reassignable), got %+v", got)
	}

	// After revoke, a normal reconcile against a matching live runner reassigns it
	// with a strictly higher generation (proving reassignability).
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-match",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}}}
	r.cfg.Lister = lister
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile after revoke: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "runner-match" {
		t.Fatalf("revoked activation must be reassignable, got owner %q", got.RunnerID)
	}
	if got.Generation <= 1 {
		t.Fatalf("reassignment must advance generation past 1, got %d", got.Generation)
	}
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
