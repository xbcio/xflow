package control

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
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

// TestInventoryReconcile_VersionMatchRenews verifies that when a runner reports
// an activation whose WorkflowVersion matches the stored activation, the lease is
// renewed and the generation stays unchanged (same as before, but with version).
func TestInventoryReconcile_VersionMatchRenews(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // WorkflowVersion: "v1"
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	ok, err := store.Assign(ctx, key, "runner-match", "sess-1", 1, now.Add(5*time.Second))
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

	// Report WITH correct version "v1".
	reported := []protocol.ActivationInventoryItem{{
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		Generation:      1,
	}}
	if err := r.ReconcileRunnerInventory(ctx, "runner-match", reported, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.Generation != 1 {
		t.Fatalf("generation must stay 1 on version-matched renew, got %d", got.Generation)
	}
	if got.RunnerID != "runner-match" {
		t.Fatalf("owner must be retained, got %q", got.RunnerID)
	}
	wantDeadline := now.Add(60 * time.Second)
	if !got.LeaseDeadline.Equal(wantDeadline) {
		t.Fatalf("lease deadline must be renewed to %v, got %v", wantDeadline, got.LeaseDeadline)
	}
}

// TestInventoryReconcile_VersionMismatchRevokes verifies that when a runner
// reports an activation with a WorkflowVersion different from the stored one (same
// workflowID+entryUnitID but another version), the activation is treated as
// unreported and is fenced+deactivated for reassignment.
func TestInventoryReconcile_VersionMismatchRevokes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // WorkflowVersion: "v1"
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

	// Report with WRONG version "v2" — same workflowID + entryUnitID.
	reported := []protocol.ActivationInventoryItem{{
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: "v2",
		EntryUnitID:     act.EntryUnitID,
		Generation:      1,
	}}
	if err := r.ReconcileRunnerInventory(ctx, "runner-match", reported, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("version-mismatched activation must be revoked, got owner %q", got.RunnerID)
	}
	if !got.Desired {
		t.Fatalf("revoked activation must stay desired (reassignable), got %+v", got)
	}
}

// TestInventoryReconcile_EmptyVersionFallbackRenews verifies the backward-
// compatibility degradation: when an old runner reports an activation with an
// empty WorkflowVersion, it still matches a versioned store activation and the
// lease is renewed — preventing false revocation of activations hosted by
// pre-version runners.
func TestInventoryReconcile_EmptyVersionFallbackRenews(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // WorkflowVersion: "v1"
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	ok, err := store.Assign(ctx, key, "runner-match", "sess-1", 1, now.Add(5*time.Second))
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

	// Old runner reports with EMPTY version (backward-compat).
	reported := []protocol.ActivationInventoryItem{{
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: "", // old runner does not know version
		EntryUnitID:     act.EntryUnitID,
		Generation:      1,
	}}
	if err := r.ReconcileRunnerInventory(ctx, "runner-match", reported, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.Generation != 1 {
		t.Fatalf("generation must stay 1 on empty-version fallback renew, got %d", got.Generation)
	}
	if got.RunnerID != "runner-match" {
		t.Fatalf("owner must be retained on empty-version fallback, got %q", got.RunnerID)
	}
	wantDeadline := now.Add(60 * time.Second)
	if !got.LeaseDeadline.Equal(wantDeadline) {
		t.Fatalf("lease deadline must be renewed to %v, got %v", wantDeadline, got.LeaseDeadline)
	}
}

// TestActivateDirectiveCarriesWorkflowVersion verifies that the activate directive
// built by the reconciler carries the activation's WorkflowVersion, which the
// runner needs to report back in its inventory on reconnect.
func TestActivateDirectiveCarriesWorkflowVersion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // WorkflowVersion: "v1"
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	runner := RunnerSnapshot{
		RunnerID:      "runner-match",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
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

	// First reconcile assigns the activation.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "runner-match" {
		t.Fatalf("expected assignment to runner-match, got %q", got.RunnerID)
	}

	// Drain the directive and check WorkflowVersion.
	dirs := r.DirectivesForRunner("runner-match")
	if dirs == nil || len(dirs.Activate) == 0 {
		t.Fatal("expected an activate directive after assignment")
	}
	d := dirs.Activate[0]
	if d.WorkflowVersion != "v1" {
		t.Fatalf("directive WorkflowVersion = %q, want %q", d.WorkflowVersion, "v1")
	}
	if d.WorkflowID != string(act.WorkflowID) {
		t.Fatalf("directive WorkflowID = %q, want %q", d.WorkflowID, act.WorkflowID)
	}
	if d.EntryUnitID != act.EntryUnitID {
		t.Fatalf("directive EntryUnitID = %q, want %q", d.EntryUnitID, act.EntryUnitID)
	}
}

// --- Default-selector fallback grace period tests (spec §11.7) ---

// testDefaultActivation returns an activation with Mode=default, {zone: a}.
func testDefaultActivation() engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-default-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "tg",
		PackageHash:     "pkg-def",
		Selector:        &types.RunnerSelector{Mode: types.RunnerSelectorModeDefault, MatchLabels: map[string]string{"zone": "a"}},
		Desired:         true,
	}
}

// TestDefaultSelector_RequiredModeNoFallback verifies that a required-mode
// activation is NEVER assigned via fallback, even well past the grace window.
func TestDefaultSelector_RequiredModeNoFallback(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testEntryActivation() // Mode: required, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	// Only runner with zone: b (non-matching).
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "r-b", Capacity: 4, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now},
	}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 5 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// First reconcile: no match.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	// Way past the grace window (10 minutes later).
	later := now.Add(10 * time.Minute)
	lister.runners[0].LastHeartbeat = later
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("required mode must never fallback, got runner %q", got.RunnerID)
	}
}

// TestDefaultSelector_MatchingRunnerNoFallback verifies that when a matching
// runner is available, it is chosen directly without fallback.
func TestDefaultSelector_MatchingRunnerNoFallback(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testDefaultActivation() // Mode: default, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	matching := RunnerSnapshot{
		RunnerID:      "r-match",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}
	nonMatching := RunnerSnapshot{
		RunnerID:      "r-other",
		Capacity:      4,
		Labels:        map[string]string{"zone": "b"},
		LastHeartbeat: now,
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{nonMatching, matching}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 5 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "r-match" {
		t.Fatalf("expected matching runner r-match, got %q", got.RunnerID)
	}
}

// TestDefaultSelector_NoMatchWithinGrace verifies that when no matching runner
// exists but the grace window hasn't elapsed, the activation stays unassigned.
func TestDefaultSelector_NoMatchWithinGrace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testDefaultActivation() // Mode: default, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	// Only runner with zone: b (non-matching but capable).
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "r-b", Capacity: 4, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now},
	}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 30 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// First reconcile: starts grace timer.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("within grace: expected unassigned, got %q", got.RunnerID)
	}

	// Second reconcile at now+10s: still within 30s grace.
	later := now.Add(10 * time.Second)
	lister.runners[0].LastHeartbeat = later
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("within grace (10s < 30s): expected unassigned, got %q", got.RunnerID)
	}
}

// TestDefaultSelector_FallbackAfterGrace verifies that once the grace window
// elapses, the activation is assigned to a capable (but non-matching) runner.
func TestDefaultSelector_FallbackAfterGrace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testDefaultActivation() // Mode: default, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	// Only runner with zone: b (non-matching but capable).
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "r-b", Capacity: 4, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now},
	}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 5 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// First reconcile: starts the grace timer.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}

	// Second reconcile: past the grace window.
	later := now.Add(10 * time.Second) // 10s > 5s grace
	lister.runners[0].LastHeartbeat = later
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "r-b" {
		t.Fatalf("after grace: expected fallback to r-b, got %q", got.RunnerID)
	}
	if got.Generation != 1 {
		t.Fatalf("expected generation 1, got %d", got.Generation)
	}
}

// TestDefaultSelector_FallbackRespectsCapability verifies that fallback never
// assigns to a runner that cannot satisfy the activation's capability
// requirements — even after the grace window.
func TestDefaultSelector_FallbackRespectsCapability(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testDefaultActivation() // Mode: default, {zone: a}
	act.Requirements = []engine.CapabilityRequirement{{NodeType: "http.request"}}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	// Only runner with zone: b, but wrong capability.
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{
			RunnerID:      "r-incapable",
			Capacity:      4,
			Labels:        map[string]string{"zone": "b"},
			Capabilities:  []protocol.Capability{{NodeType: "kafka.consume"}},
			LastHeartbeat: now,
		},
	}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 5 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// First reconcile.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	// Past grace.
	later := now.Add(10 * time.Second)
	lister.runners[0].LastHeartbeat = later
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("fallback must not bypass capability check: expected unassigned, got %q", got.RunnerID)
	}
}

// TestDefaultSelector_GraceTimerResets verifies that after a matching runner
// becomes available and assigns, the grace timer is cleared. A subsequent loss
// of the matching runner starts the timer fresh.
func TestDefaultSelector_GraceTimerResets(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	act := testDefaultActivation() // Mode: default, {zone: a}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	t0 := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	nonMatching := RunnerSnapshot{
		RunnerID: "r-b", Capacity: 4,
		Labels: map[string]string{"zone": "b"}, LastHeartbeat: t0,
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{nonMatching}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 20 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// t0: no match → timer starts.
	if err := r.Reconcile(ctx, t0); err != nil {
		t.Fatalf("Reconcile t0: %v", err)
	}
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("t0: expected unassigned, got %q", got.RunnerID)
	}

	// t0+10s: matching runner appears → assigns directly, timer clears.
	t1 := t0.Add(10 * time.Second)
	matching := RunnerSnapshot{
		RunnerID: "r-a", Capacity: 4,
		Labels: map[string]string{"zone": "a"}, LastHeartbeat: t1,
	}
	nonMatching.LastHeartbeat = t1
	lister.runners = []RunnerSnapshot{nonMatching, matching}
	if err := r.Reconcile(ctx, t1); err != nil {
		t.Fatalf("Reconcile t1: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "r-a" {
		t.Fatalf("t1: expected r-a assigned, got %q", got.RunnerID)
	}

	// Expire the lease and remove the matching runner. Non-matching still live.
	t2 := got.LeaseDeadline.Add(time.Second)
	nonMatching.LastHeartbeat = t2
	lister.runners = []RunnerSnapshot{nonMatching}
	if err := r.Reconcile(ctx, t2); err != nil {
		t.Fatalf("Reconcile t2: %v", err)
	}
	// After fence: now unassigned, grace timer restarts from t2.
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "" {
		// The reconciler fenced and now needs a new assignment. With no matching
		// runner the grace timer should have just started.
		t.Fatalf("t2: expected unassigned after lease expiry + no match, got %q", got.RunnerID)
	}

	// t2+10s: still within NEW 20s grace (started at t2) → must not fallback.
	t3 := t2.Add(10 * time.Second)
	nonMatching.LastHeartbeat = t3
	lister.runners = []RunnerSnapshot{nonMatching}
	if err := r.Reconcile(ctx, t3); err != nil {
		t.Fatalf("Reconcile t3: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("t3: timer must have reset — 10s < 20s grace, expected unassigned, got %q", got.RunnerID)
	}

	// t2+25s: past the new grace window → fallback.
	t4 := t2.Add(25 * time.Second)
	nonMatching.LastHeartbeat = t4
	lister.runners = []RunnerSnapshot{nonMatching}
	if err := r.Reconcile(ctx, t4); err != nil {
		t.Fatalf("Reconcile t4: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "r-b" {
		t.Fatalf("t4: expected fallback to r-b after new grace elapsed, got %q", got.RunnerID)
	}
}

// TestDefaultSelector_EmptyModeIsDefault verifies that an empty Mode string
// (common with omitempty JSON) behaves identically to RunnerSelectorModeDefault
// — per engine/graph/compile.go:resolveRunnerSelector which defaults empty to
// "default".
func TestDefaultSelector_EmptyModeIsDefault(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()

	// Activation with empty Mode — must behave as "default".
	act := engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-empty-mode",
		WorkflowVersion: "v1",
		EntryUnitID:     "tg",
		PackageHash:     "pkg-em",
		Selector:        &types.RunnerSelector{Mode: "", MatchLabels: map[string]string{"zone": "a"}},
		Desired:         true,
	}
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "r-b", Capacity: 4, Labels: map[string]string{"zone": "b"}, LastHeartbeat: now},
	}}

	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 5 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:    store,
		Lister:   lister,
		Selector: &sel,
		LeaseTTL: 60 * time.Second,
	})

	// First reconcile: starts grace timer.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("within grace: expected unassigned, got %q", got.RunnerID)
	}

	// Past grace → should fallback (proving empty mode = default, not required).
	later := now.Add(10 * time.Second)
	lister.runners[0].LastHeartbeat = later
	if err := r.Reconcile(ctx, later); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}
	got, _, _ = store.Get(ctx, key)
	if got.RunnerID != "r-b" {
		t.Fatalf("empty mode must behave as default with fallback, got %q", got.RunnerID)
	}
}

// removableStore wraps MemoryEntryActivationStore with the ability to drop a
// record, standing in for a workflow being unregistered. The production store
// interface has no Delete; removal happens through the manager's desired-state
// derivation.
type removableStore struct {
	*MemoryEntryActivationStore
	hidden map[engine.EntryActivationKey]bool
}

func (s *removableStore) hide(key engine.EntryActivationKey) {
	if s.hidden == nil {
		s.hidden = make(map[engine.EntryActivationKey]bool)
	}
	s.hidden[key] = true
}

func (s *removableStore) List(ctx context.Context, ns namespace.Namespace) ([]engine.EntryActivation, error) {
	acts, err := s.MemoryEntryActivationStore.List(ctx, ns)
	if err != nil {
		return nil, err
	}
	out := acts[:0]
	for _, a := range acts {
		if !s.hidden[keyOfActivation(a)] {
			out = append(out, a)
		}
	}
	return out, nil
}

// TestDefaultSelector_PrunesTrackingForRemovedActivation verifies the grace-window
// map does not retain entries for activations that no longer exist in the store.
func TestDefaultSelector_PrunesTrackingForRemovedActivation(t *testing.T) {
	ctx := context.Background()
	store := &removableStore{MemoryEntryActivationStore: NewMemoryEntryActivationStore()}

	act := testDefaultActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	t0 := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "r-b", Capacity: 4,
		Labels: map[string]string{"zone": "b"}, LastHeartbeat: t0,
	}}}
	sel := RunnerSelector{LiveTTL: 30 * time.Second, FallbackGrace: 20 * time.Second}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	// No matching runner → grace tracking starts for this key.
	if err := r.Reconcile(ctx, t0); err != nil {
		t.Fatalf("Reconcile t0: %v", err)
	}
	r.mu.Lock()
	_, tracked := r.noMatchSince[key]
	r.mu.Unlock()
	if !tracked {
		t.Fatal("expected grace tracking after a no-match pass")
	}

	// Workflow unregistered → the activation disappears from the store.
	store.hide(key)
	if err := r.Reconcile(ctx, t0.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile after removal: %v", err)
	}
	r.mu.Lock()
	_, stillTracked := r.noMatchSince[key]
	size := len(r.noMatchSince)
	r.mu.Unlock()
	if stillTracked {
		t.Fatal("grace tracking must be pruned once the activation is gone")
	}
	if size != 0 {
		t.Fatalf("expected empty tracking map, got %d entries", size)
	}
}

// --- Activation retry backoff tests (fix/activation-ack-retry, task 4) ---

// newTestReconciler returns a minimally-configured reconciler suitable for
// exercising the retry-backoff state machine in isolation (no store/lister
// interaction is needed for these tests).
func newTestReconciler(t *testing.T) *EntryActivationReconciler {
	t.Helper()
	return NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      NewMemoryEntryActivationStore(),
		Namespaces: []namespace.Namespace{namespace.Default},
	})
}

// TestActivationRetryBackoffGrowsAndCaps verifies the backoff must be
// recomputed each failure, growing until it caps, and be cleared on success —
// otherwise a supply that recovers would still be held back by a long backoff.
func TestActivationRetryBackoffGrowsAndCaps(t *testing.T) {
	r := newTestReconciler(t) // reuses the helper above
	key := engine.EntryActivationKey{WorkflowID: "w", EntryUnitID: "e"}
	base := time.Unix(1700000000, 0)

	// After the first failure, the backoff should be ~min (10s), and an
	// immediate retry must be blocked.
	r.noteActivationFailure(key, base)
	if !r.retryBlocked(key, base.Add(time.Second)) {
		t.Fatal("a retry 1s after failure must be blocked by the 10s minimum")
	}
	// Past the maximum possible backoff (10s + 20% jitter = 12s), it must be
	// allowed.
	if r.retryBlocked(key, base.Add(13*time.Second)) {
		t.Fatal("a retry 13s after failure must be allowed (10s + max jitter is 12s)")
	}

	// Consecutive failures should grow the delay and never exceed max + 20%
	// jitter.
	prev := time.Duration(0)
	for i := 0; i < 20; i++ {
		r.noteActivationFailure(key, base)
		d := r.retryDelayFor(key)
		if d > DefaultActivationRetryBackoffMax*6/5 {
			t.Fatalf("iteration %d: delay %v exceeds max+jitter", i, d)
		}
		if i > 0 && i < 5 && d <= prev {
			t.Fatalf("iteration %d: delay %v did not grow past %v", i, d, prev)
		}
		prev = d
	}

	// After success, the backoff must be cleared.
	r.clearRetryBackoff(key)
	if r.retryBlocked(key, base) {
		t.Fatal("after clearRetryBackoff a retry must be allowed immediately")
	}
}

// TestActivationRetryBackoffIsJittered verifies jitter is applied: it is the
// key defense against a synchronized retry pulse when a widely-shared supply
// fails and every activation referencing it retries at once.
func TestActivationRetryBackoffIsJittered(t *testing.T) {
	r := newTestReconciler(t)
	base := time.Unix(1700000000, 0)
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 50; i++ {
		key := engine.EntryActivationKey{WorkflowID: "w", EntryUnitID: fmt.Sprintf("e%d", i)}
		r.noteActivationFailure(key, base)
		seen[r.retryDelayFor(key)] = struct{}{}
	}
	if len(seen) < 10 {
		t.Fatalf("only %d distinct delays across 50 keys; jitter is not being applied", len(seen))
	}
}
