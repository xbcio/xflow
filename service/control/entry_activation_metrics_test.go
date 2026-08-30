package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// fakeEntryActivationMetrics implements EntryActivationMetrics for tests. It
// is a plain counter with no synchronization: every test using it drives the
// reconciler from a single goroutine.
type fakeEntryActivationMetrics struct {
	actionCounts map[string]int
	fenced       int
	activeSets   []float64
}

func newFakeEntryActivationMetrics() *fakeEntryActivationMetrics {
	return &fakeEntryActivationMetrics{actionCounts: make(map[string]int)}
}

func (f *fakeEntryActivationMetrics) OnGroupActivation(action string) {
	f.actionCounts[action]++
}

func (f *fakeEntryActivationMetrics) OnGroupActivationFenced() {
	f.fenced++
}

func (f *fakeEntryActivationMetrics) SetGroupActivationActive(value float64) {
	f.activeSets = append(f.activeSets, value)
}

// lastActive returns the most recently Set gauge value, or -1 if Set was
// never called.
func (f *fakeEntryActivationMetrics) lastActive() float64 {
	if len(f.activeSets) == 0 {
		return -1
	}
	return f.activeSets[len(f.activeSets)-1]
}

// groupTestActivation builds a minimal desired activation for the metrics
// tests below: no Selector (matches any runner) and no Requirements (so
// capabilitiesSatisfy is trivially true) keeps each test focused on the
// metrics wiring rather than selector/capability matching, which is already
// covered elsewhere in this package.
func groupTestActivation(nodeType string) engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-metrics",
		WorkflowVersion: "v1",
		EntryUnitID:     "eu",
		NodeType:        nodeType,
		PackageHash:     "pkg-metrics",
		Desired:         true,
	}
}

// raceLosingAssignStore wraps a real EntryActivationStore but forces every
// Assign call to report a lost generation race (assigned == false, err ==
// nil), the exact signal OnGroupActivationFenced exists for.
type raceLosingAssignStore struct {
	engine.EntryActivationStore
}

func (s *raceLosingAssignStore) Assign(_ context.Context, _ engine.EntryActivationKey, _, _ string, _ uint64, _ time.Time) (bool, error) {
	return false, nil
}

// TestEntryActivationReconciler_GroupActivateEmitsActivationMetric pins the
// one activate call site (assignUnowned, successful Assign branch). Deleting
// the OnGroupActivation("activate") call there (M-1) must turn this red.
func TestEntryActivationReconciler_GroupActivateEmitsActivationMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if fm.actionCounts["activate"] != 1 {
		t.Fatalf("activate count = %d, want 1", fm.actionCounts["activate"])
	}
	if fm.actionCounts["deactivate"] != 0 {
		t.Fatalf("deactivate count = %d, want 0", fm.actionCounts["deactivate"])
	}
	if fm.fenced != 0 {
		t.Fatalf("fenced count = %d, want 0", fm.fenced)
	}
	if got := fm.lastActive(); got != 1 {
		t.Fatalf("active gauge = %v, want 1", got)
	}
}

// TestEntryActivationReconciler_NonGroupEntryUnitEmitsNoActivationMetric is
// the negative control for the group discriminator (act.NodeType ==
// engine.GroupNodeType). A standalone (non-group) trigger entry unit is
// driven through BOTH an activate and a deactivate in one test, and every
// count must be EXACTLY 0 — not >= 0. Mutating the discriminator to always
// true (M-3) must turn this red.
func TestEntryActivationReconciler_NonGroupEntryUnitEmitsNoActivationMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation("kafka.source")
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	key := keyOfActivation(act)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok || got.RunnerID != "runner-a" {
		t.Fatalf("precondition: expected non-group unit assigned, got %+v ok=%v err=%v", got, ok, err)
	}

	// Drive it through a deactivate too (reconcileExisting's !Desired path),
	// so both call families are exercised in this one negative control.
	act.Desired = false
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (deactivate): %v", err)
	}

	if fm.actionCounts["activate"] != 0 {
		t.Fatalf("activate count = %d, want exactly 0 for a non-group entry unit", fm.actionCounts["activate"])
	}
	if fm.actionCounts["deactivate"] != 0 {
		t.Fatalf("deactivate count = %d, want exactly 0 for a non-group entry unit", fm.actionCounts["deactivate"])
	}
	if fm.fenced != 0 {
		t.Fatalf("fenced count = %d, want exactly 0 for a non-group entry unit", fm.fenced)
	}
	if got := fm.lastActive(); got != 0 {
		t.Fatalf("active gauge = %v, want 0 (no group activations exist)", got)
	}
}

// TestEntryActivationReconciler_LegacyEmptyNodeTypeEmitsNoActivationMetric
// covers NodeType == "" — a pre-NodeType-field legacy record. It is excluded
// by the same discriminator as a non-group unit (engine.GroupNodeType is
// "xflow.group", never ""), so a legacy GROUP record is silently undercounted
// by xflow_group_activation_total / xflow_group_activation_active. This is a
// KNOWN, ACCEPTED gap — not a bug this task fixes — because nothing
// distinguishes a legacy group record from a legacy standalone-trigger record
// once the field that would tell them apart is itself missing.
func TestEntryActivationReconciler_LegacyEmptyNodeTypeEmitsNoActivationMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation("")
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	key := keyOfActivation(act)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	got, ok, err := store.Get(ctx, key)
	if err != nil || !ok || got.RunnerID != "runner-a" {
		t.Fatalf("precondition: expected legacy unit assigned, got %+v ok=%v err=%v", got, ok, err)
	}

	act.Desired = false
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (deactivate): %v", err)
	}

	if fm.actionCounts["activate"] != 0 {
		t.Fatalf("activate count = %d, want exactly 0 for a legacy (NodeType==\"\") record", fm.actionCounts["activate"])
	}
	if fm.actionCounts["deactivate"] != 0 {
		t.Fatalf("deactivate count = %d, want exactly 0 for a legacy (NodeType==\"\") record", fm.actionCounts["deactivate"])
	}
}

// TestEntryActivationReconciler_DeactivateNotDesiredEmitsMetric pins deactivate
// call site #1 of 3: reconcileExisting's "!act.Desired" branch. Deleting its
// OnGroupActivation("deactivate") call (M-2a) must turn this red.
func TestEntryActivationReconciler_DeactivateNotDesiredEmitsMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	key := keyOfActivation(act)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	if ok, err := store.Assign(ctx, key, "runner-a", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}

	// Clear Desired without touching the assignment fields (Upsert never
	// does), so reconcileExisting takes the "!act.Desired" branch with a
	// non-empty RunnerID still on the record.
	act.Desired = false
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}

	fm := newFakeEntryActivationMetrics()
	// No Lister: this path never consults runner liveness.
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("expected fenced/unassigned after !Desired reconcile, got %+v", got)
	}
	if fm.actionCounts["deactivate"] != 1 {
		t.Fatalf("deactivate count = %d, want 1 (reconcileExisting !Desired path)", fm.actionCounts["deactivate"])
	}
	if fm.actionCounts["activate"] != 0 {
		t.Fatalf("activate count = %d, want 0", fm.actionCounts["activate"])
	}
}

// TestEntryActivationReconciler_DeactivateStaleOwnerEmitsMetric pins
// deactivate call site #2 of 3: reconcileExisting's stale/dead/mismatched
// owner branch (act.Desired stays true throughout, so this cannot be
// satisfied by the !Desired branch pinned above). Deleting its
// OnGroupActivation("deactivate") call (M-2b) must turn this red.
func TestEntryActivationReconciler_DeactivateStaleOwnerEmitsMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	key := keyOfActivation(act)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour) // not expired
	if ok, err := store.Assign(ctx, key, "runner-old", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}

	// runner-old is dead/gone (absent from the live snapshot) — ownerLive
	// becomes false, driving the stale-owner deactivate branch.
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-new", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "runner-new" {
		t.Fatalf("expected reassignment to runner-new, got %+v", got)
	}
	if fm.actionCounts["deactivate"] != 1 {
		t.Fatalf("deactivate count = %d, want 1 (reconcileExisting stale-owner path)", fm.actionCounts["deactivate"])
	}
	if fm.actionCounts["activate"] != 1 {
		t.Fatalf("activate count = %d, want 1 (reassignment to runner-new)", fm.actionCounts["activate"])
	}
}

// TestEntryActivationReconciler_InventoryUnreportedDeactivateEmitsMetric pins
// deactivate call site #3 of 3: ReconcileRunnerInventory's "assigned but not
// reported" branch, reached without ever calling Reconcile. Deleting its
// OnGroupActivation("deactivate") call (M-2c) must turn this red.
func TestEntryActivationReconciler_InventoryUnreportedDeactivateEmitsMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	key := keyOfActivation(act)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	if ok, err := store.Assign(ctx, key, "runner-a", "sess-1", 3, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}

	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	// runner-a reconnects and reports NOTHING for this activation → treated
	// as unreported → Fence + Deactivate.
	if err := r.ReconcileRunnerInventory(ctx, "runner-a", nil, now); err != nil {
		t.Fatalf("ReconcileRunnerInventory: %v", err)
	}

	got, _, _ := store.Get(ctx, key)
	if got.RunnerID != "" {
		t.Fatalf("expected revoke after unreported inventory, got %+v", got)
	}
	if fm.actionCounts["deactivate"] != 1 {
		t.Fatalf("deactivate count = %d, want 1 (ReconcileRunnerInventory unreported path)", fm.actionCounts["deactivate"])
	}
	if fm.actionCounts["activate"] != 0 {
		t.Fatalf("activate count = %d, want 0", fm.actionCounts["activate"])
	}
}

// TestEntryActivationReconciler_FencedEmitsMetricOnLostGenerationRace pins the
// fenced call site: assignUnowned's Store.Assign(assigned==false, err==nil)
// branch. Moving the OnGroupActivationFenced call into the assigned==true
// branch (M-4) must turn this red.
func TestEntryActivationReconciler_FencedEmitsMetricOnLostGenerationRace(t *testing.T) {
	ctx := context.Background()
	inner := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := inner.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	store := &raceLosingAssignStore{EntryActivationStore: inner}

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if fm.fenced != 1 {
		t.Fatalf("fenced count = %d, want 1 (Assign lost the generation race)", fm.fenced)
	}
	if fm.actionCounts["activate"] != 0 {
		t.Fatalf("activate count = %d, want 0 (Assign never succeeded)", fm.actionCounts["activate"])
	}
}

// TestEntryActivationReconciler_NonGroupFencedRaceEmitsNoMetric is the
// negative control for recordGroupActivationFenced's own discriminator
// (act.NodeType == engine.GroupNodeType), independent of
// recordGroupActivation's copy of the same check. A standalone (non-group)
// entry unit that loses the exact same generation race as
// TestEntryActivationReconciler_FencedEmitsMetricOnLostGenerationRace must
// report fenced == 0. Removing the NodeType guard from
// recordGroupActivationFenced specifically (leaving recordGroupActivation's
// guard untouched) must turn this red.
func TestEntryActivationReconciler_NonGroupFencedRaceEmitsNoMetric(t *testing.T) {
	ctx := context.Background()
	inner := NewMemoryEntryActivationStore()
	act := groupTestActivation("kafka.source")
	if err := inner.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	store := &raceLosingAssignStore{EntryActivationStore: inner}

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if fm.fenced != 0 {
		t.Fatalf("fenced count = %d, want exactly 0 for a non-group entry unit", fm.fenced)
	}
}

// TestEntryActivationReconciler_ActiveGaugeExcludesNonGroupAndLegacy pins the
// gauge's OWN copy of the group discriminator at the Reconcile driver
// (entry_activation_reconciler.go's "activations[i].NodeType ==
// engine.GroupNodeType && activations[i].RunnerID != \"\"" check), which is a
// third, independent occurrence of the same condition guarded separately by
// recordGroupActivation and recordGroupActivationFenced. Neither of the
// counter-focused negative controls above observes the gauge while a
// non-group / legacy activation is actively assigned (RunnerID != ""), so
// deleting "activations[i].NodeType == engine.GroupNodeType &&" from the
// gauge's own condition (leaving both recordGroupActivation* discriminators
// untouched) would NOT be caught by them. This test assigns one non-group,
// one legacy (NodeType==""), and one GROUP activation — all left with a live
// RunnerID after the same Reconcile pass — and asserts the gauge is EXACTLY
// 1, not >= 1.
func TestEntryActivationReconciler_ActiveGaugeExcludesNonGroupAndLegacy(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)

	nonGroup := groupTestActivation("kafka.source")
	nonGroup.WorkflowID = "wf-non-group"
	if err := store.Upsert(ctx, nonGroup); err != nil {
		t.Fatal(err)
	}
	nonGroupKey := keyOfActivation(nonGroup)
	if ok, err := store.Assign(ctx, nonGroupKey, "runner-non-group", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign non-group: ok=%v err=%v", ok, err)
	}

	legacy := groupTestActivation("")
	legacy.WorkflowID = "wf-legacy"
	if err := store.Upsert(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	legacyKey := keyOfActivation(legacy)
	if ok, err := store.Assign(ctx, legacyKey, "runner-legacy", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign legacy: ok=%v err=%v", ok, err)
	}

	group := groupTestActivation(engine.GroupNodeType)
	group.WorkflowID = "wf-group"
	if err := store.Upsert(ctx, group); err != nil {
		t.Fatal(err)
	}
	groupKey := keyOfActivation(group)
	if ok, err := store.Assign(ctx, groupKey, "runner-group", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign group: ok=%v err=%v", ok, err)
	}

	// All three owners are live and matching, so reconcileExisting keeps every
	// assignment as-is (no fencing, no deactivation) — RunnerID stays non-empty
	// on all three when the gauge is computed.
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "runner-non-group", Capacity: 4, LastHeartbeat: now},
		{RunnerID: "runner-legacy", Capacity: 4, LastHeartbeat: now},
		{RunnerID: "runner-group", Capacity: 4, LastHeartbeat: now},
	}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Precondition: all three activations are still assigned after the pass
	// (otherwise this test would trivially pass with gauge == 0 regardless of
	// the discriminator under test).
	for _, k := range []struct {
		name string
		key  engine.EntryActivationKey
		want string
	}{
		{"non-group", nonGroupKey, "runner-non-group"},
		{"legacy", legacyKey, "runner-legacy"},
		{"group", groupKey, "runner-group"},
	} {
		got, ok, err := store.Get(ctx, k.key)
		if err != nil || !ok || got.RunnerID != k.want {
			t.Fatalf("precondition %s: expected RunnerID=%q, got %+v ok=%v err=%v", k.name, k.want, got, ok, err)
		}
	}

	if got := fm.lastActive(); got != 1 {
		t.Fatalf("active gauge = %v, want exactly 1 (only the GROUP activation counts; non-group and legacy must be excluded)", got)
	}
}

// TestEntryActivationReconciler_ActiveGaugeSetOncePerPassAcrossNamespaces pins
// the gauge trap called out in the task brief: SetGroupActivationActive
// carries no namespace label, so it must be Set exactly once per Reconcile
// pass with the total summed across every configured namespace. Moving the
// Set call into the per-namespace loop (M-5) must turn this red.
func TestEntryActivationReconciler_ActiveGaugeSetOncePerPassAcrossNamespaces(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	nsA := namespace.Namespace("ns-a")
	nsB := namespace.Namespace("ns-b")
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)

	actA := groupTestActivation(engine.GroupNodeType)
	actA.Namespace = nsA
	actA.WorkflowID = "wf-a"
	if err := store.Upsert(ctx, actA); err != nil {
		t.Fatal(err)
	}
	keyA := keyOfActivation(actA)
	if ok, err := store.Assign(ctx, keyA, "runner-a", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign A: ok=%v err=%v", ok, err)
	}

	actB := groupTestActivation(engine.GroupNodeType)
	actB.Namespace = nsB
	actB.WorkflowID = "wf-b"
	if err := store.Upsert(ctx, actB); err != nil {
		t.Fatal(err)
	}
	keyB := keyOfActivation(actB)
	if ok, err := store.Assign(ctx, keyB, "runner-b", "", 1, deadline); err != nil || !ok {
		t.Fatalf("Assign B: ok=%v err=%v", ok, err)
	}

	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now},
		{RunnerID: "runner-b", Capacity: 4, LastHeartbeat: now},
	}}
	sel := DefaultRunnerSelector()
	fm := newFakeEntryActivationMetrics()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{nsA, nsB}, LeaseTTL: time.Minute,
		Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Exactly one Set call for the whole pass — never one per namespace.
	if len(fm.activeSets) != 1 {
		t.Fatalf("SetGroupActivationActive called %d times, want exactly 1 (once per Reconcile pass, not per namespace): %v", len(fm.activeSets), fm.activeSets)
	}
	// Both namespaces' active group activations must be summed. A
	// per-namespace Set (the bug this test pins) would report 1 — whichever
	// namespace iterated last — silently undercounting.
	if got := fm.lastActive(); got != 2 {
		t.Fatalf("active gauge = %v, want 2 (summed across ns-a and ns-b)", got)
	}
}

// TestEntryActivationReconciler_NilMetricsDoesNotPanic verifies a nil
// EntryActivationMetrics (the default — Metrics is opt-in) never panics
// across activate, deactivate, fenced, or the gauge.
func TestEntryActivationReconciler_NilMetricsDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := groupTestActivation(engine.GroupNodeType)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-a", Capacity: 4, LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
		// Metrics intentionally left nil.
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile with nil Metrics must not panic or error: %v", err)
	}
}
