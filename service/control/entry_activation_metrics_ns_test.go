package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

const (
	metricsNSA namespace.Namespace = "ns-a"
	metricsNSB namespace.Namespace = "ns-b"
)

// nsMetricsActivation is a desired GROUP activation in ns. Distinct unit IDs
// keep several activations of one namespace apart in the store.
func nsMetricsActivation(ns namespace.Namespace, unit string) engine.EntryActivation {
	act := groupTestActivation(engine.GroupNodeType)
	act.Namespace = ns
	act.EntryUnitID = unit
	return act
}

// nsMetricsRunner is a live runner that declares both test namespaces; without
// the declaration canServeNamespace would only admit the default namespace.
func nsMetricsRunner(now time.Time) *mockRunnerLister {
	return &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-a",
		Capacity:      8,
		LastHeartbeat: now,
		Namespaces:    []namespace.Namespace{metricsNSA, metricsNSB},
	}}}
}

func newNSMetricsReconciler(store engine.EntryActivationStore, lister ActivationRunnerLister, sel RunnerSelector, fm *fakeEntryActivationMetrics) *EntryActivationReconciler {
	return NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{metricsNSA, metricsNSB}, LeaseTTL: time.Minute,
		Metrics: fm,
	})
}

// The counts differ per namespace (2 vs 1) so that a swapped or collapsed
// bucket cannot hide behind symmetric totals.
// Mutation: passing namespace.Default (or "") instead of act.Namespace in
// recordGroupActivation moves both counts to the wrong series.
func TestEntryActivationReconciler_ActivateIsAttributedToItsNamespace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	for _, act := range []engine.EntryActivation{
		nsMetricsActivation(metricsNSA, "eu-1"),
		nsMetricsActivation(metricsNSA, "eu-2"),
		nsMetricsActivation(metricsNSB, "eu-1"),
	} {
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	fm := newFakeEntryActivationMetrics()
	r := newNSMetricsReconciler(store, nsMetricsRunner(now), DefaultRunnerSelector(), fm)

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := fm.actionByNS[nsAction{metricsNSA, "activate"}]; got != 2 {
		t.Fatalf("%s activate = %d, want 2", metricsNSA, got)
	}
	if got := fm.actionByNS[nsAction{metricsNSB, "activate"}]; got != 1 {
		t.Fatalf("%s activate = %d, want 1", metricsNSB, got)
	}
	if len(fm.actionByNS) != 2 {
		t.Fatalf("activation series = %v, want exactly the two configured namespaces", fm.actionByNS)
	}
	if got := fm.lastActiveIn(metricsNSA); got != 2 {
		t.Fatalf("%s active gauge = %v, want 2", metricsNSA, got)
	}
	if got := fm.lastActiveIn(metricsNSB); got != 1 {
		t.Fatalf("%s active gauge = %v, want 1", metricsNSB, got)
	}
}

// Only ns-b's activation stops being desired, so only ns-b may report a
// deactivation while ns-a's healthy assignment stays untouched.
// Mutation: dropping act.Namespace in recordGroupActivation attributes the
// deactivation to the wrong namespace (or to both).
func TestEntryActivationReconciler_DeactivateIsAttributedToItsNamespace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)

	actA := nsMetricsActivation(metricsNSA, "eu")
	actB := nsMetricsActivation(metricsNSB, "eu")
	for _, act := range []engine.EntryActivation{actA, actB} {
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Assign(ctx, keyOfActivation(act), "runner-a", "", 1, deadline); err != nil || !ok {
			t.Fatalf("Assign %s: ok=%v err=%v", act.Namespace, ok, err)
		}
	}
	// Upsert never touches the assignment fields, so this reaches the
	// "!act.Desired" branch with a non-empty RunnerID still on the record.
	actB.Desired = false
	if err := store.Upsert(ctx, actB); err != nil {
		t.Fatal(err)
	}

	fm := newFakeEntryActivationMetrics()
	r := newNSMetricsReconciler(store, nsMetricsRunner(now), DefaultRunnerSelector(), fm)

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := fm.actionByNS[nsAction{metricsNSB, "deactivate"}]; got != 1 {
		t.Fatalf("%s deactivate = %d, want 1", metricsNSB, got)
	}
	if got := fm.actionByNS[nsAction{metricsNSA, "deactivate"}]; got != 0 {
		t.Fatalf("%s deactivate = %d, want 0 (its assignment is healthy)", metricsNSA, got)
	}
	if len(fm.actionByNS) != 1 {
		t.Fatalf("activation series = %v, want only the %s deactivate", fm.actionByNS, metricsNSB)
	}
	if got := fm.lastActiveIn(metricsNSA); got != 1 {
		t.Fatalf("%s active gauge = %v, want 1", metricsNSA, got)
	}
	if got := fm.lastActiveIn(metricsNSB); got != 0 {
		t.Fatalf("%s active gauge = %v, want 0 after its activation was released", metricsNSB, got)
	}
}

// Mutation: passing namespace.Default (or "") to OnGroupActivationFenced in
// recordGroupActivationFenced, or swapping the two namespaces' buckets.
func TestEntryActivationReconciler_FencedIsAttributedToItsNamespace(t *testing.T) {
	ctx := context.Background()
	inner := NewMemoryEntryActivationStore()
	for _, act := range []engine.EntryActivation{
		nsMetricsActivation(metricsNSA, "eu-1"),
		nsMetricsActivation(metricsNSA, "eu-2"),
		nsMetricsActivation(metricsNSB, "eu-1"),
	} {
		if err := inner.Upsert(ctx, act); err != nil {
			t.Fatal(err)
		}
	}
	store := &raceLosingAssignStore{EntryActivationStore: inner}

	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	fm := newFakeEntryActivationMetrics()
	r := newNSMetricsReconciler(store, nsMetricsRunner(now), DefaultRunnerSelector(), fm)

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := fm.fencedByNS[metricsNSA]; got != 2 {
		t.Fatalf("%s fenced = %d, want 2", metricsNSA, got)
	}
	if got := fm.fencedByNS[metricsNSB]; got != 1 {
		t.Fatalf("%s fenced = %d, want 1", metricsNSB, got)
	}
	if len(fm.fencedByNS) != 2 {
		t.Fatalf("fenced series = %v, want exactly the two configured namespaces", fm.fencedByNS)
	}
	if len(fm.actionByNS) != 0 {
		t.Fatalf("activation series = %v, want none (no Assign succeeded)", fm.actionByNS)
	}
}

// ns-b's activation needs the selector fallback; ns-a's matches directly. Only
// ns-b may count a fallback.
// Mutation: passing namespace.Default (or "") to OnGroupSelectorFallback in
// recordGroupSelectorFallback, or reading the wrong namespace's activation.
func TestEntryActivationReconciler_SelectorFallbackIsAttributedToItsNamespace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	direct := nsMetricsActivation(metricsNSA, "eu-direct")
	fallback := groupFallbackTestActivation(engine.GroupNodeType)
	fallback.Namespace = metricsNSB
	for _, act := range []engine.EntryActivation{direct, fallback} {
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	// A short FallbackGrace keeps the runner live when the second pass lands
	// after the grace window; see the default-namespace fallback test.
	sel := DefaultRunnerSelector()
	sel.FallbackGrace = time.Second
	fm := newFakeEntryActivationMetrics()
	r := newNSMetricsReconciler(store, nsMetricsRunner(now), sel, fm)

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (grace start): %v", err)
	}
	if len(fm.fallbackByNS) != 0 {
		t.Fatalf("fallback series = %v, want none before the grace window elapses", fm.fallbackByNS)
	}
	if err := r.Reconcile(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("Reconcile (fallback): %v", err)
	}

	got, ok, err := store.Get(ctx, keyOfActivation(fallback))
	if err != nil || !ok || got.RunnerID != "runner-a" {
		t.Fatalf("expected fallback assignment to runner-a, got %+v ok=%v err=%v", got, ok, err)
	}
	if got := fm.fallbackByNS[metricsNSB]; got != 1 {
		t.Fatalf("%s selector fallback = %d, want 1", metricsNSB, got)
	}
	if len(fm.fallbackByNS) != 1 {
		t.Fatalf("fallback series = %v, want only %s", fm.fallbackByNS, metricsNSB)
	}
}

// A lost Activate directive for a non-default namespace must be counted under
// that namespace; redelivery is the one counter fed from the inventory path
// rather than from assignUnowned.
// Mutation: passing namespace.Default (or "") to OnGroupActivationRedelivered
// in entry_activation_redelivery.go.
func TestReconcilerRedeliveryIsAttributedToItsNamespace(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	act.Namespace = metricsNSB
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-1",
		SessionID:     "sess-1",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		Namespaces:    []namespace.Namespace{metricsNSB},
		LastHeartbeat: now,
	}}}
	fm := newFakeEntryActivationMetrics()
	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: lister, Selector: &sel,
		Namespaces: []namespace.Namespace{metricsNSB}, LeaseTTL: 60 * time.Second,
		Delivery: delivery, Metrics: fm,
	})

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	assigned, _, _ := store.Get(ctx, key)
	if assigned.RunnerID != "runner-1" {
		t.Fatalf("assignment runner = %q, want runner-1", assigned.RunnerID)
	}
	// Drain the assignment's directive as if it were delivered; the runner
	// then reports it hosts nothing, i.e. the directive was lost.
	if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
		t.Fatalf("drain assignment directive: %v", err)
	}
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}
	if err := r.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile (redeliver): %v", err)
	}

	if got := fm.redeliveryByNS[nsAction{metricsNSB, "activate"}]; got != 1 {
		t.Fatalf("%s activate redeliveries = %d, want 1", metricsNSB, got)
	}
	if len(fm.redeliveryByNS) != 1 {
		t.Fatalf("redelivery series = %v, want only the %s activate", fm.redeliveryByNS, metricsNSB)
	}
}
