package control

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// singleTriggerGraph compiles a workflow whose entry is a single remote-hosted
// trigger node (kafka.source) carrying a node-level RunnerSelector and trigger
// Params. It is the production shape the EntryActivationManager derives a
// node-generic activation from. selectorLabels sets the node selector's
// MatchLabels (node-level selectors must leave Mode empty).
func singleTriggerGraph(t *testing.T, selectorLabels map[string]string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "single-trig",
		Nodes: []types.NodeDef{
			{
				Name: "trig", Type: "kafka.source", Version: 1, Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: selectorLabels},
				Parameters:     map[string]any{"topic": "orders"},
			},
			{Name: "work", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestEntryActivationDelivery verifies the reconciler enqueues an Activate
// directive after a successful Assign and delivers it (drain-once) via
// DirectivesForRunner, and that clearing the desired state enqueues a
// Deactivate directive to the previously-hosting runner.
//
// This exercises the store-level clear path (bare Upsert(Desired=false) with the
// owner still set). The production RemoveWorkflow path — where the manager writes
// desired-state only and the reconciler is the fence+deactivate authority — is
// covered by TestEntryActivationDelivery_DeactivateOnRemove.
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

// TestEntryActivationDelivery_DeactivateOnRemove drives the FULL production
// remove path: EntryActivationManager.RemoveWorkflow (which now writes
// desired-state only, no pre-fence), then Reconcile. The previously-hosting
// runner MUST receive a Deactivate. This reproduces the bug where the manager's
// pre-fence cleared RunnerID before the reconciler observed the record, so the
// deactivate branch never fired and the runner's subscription was orphaned.
func TestEntryActivationDelivery_DeactivateOnRemove(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	g := singleTriggerGraph(t, map[string]string{"zone": "a"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
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

	// First reconcile assigns runner-1 and delivers an Activate.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	dir := r.DirectivesForRunner("runner-1")
	if dir == nil || len(dir.Activate) != 1 || dir.Activate[0].EntryUnitID != "trig" {
		t.Fatalf("expected an Activate for trig, got %+v", dir)
	}
	assignedGen := dir.Activate[0].Generation

	// Remove the workflow via the PRODUCTION path (manager fences? no — it must
	// only clear desired state). Then reconcile.
	if err := mgr.RemoveWorkflow(ctx, namespace.Default, "wf-x", "v1", g); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	// Sanity: after RemoveWorkflow the record must still carry the owner (the
	// manager must NOT have pre-fenced it away), otherwise the reconciler could
	// never deliver a Deactivate.
	rec, ok, err := store.Get(ctx, engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-x", WorkflowVersion: "v1", EntryUnitID: "trig",
	})
	if err != nil || !ok {
		t.Fatalf("Get after remove: ok=%v err=%v", ok, err)
	}
	if rec.Desired {
		t.Fatal("RemoveWorkflow must set Desired=false")
	}
	if rec.RunnerID == "" {
		t.Fatal("RemoveWorkflow must NOT pre-fence: RunnerID must still be set so the reconciler can deactivate the owner")
	}

	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (remove): %v", err)
	}
	dir = r.DirectivesForRunner("runner-1")
	if dir == nil {
		t.Fatal("DeactivateOnRemove: previously-hosting runner MUST receive a Deactivate, got nil")
	}
	if len(dir.Deactivate) != 1 {
		t.Fatalf("expected exactly 1 Deactivate, got %+v", dir)
	}
	d := dir.Deactivate[0]
	if d.EntryUnitID != "trig" {
		t.Errorf("Deactivate.EntryUnitID = %q, want trig", d.EntryUnitID)
	}
	if d.Generation != assignedGen {
		t.Errorf("Deactivate.Generation = %d, want %d (the assigned generation)", d.Generation, assignedGen)
	}
	// The record must now be fenced (owner cleared, generation advanced past the
	// deactivated one) and left unassigned.
	rec, _, _ = store.Get(ctx, engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-x", WorkflowVersion: "v1", EntryUnitID: "trig",
	})
	if rec.RunnerID != "" {
		t.Fatalf("after reconcile the removed activation must be fenced (unassigned), got runner=%q", rec.RunnerID)
	}
	if rec.Generation < assignedGen {
		t.Fatalf("fence must not lower the generation floor: got %d, was %d", rec.Generation, assignedGen)
	}
}

// TestEntryActivationDelivery_DeactivateOnMaterialChangeReassign drives a
// material change (the runner selector narrows so the current owner no longer
// qualifies) through the production AddOrUpdate path (manager writes
// desired-state only). After reconcile the OLD owner MUST receive a Deactivate
// AND a new, matching runner MUST receive an Activate at a higher generation.
func TestEntryActivationDelivery_DeactivateOnMaterialChangeReassign(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	// Initial desired state: selector {zone: a}.
	gA := singleTriggerGraph(t, map[string]string{"zone": "a"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", gA); err != nil {
		t.Fatalf("AddOrUpdateWorkflow (zone a): %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	runnerA := RunnerSnapshot{
		RunnerID:      "runner-a",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.source"}},
		LastHeartbeat: now,
	}
	runnerB := RunnerSnapshot{
		RunnerID:      "runner-b",
		Capacity:      4,
		Labels:        map[string]string{"zone": "b"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.source"}},
		LastHeartbeat: now,
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{runnerA, runnerB}}
	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
	})

	// First reconcile assigns runner-a (only zone:a runner matches).
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign a): %v", err)
	}
	dir := r.DirectivesForRunner("runner-a")
	if dir == nil || len(dir.Activate) != 1 {
		t.Fatalf("expected an Activate for runner-a, got %+v", dir)
	}
	genA := dir.Activate[0].Generation

	// Material change: narrow the selector to {zone: b} via the production
	// AddOrUpdate path (manager writes desired-state only — no pre-fence).
	gB := singleTriggerGraph(t, map[string]string{"zone": "b"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", gB); err != nil {
		t.Fatalf("AddOrUpdateWorkflow (zone b): %v", err)
	}
	// The owner (runner-a, zone:a) no longer satisfies the new {zone:b} selector.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (material change): %v", err)
	}

	// OLD owner runner-a MUST receive a Deactivate at its assigned generation.
	dirA := r.DirectivesForRunner("runner-a")
	if dirA == nil || len(dirA.Deactivate) != 1 {
		t.Fatalf("OLD owner runner-a MUST receive a Deactivate on material change, got %+v", dirA)
	}
	if dirA.Deactivate[0].Generation != genA {
		t.Errorf("Deactivate.Generation = %d, want %d", dirA.Deactivate[0].Generation, genA)
	}

	// NEW runner runner-b MUST receive an Activate at a higher generation.
	dirB := r.DirectivesForRunner("runner-b")
	if dirB == nil || len(dirB.Activate) != 1 {
		t.Fatalf("NEW runner runner-b MUST receive an Activate on reassign, got %+v", dirB)
	}
	if dirB.Activate[0].Generation <= genA {
		t.Errorf("reassigned generation %d must exceed old generation %d", dirB.Activate[0].Generation, genA)
	}
	if dirB.Activate[0].EntryUnitID != "trig" {
		t.Errorf("Activate.EntryUnitID = %q, want trig", dirB.Activate[0].EntryUnitID)
	}

	// Store must now show runner-b as the owner.
	rec, _, _ := store.Get(ctx, engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-x", WorkflowVersion: "v1", EntryUnitID: "trig",
	})
	if rec.RunnerID != "runner-b" {
		t.Fatalf("after material change the owner must be runner-b, got %q", rec.RunnerID)
	}
}

// TestEntryActivationDelivery_DeactivateOnParamsChangeSameVersion drives a
// within-version trigger content change: same workflow version, same node type,
// same selector, but changed trigger Params. This changes the derived
// PackageHash, so the current owner (running the OLD params) is stale. After
// reconcile the owner MUST receive a Deactivate for its old generation AND an
// Activate at a HIGHER generation carrying the NEW params (typically the same
// runner, since the selector still matches). This is the "package" dimension of
// the material-change contract and a regression guard: it fails if the reconciler
// only checks selector/capability and not content (PackageHash).
func TestEntryActivationDelivery_DeactivateOnParamsChangeSameVersion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	// Initial: params {topic: orders}.
	gV1 := paramTriggerGraph(t, map[string]any{"topic": "orders"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", gV1); err != nil {
		t.Fatalf("AddOrUpdateWorkflow (orders): %v", err)
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

	// First reconcile assigns runner-1 with the OLD params.
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	dir := r.DirectivesForRunner("runner-1")
	if dir == nil || len(dir.Activate) != 1 {
		t.Fatalf("expected an Activate, got %+v", dir)
	}
	genOld := dir.Activate[0].Generation
	if got := dir.Activate[0].Params["topic"]; got != "orders" {
		t.Fatalf("initial Activate params topic = %v, want orders", got)
	}

	// Within-version content change: same version, same selector, changed params.
	gV1b := paramTriggerGraph(t, map[string]any{"topic": "payments"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-x", "v1", gV1b); err != nil {
		t.Fatalf("AddOrUpdateWorkflow (payments): %v", err)
	}
	if err := r.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (params change): %v", err)
	}

	dir = r.DirectivesForRunner("runner-1")
	if dir == nil {
		t.Fatal("params change: owner MUST receive directives, got nil")
	}
	if len(dir.Deactivate) != 1 {
		t.Fatalf("params change: expected 1 Deactivate for the stale generation, got %+v", dir.Deactivate)
	}
	if dir.Deactivate[0].Generation != genOld {
		t.Errorf("Deactivate.Generation = %d, want %d (old generation)", dir.Deactivate[0].Generation, genOld)
	}
	if len(dir.Activate) != 1 {
		t.Fatalf("params change: expected 1 Activate carrying the NEW params, got %+v", dir.Activate)
	}
	a := dir.Activate[0]
	if a.Generation <= genOld {
		t.Errorf("new Activate generation %d must exceed old %d", a.Generation, genOld)
	}
	if got := a.Params["topic"]; got != "payments" {
		t.Errorf("new Activate params topic = %v, want payments (the updated value)", got)
	}
}

// paramTriggerGraph compiles a single remote-hosted kafka.source trigger node
// with a fixed {zone: a} selector and the given trigger Params. Used to drive a
// within-version params change (same version, same selector, different params).
func paramTriggerGraph(t *testing.T, params map[string]any) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "param-trig",
		Nodes: []types.NodeDef{
			{
				Name: "trig", Type: "kafka.source", Version: 1, Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"zone": "a"}},
				Parameters:     params,
			},
			{Name: "work", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}
