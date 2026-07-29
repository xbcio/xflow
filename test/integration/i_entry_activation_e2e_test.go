//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"

	"github.com/redis/go-redis/v9"
)

// staticRunnerLister is a control.ActivationRunnerLister returning a fixed set
// of runner snapshots. The EntryActivationReconciler only consults the lister
// for snapshots, so a static set is sufficient to drive assignment against a
// real EntryActivationStore.
type staticRunnerLister struct {
	runners []control.RunnerSnapshot
}

func (s *staticRunnerLister) ListLiveRunners(context.Context) []control.RunnerSnapshot {
	return s.runners
}

// entryActivationE2EWorkflow compiles a workflow whose single trigger entry unit
// carries a RunnerSelector, so it is a remote-hosted entry activation. The
// selector mode is workflow-level (required); the trigger node inherits the
// resolved selector.
func entryActivationE2EWorkflow(t *testing.T, matchLabels map[string]string) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:           "i-entry-activation-e2e",
		Version:        "1",
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: matchLabels},
		Nodes: []types.NodeDef{
			{Name: "kafka-in", Kind: types.NodeKindTrigger, Type: "kafka.trigger"},
			{Name: "body", Kind: types.NodeKindAction, Type: "test.tg.body"},
		},
		Connections: types.Connections{
			"kafka-in": {"main": {{Node: "body", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	return g
}

// TestEntryActivationLifecycleE2E_Memory exercises the workflow add/update/remove
// lifecycle against a memory-backed EntryActivationStore driven through the
// apiserver + ControlPlane harness (apiserver.New + WithControlPlane + Start).
// It verifies:
//   - add: a desired activation is created and the reconciler assigns the
//     selector-matching runner.
//   - update: a new selector fences the old generation and the reconciler
//     reassigns with a strictly higher generation.
//   - remove: the activation is deactivated (desired=false, unassigned).
func TestEntryActivationLifecycleE2E_Memory(t *testing.T) {
	store := control.NewMemoryEntryActivationStore()
	runEntryActivationLifecycleE2E(t, store)
}

// TestEntryActivationLifecycleE2E_Redis runs the same lifecycle against the
// Redis-backed EntryActivationStore. It SKIPS cleanly when XFLOW_TEST_REDIS_ADDR
// is unset or the Redis endpoint is unreachable (podman 6380 may be down).
func TestEntryActivationLifecycleE2E_Redis(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping Redis-backed lifecycle e2e")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("Redis at %s unreachable: %v", addr, err)
	}
	_ = rdb.Close()

	be, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	store := be.NewEntryActivationStore(time.Minute)
	runEntryActivationLifecycleE2E(t, store)
}

func runEntryActivationLifecycleE2E(t *testing.T, store engine.EntryActivationStore) {
	t.Helper()

	// Stand up a real control plane + apiserver over the local backend, with the
	// EntryActivationStore wired in (so the HTTP seed path also gets generation
	// fencing).
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: store,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("apiserver.Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	if cp.EntryActivationStore() == nil {
		t.Fatal("control plane must expose the wired EntryActivationStore")
	}

	const (
		wfID = types.WorkflowID("wf-entry-activation")
		wfV  = "1"
	)

	mgr := control.NewEntryActivationManager(store)

	now := time.Now()
	// The kafka-in trigger entry unit derives capability Requirements
	// {NodeType:"kafka.trigger"} (Task 3), and the reconciler fail-closes on
	// capability match — so the hosting runner must advertise that capability
	// (plus test.tg.body, the downstream action the group projection pulls in) or
	// it is correctly skipped and nothing is assigned.
	matching := control.RunnerSnapshot{
		RunnerID:      "runner-zone-a",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.trigger"}, {NodeType: "test.tg.body"}},
		LastHeartbeat: now,
	}
	other := control.RunnerSnapshot{
		RunnerID:      "runner-zone-b",
		Capacity:      4,
		Labels:        map[string]string{"zone": "b"},
		Capabilities:  []protocol.Capability{{NodeType: "kafka.trigger"}, {NodeType: "test.tg.body"}},
		LastHeartbeat: now,
	}
	lister := &staticRunnerLister{runners: []control.RunnerSnapshot{other, matching}}
	sel := control.DefaultRunnerSelector()
	reconciler := control.NewEntryActivationReconciler(control.EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   time.Minute,
	})

	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      wfID,
		WorkflowVersion: wfV,
		EntryUnitID:     "kafka-in",
	}

	// --- add: selector zone=a → desired activation created + assigned. ---
	g1 := entryActivationE2EWorkflow(t, map[string]string{"zone": "a"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, wfID, wfV, g1); err != nil {
		t.Fatalf("AddOrUpdateWorkflow(add): %v", err)
	}
	act, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after add: ok=%v err=%v", ok, err)
	}
	if !act.Desired || act.RunnerID != "" {
		t.Fatalf("after add: want desired+unassigned, got %+v", act)
	}
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile(add): %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "runner-zone-a" {
		t.Fatalf("after add reconcile: want runner-zone-a assigned, got %q", act.RunnerID)
	}
	gen1 := act.Generation
	if gen1 == 0 {
		t.Fatal("assigned generation must be > 0")
	}

	// --- update: change selector to zone=b → old generation fenced, reassign. ---
	g2 := entryActivationE2EWorkflow(t, map[string]string{"zone": "b"})
	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, wfID, wfV, g2); err != nil {
		t.Fatalf("AddOrUpdateWorkflow(update): %v", err)
	}
	// The manager writes desired-state only and never fences (the reconciler is
	// the single fence+reassign authority). So pre-reconcile the old owner is
	// still assigned; the reconciler observes the selector mismatch, fences the
	// old generation, and reassigns in the same pass.
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "runner-zone-a" {
		t.Fatalf("after update (pre-reconcile): manager must not fence, want runner-zone-a still owner, got %q", act.RunnerID)
	}
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile(update): %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "runner-zone-b" {
		t.Fatalf("after update reconcile: want runner-zone-b, got %q", act.RunnerID)
	}
	if act.Generation <= gen1 {
		t.Fatalf("update must advance generation past %d, got %d", gen1, act.Generation)
	}

	// --- remove: workflow removed → activation deactivated + unassigned. ---
	if err := mgr.RemoveWorkflow(ctx, namespace.Default, wfID, wfV, g2); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	act, ok, err = store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after remove: %v", err)
	}
	if !ok {
		t.Fatal("activation record should still exist (deactivated), got absent")
	}
	if act.Desired {
		t.Fatalf("after remove: want desired=false, got %+v", act)
	}
	// The manager sets desired=false only; it does not fence, so the owner is
	// still recorded until the reconciler deactivates it.
	if act.RunnerID != "runner-zone-b" {
		t.Fatalf("after remove (pre-reconcile): manager must not fence, want runner-zone-b still owner, got %q", act.RunnerID)
	}
	// A reconcile after removal must deactivate the owner and NOT re-assign a
	// non-desired activation.
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile(after remove): %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "" {
		t.Fatalf("reconcile must deactivate + not assign a non-desired activation, got %q", act.RunnerID)
	}
}
