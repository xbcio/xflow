package control

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

func testActivateDirective(workflowID, entryUnitID string, generation uint64) protocol.ActivateDirective {
	return protocol.ActivateDirective{
		Namespace:       "default",
		WorkflowID:      workflowID,
		WorkflowVersion: "v1",
		EntryUnitID:     entryUnitID,
		Generation:      generation,
		NodeType:        "kafka.source",
	}
}

func testDeactivateDirective(workflowID, entryUnitID string, generation uint64) protocol.DeactivateDirective {
	return protocol.DeactivateDirective{
		Namespace:       "default",
		WorkflowID:      workflowID,
		WorkflowVersion: "v1",
		EntryUnitID:     entryUnitID,
		Generation:      generation,
	}
}

func TestActivationDeliveryQueueRoundTrip(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()

	activate := testActivateDirective("wf-a", "tg-a", 3)
	deactivate := testDeactivateDirective("wf-b", "tg-b", 2)
	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", activate); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}
	if err := directory.EnqueueDeactivationDirective(ctx, "runner-1", "sess-1", deactivate); err != nil {
		t.Fatalf("EnqueueDeactivationDirective: %v", err)
	}

	activates, deactivates, err := directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives: %v", err)
	}
	if len(activates) != 1 || activates[0].WorkflowID != "wf-a" || activates[0].Generation != 3 {
		t.Fatalf("activates = %+v, want the enqueued wf-a@3", activates)
	}
	if len(deactivates) != 1 || deactivates[0].WorkflowID != "wf-b" || deactivates[0].Generation != 2 {
		t.Fatalf("deactivates = %+v, want the enqueued wf-b@2", deactivates)
	}

	// Drain-once: a second drain returns nothing.
	activates, deactivates, err = directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives (second): %v", err)
	}
	if len(activates) != 0 || len(deactivates) != 0 {
		t.Fatalf("second drain returned %d/%d directives, want none", len(activates), len(deactivates))
	}
}

func TestActivationDeliveryQueueSessionIsolation(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()

	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 1)); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}

	// A directive enqueued for sess-1 must be unreachable from sess-2: after a
	// reconnect the old session's directives are void by construction.
	activates, deactivates, err := directory.ActivationDirectives(ctx, "runner-1", "sess-2")
	if err != nil {
		t.Fatalf("ActivationDirectives (other session): %v", err)
	}
	if len(activates) != 0 || len(deactivates) != 0 {
		t.Fatalf("other session drained %d/%d directives, want none", len(activates), len(deactivates))
	}

	// The original session still drains its own queue.
	activates, _, err = directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives (original session): %v", err)
	}
	if len(activates) != 1 {
		t.Fatalf("original session drained %d activates, want 1", len(activates))
	}
}

func TestActivationDeliveryQueueDeduplicatesByIdentity(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()

	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 1)); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 2)); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	// A different replica of the same workflow is a different identity.
	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", protocol.ActivateDirective{
		Namespace: "default", WorkflowID: "wf-a", WorkflowVersion: "v1", EntryUnitID: "tg-a", ReplicaIndex: 1, Generation: 1,
	}); err != nil {
		t.Fatalf("replica enqueue: %v", err)
	}

	activates, _, err := directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives: %v", err)
	}
	if len(activates) != 2 {
		t.Fatalf("activates = %+v, want 2 (one overwritten identity + one replica)", activates)
	}
	for _, activate := range activates {
		if activate.ReplicaIndex == 0 && activate.Generation != 2 {
			t.Fatalf("re-enqueued identity delivered generation %d, want the newest (2)", activate.Generation)
		}
	}
}

// TestActivationDeliveryQueueExpiresStaleSession verifies the TTL semantics:
// a queue nobody drained for its session does not linger forever.
func TestActivationDeliveryQueueExpiresStaleSession(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	directory := NewMemoryRunnerDirectory(WithMemoryRunnerDirectoryClock(func() time.Time { return now }))

	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 1)); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}

	now = now.Add(activationDirectiveQueueTTL + time.Second)
	activates, _, err := directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives: %v", err)
	}
	if len(activates) != 0 {
		t.Fatalf("expired queue delivered %d activates, want none", len(activates))
	}
}

// TestResolveActivationDirectiveConflicts pins the cross-type rule: the higher
// generation wins, and a tie resolves to the deactivate (the stop is the later
// intent of the two).
func TestResolveActivationDirectiveConflicts(t *testing.T) {
	key := activationDirectiveKey("default", "wf-a", "v1", "tg-a", 0)
	encode := func(v any) string {
		t.Helper()
		payload, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return string(payload)
	}

	t.Run("newer deactivate wins", func(t *testing.T) {
		activates, deactivates, err := resolveActivationDirectiveConflicts(map[string]string{
			activationDirectiveFieldActivate + key:   encode(testActivateDirective("wf-a", "tg-a", 1)),
			activationDirectiveFieldDeactivate + key: encode(testDeactivateDirective("wf-a", "tg-a", 2)),
		})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(activates) != 0 || len(deactivates) != 1 || deactivates[0].Generation != 2 {
			t.Fatalf("activates = %+v, deactivates = %+v; want only deactivate@2", activates, deactivates)
		}
	})

	t.Run("newer activate wins", func(t *testing.T) {
		activates, deactivates, err := resolveActivationDirectiveConflicts(map[string]string{
			activationDirectiveFieldDeactivate + key: encode(testDeactivateDirective("wf-a", "tg-a", 1)),
			activationDirectiveFieldActivate + key:   encode(testActivateDirective("wf-a", "tg-a", 2)),
		})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(activates) != 1 || activates[0].Generation != 2 || len(deactivates) != 0 {
			t.Fatalf("activates = %+v, deactivates = %+v; want only activate@2", activates, deactivates)
		}
	})

	t.Run("tie resolves to deactivate", func(t *testing.T) {
		activates, deactivates, err := resolveActivationDirectiveConflicts(map[string]string{
			activationDirectiveFieldActivate + key:   encode(testActivateDirective("wf-a", "tg-a", 5)),
			activationDirectiveFieldDeactivate + key: encode(testDeactivateDirective("wf-a", "tg-a", 5)),
		})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(activates) != 0 || len(deactivates) != 1 {
			t.Fatalf("activates = %+v, deactivates = %+v; want only deactivate@5", activates, deactivates)
		}
	})

	t.Run("distinct identities both survive", func(t *testing.T) {
		otherKey := activationDirectiveKey("default", "wf-b", "v1", "tg-b", 0)
		activates, deactivates, err := resolveActivationDirectiveConflicts(map[string]string{
			activationDirectiveFieldActivate + key:        encode(testActivateDirective("wf-a", "tg-a", 1)),
			activationDirectiveFieldDeactivate + otherKey: encode(testDeactivateDirective("wf-b", "tg-b", 1)),
		})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(activates) != 1 || len(deactivates) != 1 {
			t.Fatalf("activates = %+v, deactivates = %+v; want one of each", activates, deactivates)
		}
	})
}

// TestReconcilerPersistsDirectivesThroughDeliveryCapability verifies the
// wiring end to end at the reconciler boundary: with the capability present,
// an assignment enqueues into the durable per-session queue (not the
// leader-local map) and the heartbeat-facing drain reads that queue.
func TestReconcilerPersistsDirectivesThroughDeliveryCapability(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := testEntryActivation()
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-1",
		SessionID:     "sess-1",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
		Delivery:   delivery,
	})

	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The leader-local queue must stay empty: the directive lives in the
	// session-scoped durable queue now.
	if legacy := reconciler.DirectivesForRunner("runner-1"); legacy != nil {
		t.Fatalf("leader-local queue = %+v, want empty once the capability is wired", legacy)
	}

	activations, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("DirectivesForRunnerPersistent: %v", err)
	}
	if activations == nil || len(activations.Activate) != 1 {
		t.Fatalf("persistent drain = %+v, want one activate", activations)
	}
	if got := activations.Activate[0]; got.WorkflowID != "wf-1" || got.EntryUnitID != "tg" || got.Generation != 1 {
		t.Fatalf("activate directive = %+v, want wf-1/tg@1", got)
	}

	// Drain-once, and a different session sees nothing.
	if again, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || again != nil {
		t.Fatalf("second persistent drain = %+v, err = %v; want nil", again, err)
	}
	if other, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-2"); err != nil || other != nil {
		t.Fatalf("other-session drain = %+v, err = %v; want nil", other, err)
	}
}

// TestReconcilerDeliveryErrorSurfaces verifies a durable-queue drain failure
// is returned to the caller (the heartbeat handler turns it into a failed
// heartbeat) rather than being reported as "no directives".
func TestReconcilerDeliveryErrorSurfaces(t *testing.T) {
	ctx := context.Background()
	failing := &failingActivationDeliveryDirectory{err: errTestDeliveryUnavailable}
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      NewMemoryEntryActivationStore(),
		Namespaces: []namespace.Namespace{namespace.Default},
		Delivery:   failing,
	})
	if _, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); !errors.Is(err, errTestDeliveryUnavailable) {
		t.Fatalf("err = %v, want the delivery error surfaced", err)
	}
	// Without a session the durable path is unusable; the legacy queue is the
	// documented fallback and must not touch the directory.
	if acts, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", ""); err != nil || acts != nil {
		t.Fatalf("sessionless drain = %+v, err = %v; want the legacy empty result", acts, err)
	}
	if failing.calls != 1 {
		t.Fatalf("directory calls = %d, want 1 (the sessionless call must not reach it)", failing.calls)
	}
}

var errTestDeliveryUnavailable = errors.New("delivery unavailable")

// failingActivationDeliveryDirectory implements ActivationDeliveryDirectory by
// failing the drain; the enqueue/record/report methods are never exercised
// through it.
type failingActivationDeliveryDirectory struct {
	err   error
	calls int
}

func (d *failingActivationDeliveryDirectory) EnqueueActivationDirective(context.Context, string, string, protocol.ActivateDirective) error {
	return nil
}

func (d *failingActivationDeliveryDirectory) EnqueueDeactivationDirective(context.Context, string, string, protocol.DeactivateDirective) error {
	return nil
}

func (d *failingActivationDeliveryDirectory) ActivationDirectives(context.Context, string, string) ([]protocol.ActivateDirective, []protocol.DeactivateDirective, error) {
	d.calls++
	return nil, nil, d.err
}

func (d *failingActivationDeliveryDirectory) RecordHostedActivations(context.Context, string, string, []protocol.ActivationInventoryItem) error {
	return nil
}

func (d *failingActivationDeliveryDirectory) HostedActivations(context.Context, string) (HostedActivationsReport, bool, error) {
	return HostedActivationsReport{}, false, nil
}

func TestRedisActivationDeliveryQueueRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 3)); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}
	if err := directory.EnqueueDeactivationDirective(ctx, "runner-1", "sess-1", testDeactivateDirective("wf-b", "tg-b", 2)); err != nil {
		t.Fatalf("EnqueueDeactivationDirective: %v", err)
	}

	// A different session cannot see the queue (session-scoped key).
	activates, deactivates, err := directory.ActivationDirectives(ctx, "runner-1", "sess-2")
	if err != nil {
		t.Fatalf("drain other session: %v", err)
	}
	if len(activates) != 0 || len(deactivates) != 0 {
		t.Fatalf("other session drained %d/%d, want none", len(activates), len(deactivates))
	}

	activates, deactivates, err = directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(activates) != 1 || activates[0].WorkflowID != "wf-a" || activates[0].Generation != 3 {
		t.Fatalf("activates = %+v, want wf-a@3", activates)
	}
	if len(deactivates) != 1 || deactivates[0].WorkflowID != "wf-b" {
		t.Fatalf("deactivates = %+v, want wf-b", deactivates)
	}
	if server.Exists(directory.keys.activationDirectiveQueueKey("runner-1", "sess-1")) {
		t.Fatal("drained queue key still exists; drain must delete what it read")
	}

	// Drain-once across a fresh call.
	activates, deactivates, err = directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if len(activates) != 0 || len(deactivates) != 0 {
		t.Fatalf("second drain returned %d/%d, want none", len(activates), len(deactivates))
	}
}

func TestRedisActivationDeliveryQueueExpires(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", testActivateDirective("wf-a", "tg-a", 1)); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}
	server.FastForward(activationDirectiveQueueTTL + time.Minute)

	activates, _, err := directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("drain after TTL: %v", err)
	}
	if len(activates) != 0 {
		t.Fatalf("expired queue delivered %d activates, want none", len(activates))
	}
}

// TestRedisHostedActivationsReportRoundTripAndFreshness pins the report
// contract: what was recorded is readable (with exact uint64 generations),
// it carries the reporting session, and it stops being fresh once its TTL
// lapses.
func TestRedisHostedActivationsReportRoundTripAndFreshness(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	items := []protocol.ActivationInventoryItem{{
		WorkflowID:      "wf-a",
		WorkflowVersion: "v1",
		EntryUnitID:     "tg-a",
		ReplicaIndex:    1,
		// Above 2^53: the report codec must not lose exact fencing values.
		Generation: 1<<53 + 7,
	}}
	if err := directory.RecordHostedActivations(ctx, "runner-1", "sess-1", items); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}

	report, ok, err := directory.HostedActivations(ctx, "runner-1")
	if err != nil || !ok {
		t.Fatalf("HostedActivations: ok=%v err=%v", ok, err)
	}
	if report.SessionID != "sess-1" {
		t.Fatalf("report session = %q, want sess-1", report.SessionID)
	}
	if len(report.Items) != 1 || report.Items[0].Generation != 1<<53+7 || report.Items[0].ReplicaIndex != 1 {
		t.Fatalf("report items = %+v, want the recorded item with an exact generation", report.Items)
	}

	// A runner that never reported has no report.
	if _, ok, err := directory.HostedActivations(ctx, "runner-2"); err != nil || ok {
		t.Fatalf("unknown runner: ok=%v err=%v, want ok=false", ok, err)
	}

	server.FastForward(activationHostedReportTTL + time.Second)
	if _, ok, err := directory.HostedActivations(ctx, "runner-1"); err != nil || ok {
		t.Fatalf("stale report: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestMemoryHostedActivationsReportFreshness(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	directory := NewMemoryRunnerDirectory(WithMemoryRunnerDirectoryClock(func() time.Time { return now }))

	if err := directory.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}
	// An empty (but present) report must read as fresh and explicitly empty —
	// that is the "I report, and I host nothing" signal the reconciler needs.
	report, ok, err := directory.HostedActivations(ctx, "runner-1")
	if err != nil || !ok {
		t.Fatalf("fresh empty report: ok=%v err=%v", ok, err)
	}
	if report.SessionID != "sess-1" || len(report.Items) != 0 {
		t.Fatalf("report = %+v, want empty items with session sess-1", report)
	}

	now = now.Add(activationHostedReportTTL + time.Second)
	if _, ok, err := directory.HostedActivations(ctx, "runner-1"); err != nil || ok {
		t.Fatalf("stale report: ok=%v err=%v, want ok=false", ok, err)
	}
}

// TestActivationDirectiveKeyIsDelimiterSafe pins the F6 fix: the dedup key is
// a length-prefixed SHA-256 of the identity components, so two distinct
// activations whose components only differ in how a delimiter would have split
// them can never share a queue field and evict each other.
func TestActivationDirectiveKeyIsDelimiterSafe(t *testing.T) {
	left := activationDirectiveKey("default", "wf-a", "v1/x", "tg", 0)
	right := activationDirectiveKey("default", "wf-a", "v1", "x/tg", 0)
	if left == right {
		t.Fatalf("ambiguous components collided on one key: %q", left)
	}
	// Identical identities still render identically — the dedup feature the
	// key exists for is unchanged.
	if activationDirectiveKey("default", "wf-a", "v1", "tg", 2) != activationDirectiveKey("default", "wf-a", "v1", "tg", 2) {
		t.Fatal("same identity rendered two different keys")
	}
	if activationDirectiveKey("default", "wf-a", "v1", "tg", 0) == activationDirectiveKey("default", "wf-a", "v1", "tg", 1) {
		t.Fatal("different replicas rendered the same key")
	}

	// End to end: both ambiguous activations can be pending at once.
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	first := protocol.ActivateDirective{Namespace: "default", WorkflowID: "wf-a", WorkflowVersion: "v1/x", EntryUnitID: "tg", Generation: 1}
	second := protocol.ActivateDirective{Namespace: "default", WorkflowID: "wf-a", WorkflowVersion: "v1", EntryUnitID: "x/tg", Generation: 1}
	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", first); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := directory.EnqueueActivationDirective(ctx, "runner-1", "sess-1", second); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	activates, _, err := directory.ActivationDirectives(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("ActivationDirectives: %v", err)
	}
	if len(activates) != 2 {
		t.Fatalf("drained %d activates, want both ambiguous identities to survive", len(activates))
	}
}
