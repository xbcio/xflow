package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// redeliveryTestGroupActivation is testEntryActivation with a GROUP node type
// so the redelivery counts on the group metrics series, and without the
// capability requirement, keeping each test focused on the report-driven
// behavior.
func redeliveryTestGroupActivation() engine.EntryActivation {
	act := testEntryActivation()
	act.NodeType = engine.GroupNodeType
	return act
}

func redeliveryReportItem(act engine.EntryActivation, generation uint64) protocol.ActivationInventoryItem {
	return protocol.ActivationInventoryItem{
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		ReplicaIndex:    act.ReplicaIndex,
		Generation:      generation,
	}
}

func newRedeliveryTestReconciler(store engine.EntryActivationStore, lister ActivationRunnerLister, delivery ActivationDeliveryDirectory, metrics EntryActivationMetrics) *EntryActivationReconciler {
	sel := DefaultRunnerSelector()
	return NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
		Delivery:   delivery,
		Metrics:    metrics,
	})
}

// TestReconcilerRedeliversActivationMissingFromRunnerReport is the core
// scenario: the assignment is healthy (live, matching owner, renewed lease)
// but the runner's own report does not contain it — the Activate directive
// never arrived. The reconciler must re-send it at the store's current
// generation, without touching ownership.
func TestReconcilerRedeliversActivationMissingFromRunnerReport(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-1",
		SessionID:     "sess-1",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}}}
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

	if err := reconciler.Reconcile(ctx, now); err != nil {
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

	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile (redeliver): %v", err)
	}

	redelivered, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1")
	if err != nil {
		t.Fatalf("DirectivesForRunnerPersistent: %v", err)
	}
	if redelivered == nil || len(redelivered.Activate) != 1 {
		t.Fatalf("redelivered = %+v, want one activate", redelivered)
	}
	directive := redelivered.Activate[0]
	if directive.Generation != assigned.Generation {
		t.Fatalf("redelivered generation = %d, want the store's current %d", directive.Generation, assigned.Generation)
	}
	if directive.WorkflowID != string(act.WorkflowID) || directive.EntryUnitID != act.EntryUnitID {
		t.Fatalf("redelivered directive = %+v, want the assigned activation", directive)
	}
	// Ownership must be untouched by a redelivery: same owner, same generation.
	after, _, _ := store.Get(ctx, key)
	if after.RunnerID != assigned.RunnerID || after.Generation != assigned.Generation || after.SessionID != assigned.SessionID {
		t.Fatalf("redelivery changed ownership: before %+v, after %+v", assigned, after)
	}
	if metrics.redeliveries["activate"] != 1 {
		t.Fatalf("activate redeliveries = %d, want 1", metrics.redeliveries["activate"])
	}

	// Once the runner reports hosting it, the next pass sends nothing.
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", []protocol.ActivationInventoryItem{redeliveryReportItem(act, assigned.Generation)}); err != nil {
		t.Fatalf("RecordHostedActivations (hosted): %v", err)
	}
	if err := reconciler.Reconcile(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("Reconcile (hosted): %v", err)
	}
	if extra, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || extra != nil {
		t.Fatalf("after the report shows it hosted: directives = %+v, err = %v; want none", extra, err)
	}
	if metrics.redeliveries["activate"] != 1 {
		t.Fatalf("activate redeliveries = %d, want still 1", metrics.redeliveries["activate"])
	}
}

// TestReconcilerRedeliversAtNewerReportedGenerationStaysQuiet verifies the
// redelivery is skipped when the runner reports the activation at a generation
// at or above the ledger's: it is already hosting.
func TestReconcilerSkipsRedeliveryWhenReportCoversActivation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
		Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
	}}}
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	assigned, _, _ := store.Get(ctx, key)
	if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", []protocol.ActivationInventoryItem{redeliveryReportItem(act, assigned.Generation)}); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if directives, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || directives != nil {
		t.Fatalf("directives = %+v, err = %v; want none when the report covers the assignment", directives, err)
	}
	if metrics.redeliveries["activate"] != 0 {
		t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
	}
}

// TestReconcilerSkipsRedeliveryForUnusableReports pins the two backward-
// compatibility gates: no report at all (old runner) and a report from a
// different session both mean "cannot conclude anything", never "redeliver".
func TestReconcilerSkipsRedeliveryForUnusableReports(t *testing.T) {
	t.Run("no report", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		metrics := newFakeEntryActivationMetrics()
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile (assign): %v", err)
		}
		if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
			t.Fatalf("drain: %v", err)
		}
		// No report was ever recorded.
		if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if directives, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || directives != nil {
			t.Fatalf("directives = %+v, err = %v; want none without a report", directives, err)
		}
		if metrics.redeliveries["activate"] != 0 {
			t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
		}
	})

	t.Run("stale session", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		metrics := newFakeEntryActivationMetrics()
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile (assign): %v", err)
		}
		if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
			t.Fatalf("drain: %v", err)
		}
		// A report from the replaced session must not be read as this
		// session's "hosts nothing".
		if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-old", nil); err != nil {
			t.Fatalf("RecordHostedActivations: %v", err)
		}
		if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if directives, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || directives != nil {
			t.Fatalf("directives = %+v, err = %v; want none for a session-mismatched report", directives, err)
		}
		if metrics.redeliveries["activate"] != 0 {
			t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
		}
	})
}

// TestReconcilerRedeliveryReReadsStore pins the last-line defense: when the
// re-read disagrees with the pass's snapshot (owner moved) or fails, nothing
// is sent. A directive built from a stale owner or generation is worse than a
// missed redelivery — the next pass re-derives everything from the store.
func TestReconcilerRedeliveryReReadsStore(t *testing.T) {
	t.Run("owner moved", func(t *testing.T) {
		ctx := context.Background()
		base := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		if err := base.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		store := &ownerOverrideEntryActivationStore{EntryActivationStore: base, runnerID: "runner-other", sessionID: "sess-other"}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		metrics := newFakeEntryActivationMetrics()
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile (assign): %v", err)
		}
		if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
			t.Fatalf("RecordHostedActivations: %v", err)
		}
		if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if directives, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || directives != nil {
			t.Fatalf("directives = %+v, err = %v; want none when the re-read shows a moved owner", directives, err)
		}
		if metrics.redeliveries["activate"] != 0 {
			t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
		}
	})

	t.Run("re-read fails", func(t *testing.T) {
		ctx := context.Background()
		base := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		if err := base.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		store := &getErrorEntryActivationStore{EntryActivationStore: base, err: errors.New("store unavailable")}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		metrics := newFakeEntryActivationMetrics()
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile (assign): %v", err)
		}
		if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
			t.Fatalf("RecordHostedActivations: %v", err)
		}
		if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if directives, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1"); err != nil || directives != nil {
			t.Fatalf("directives = %+v, err = %v; want none when the re-read fails", directives, err)
		}
		if metrics.redeliveries["activate"] != 0 {
			t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
		}
	})
}

// TestReconcilerSendsDeactivateForForeignHostedActivation covers the reverse
// direction: a runner reports hosting an activation the ledger no longer
// assigns to it (here: owner empty after a fence). It must receive a
// deactivate carrying the ledger's generation — and the ledger must not be
// written to.
func TestReconcilerSendsDeactivateForForeignHostedActivation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	// Sorted runner IDs decide assignment order: runner-a takes the first pass.
	lister := &mockRunnerLister{runners: []RunnerSnapshot{
		{RunnerID: "runner-a", SessionID: "sess-a", Capacity: 4, Labels: map[string]string{"zone": "a"}, LastHeartbeat: now},
		{RunnerID: "runner-b", SessionID: "sess-b", Capacity: 4, Labels: map[string]string{"zone": "a"}, LastHeartbeat: now},
	}}
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	assigned, _, _ := store.Get(ctx, key)
	if assigned.RunnerID != "runner-a" {
		t.Fatalf("assignment runner = %q, want runner-a", assigned.RunnerID)
	}
	if _, _, err := delivery.ActivationDirectives(ctx, "runner-a", "sess-a"); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// The ledger moves on (fence), while runner-b keeps reporting that it
	// hosts the activation at the old generation.
	if err := store.Fence(ctx, key, assigned.Generation); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if err := delivery.RecordHostedActivations(ctx, "runner-b", "sess-b", []protocol.ActivationInventoryItem{redeliveryReportItem(act, assigned.Generation)}); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}

	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile (cleanup): %v", err)
	}
	_, deactivates, err := delivery.ActivationDirectives(ctx, "runner-b", "sess-b")
	if err != nil {
		t.Fatalf("drain runner-b: %v", err)
	}
	if len(deactivates) != 1 {
		t.Fatalf("runner-b deactivates = %+v, want exactly one cleanup directive", deactivates)
	}
	if deactivates[0].Generation != assigned.Generation {
		t.Fatalf("cleanup generation = %d, want the ledger's %d", deactivates[0].Generation, assigned.Generation)
	}
	if metrics.redeliveries["deactivate"] != 1 {
		t.Fatalf("deactivate redeliveries = %d, want 1", metrics.redeliveries["deactivate"])
	}
	// runner-a never reported hosting anything, so it must not be told to
	// stop; it received the reassignment instead.
	activationsA, _, err := delivery.ActivationDirectives(ctx, "runner-a", "sess-a")
	if err != nil {
		t.Fatalf("drain runner-a: %v", err)
	}
	if len(activationsA) != 1 || activationsA[0].Generation <= assigned.Generation {
		t.Fatalf("runner-a directives = %+v, want the reassignment at a newer generation", activationsA)
	}
	// The cleanup wrote nothing to the ledger: fencing stays the reconciler's
	// ownership decision, not this check's.
	after, _, _ := store.Get(ctx, key)
	if after.Generation < assigned.Generation {
		t.Fatalf("ledger generation moved backwards: %d -> %d", assigned.Generation, after.Generation)
	}
}

// TestReconcilerNeverCleansUpTheLedgerOwner verifies the negative case of the
// reverse direction: when the reporter IS the ledger owner, no deactivate is
// sent.
func TestReconcilerNeverCleansUpTheLedgerOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
		Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
	}}}
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("Reconcile (assign): %v", err)
	}
	assigned, _, _ := store.Get(ctx, key)
	if _, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", []protocol.ActivationInventoryItem{redeliveryReportItem(act, assigned.Generation)}); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
		t.Fatalf("drain: %v", err)
	} else if len(deactivates) != 0 {
		t.Fatalf("deactivates = %+v, want none for the ledger owner", deactivates)
	}
	if metrics.redeliveries["deactivate"] != 0 {
		t.Fatalf("deactivate redeliveries = %d, want 0", metrics.redeliveries["deactivate"])
	}
}

// TestHeartbeatRecordsHostedActivationsReport pins the server-side storage
// contract: a present report is recorded verbatim for the validated session,
// and an absent one (old runner) writes nothing at all.
func TestHeartbeatRecordsHostedActivationsReport(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	directory := NewMemoryRunnerDirectory()
	session := registerCleanupRunner(t, ctx, directory, "runner-hosted", now, nil)
	core := &Core{runners: directory}

	// An old runner (nil field) records nothing.
	if _, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 2, Timestamp: now.Unix(),
	}, TransportInfo{}); err != nil {
		t.Fatalf("heartbeat without report: %v", err)
	}
	if _, ok, err := directory.HostedActivations(ctx, session.RunnerID); err != nil || ok {
		t.Fatalf("report after a nil-field heartbeat: ok=%v err=%v, want none", ok, err)
	}

	items := []protocol.ActivationInventoryItem{{WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "tg", Generation: 3}}
	if _, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 2, Timestamp: now.Unix(),
		HostedActivations: &protocol.HostedActivationsReport{Activations: items},
	}, TransportInfo{}); err != nil {
		t.Fatalf("heartbeat with report: %v", err)
	}
	report, ok, err := directory.HostedActivations(ctx, session.RunnerID)
	if err != nil || !ok {
		t.Fatalf("HostedActivations: ok=%v err=%v", ok, err)
	}
	if report.SessionID != session.SessionID || len(report.Items) != 1 || report.Items[0].Generation != 3 {
		t.Fatalf("recorded report = %+v, want the sent item under session %q", report, session.SessionID)
	}

	// A present-but-empty report is a real write: "hosts nothing".
	if _, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 2, Timestamp: now.Unix(),
		HostedActivations: &protocol.HostedActivationsReport{},
	}, TransportInfo{}); err != nil {
		t.Fatalf("heartbeat with empty report: %v", err)
	}
	report, ok, err = directory.HostedActivations(ctx, session.RunnerID)
	if err != nil || !ok {
		t.Fatalf("HostedActivations after empty report: ok=%v err=%v", ok, err)
	}
	if len(report.Items) != 0 {
		t.Fatalf("recorded report = %+v, want the empty report to replace the previous one", report)
	}
}

// ownerOverrideEntryActivationStore disguises the stored owner, simulating a
// concurrent fence/reassign landing between the pass's List and the
// redelivery's re-read.
type ownerOverrideEntryActivationStore struct {
	engine.EntryActivationStore
	runnerID  string
	sessionID string
}

func (s *ownerOverrideEntryActivationStore) Get(ctx context.Context, key engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	act, ok, err := s.EntryActivationStore.Get(ctx, key)
	if err != nil || !ok {
		return act, ok, err
	}
	act.RunnerID = s.runnerID
	act.SessionID = s.sessionID
	return act, true, nil
}

// getErrorEntryActivationStore fails every Get, simulating the store being
// unreadable at re-read time.
type getErrorEntryActivationStore struct {
	engine.EntryActivationStore
	err error
}

func (s *getErrorEntryActivationStore) Get(context.Context, engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	return engine.EntryActivation{}, false, s.err
}
