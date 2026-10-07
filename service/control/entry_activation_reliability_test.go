package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// --- F1a: redelivery is paced by the shared retry backoff ------------------

// TestReconcilerPacesRedeliveryThroughSharedBackoff pins the livelock fix: a
// lost directive and a lost ack must not re-send once per reconcile pass. The
// first missing-report pass redelivers immediately; a pass inside the backoff
// window withholds the directive without counting anything; once the window
// elapses the delay has doubled; and a report that flips back to "hosted"
// clears the scheduling half so the ladder restarts at the minimum on the next
// loss.
func TestReconcilerPacesRedeliveryThroughSharedBackoff(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := redeliveryTestGroupActivation()
	key := keyOfActivation(act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	t0 := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	delivery := NewMemoryRunnerDirectory()
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID: "runner-1", SessionID: "sess-1", Capacity: 4,
		Labels: map[string]string{"zone": "a"}, LastHeartbeat: t0,
	}}}
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(store, lister, delivery, metrics)

	// Every pass after the first simulates a fresh heartbeat so the runner
	// stays live as the fake clock advances.
	reconcileAt := func(at time.Time) {
		t.Helper()
		lister.runners[0].LastHeartbeat = at
		if err := reconciler.Reconcile(ctx, at); err != nil {
			t.Fatalf("Reconcile at %s: %v", at, err)
		}
	}
	drain := func() *protocol.HeartbeatActivations {
		t.Helper()
		got, err := reconciler.DirectivesForRunnerPersistent(ctx, "runner-1", "sess-1")
		if err != nil {
			t.Fatalf("DirectivesForRunnerPersistent: %v", err)
		}
		return got
	}

	reconcileAt(t0)
	assigned, _, _ := store.Get(ctx, key)
	if drain() == nil {
		t.Fatal("assignment directive missing")
	}
	// The runner reports hosting nothing: the directive was lost.
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
		t.Fatalf("RecordHostedActivations: %v", err)
	}

	// Pass 1: first missing report, no recorded failure — redeliver at once.
	reconcileAt(t0.Add(time.Second))
	if got := drain(); got == nil || len(got.Activate) != 1 {
		t.Fatalf("first redelivery = %+v, want one activate", got)
	}
	if metrics.redeliveries["activate"] != 1 {
		t.Fatalf("activate redeliveries = %d, want 1", metrics.redeliveries["activate"])
	}
	delay1 := reconciler.retryDelayFor(key)
	if delay1 <= 0 {
		t.Fatal("a redelivery must record a failure so the next attempt is paced")
	}

	// Pass 2: inside the backoff window — nothing sent, nothing counted, and
	// the delay did not grow (a withheld attempt is not a failure).
	reconcileAt(t0.Add(2 * time.Second))
	if got := drain(); got != nil {
		t.Fatalf("directives inside the backoff window = %+v, want none", got)
	}
	if metrics.redeliveries["activate"] != 1 {
		t.Fatalf("activate redeliveries = %d, want still 1", metrics.redeliveries["activate"])
	}
	if got := reconciler.retryDelayFor(key); got != delay1 {
		t.Fatalf("retry delay after a withheld pass = %s, want unchanged %s", got, delay1)
	}

	// Pass 3: past the window (max jittered delay is 1.2*10s = 12s; t0+15s is
	// beyond it) — redelivered, and the delay has doubled.
	reconcileAt(t0.Add(15 * time.Second))
	if got := drain(); got == nil || len(got.Activate) != 1 {
		t.Fatalf("second redelivery = %+v, want one activate", got)
	}
	if metrics.redeliveries["activate"] != 2 {
		t.Fatalf("activate redeliveries = %d, want 2", metrics.redeliveries["activate"])
	}
	delay2 := reconciler.retryDelayFor(key)
	if delay2 <= delay1 {
		t.Fatalf("retry delay did not grow: %s -> %s", delay1, delay2)
	}

	// Pass 4: t0+45s is past max jitter of the second delay (15s + 24s), so a
	// third redelivery lands and the delay doubles again.
	reconcileAt(t0.Add(45 * time.Second))
	if got := drain(); got == nil || len(got.Activate) != 1 {
		t.Fatalf("third redelivery = %+v, want one activate", got)
	}
	delay3 := reconciler.retryDelayFor(key)
	if delay3 <= delay2 {
		t.Fatalf("retry delay did not grow: %s -> %s", delay2, delay3)
	}

	// The report flips back to "hosted": the next pass clears the scheduling
	// half (diagnostics retained) and sends nothing.
	current, _, _ := store.Get(ctx, key)
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", []protocol.ActivationInventoryItem{redeliveryReportItem(act, current.Generation)}); err != nil {
		t.Fatalf("RecordHostedActivations (hosted): %v", err)
	}
	reconcileAt(t0.Add(46 * time.Second))
	if got := drain(); got != nil {
		t.Fatalf("directives after recovery = %+v, want none", got)
	}
	if got := reconciler.retryDelayFor(key); got != 0 {
		t.Fatalf("retry delay after recovery = %s, want 0 (scheduling cleared)", got)
	}

	// And a fresh loss starts over at the minimum delay instead of the
	// lingering long one.
	if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-1", nil); err != nil {
		t.Fatalf("RecordHostedActivations (lost again): %v", err)
	}
	reconcileAt(t0.Add(47 * time.Second))
	if got := drain(); got == nil || len(got.Activate) != 1 {
		t.Fatalf("redelivery after a fresh loss = %+v, want one activate", got)
	}
	if delay4 := reconciler.retryDelayFor(key); delay4 >= delay3 {
		t.Fatalf("ladder did not restart: delay after recovery = %s, want below %s", delay4, delay3)
	}

	// Pacing never touched ownership.
	after, _, _ := store.Get(ctx, key)
	if after.Generation != assigned.Generation || after.RunnerID != assigned.RunnerID {
		t.Fatalf("pacing changed ownership: before %+v, after %+v", assigned, after)
	}
}

// --- F2: session-less directives are never written to a dead-letter map ----

// TestDirectiveDeliveryDropsSessionlessDirectivesWhenCapabilityWired pins the
// dead-letter fix: with the durable capability wired, the leader-local map is
// drained by nobody (heartbeats read the durable queue), so a session-less
// directive must be dropped loudly rather than silently parked forever.
func TestDirectiveDeliveryDropsSessionlessDirectivesWhenCapabilityWired(t *testing.T) {
	ctx := context.Background()
	delivery := NewMemoryRunnerDirectory()
	metrics := newFakeEntryActivationMetrics()
	reconciler := newRedeliveryTestReconciler(NewMemoryEntryActivationStore(), &mockRunnerLister{}, delivery, metrics)

	reconciler.deliverActivationDirective(ctx, "runner-1", "", testActivateDirective("wf-1", "tg", 4))
	reconciler.deliverDeactivationDirective(ctx, "runner-1", "", testDeactivateDirective("wf-1", "tg", 4))
	if got := reconciler.DirectivesForRunner("runner-1"); got != nil {
		t.Fatalf("session-less directives landed in the leader-local map: %+v", got)
	}
	// And they did not leak into some session's durable queue either.
	if activates, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-1"); err != nil {
		t.Fatalf("ActivationDirectives: %v", err)
	} else if len(activates) != 0 || len(deactivates) != 0 {
		t.Fatalf("session-less directives reached a durable queue: %d/%d", len(activates), len(deactivates))
	}
}

// TestReconcilerFenceResolvesSessionForDelivery covers the write side of the
// session resolution: fencing a record whose SessionID predates session
// tracking must address the directive to a real session — the recorded one
// when present, otherwise the live snapshot's — instead of dead-lettering it.
func TestReconcilerFenceResolvesSessionForDelivery(t *testing.T) {
	t.Run("recorded session wins", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		act.Desired = false
		act.RunnerID = "runner-1"
		act.SessionID = "sess-recorded"
		act.Generation = 3
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-live", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, newFakeEntryActivationMetrics())

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if _, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-recorded"); err != nil {
			t.Fatalf("drain recorded session: %v", err)
		} else if len(deactivates) != 1 || deactivates[0].Generation != 3 {
			t.Fatalf("deactivates in the recorded session = %+v, want one at generation 3", deactivates)
		}
		if _, deactivates, _ := delivery.ActivationDirectives(ctx, "runner-1", "sess-live"); len(deactivates) != 0 {
			t.Fatalf("deactivates leaked into the live session: %+v", deactivates)
		}
	})

	t.Run("empty session resolved from the live snapshot", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		act.Desired = false
		act.RunnerID = "runner-1"
		act.SessionID = ""
		act.Generation = 3
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-live", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, newFakeEntryActivationMetrics())

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if _, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-live"); err != nil {
			t.Fatalf("drain live session: %v", err)
		} else if len(deactivates) != 1 || deactivates[0].Generation != 3 {
			t.Fatalf("deactivates in the live session = %+v, want one at generation 3", deactivates)
		}
		if got := reconciler.DirectivesForRunner("runner-1"); got != nil {
			t.Fatalf("fence directive dead-lettered into the leader-local map: %+v", got)
		}
	})

	t.Run("invalid owner fence resolves an empty session", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		act.RunnerID = "runner-1"
		act.SessionID = ""
		act.Generation = 4
		// The desired selector no longer matches the runner's labels, so the
		// health check fences rather than renews.
		act.Selector = &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "b"}}
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-live", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		reconciler := newRedeliveryTestReconciler(store, lister, delivery, newFakeEntryActivationMetrics())

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if _, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-live"); err != nil {
			t.Fatalf("drain live session: %v", err)
		} else if len(deactivates) != 1 || deactivates[0].Generation != 4 {
			t.Fatalf("deactivates in the live session = %+v, want one at generation 4", deactivates)
		}
		if got := reconciler.DirectivesForRunner("runner-1"); got != nil {
			t.Fatalf("fence directive dead-lettered into the leader-local map: %+v", got)
		}
	})

	t.Run("inventory revoke resolves an empty session", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		act.RunnerID = "runner-1"
		act.SessionID = ""
		act.Generation = 6
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		delivery := NewMemoryRunnerDirectory()
		reconciler := newRedeliveryTestReconciler(store, &mockRunnerLister{}, delivery, newFakeEntryActivationMetrics())

		// The reconnected session reports an EMPTY inventory: the activation is
		// not hosted anymore and must be revoked toward the reporting session.
		if err := reconciler.ReconcileRunnerInventorySession(ctx, "runner-1", "sess-inv", nil, now); err != nil {
			t.Fatalf("ReconcileRunnerInventorySession: %v", err)
		}
		if _, deactivates, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-inv"); err != nil {
			t.Fatalf("drain reporting session: %v", err)
		} else if len(deactivates) != 1 || deactivates[0].Generation != 6 {
			t.Fatalf("deactivates in the reporting session = %+v, want one at generation 6", deactivates)
		}
		if got := reconciler.DirectivesForRunner("runner-1"); got != nil {
			t.Fatalf("revoke directive dead-lettered into the leader-local map: %+v", got)
		}
	})
}

// --- F3: empty-session records are repaired through the report's session ---

// TestReconcilerRedeliversForEmptySessionRecord covers the population that
// predates session tracking: the owner is known, the session is not, and
// Renew never backfills it — so the report's session is the only address the
// repair can use. Without a report there is no address and the record is left
// alone.
func TestReconcilerRedeliversForEmptySessionRecord(t *testing.T) {
	setup := func(t *testing.T) (context.Context, *MemoryEntryActivationStore, *MemoryRunnerDirectory, *EntryActivationReconciler, *fakeEntryActivationMetrics, time.Time) {
		t.Helper()
		ctx := context.Background()
		store := NewMemoryEntryActivationStore()
		act := redeliveryTestGroupActivation()
		act.RunnerID = "runner-1"
		act.SessionID = ""
		act.Generation = 5
		now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		act.LeaseDeadline = now.Add(time.Minute)
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		delivery := NewMemoryRunnerDirectory()
		lister := &mockRunnerLister{runners: []RunnerSnapshot{{
			RunnerID: "runner-1", SessionID: "sess-live", Capacity: 4,
			Labels: map[string]string{"zone": "a"}, LastHeartbeat: now,
		}}}
		metrics := newFakeEntryActivationMetrics()
		return ctx, store, delivery, newRedeliveryTestReconciler(store, lister, delivery, metrics), metrics, now
	}

	t.Run("report supplies the delivery session", func(t *testing.T) {
		ctx, _, delivery, reconciler, metrics, now := setup(t)
		if err := delivery.RecordHostedActivations(ctx, "runner-1", "sess-live", nil); err != nil {
			t.Fatalf("RecordHostedActivations: %v", err)
		}

		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		activates, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-live")
		if err != nil {
			t.Fatalf("drain live session: %v", err)
		}
		if len(activates) != 1 {
			t.Fatalf("activates = %+v, want the redelivered directive in the report's session", activates)
		}
		if activates[0].Generation != 5 {
			t.Fatalf("redelivered generation = %d, want the ledger's 5", activates[0].Generation)
		}
		if metrics.redeliveries["activate"] != 1 {
			t.Fatalf("activate redeliveries = %d, want 1", metrics.redeliveries["activate"])
		}
	})

	t.Run("without a report there is no address", func(t *testing.T) {
		ctx, _, delivery, reconciler, metrics, now := setup(t)
		if err := reconciler.Reconcile(ctx, now); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if activates, _, err := delivery.ActivationDirectives(ctx, "runner-1", "sess-live"); err != nil {
			t.Fatalf("drain live session: %v", err)
		} else if len(activates) != 0 {
			t.Fatalf("activates = %+v, want none without a report", activates)
		}
		if metrics.redeliveries["activate"] != 0 {
			t.Fatalf("activate redeliveries = %d, want 0", metrics.redeliveries["activate"])
		}
	})
}

// --- F4: the destructive drain runs last in the heartbeat ------------------

// failOnceHostedReportDirectory fails the first RecordHostedActivations call
// and delegates everything else to the embedded directory.
type failOnceHostedReportDirectory struct {
	*MemoryRunnerDirectory
	err    error
	failed bool
}

func (d *failOnceHostedReportDirectory) RecordHostedActivations(ctx context.Context, runnerID, sessionID string, items []protocol.ActivationInventoryItem) error {
	if !d.failed {
		d.failed = true
		return d.err
	}
	return d.MemoryRunnerDirectory.RecordHostedActivations(ctx, runnerID, sessionID, items)
}

// TestHeartbeatDrainsDirectivesLast pins the ordering fix: the durable drain
// deletes what it returns, so it must run after every other heartbeat step
// that can fail. A failed hosted-report write must leave the queued directives
// intact for the next heartbeat instead of destroying them with the response.
func TestHeartbeatDrainsDirectivesLast(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	directory := NewMemoryRunnerDirectory()
	session := registerCleanupRunner(t, ctx, directory, "runner-order", now, nil)

	directive := protocol.ActivateDirective{
		Namespace: "default", WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "tg", Generation: 4,
	}
	if err := directory.EnqueueActivationDirective(ctx, session.RunnerID, session.SessionID, directive); err != nil {
		t.Fatalf("EnqueueActivationDirective: %v", err)
	}

	reconciler := newRedeliveryTestReconciler(NewMemoryEntryActivationStore(), directory, directory, newFakeEntryActivationMetrics())
	core := &Core{
		runners:         &failOnceHostedReportDirectory{MemoryRunnerDirectory: directory, err: errors.New("report write failed")},
		entryReconciler: reconciler,
	}

	// The report write fails: the heartbeat fails, and — because the drain has
	// not run yet — the queued directive is still there.
	request := protocol.HeartbeatRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Capacity: 2, Timestamp: now.Unix(),
		HostedActivations: &protocol.HostedActivationsReport{},
	}
	if _, err := core.heartbeat(ctx, request, TransportInfo{}); err == nil {
		t.Fatal("heartbeat with a failing hosted-report write must fail")
	}

	// The next heartbeat (report write now succeeds) delivers it.
	resp, err := core.heartbeat(ctx, request, TransportInfo{})
	if err != nil {
		t.Fatalf("retry heartbeat: %v", err)
	}
	if resp.Activations == nil || len(resp.Activations.Activate) != 1 {
		t.Fatalf("activations after the failed heartbeat = %+v, want the queued directive", resp.Activations)
	}
	if resp.Activations.Activate[0].Generation != 4 || resp.Activations.Activate[0].WorkflowID != "wf-1" {
		t.Fatalf("delivered directive = %+v, want the queued wf-1@4", resp.Activations.Activate[0])
	}
}
