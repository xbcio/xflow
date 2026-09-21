package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// The production lease constants, spelled out rather than read from the
// defaults: these tests are about the arithmetic between them, so a change to a
// default must not silently retune what they assert.
const (
	renewTestTTL        = 60 * time.Second
	renewTestThreshold  = 20 * time.Second
	renewTestPeriod     = 10 * time.Second
	renewTestLeaseGrace = 60 * time.Second
)

// renewFixture drives the reconcile loop the way runEntryReconciler does: one
// pass per tick, with the owner's heartbeat refreshed on every pass, i.e. a
// runner that is healthy throughout — including while the control plane's own
// cadence slips. Only the pass times vary, which is exactly the production
// variable the lease has to tolerate.
type renewFixture struct {
	reconciler *EntryActivationReconciler
	store      *MemoryEntryActivationStore
	lister     *mockRunnerLister
	logger     *recordingLogger
	runner     RunnerSnapshot
	key        engine.EntryActivationKey
	ctx        context.Context
}

func newRenewFixture(t *testing.T) *renewFixture {
	t.Helper()
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	store := NewMemoryEntryActivationStore()
	if err := store.Upsert(ctx, testEntryActivation()); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	logger := &recordingLogger{}
	runner := RunnerSnapshot{
		RunnerID: "runner-a",
		Capacity: 4,
		Labels:   map[string]string{"zone": "a"},
	}
	lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:           store,
		Lister:          lister,
		Selector:        &sel,
		Namespaces:      []namespace.Namespace{namespace.Default},
		LeaseTTL:        renewTestTTL,
		RenewThreshold:  renewTestThreshold,
		ReconcilePeriod: renewTestPeriod,
		Logger:          logger,
	})
	return &renewFixture{
		reconciler: r,
		store:      store,
		lister:     lister,
		logger:     logger,
		runner:     runner,
		key:        keyOfActivation(testEntryActivation()),
		ctx:        ctx,
	}
}

// pass runs one reconcile pass at at with the healthy owner still live.
func (f *renewFixture) pass(t *testing.T, at time.Time) {
	t.Helper()
	f.runner.LastHeartbeat = at
	f.passWith(t, at, f.runner)
}

// passWith runs one pass against an explicit liveness snapshot.
func (f *renewFixture) passWith(t *testing.T, at time.Time, runners ...RunnerSnapshot) {
	t.Helper()
	f.lister.runners = runners
	if err := f.reconciler.Reconcile(f.ctx, at); err != nil {
		t.Fatalf("Reconcile at %v: %v", at, err)
	}
}

func (f *renewFixture) record(t *testing.T) engine.EntryActivation {
	t.Helper()
	act, ok, err := f.store.Get(f.ctx, f.key)
	if err != nil || !ok {
		t.Fatalf("Get activation: ok=%v err=%v", ok, err)
	}
	return act
}

// directives drains runner-a's pending activate/deactivate directives.
func (f *renewFixture) directives(t *testing.T) *protocol.HeartbeatActivations {
	t.Helper()
	return f.reconciler.DirectivesForRunner("runner-a")
}

// settle assigns the activation and discards the Activate directive the
// assignment itself produces, so a later directives() call reports only what
// the passes under test queued.
func (f *renewFixture) settle(t *testing.T, at time.Time) engine.EntryActivation {
	t.Helper()
	f.pass(t, at)
	if d := f.directives(t); d == nil || len(d.Activate) != 1 {
		t.Fatalf("initial assignment directives = %+v, want exactly one activate", d)
	}
	return f.record(t)
}

func deactivateGens(d *protocol.HeartbeatActivations) []uint64 {
	if d == nil {
		return nil
	}
	out := make([]uint64, 0, len(d.Deactivate))
	for _, dec := range d.Deactivate {
		out = append(out, dec.Generation)
	}
	return out
}

func activateGens(d *protocol.HeartbeatActivations) []uint64 {
	if d == nil {
		return nil
	}
	out := make([]uint64, 0, len(d.Activate))
	for _, dec := range d.Activate {
		out = append(out, dec.Generation)
	}
	return out
}

// TestEntryActivationReconciler_NominalCadenceKeepsTheLease is the baseline the
// rest of this file is measured against: when every pass lands one
// ReconcilePeriod apart, the lease is renewed inside the RenewThreshold window
// and the owner is never disturbed. It pins that the renewal path itself works,
// so a failure below cannot be blamed on the cadence being "reasonable".
func TestEntryActivationReconciler_NominalCadenceKeepsTheLease(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)
	if assigned.Generation != 1 || assigned.RunnerID != "runner-a" {
		t.Fatalf("initial assignment = gen %d runner %q, want gen 1 runner-a", assigned.Generation, assigned.RunnerID)
	}

	// 20 minutes of nominal 10s ticks.
	deadlines := map[time.Time]struct{}{assigned.LeaseDeadline: {}}
	for elapsed := renewTestPeriod; elapsed <= 20*time.Minute; elapsed += renewTestPeriod {
		at := base.Add(elapsed)
		f.pass(t, at)
		got := f.record(t)
		if got.Generation != 1 || got.RunnerID != "runner-a" {
			t.Fatalf("at %v the assignment changed to gen %d runner %q: a nominal cadence must only renew", elapsed, got.Generation, got.RunnerID)
		}
		if !got.LeaseDeadline.After(at) {
			t.Fatalf("at %v the lease had already lapsed (deadline %v): the renewal window was missed on a nominal cadence", elapsed, got.LeaseDeadline)
		}
		deadlines[got.LeaseDeadline] = struct{}{}
	}
	if len(deadlines) < 2 {
		t.Fatal("the lease never advanced: no renewal happened across 20 minutes of nominal ticks")
	}
	if final := f.record(t); !final.LeaseDeadline.After(base.Add(2 * renewTestTTL)) {
		t.Fatalf("lease deadline after 20 minutes = %v, want the lease pushed forward repeatedly", final.LeaseDeadline)
	}
	if d := f.directives(t); d != nil {
		t.Fatalf("a nominal cadence queued directives %+v: renewals must not disturb the hosting runner", d)
	}
	if got := escalationCounts(t, f.logger); len(got) != 0 {
		t.Fatalf("a nominal cadence escalated %v: renewals are not failures", got)
	}
}

// TestEntryActivationReconciler_LeaseLapsedByStalledPassIsRevivedNotFenced is
// the regression test for the renewal gap. It reproduces, with the production
// constants (TTL 60s, threshold 20s, period 10s), the exact condition observed
// in production — a pass landing with the lease deadline already in the past —
// and requires the healthy owner to be renewed rather than fenced.
//
// The slip is produced the way production produces it: the pass that would have
// renewed the lease (it lands when the remaining lease is 10s, i.e. renewTestTTL
// - 50s into the lease) does not happen, because a pass overran its period and
// the loop's ticker coalesced the ticks it missed. The next pass therefore lands
// 55s later, 35s after the deadline — the observed "deadline-now = -35.4s".
func TestEntryActivationReconciler_LeaseLapsedByStalledPassIsRevivedNotFenced(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)
	if assigned.Generation != 1 || assigned.RunnerID != "runner-a" {
		t.Fatalf("initial assignment = gen %d runner %q, want gen 1 runner-a", assigned.Generation, assigned.RunnerID)
	}

	// Nominal ticks up to the point where the lease is still 20s from expiry:
	// 20s is not yet inside the renewal window (the check is strictly
	// "remaining < RenewThreshold"), so nothing has been renewed yet.
	for elapsed := renewTestPeriod; elapsed <= 40*time.Second; elapsed += renewTestPeriod {
		f.pass(t, base.Add(elapsed))
		got := f.record(t)
		if !got.LeaseDeadline.Equal(assigned.LeaseDeadline) {
			t.Fatalf("lease renewed at %v while %v remained: the renewal window must not open early", elapsed, got.LeaseDeadline.Sub(base.Add(elapsed)))
		}
	}

	// The stalled pass: 55s after the previous one instead of 10s.
	stalledAt := base.Add(95 * time.Second)
	lapse := stalledAt.Sub(assigned.LeaseDeadline)
	if lapse <= 0 {
		t.Fatalf("the stalled pass must land after the deadline to reproduce the slip; lapse = %v", lapse)
	}
	if lapse > renewTestLeaseGrace {
		t.Fatalf("the stalled pass lapsed the lease by %v, which is outside the revive grace under test (%v): the stall must stay recoverable", lapse, renewTestLeaseGrace)
	}
	t.Logf("reproduced: stalled pass at %v, deadline %v, deadline-now=%v", stalledAt, assigned.LeaseDeadline, -lapse)

	f.pass(t, stalledAt)

	got := f.record(t)
	if got.Generation != assigned.Generation {
		t.Fatalf("generation advanced %d -> %d: a live owner whose renewal was merely missed must not be re-assigned", assigned.Generation, got.Generation)
	}
	if got.RunnerID != "runner-a" {
		t.Fatalf("owner changed to %q: a renewal miss is not a reason to move the activation", got.RunnerID)
	}
	if !got.LeaseDeadline.After(stalledAt) {
		t.Fatalf("lease deadline after the stalled pass = %v, want it renewed past %v", got.LeaseDeadline, stalledAt)
	}
	if want := stalledAt.Add(renewTestTTL); !got.LeaseDeadline.Equal(want) {
		t.Fatalf("renewed deadline = %v, want %v (a full TTL from the pass)", got.LeaseDeadline, want)
	}
	if d := f.directives(t); d != nil {
		t.Fatalf("the stalled pass queued directives %+v: a revived lease must not deactivate (or re-activate) a healthy owner", d)
	}
	if got := escalationCounts(t, f.logger); len(got) != 0 {
		t.Fatalf("a revived lease escalated %v: the revive is a success, not a failure", got)
	}
	f.reconciler.mu.Lock()
	_, retained := f.reconciler.retryBackoff[f.key]
	f.reconciler.mu.Unlock()
	if retained {
		t.Fatal("a revived lease retained failure state: the ladder would report a healthy owner as failing")
	}

	// The activation still holds exactly one generation, and only that
	// generation is renewable: the revive did not weaken the lease's
	// generation gate.
	stale, err := f.store.Assign(f.ctx, f.key, "runner-b", "sess-b", got.Generation, stalledAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("Assign at the live generation: %v", err)
	}
	if stale {
		t.Fatal("another runner claimed the activation at the live generation: the generation gate must reject a non-fenced claim")
	}
}

// TestEntryActivationReconciler_ReviveCannotResurrectAFencedOwner pins the
// property that makes the revive safe: it can only EXTEND the lease of the
// owner the record currently names, never restore a fenced one. A fence clears
// RunnerID and raises the generation, so the old runner has no generation left
// to be revived at — its liveness, however healthy, buys it nothing, and it
// cannot reclaim the activation without a new fence+assign.
func TestEntryActivationReconciler_ReviveCannotResurrectAFencedOwner(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)

	replacement := RunnerSnapshot{
		RunnerID: "runner-b",
		Capacity: 4,
		Labels:   map[string]string{"zone": "a"},
	}
	at := base.Add(renewTestPeriod)
	replacement.LastHeartbeat = at
	f.passWith(t, at, replacement) // runner-a is gone → fenced and reassigned

	fenced := f.record(t)
	if fenced.RunnerID != "runner-b" || fenced.Generation <= assigned.Generation {
		t.Fatalf("fence+reassign = gen %d runner %q, want a higher generation owned by runner-b", fenced.Generation, fenced.RunnerID)
	}
	if gens := deactivateGens(f.directives(t)); len(gens) != 1 || gens[0] != assigned.Generation {
		t.Fatalf("deactivate to the fenced owner = %v, want exactly [%d]", gens, assigned.Generation)
	}

	// runner-a comes back and heartbeats on every later pass, while the lease
	// lapses along the way: nothing may move the activation back to it.
	for _, elapsed := range []time.Duration{70 * time.Second, 95 * time.Second, 2 * time.Minute} {
		later := base.Add(elapsed)
		f.runner.LastHeartbeat = later
		replacement.LastHeartbeat = later
		f.passWith(t, later, f.runner, replacement)
		got := f.record(t)
		if got.Generation != fenced.Generation || got.RunnerID != "runner-b" {
			t.Fatalf("at %v the activation moved to gen %d runner %q: a fenced owner must never be revived", elapsed, got.Generation, got.RunnerID)
		}
	}

	stale, err := f.store.Assign(f.ctx, f.key, "runner-a", "sess-a", fenced.Generation, base.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("Assign at the fenced generation: %v", err)
	}
	if stale {
		t.Fatal("the fenced owner reclaimed the activation at the current generation without a fence")
	}
}

// TestEntryActivationReconciler_ReviveIsBoundedByTheGrace pins the bound that
// keeps the revive from becoming "a live runner owns the activation forever":
// a lapse beyond the grace is still fenced and reassigned, which is what
// recovers an owner that is heartbeating but no longer hosting (the activate
// directive is delivered once, from an in-process queue, so a missed delivery
// is only ever repaired by a re-assignment).
func TestEntryActivationReconciler_ReviveIsBoundedByTheGrace(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)

	lapsedAt := base.Add(renewTestTTL + renewTestLeaseGrace + time.Second)
	if lapse := lapsedAt.Sub(assigned.LeaseDeadline); lapse <= renewTestLeaseGrace {
		t.Fatalf("lapse = %v, want it past the grace %v", lapse, renewTestLeaseGrace)
	}
	f.pass(t, lapsedAt)

	got := f.record(t)
	if got.Generation <= assigned.Generation {
		t.Fatalf("generation %d did not advance past %d: a lapse beyond the grace must still be fenced", got.Generation, assigned.Generation)
	}
	if got.RunnerID != "runner-a" {
		t.Fatalf("owner after the fence = %q, want it reassigned to the live runner-a", got.RunnerID)
	}
	d := f.directives(t)
	if gens := deactivateGens(d); len(gens) != 1 || gens[0] != assigned.Generation {
		t.Fatalf("deactivate generations = %v, want exactly [%d] so the stale owner stops hosting", gens, assigned.Generation)
	}
	if gens := activateGens(d); len(gens) != 1 || gens[0] != got.Generation {
		t.Fatalf("activate generations = %v, want exactly [%d]", gens, got.Generation)
	}
	f.reconciler.mu.Lock()
	state := f.reconciler.retryBackoff[f.key]
	f.reconciler.mu.Unlock()
	if state.reason != fenceReasonLeaseExpired {
		t.Fatalf("retained fence reason = %q, want %q", state.reason, fenceReasonLeaseExpired)
	}
}

// TestEntryActivationReconciler_DeadOwnerIsFencedEvenWithinReviveGrace is the
// safety half of the fix: the revive is gated on the owner being live and still
// matching the desired state, NOT on the deadline alone. A runner that is gone
// while the lease has lapsed (and would therefore be revived if the grace were
// the only condition) must still be fenced, and the reassignment must move to
// the live runner at a strictly higher generation, with a deactivate for the
// dead owner at the old one.
func TestEntryActivationReconciler_DeadOwnerIsFencedEvenWithinReviveGrace(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)

	// runner-a stops heartbeating; runner-b is live and matches the selector.
	replacement := RunnerSnapshot{
		RunnerID: "runner-b",
		Capacity: 4,
		Labels:   map[string]string{"zone": "a"},
	}
	stalledAt := base.Add(95 * time.Second)
	replacement.LastHeartbeat = stalledAt
	if lapse := stalledAt.Sub(assigned.LeaseDeadline); lapse <= 0 || lapse > renewTestLeaseGrace {
		t.Fatalf("lapse = %v, want it inside the revive grace (%v) so liveness is the only disqualifying condition", lapse, renewTestLeaseGrace)
	}
	f.passWith(t, stalledAt, replacement)

	got := f.record(t)
	if got.RunnerID != "runner-b" {
		t.Fatalf("owner = %q after the dead owner's lease lapsed, want runner-b", got.RunnerID)
	}
	if got.Generation <= assigned.Generation {
		t.Fatalf("generation %d did not advance past %d", got.Generation, assigned.Generation)
	}
	d := f.directives(t) // runner-a's queue: the fenced owner must be told to stop
	if gens := deactivateGens(d); len(gens) != 1 || gens[0] != assigned.Generation {
		t.Fatalf("deactivate to the dead owner = %v, want exactly [%d]", gens, assigned.Generation)
	}
}

// TestEntryActivationReconciler_DeadOwnerWithLiveLeaseIsFenced keeps the
// pre-existing eviction reason distinct from the lease: a dead owner is fenced
// on liveness alone, while its lease is still valid, and is reported as such.
func TestEntryActivationReconciler_DeadOwnerWithLiveLeaseIsFenced(t *testing.T) {
	f := newRenewFixture(t)
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	assigned := f.settle(t, base)

	at := base.Add(time.Minute) // deadline is base+60s: still in the future
	replacement := RunnerSnapshot{
		RunnerID:      "runner-b",
		Capacity:      4,
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: at,
	}
	f.passWith(t, at, replacement)

	got := f.record(t)
	if got.RunnerID != "runner-b" || got.Generation <= assigned.Generation {
		t.Fatalf("dead owner was not replaced: gen %d runner %q (was gen %d runner %q)", got.Generation, got.RunnerID, assigned.Generation, "runner-a")
	}
	f.reconciler.mu.Lock()
	state := f.reconciler.retryBackoff[f.key]
	f.reconciler.mu.Unlock()
	if state.reason != fenceReasonOwnerNotLive {
		t.Fatalf("retained fence reason = %q, want %q", state.reason, fenceReasonOwnerNotLive)
	}
}
