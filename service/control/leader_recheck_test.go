package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// flipAfterGate reports leadership for its first allow calls and false
// afterwards. Every long pass checks at entry first, then again every
// leaderRecheckInterval candidates, so allow=1 makes the first mid-pass check
// observe the loss.
type flipAfterGate struct {
	mu    sync.Mutex
	calls int
	allow int
}

func (g *flipAfterGate) IsLeader() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls++
	return g.calls <= g.allow
}

func (g *flipAfterGate) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// progressFlipReclaimer flips the shared elector once it has reclaimed flipAt
// leases, standing in for leadership being lost part-way through a sweep.
type progressFlipReclaimer struct {
	*fakeReclaimer
	elector *fakeElector
	flipAt  int
	count   int
}

func (r *progressFlipReclaimer) ReclaimLease(ctx context.Context, lease engine.ExpiredLease) (bool, error) {
	r.count++
	if r.count >= r.flipAt {
		r.elector.leader.Store(false)
	}
	return r.fakeReclaimer.ReclaimLease(ctx, lease)
}

func expireLeases(n int) []engine.ExpiredLease {
	out := make([]engine.ExpiredLease, n)
	for i := range out {
		out[i] = engine.ExpiredLease{
			ExecutionID: types.ExecutionID(fmt.Sprintf("exec-%02d", i)),
			NodeName:    "n",
			LeaseToken:  "token",
			IssuedAt:    time.Now().Add(-time.Minute),
			TTL:         time.Second,
		}
	}
	return out
}

// TestSweepOnceStopsWhenLeadershipIsLostMidPass pins that the sweep's entry
// gate is not the only leadership check: a replica demoted during a bounded
// scan stops reclaiming and leaves the remaining candidates alone.
func TestSweepOnceStopsWhenLeadershipIsLostMidPass(t *testing.T) {
	const total, processed = 40, leaderRecheckInterval
	state := &fakeLeaseLister{expired: expireLeases(total)}
	elector := &fakeElector{}
	elector.leader.Store(true)
	reclaimer := &progressFlipReclaimer{
		fakeReclaimer: &fakeReclaimer{},
		elector:       elector,
		flipAt:        processed,
	}
	sw := NewLeaseSweeper(state, reclaimer, LeaseSweeperConfig{Elector: elector})

	if got := sw.SweepOnce(context.Background()); got != processed {
		t.Fatalf("SweepOnce() = %d, want %d reclaimed before the leadership re-check", got, processed)
	}
	if got := len(reclaimer.reclaimed); got != processed {
		t.Fatalf("ReclaimLease calls = %d, want %d: the remaining %d candidates must be left for the new leader",
			got, processed, total-processed)
	}
}

// progressFlipCanceler flips the shared elector once it has re-driven flipAt
// cancels, standing in for leadership being lost part-way through the pass.
type progressFlipCanceler struct {
	*fakeCanceler
	elector *fakeElector
	flipAt  int
	count   int
}

func (c *progressFlipCanceler) Cancel(ctx context.Context, id types.ExecutionID) error {
	c.count++
	if c.count >= c.flipAt {
		c.elector.leader.Store(false)
	}
	return c.fakeCanceler.Cancel(ctx, id)
}

// TestReapStuckCancelingStopsWhenLeadershipIsLostMidPass pins the same
// re-check for the stale-canceling reaper, including its skipped-not-leader
// pass report.
func TestReapStuckCancelingStopsWhenLeadershipIsLostMidPass(t *testing.T) {
	const total, processed = 40, leaderRecheckInterval
	state := &fakeCancelingLister{fakeLeaseLister: &fakeLeaseLister{}}
	candidates := make([]engine.CancelingExecution, total)
	for i := range candidates {
		candidates[i] = engine.CancelingExecution{ExecutionID: types.ExecutionID(fmt.Sprintf("exec-%02d", i))}
	}
	state.candidates = candidates
	elector := &fakeElector{}
	elector.leader.Store(true)
	canceler := &progressFlipCanceler{
		fakeCanceler: &fakeCanceler{fakeReclaimer: &fakeReclaimer{}, errs: map[types.ExecutionID]error{}},
		elector:      elector,
		flipAt:       processed,
	}
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	sw := NewLeaseSweeper(state, canceler, LeaseSweeperConfig{Elector: elector, Observer: observer})

	if got := sw.ReapStuckCancelingExecutionsOnce(context.Background()); got != processed {
		t.Fatalf("ReapStuckCancelingExecutionsOnce() = %d, want %d", got, processed)
	}
	if got := len(canceler.snapshot()); got != processed {
		t.Fatalf("Cancel calls = %d, want %d: the remaining candidates must be left for the new leader", got, processed)
	}
	events := observer.snapshot()
	if len(events) != 1 {
		t.Fatalf("pass events = %d, want 1", len(events))
	}
	if events[0].outcome != SweepPassOutcomeSkippedNotLeader {
		t.Fatalf("pass outcome = %q, want %q", events[0].outcome, SweepPassOutcomeSkippedNotLeader)
	}
	if events[0].released != processed {
		t.Fatalf("reported released = %d, want the %d cancels driven before the loss", events[0].released, processed)
	}
}

// TestAuditReconcileStopsWhenLeadershipIsLostMidSweep pins that the worker
// stops settling candidates once it is no longer the leader: the next leader
// re-scans them from its own cursor.
func TestAuditReconcileStopsWhenLeadershipIsLostMidSweep(t *testing.T) {
	const total, processed = 64, leaderRecheckInterval
	audit := &fakeAuditReconciler{}
	effects := make(map[string]MutationEffect, total)
	for i := 0; i < total; i++ {
		reqID := fmt.Sprintf("req-%02d", i)
		effects[reqID] = EffectConfirmed
		audit.addAdmission(admissionForRow(reqID, "namespace-a", opWorkflowCreate, reqID, time.Now().Add(-time.Minute)))
	}
	authority := &fakeAuthority{effects: effects}
	gate := &flipAfterGate{allow: 1}
	w := newWorkerForTest(audit, authority, gate, nil)

	if n := w.ReconcileOnce(context.Background()); n != processed {
		t.Fatalf("ReconcileOnce() settled = %d, want %d before the mid-sweep leadership check", n, processed)
	}
	if got := audit.outcomeCount(); got != processed {
		t.Fatalf("outcome rows = %d, want %d: the remaining %d admissions must be left for the next leader",
			got, processed, total-processed)
	}
	if got := gate.callCount(); got != 2 {
		t.Fatalf("leadership checks = %d, want 2 (entry + one mid-sweep re-check)", got)
	}
}

// TestEntryActivationReconcileStopsWhenLeadershipIsLostMidPass pins the
// assignment pass: losing leadership mid-pass stops further assignments and
// reports the skip instead of finishing the remaining activations.
func TestEntryActivationReconcileStopsWhenLeadershipIsLostMidPass(t *testing.T) {
	ctx := context.Background()
	const total, assigned = 40, leaderRecheckInterval
	store := NewMemoryEntryActivationStore()
	for i := 0; i < total; i++ {
		act := testEntryActivation()
		act.WorkflowID = types.WorkflowID(fmt.Sprintf("wf-%02d", i))
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-a",
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	// Entry check and the phase-one re-check see a leader; the phase-two
	// re-check observes the loss after 32 activations have been assigned.
	gate := &flipAfterGate{allow: 2}
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
		Leader:     gate,
	})

	err := r.Reconcile(ctx, now)
	if !errors.Is(err, ErrReconcileSkippedNotLeader) {
		t.Fatalf("Reconcile() error = %v, want ErrReconcileSkippedNotLeader", err)
	}
	acts, err := store.List(ctx, namespace.Default)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	got := 0
	for _, act := range acts {
		if act.RunnerID != "" {
			got++
		}
	}
	if got != assigned {
		t.Fatalf("assigned activations = %d, want %d: the remaining %d must be left for the next leader",
			got, assigned, total-assigned)
	}
}

// TestEntryActivationReconcileSkipsWhenNotLeaderAtEntry pins the entry gate:
// a replica that is not the leader performs no work at all.
func TestEntryActivationReconcileSkipsWhenNotLeaderAtEntry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	act := testEntryActivation()
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	lister := &mockRunnerLister{runners: []RunnerSnapshot{{
		RunnerID:      "runner-a",
		Labels:        map[string]string{"zone": "a"},
		LastHeartbeat: now,
	}}}
	sel := DefaultRunnerSelector()
	r := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store:      store,
		Lister:     lister,
		Selector:   &sel,
		Namespaces: []namespace.Namespace{namespace.Default},
		LeaseTTL:   60 * time.Second,
		Leader:     &flipAfterGate{allow: 0},
	})

	if err := r.Reconcile(ctx, now); !errors.Is(err, ErrReconcileSkippedNotLeader) {
		t.Fatalf("Reconcile() error = %v, want ErrReconcileSkippedNotLeader", err)
	}
	got, ok, err := store.Get(ctx, keyOfActivation(act))
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.RunnerID != "" {
		t.Fatalf("activation assigned to %q by a non-leader pass, want no assignment", got.RunnerID)
	}
}

// Compile-time proof the gate types used above satisfy the interfaces the
// production call sites require.
var (
	_ LeaderGate            = (*flipAfterGate)(nil)
	_ store.AuditReconciler = (*fakeAuditReconciler)(nil)
)
