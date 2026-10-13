package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// concurrencySlotCalls records how the slot reconciler was invoked, so the
// tests can pin the bound it was given and whether it was reached at all. It
// carries the ReconcileConcurrencySlots method and nothing else, so the two
// state fakes below can compose the capabilities they need.
type concurrencySlotCalls struct {
	mu           sync.Mutex
	calls        int
	lastLimit    int
	swept        int
	reconcileErr error
}

func (c *concurrencySlotCalls) ReconcileConcurrencySlots(_ context.Context, limit int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastLimit = limit
	return c.swept, c.reconcileErr
}

func (c *concurrencySlotCalls) snapshot() (calls, lastLimit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.lastLimit
}

// concurrencyState has both concurrency capabilities — slot reconciliation and
// queue-timeout expiry — and no lease-index repair: they are separate
// interfaces, and the sweeper must not require any of them together.
type concurrencyState struct {
	*fakeLeaseLister
	*concurrencySlotCalls
	*concurrencyQueueCalls
}

// concurrencyQueueCalls records how the queue-timeout expiry was invoked, so
// the tests can pin the bound it was given and that the pass reached it at all.
type concurrencyQueueCalls struct {
	mu        sync.Mutex
	calls     int
	lastLimit int
	expired   int
	expireErr error
}

func (c *concurrencyQueueCalls) ExpireQueuedExecutions(_ context.Context, limit int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastLimit = limit
	return c.expired, c.expireErr
}

func (c *concurrencyQueueCalls) snapshot() (calls, lastLimit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.lastLimit
}

// repairAndSlotState has both capabilities, which is what the ride-along
// assertions need.
type repairAndSlotState struct {
	*concurrencyState

	mu           sync.Mutex
	reconciled   int
	reconcileErr error
}

func (s *repairAndSlotState) RepairLeaseIndex(context.Context, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconciled, s.reconcileErr
}

type concurrencySlotSweeper struct {
	*LeaseSweeper
	slots   *concurrencySlotCalls
	queue   *concurrencyQueueCalls
	repairs *repairAndSlotState
	elector *fakeElector
	now     *time.Time
}

// newConcurrencySlotSweeper builds a sweeper over a state with both
// capabilities, which the ride-along cases narrow by replacing sw.state.
func newConcurrencySlotSweeper(observer SweepObserver, batch int) *concurrencySlotSweeper {
	slots := &concurrencySlotCalls{}
	queue := &concurrencyQueueCalls{}
	state := &repairAndSlotState{concurrencyState: &concurrencyState{
		fakeLeaseLister:       &fakeLeaseLister{},
		concurrencySlotCalls:  slots,
		concurrencyQueueCalls: queue,
	}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(state, &fakeReclaimer{}, LeaseSweeperConfig{
		Elector:           elector,
		Observer:          observer,
		LeaseRepairPeriod: time.Minute,
		LeaseRepairBatch:  batch,
	})
	now := time.Date(2026, 10, 12, 1, 30, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	return &concurrencySlotSweeper{LeaseSweeper: sw, slots: slots, queue: queue, repairs: state, elector: elector, now: &now}
}

// TestLeaseSweeperRunsTheConcurrencySlotSweepAfterRepair pins the ride-along: a
// successful lease-index repair is what drives the slot sweep, and the two
// report separately. Folding the swept count into the repair count would make
// "how many lease index entries were repaired" and "how many workflow slots
// were leaked" indistinguishable in the operator's reading.
func TestLeaseSweeperRunsTheConcurrencySlotSweepAfterRepair(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.repairs.reconciled = 3
	h.slots.swept = 2
	h.queue.expired = 1

	if got := h.RepairOnce(context.Background()); got != 3 {
		t.Fatalf("RepairOnce() = %d, want the lease-index repairs alone", got)
	}

	want := []passEvent{
		{pass: SweepPassRepair, outcome: SweepPassOutcomeRan, released: 3},
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 2},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan, released: 1},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
	if calls, limit := h.slots.snapshot(); calls != 1 || limit != 64 {
		t.Fatalf("slot sweep calls/limit = %d/%d, want 1/64: the pass has to run at the "+
			"lease-repair batch bound, not an unbounded scan", calls, limit)
	}
	if calls, limit := h.queue.snapshot(); calls != 1 || limit != 64 {
		t.Fatalf("queue expiry calls/limit = %d/%d, want 1/64: the expiry is bounded the "+
			"same way, so one workflow's backlog cannot stall the pass", calls, limit)
	}
}

// TestLeaseSweeperConcurrencySlotSweepRespectsItsCadence pins that the pass
// sweeps at the repair period rather than at every call. It is driven from
// SweepOnce, whose period is six times shorter by default, and re-reading every
// workflow's slot set every few seconds is load with nothing to show for it: a
// leaked slot costs capacity, it does not corrupt anything.
func TestLeaseSweeperConcurrencySlotSweepRespectsItsCadence(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.slots.swept = 1

	h.SweepConcurrencySlotsOnce(context.Background())
	*h.now = h.now.Add(30 * time.Second)
	h.SweepConcurrencySlotsOnce(context.Background())
	*h.now = h.now.Add(30 * time.Second)
	h.SweepConcurrencySlotsOnce(context.Background())

	want := []passEvent{
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 1},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan},
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeSkippedCadence},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeSkippedCadence},
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 1},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
	if calls, _ := h.slots.snapshot(); calls != 2 {
		t.Fatalf("slot sweep calls = %d, want 2", calls)
	}
	// Both halves share the window: a queue expiry that ran on a cadence the
	// slot reconciliation skipped would be a second maintenance loop wearing the
	// first one's gate.
	if calls, _ := h.queue.snapshot(); calls != 2 {
		t.Fatalf("queue expiry calls = %d, want 2, the same windows as the slot sweep", calls)
	}
}

// TestLeaseSweeperRunsTheConcurrencySlotSweepWhenRepairFails pins the
// independence of the two passes. They reconcile unrelated structures, so a
// lease index that keeps failing must not also stop leaked slots from being
// reclaimed — that would turn one broken index into a workflow that slowly
// loses capacity.
func TestLeaseSweeperRunsTheConcurrencySlotSweepWhenRepairFails(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.repairs.reconcileErr = errors.New("redis unavailable")
	h.slots.swept = 1

	if got := h.RepairOnce(context.Background()); got != 0 {
		t.Fatalf("RepairOnce() = %d, want 0 for a failed repair", got)
	}

	want := []passEvent{
		{pass: SweepPassRepair, outcome: SweepPassOutcomeError},
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 1},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

// TestLeaseSweeperRunsTheConcurrencySlotSweepWithoutARepairer is the same
// independence one level up: the capabilities are separate interfaces, so a
// backend that exposes slot reconciliation but no lease-index repair still gets
// swept.
func TestLeaseSweeperRunsTheConcurrencySlotSweepWithoutARepairer(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.LeaseSweeper.state = &concurrencyState{
		fakeLeaseLister:       &fakeLeaseLister{},
		concurrencySlotCalls:  h.slots,
		concurrencyQueueCalls: h.queue,
	}
	h.slots.swept = 4

	h.RepairOnce(context.Background())

	want := []passEvent{
		{pass: SweepPassRepair, outcome: SweepPassOutcomeUnsupported},
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 4},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

// TestLeaseSweeperReportsMissingConcurrencySlotCapability pins the two-phase
// rollout: every backend that does not implement the reconciler reports
// "unsupported" for the pass instead of silently doing nothing. A silent no-op
// would read, from the metrics alone, exactly like a sweep that ran and found
// no leaks.
func TestLeaseSweeperReportsMissingConcurrencySlotCapability(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.LeaseSweeper.state = &fakeLeaseLister{}

	h.SweepConcurrencySlotsOnce(context.Background())

	// The queue half is reported separately, not skipped along with the slot
	// half: the capabilities are separate interfaces, and a backend can expire
	// queued submissions without being able to reconcile slot sets.
	want := []passEvent{
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeUnsupported},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeUnsupported},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
	if calls, _ := h.slots.snapshot(); calls != 0 {
		t.Fatalf("slot sweep calls = %d although the capability was replaced", calls)
	}
	if calls, _ := h.queue.snapshot(); calls != 0 {
		t.Fatalf("queue expiry calls = %d although the capability was replaced", calls)
	}
}

// TestLeaseSweeperConcurrencySlotSweepIsLeaderGated keeps the cluster-wide
// work on one replica: the slot sets are shared, so every replica sweeping them
// multiplies the scan without changing the outcome.
func TestLeaseSweeperConcurrencySlotSweepIsLeaderGated(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.elector.leader.Store(false)

	if got := h.SweepConcurrencySlotsOnce(context.Background()); got != 0 {
		t.Fatalf("SweepConcurrencySlotsOnce() = %d, want 0 on a non-leader", got)
	}

	want := []passEvent{
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeSkippedNotLeader},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeSkippedNotLeader},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
	if calls, _ := h.slots.snapshot(); calls != 0 {
		t.Fatalf("slot sweep calls = %d on a non-leader, want 0", calls)
	}
	if calls, _ := h.queue.snapshot(); calls != 0 {
		t.Fatalf("queue expiry calls = %d on a non-leader, want 0", calls)
	}
	if !h.lastSlotSweep.IsZero() {
		t.Fatal("a non-leader stamped the concurrency window: the leader's cadence would be inherited by a replica that did no work")
	}
}

// TestLeaseSweeperConcurrencySlotSweepSurvivesAFailure keeps a broken sweep
// from taking the maintenance loop down with it: the leak costs the workflow
// capacity, it does not corrupt it, so the pass reports the error and retries on
// the next cadence.
func TestLeaseSweeperConcurrencySlotSweepSurvivesAFailure(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.slots.reconcileErr = errors.New("redis unavailable")

	if got := h.SweepConcurrencySlotsOnce(context.Background()); got != 0 {
		t.Fatalf("SweepConcurrencySlotsOnce() = %d, want 0 for a failed sweep", got)
	}

	// The queue half still runs: a slot sweep that keeps failing must not stop
	// queued submissions from timing out.
	want := []passEvent{
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeError},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeRan},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

// TestLeaseSweeperConcurrencyQueueExpirySurvivesAFailure is the mirror: an
// expiry that keeps failing reports under its own pass and releases nothing,
// and the slot reconciliation beside it is unaffected. The mirror matters
// because the two failure modes read the same in a log line and have opposite
// causes — a leaked slot versus a queue nobody can drain.
func TestLeaseSweeperConcurrencyQueueExpirySurvivesAFailure(t *testing.T) {
	observer := &recordingPassObserver{fakeObserver: &fakeObserver{}}
	h := newConcurrencySlotSweeper(observer, 64)
	h.slots.swept = 1
	h.queue.expireErr = errors.New("redis unavailable")

	if got := h.SweepConcurrencySlotsOnce(context.Background()); got != 1 {
		t.Fatalf("SweepConcurrencySlotsOnce() = %d, want the slot sweep's count alone", got)
	}

	want := []passEvent{
		{pass: SweepPassConcurrencySlot, outcome: SweepPassOutcomeRan, released: 1},
		{pass: SweepPassConcurrencyQueue, outcome: SweepPassOutcomeError},
	}
	if got := observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}
