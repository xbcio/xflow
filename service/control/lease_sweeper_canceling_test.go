package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// fakeCancelingLister is a LeaseLister that also implements the canceling index
// the reaper is gated on. It records the arguments of the last scan so the
// cadence/threshold contract can be asserted rather than inferred.
type fakeCancelingLister struct {
	*fakeLeaseLister
	mu         sync.Mutex
	candidates []engine.CancelingExecution
	err        error
	calls      int
	lastOlder  time.Time
	lastLimit  int
}

func (f *fakeCancelingLister) ListCancelingExecutions(_ context.Context, olderThan time.Time, limit int) ([]engine.CancelingExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastOlder = olderThan
	f.lastLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return append([]engine.CancelingExecution(nil), f.candidates...), nil
}

func (f *fakeCancelingLister) scanCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type cancelCall struct {
	id types.ExecutionID
	ns namespace.Namespace
}

// fakeCanceler is an execution.Engine that also implements Cancel, recording
// each call's execution and the namespace it was scoped to.
type fakeCanceler struct {
	*fakeReclaimer
	mu    sync.Mutex
	calls []cancelCall
	errs  map[types.ExecutionID]error
}

func (c *fakeCanceler) Cancel(ctx context.Context, id types.ExecutionID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, cancelCall{id: id, ns: namespace.FromContext(ctx)})
	if err, ok := c.errs[id]; ok {
		return err
	}
	return nil
}

func (c *fakeCanceler) snapshot() []cancelCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cancelCall(nil), c.calls...)
}

type cancelingHarness struct {
	*LeaseSweeper
	state    *fakeCancelingLister
	canceler *fakeCanceler
	elector  *fakeElector
	observer *recordingPassObserver
	now      *time.Time
}

func newCancelingHarness(observer SweepObserver, logger engine.Logger) *cancelingHarness {
	state := &fakeCancelingLister{fakeLeaseLister: &fakeLeaseLister{}}
	canceler := &fakeCanceler{fakeReclaimer: &fakeReclaimer{}, errs: map[types.ExecutionID]error{}}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(state, canceler, LeaseSweeperConfig{
		Elector:  elector,
		Observer: observer,
		Logger:   logger,
	})
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	return &cancelingHarness{
		LeaseSweeper: sw,
		state:        state,
		canceler:     canceler,
		elector:      elector,
		observer:     observer.(*recordingPassObserver),
		now:          &now,
	}
}

func newDefaultCancelingHarness() *cancelingHarness {
	return newCancelingHarness(&recordingPassObserver{fakeObserver: &fakeObserver{}}, nil)
}

func TestLeaseSweeperReDrivesStuckCancelingExecutions(t *testing.T) {
	h := newDefaultCancelingHarness()
	h.state.candidates = []engine.CancelingExecution{
		{ExecutionID: "exec-stuck-1", Namespace: "tenant-a", Since: h.now.Add(-2 * time.Minute)},
		{ExecutionID: "exec-stuck-2", Since: h.now.Add(-90 * time.Second)},
	}

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 2 {
		t.Fatalf("ReapStuckCancelingExecutionsOnce() = %d, want 2", got)
	}

	if got := h.state.scanCalls(); got != 1 {
		t.Fatalf("ListCancelingExecutions calls = %d, want 1", got)
	}
	if want := h.now.Add(-DefaultCancelingExecutionStuckAfter); !h.state.lastOlder.Equal(want) {
		t.Fatalf("olderThan = %v, want %v (the stuck threshold behind now)", h.state.lastOlder, want)
	}
	if h.state.lastLimit != defaultCancelingExecutionReapBatch {
		t.Fatalf("limit = %d, want %d", h.state.lastLimit, defaultCancelingExecutionReapBatch)
	}

	calls := h.canceler.snapshot()
	if len(calls) != 2 {
		t.Fatalf("Cancel calls = %+v, want both candidates re-driven", calls)
	}
	byID := make(map[types.ExecutionID]namespace.Namespace, len(calls))
	for _, call := range calls {
		byID[call.id] = call.ns
	}
	if byID["exec-stuck-1"] != "tenant-a" {
		t.Fatalf("exec-stuck-1 cancel namespace = %q, want tenant-a", byID["exec-stuck-1"])
	}
	// A candidate without a namespace is cancelled on the sweeper's own context;
	// the sweeper must not invent a scope for it.
	if want := namespace.FromContext(context.Background()); byID["exec-stuck-2"] != want {
		t.Fatalf("exec-stuck-2 cancel namespace = %q, want the sweeper context's %q", byID["exec-stuck-2"], want)
	}

	want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeRan, released: 2}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

func TestLeaseSweeperCancelingReapIsLeaderGated(t *testing.T) {
	h := newDefaultCancelingHarness()
	h.elector.leader.Store(false)
	h.state.candidates = []engine.CancelingExecution{{ExecutionID: "exec-stuck"}}

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
		t.Fatalf("non-leader reap = %d, want 0", got)
	}
	if got := h.state.scanCalls(); got != 0 {
		t.Fatalf("non-leader scanned the index %d times, want 0", got)
	}
	if got := len(h.canceler.snapshot()); got != 0 {
		t.Fatalf("non-leader drove %d cancels, want 0", got)
	}
	want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeSkippedNotLeader}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

func TestLeaseSweeperCancelingReapHonorsCadence(t *testing.T) {
	h := newDefaultCancelingHarness()

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
		t.Fatalf("first reap = %d, want 0 candidates", got)
	}
	if got := h.state.scanCalls(); got != 1 {
		t.Fatalf("first reap scans = %d, want 1", got)
	}

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
		t.Fatalf("second immediate reap = %d, want 0", got)
	}
	if got := h.state.scanCalls(); got != 1 {
		t.Fatalf("an immediate second reap scanned again (%d scans): the cadence gate did not hold", got)
	}

	*h.now = h.now.Add(DefaultCancelingExecutionReapPeriod)
	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
		t.Fatalf("reap after the period = %d, want 0 candidates", got)
	}
	if got := h.state.scanCalls(); got != 2 {
		t.Fatalf("reap after the period scans = %d, want 2", got)
	}

	want := []passEvent{
		{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeRan},
		{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeSkippedCadence},
		{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeRan},
	}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

func TestLeaseSweeperCancelingReapReportsUnsupportedCapabilities(t *testing.T) {
	t.Run("store without the index", func(t *testing.T) {
		h := newDefaultCancelingHarness()
		h.LeaseSweeper.state = &fakeLeaseLister{}

		if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
			t.Fatalf("reap without a lister = %d, want 0", got)
		}
		if got := len(h.canceler.snapshot()); got != 0 {
			t.Fatalf("reap without a lister drove %d cancels, want 0", got)
		}
		want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeUnsupported}}
		if got := h.observer.snapshot(); !equalPassEvents(got, want) {
			t.Fatalf("pass events = %+v, want %+v", got, want)
		}
	})

	t.Run("engine without Cancel", func(t *testing.T) {
		h := newDefaultCancelingHarness()
		h.LeaseSweeper.engine = &fakeReclaimer{}
		h.state.candidates = []engine.CancelingExecution{{ExecutionID: "exec-stuck"}}

		if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
			t.Fatalf("reap without a canceler = %d, want 0", got)
		}
		if got := h.state.scanCalls(); got != 0 {
			t.Fatalf("reap without a canceler scanned %d times, want 0", got)
		}
		want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeUnsupported}}
		if got := h.observer.snapshot(); !equalPassEvents(got, want) {
			t.Fatalf("pass events = %+v, want %+v", got, want)
		}
	})
}

// TestLeaseSweeperCancelingReapContinuesPastFailures pins the per-candidate
// contract: one broken execution must not stop the others, an already-inactive
// execution is not an error worth logging, and the pass reports both what it
// inspected and what it actually completed.
func TestLeaseSweeperCancelingReapContinuesPastFailures(t *testing.T) {
	logger := &recordingLogger{}
	h := newCancelingHarness(&recordingPassObserver{fakeObserver: &fakeObserver{}}, logger)
	h.state.candidates = []engine.CancelingExecution{
		{ExecutionID: "exec-inactive"},
		{ExecutionID: "exec-broken"},
		{ExecutionID: "exec-fixed"},
	}
	h.canceler.errs["exec-inactive"] = fmt.Errorf("execution %q: %w", "exec-inactive", engine.ErrExecutionInactive)
	h.canceler.errs["exec-broken"] = errors.New("store unavailable")

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 1 {
		t.Fatalf("reap = %d, want 1 released", got)
	}
	if calls := h.canceler.snapshot(); len(calls) != 3 {
		t.Fatalf("Cancel calls = %+v, want all three candidates attempted", calls)
	}
	want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeRan, released: 1}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}

	var errorEntries []recordedLogEntry
	for _, entry := range logger.entries {
		if entry.level == "error" {
			errorEntries = append(errorEntries, entry)
		}
	}
	if len(errorEntries) != 1 {
		t.Fatalf("error log entries = %+v, want exactly the generic failure (inactive is benign)", errorEntries)
	}
	if exec := errorEntries[0].field("exec"); exec != "exec-broken" {
		t.Fatalf("logged exec = %v, want exec-broken", exec)
	}
}

// TestLeaseSweeperCancelingReapReportsListError pins that a failed scan is an
// error outcome, not a pass that quietly found nothing.
func TestLeaseSweeperCancelingReapReportsListError(t *testing.T) {
	h := newDefaultCancelingHarness()
	h.state.err = errors.New("index unreadable")

	if got := h.ReapStuckCancelingExecutionsOnce(context.Background()); got != 0 {
		t.Fatalf("reap on a failed scan = %d, want 0", got)
	}
	want := []passEvent{{pass: SweepPassCancelingExecution, outcome: SweepPassOutcomeError}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

// TestLeaseSweeperResumesInterruptedCancelEndToEnd wires the reaper to the real
// in-memory backend and a real engine: the execution is left in canceling the
// way a crash between Cancel's steps leaves it, the sweeper finds it through
// the backend's canceling index, and the Cancel call it makes carries the
// execution to canceled and empties the index.
func TestLeaseSweeperResumesInterruptedCancelEndToEnd(t *testing.T) {
	be := local.New()
	state := be.State()
	eng := engine.New(state, be.Queue())
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "cancel-sweep",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "n", Type: "test.noop"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	const id types.ExecutionID = "exec-interrupted-cancel"
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	// The interrupted-Cancel state: the canceling write landed, the canceled
	// write never did.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}

	sw := NewLeaseSweeper(state, eng, LeaseSweeperConfig{
		CancelingExecutionReapPeriod: time.Nanosecond,
		CancelingExecutionStuckAfter: time.Nanosecond,
	})
	now := time.Now()
	sw.clock = func() time.Time { return now }
	if got := sw.ReapStuckCancelingExecutionsOnce(ctx); got != 1 {
		t.Fatalf("reap = %d, want the interrupted cancel re-driven", got)
	}

	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	if snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("status after the sweep = %q, want canceled", snap.Status)
	}
	candidates, err := state.(engine.CancelingExecutionLister).ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("canceling index still reports %+v after the resumed cancel", candidates)
	}
}
