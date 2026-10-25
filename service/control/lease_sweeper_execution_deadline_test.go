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
	"github.com/xbcio/xflow/types"
)

// fakeDeadlineLister is a LeaseLister that also implements the deadline index
// the pass is gated on. entries are kept oldest deadline first, like the real
// index, and an entry stays listed until it is acknowledged, so an execution
// that keeps failing keeps coming back at the head of the page.
type fakeDeadlineLister struct {
	*fakeLeaseLister
	mu         sync.Mutex
	entries    []engine.DeadlineExpiredExecution
	err        error
	calls      int
	lastBefore time.Time
	lastLimit  int
}

var _ engine.ExecutionDeadlineExpirer = (*fakeDeadlineLister)(nil)

func (f *fakeDeadlineLister) ListDeadlineExceededExecutions(_ context.Context, before time.Time, limit int) ([]engine.DeadlineExpiredExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastBefore = before
	f.lastLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	var out []engine.DeadlineExpiredExecution
	for _, entry := range f.entries {
		if limit <= 0 || len(out) == limit {
			break
		}
		if entry.Deadline.After(before) {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

func (f *fakeDeadlineLister) AckDeadlineExpired(_ context.Context, id types.ExecutionID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.entries[:0]
	for _, entry := range f.entries {
		if entry.ExecutionID != id {
			kept = append(kept, entry)
		}
	}
	f.entries = kept
	return nil
}

func (f *fakeDeadlineLister) scanCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeDeadlineLister) listed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

type expireCall struct {
	id types.ExecutionID
	ns namespace.Namespace
}

// fakeExpirer is an execution.Engine that also implements ExpireExecution. A
// successful call acknowledges the entry, the way the engine does once an
// execution is handled; an execution in errs fails every time, which is how a
// permanently broken execution is made.
type fakeExpirer struct {
	*fakeReclaimer
	mu       sync.Mutex
	calls    []expireCall
	outcomes map[types.ExecutionID]engine.ExpireOutcome
	errs     map[types.ExecutionID]error
	ack      func(types.ExecutionID)
}

func (e *fakeExpirer) ExpireExecution(ctx context.Context, id types.ExecutionID) (engine.ExpireOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, expireCall{id: id, ns: namespace.FromContext(ctx)})
	if err, ok := e.errs[id]; ok {
		return "", err
	}
	outcome := engine.ExpireOutcomeExpired
	if o, ok := e.outcomes[id]; ok {
		outcome = o
	}
	if e.ack != nil {
		e.ack(id)
	}
	return outcome, nil
}

func (e *fakeExpirer) snapshot() []expireCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]expireCall(nil), e.calls...)
}

func (e *fakeExpirer) callsFor(id types.ExecutionID) int {
	n := 0
	for _, call := range e.snapshot() {
		if call.id == id {
			n++
		}
	}
	return n
}

type deadlineHarness struct {
	*LeaseSweeper
	state    *fakeDeadlineLister
	expirer  *fakeExpirer
	elector  *fakeElector
	observer *recordingCandidateObserver
	logger   *recordingLogger
	now      *time.Time
}

func newDeadlineHarness(cfg LeaseSweeperConfig) *deadlineHarness {
	state := &fakeDeadlineLister{fakeLeaseLister: &fakeLeaseLister{}}
	expirer := &fakeExpirer{
		fakeReclaimer: &fakeReclaimer{},
		outcomes:      map[types.ExecutionID]engine.ExpireOutcome{},
		errs:          map[types.ExecutionID]error{},
		ack:           func(id types.ExecutionID) { _ = state.AckDeadlineExpired(context.Background(), id) },
	}
	elector := &fakeElector{}
	elector.leader.Store(true)
	observer := newRecordingCandidateObserver()
	logger := &recordingLogger{}
	cfg.Elector, cfg.Observer, cfg.Logger = elector, observer, logger
	sw := NewLeaseSweeper(state, expirer, cfg)
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	return &deadlineHarness{
		LeaseSweeper: sw,
		state:        state,
		expirer:      expirer,
		elector:      elector,
		observer:     observer,
		logger:       logger,
		now:          &now,
	}
}

// everyPass is a config whose cadence never gates a pass, so a test moves the
// clock only to move the backoff windows.
func everyPass(batch int) LeaseSweeperConfig {
	return LeaseSweeperConfig{ExecutionDeadlinePeriod: time.Nanosecond, ExecutionDeadlineBatch: batch}
}

func (h *deadlineHarness) pass() int {
	return h.ReapExpiredExecutionsOnce(context.Background())
}

// list adds n entries named prefix-0 .. prefix-(n-1), due one second apart
// from base, after the entries already listed.
func (h *deadlineHarness) list(prefix string, n int, base time.Time) []types.ExecutionID {
	ids := make([]types.ExecutionID, 0, n)
	for i := 0; i < n; i++ {
		id := types.ExecutionID(fmt.Sprintf("%s-%d", prefix, i))
		ids = append(ids, id)
		h.state.entries = append(h.state.entries, engine.DeadlineExpiredExecution{
			ExecutionID: id,
			Deadline:    base.Add(time.Duration(i) * time.Second),
		})
	}
	return ids
}

func (h *deadlineHarness) warnings(msg string) []recordedLogEntry {
	var out []recordedLogEntry
	for _, entry := range h.logger.withMsg(msg) {
		if entry.level == "warn" {
			out = append(out, entry)
		}
	}
	return out
}

const keepsFailingMsg = "execution keeps failing to expire past its deadline"

var errStoreDown = errors.New("store unavailable")

func TestLeaseSweeperExpiresExecutionsPastTheirDeadline(t *testing.T) {
	h := newDeadlineHarness(LeaseSweeperConfig{})
	base := h.now.Add(-time.Hour)
	h.state.entries = []engine.DeadlineExpiredExecution{
		{ExecutionID: "exec-expired", Namespace: "tenant-a", Deadline: base},
		{ExecutionID: "exec-resumed", Deadline: base.Add(time.Second)},
		{ExecutionID: "exec-not-due", Deadline: base.Add(2 * time.Second)},
		{ExecutionID: "exec-terminal", Deadline: base.Add(3 * time.Second)},
		{ExecutionID: "exec-canceling", Deadline: base.Add(4 * time.Second)},
		{ExecutionID: "exec-gone", Deadline: base.Add(5 * time.Second)},
		{ExecutionID: "exec-inactive", Deadline: base.Add(6 * time.Second)},
	}
	h.expirer.outcomes["exec-resumed"] = engine.ExpireOutcomeResumed
	h.expirer.outcomes["exec-not-due"] = engine.ExpireOutcomeNotDue
	h.expirer.outcomes["exec-terminal"] = engine.ExpireOutcomeAlreadyTerminal
	h.expirer.outcomes["exec-canceling"] = engine.ExpireOutcomeCanceling
	h.expirer.outcomes["exec-gone"] = engine.ExpireOutcomeGone
	h.expirer.errs["exec-inactive"] = fmt.Errorf("execution %q: %w", "exec-inactive", engine.ErrExecutionInactive)

	if got := h.pass(); got != 2 {
		t.Fatalf("ReapExpiredExecutionsOnce() = %d, want 2 (expired and resumed)", got)
	}

	if !h.state.lastBefore.Equal(*h.now) {
		t.Fatalf("scan before = %v, want now %v (the deadline is the threshold)", h.state.lastBefore, *h.now)
	}
	if h.state.lastLimit != defaultExecutionDeadlineBatch {
		t.Fatalf("scan limit = %d, want the batch %d", h.state.lastLimit, defaultExecutionDeadlineBatch)
	}
	calls := h.expirer.snapshot()
	if len(calls) != 7 {
		t.Fatalf("ExpireExecution calls = %+v, want every candidate attempted", calls)
	}
	if calls[0].id != "exec-expired" || calls[0].ns != "tenant-a" {
		t.Fatalf("first call = %+v, want exec-expired scoped to tenant-a", calls[0])
	}
	if want := namespace.FromContext(context.Background()); calls[1].ns != want {
		t.Fatalf("namespace-less candidate ran under %q, want the sweeper context's %q", calls[1].ns, want)
	}
	if errs := h.logger.withMsg("expire execution past its deadline"); len(errs) != 0 {
		t.Fatalf("log entries = %+v, want none for benign outcomes", errs)
	}
	if got := len(h.warnings(keepsFailingMsg)); got != 0 {
		t.Fatalf("failure warnings = %d, want 0: nothing failed", got)
	}
	for _, id := range []types.ExecutionID{"exec-not-due", "exec-inactive"} {
		key := executionDeadlineKey(engine.DeadlineExpiredExecution{ExecutionID: id})
		if got := h.deadlineBackoff.Failures(key); got != 0 {
			t.Fatalf("Failures(%s) = %d, want a benign result not to back off", id, got)
		}
	}

	wantPass := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeRan, released: 2}}
	if got := h.observer.snapshot(); !equalPassEvents(got, wantPass) {
		t.Fatalf("pass events = %+v, want %+v", got, wantPass)
	}
	wantCandidates := []candidateEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeRan, inspected: 7}}
	if got := h.observer.candidateSnapshot(); !equalCandidateEvents(got, wantCandidates) {
		t.Fatalf("candidate events = %+v, want %+v", got, wantCandidates)
	}
}

func TestLeaseSweeperExecutionDeadlinePassIsLeaderGated(t *testing.T) {
	h := newDeadlineHarness(LeaseSweeperConfig{})
	h.elector.leader.Store(false)
	h.list("exec", 1, h.now.Add(-time.Hour))

	if got := h.pass(); got != 0 {
		t.Fatalf("non-leader pass = %d, want 0", got)
	}
	if got := h.state.scanCalls(); got != 0 {
		t.Fatalf("non-leader scanned the index %d times, want 0", got)
	}
	if got := len(h.expirer.snapshot()); got != 0 {
		t.Fatalf("non-leader expired %d executions, want 0", got)
	}
	want := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeSkippedNotLeader}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
	if got := h.observer.candidateSnapshot(); len(got) != 1 || got[0].inspected != 0 {
		t.Fatalf("candidate events = %+v, want one zero-count skip event", got)
	}
}

func TestLeaseSweeperExecutionDeadlinePassHonorsCadence(t *testing.T) {
	h := newDeadlineHarness(LeaseSweeperConfig{})

	h.pass()
	if got := h.state.scanCalls(); got != 1 {
		t.Fatalf("first pass scans = %d, want 1", got)
	}
	h.pass()
	if got := h.state.scanCalls(); got != 1 {
		t.Fatalf("an immediate second pass scanned again (%d scans): the cadence gate did not hold", got)
	}
	*h.now = h.now.Add(DefaultSweepPeriod)
	h.pass()
	if got := h.state.scanCalls(); got != 2 {
		t.Fatalf("pass after the period scans = %d, want 2", got)
	}

	want := []passEvent{
		{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeRan},
		{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeSkippedCadence},
		{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeRan},
	}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

func TestLeaseSweeperExecutionDeadlinePassReportsUnsupportedCapabilities(t *testing.T) {
	want := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeUnsupported}}

	t.Run("store without the index", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.LeaseSweeper.state = &fakeLeaseLister{}
		if got := h.pass(); got != 0 {
			t.Fatalf("pass without a lister = %d, want 0", got)
		}
		if got := len(h.expirer.snapshot()); got != 0 {
			t.Fatalf("pass without a lister expired %d executions, want 0", got)
		}
		if got := h.observer.snapshot(); !equalPassEvents(got, want) {
			t.Fatalf("pass events = %+v, want %+v", got, want)
		}
	})

	t.Run("engine without ExpireExecution", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.engine = &fakeReclaimer{}
		h.list("exec", 1, h.now.Add(-time.Hour))
		if got := h.pass(); got != 0 {
			t.Fatalf("pass without an expirer = %d, want 0", got)
		}
		if got := h.state.scanCalls(); got != 0 {
			t.Fatalf("pass without an expirer scanned %d times, want 0", got)
		}
		if got := h.observer.snapshot(); !equalPassEvents(got, want) {
			t.Fatalf("pass events = %+v, want %+v", got, want)
		}
	})
}

func TestLeaseSweeperExecutionDeadlinePassReportsListError(t *testing.T) {
	h := newDeadlineHarness(LeaseSweeperConfig{})
	h.state.err = errors.New("index unreadable")

	if got := h.pass(); got != 0 {
		t.Fatalf("pass on a failed scan = %d, want 0", got)
	}
	want := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeError}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

// TestLeaseSweeperExecutionDeadlineFailuresDoNotStarveNewEntries is the
// starvation regression. The index lists the oldest deadlines first and keeps a
// failing execution listed, so without per-execution backoff the first batch
// entries that fail occupy every page and an execution that became due later is
// never reached. Here 40 permanently failing entries sit ahead of one fresh one
// with a batch of 10: the fresh one must still be expired once the failing ones
// are backing off.
func TestLeaseSweeperExecutionDeadlineFailuresDoNotStarveNewEntries(t *testing.T) {
	const batch = 10
	h := newDeadlineHarness(everyPass(batch))
	base := h.now.Add(-time.Hour)
	for _, id := range h.list("broken", batch+30, base) {
		h.expirer.errs[id] = errStoreDown
	}
	fresh := h.list("fresh", 1, base.Add(time.Minute))[0]

	for round := 0; round < 12; round++ {
		h.pass()
		*h.now = h.now.Add(10 * time.Millisecond)
		if h.expirer.callsFor(fresh) > 0 {
			break
		}
	}

	if got := h.expirer.callsFor(fresh); got != 1 {
		t.Fatalf("the fresh execution was expired %d times, want exactly 1: failing entries starved it", got)
	}
	if got := h.state.listed(); got != batch+30 {
		t.Fatalf("entries still listed = %d, want only the broken ones (%d)", got, batch+30)
	}
}

// TestLeaseSweeperExecutionDeadlineBacksOffAFailingExecution pins that a failing
// execution is not called again inside its backoff window, and is again once the
// window has passed, with the window doubling.
func TestLeaseSweeperExecutionDeadlineBacksOffAFailingExecution(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	broken := h.list("broken", 1, h.now.Add(-time.Hour))[0]
	h.expirer.errs[broken] = errStoreDown

	steps := []struct {
		advance time.Duration
		calls   int
	}{
		{0, 1},                       // first failure: wait 1s
		{500 * time.Millisecond, 1},  // inside the window
		{500 * time.Millisecond, 2},  // window over; second failure: wait 2s
		{1500 * time.Millisecond, 2}, // 1.5s into the 2s window
		{500 * time.Millisecond, 3},  // window over; third failure: wait 4s
		{3999 * time.Millisecond, 3}, // just short of the 4s window
		{time.Millisecond, 4},
	}
	for i, step := range steps {
		*h.now = h.now.Add(step.advance)
		h.pass()
		if got := h.expirer.callsFor(broken); got != step.calls {
			t.Fatalf("step %d (+%v): ExpireExecution calls = %d, want %d", i, step.advance, got, step.calls)
		}
	}
}

func TestLeaseSweeperExecutionDeadlineRecoversAfterABackoff(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	flaky := h.list("flaky", 1, h.now.Add(-time.Hour))[0]
	h.expirer.errs[flaky] = errStoreDown

	h.pass()
	if got := h.deadlineBackoff.Failures(executionDeadlineKey(h.state.entries[0])); got != 1 {
		t.Fatalf("Failures after the first attempt = %d, want 1", got)
	}
	delete(h.expirer.errs, flaky)
	*h.now = h.now.Add(time.Second)
	if got := h.pass(); got != 1 {
		t.Fatalf("pass after the store recovered = %d, want the execution expired", got)
	}
	if got := h.deadlineBackoff.Len(); got != 0 {
		t.Fatalf("backoff records = %d, want the success to clear them", got)
	}
}

// TestLeaseSweeperExecutionDeadlineAlertsOnlyAtFailureThresholds pins the
// failure-count alert: a stuck execution warns when it reaches 5, 20 and 100
// consecutive failures and not on the attempts in between.
func TestLeaseSweeperExecutionDeadlineAlertsOnlyAtFailureThresholds(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	broken := h.list("broken", 1, h.now.Add(-time.Hour))[0]
	h.expirer.errs[broken] = errStoreDown

	for attempt := 1; attempt <= 120; attempt++ {
		h.pass()
		// Past the longest backoff, so every pass is an attempt.
		*h.now = h.now.Add(10 * time.Minute)
	}

	if got := h.expirer.callsFor(broken); got != 120 {
		t.Fatalf("ExpireExecution calls = %d, want 120", got)
	}
	warns := h.warnings(keepsFailingMsg)
	var counts []int
	for _, entry := range warns {
		n, _ := entry.field("consecutive_failures").(int)
		counts = append(counts, n)
		if exec := entry.field("exec"); exec != string(broken) {
			t.Fatalf("warning exec = %v, want %s", exec, broken)
		}
		if err, _ := entry.field("err").(error); !errors.Is(err, errStoreDown) {
			t.Fatalf("warning err = %v, want the underlying failure", entry.field("err"))
		}
	}
	if want := []int{5, 20, 100}; fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Fatalf("alerted at failure counts %v, want %v", counts, want)
	}
}

// TestLeaseSweeperExecutionDeadlineWarnsOnceWhenFailuresSaturateTheScan covers
// the bound on the over-fetch: with a batch of 2 the scan reads at most 20, so
// 30 executions that keep failing fill it, and the pass says so once instead of
// on every cadence.
func TestLeaseSweeperExecutionDeadlineWarnsOnceWhenFailuresSaturateTheScan(t *testing.T) {
	const batch = 2
	h := newDeadlineHarness(everyPass(batch))
	for _, id := range h.list("broken", 30, h.now.Add(-time.Hour)) {
		h.expirer.errs[id] = errStoreDown
	}

	for round := 0; round < 16; round++ {
		h.pass()
		*h.now = h.now.Add(time.Millisecond)
	}

	if want := batch * executionDeadlineFetchFactor; h.state.lastLimit != want {
		t.Fatalf("scan limit = %d, want it held at %d", h.state.lastLimit, want)
	}
	const saturatedMsg = "execution deadline scan is saturated by executions that keep failing to expire"
	if got := len(h.warnings(saturatedMsg)); got != 1 {
		t.Fatalf("saturation warnings = %d, want exactly 1 for the episode", got)
	}
}

func TestLeaseSweeperExecutionDeadlineStopsWhenLeadershipIsLost(t *testing.T) {
	h := newDeadlineHarness(everyPass(100))
	h.list("exec", 3*leaderRecheckInterval, h.now.Add(-time.Hour))
	acked := 0
	h.expirer.ack = func(id types.ExecutionID) {
		_ = h.state.AckDeadlineExpired(context.Background(), id)
		acked++
		if acked == leaderRecheckInterval {
			h.elector.leader.Store(false)
		}
	}

	if got := h.pass(); got != leaderRecheckInterval {
		t.Fatalf("pass = %d, want it to stop at the first leadership re-check (%d)", got, leaderRecheckInterval)
	}
	want := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeSkippedNotLeader, released: leaderRecheckInterval}}
	if got := h.observer.snapshot(); !equalPassEvents(got, want) {
		t.Fatalf("pass events = %+v, want %+v", got, want)
	}
}

func TestLeaseSweeperExecutionDeadlineBackoffIsPerNamespace(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	base := h.now.Add(-time.Hour)
	h.state.entries = []engine.DeadlineExpiredExecution{
		{ExecutionID: "shared", Namespace: "tenant-a", Deadline: base},
		{ExecutionID: "shared", Namespace: "tenant-b", Deadline: base.Add(time.Second)},
	}
	h.expirer.errs["shared"] = errStoreDown

	h.pass()
	if got := h.deadlineBackoff.Len(); got != 2 {
		t.Fatalf("backoff records = %d, want one per namespace", got)
	}
}
