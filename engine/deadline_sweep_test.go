package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var sweepEpoch = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

var errSweepStoreDown = errors.New("store unavailable")

// sweepFakeIndex is a deadline index that lists oldest first and keeps an entry
// until it is acknowledged, like the real ones, so an execution that keeps
// failing keeps coming back at the head of the page.
type sweepFakeIndex struct {
	mu        sync.Mutex
	entries   []DeadlineExpiredExecution
	err       error
	lastLimit int
}

func (f *sweepFakeIndex) ListDeadlineExceededExecutions(_ context.Context, before time.Time, limit int) ([]DeadlineExpiredExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	var out []DeadlineExpiredExecution
	for _, entry := range f.entries {
		if len(out) >= limit {
			break
		}
		if !entry.Deadline.After(before) {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (f *sweepFakeIndex) AckDeadlineExpired(_ context.Context, id types.ExecutionID) error {
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

func (f *sweepFakeIndex) add(prefix string, n int, base time.Time) []types.ExecutionID {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]types.ExecutionID, 0, n)
	for i := 0; i < n; i++ {
		id := types.ExecutionID(fmt.Sprintf("%s-%d", prefix, i))
		ids = append(ids, id)
		f.entries = append(f.entries, DeadlineExpiredExecution{
			ExecutionID: id,
			Deadline:    base.Add(time.Duration(i) * time.Second),
		})
	}
	return ids
}

func (f *sweepFakeIndex) listed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

// sweepFakeExpirer answers ExpireExecution from tables. A call that returns no
// error acknowledges the entry unless the outcome leaves it listed (Canceling and
// Gone), the way the engine does.
type sweepFakeExpirer struct {
	mu       sync.Mutex
	index    *sweepFakeIndex
	outcomes map[types.ExecutionID]ExpireOutcome
	errs     map[types.ExecutionID]error
	calls    map[types.ExecutionID]int
	nsSeen   map[types.ExecutionID]namespace.Namespace
}

func newSweepFakeExpirer(index *sweepFakeIndex) *sweepFakeExpirer {
	return &sweepFakeExpirer{
		index:    index,
		outcomes: map[types.ExecutionID]ExpireOutcome{},
		errs:     map[types.ExecutionID]error{},
		calls:    map[types.ExecutionID]int{},
		nsSeen:   map[types.ExecutionID]namespace.Namespace{},
	}
}

func (e *sweepFakeExpirer) ExpireExecution(ctx context.Context, id types.ExecutionID) (ExpireOutcome, error) {
	e.mu.Lock()
	e.calls[id]++
	e.nsSeen[id] = namespace.FromContext(ctx)
	err := e.errs[id]
	outcome, ok := e.outcomes[id]
	e.mu.Unlock()
	if err != nil {
		return "", err
	}
	if !ok {
		outcome = ExpireOutcomeExpired
	}
	if outcome != ExpireOutcomeCanceling && outcome != ExpireOutcomeGone {
		_ = e.index.AckDeadlineExpired(ctx, id)
	}
	return outcome, nil
}

func (e *sweepFakeExpirer) callsFor(id types.ExecutionID) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[id]
}

func (e *sweepFakeExpirer) setErr(id types.ExecutionID, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err == nil {
		delete(e.errs, id)
		return
	}
	e.errs[id] = err
}

type sweepWarning struct {
	msg  string
	args []any
}

func (w sweepWarning) field(key string) any {
	for i := 0; i+1 < len(w.args); i += 2 {
		if w.args[i] == key {
			return w.args[i+1]
		}
	}
	return nil
}

type sweepWarnLog struct {
	mu    sync.Mutex
	warns []sweepWarning
}

func (l *sweepWarnLog) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, sweepWarning{msg: msg, args: append([]any(nil), args...)})
}

func (l *sweepWarnLog) withMsg(msg string) []sweepWarning {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []sweepWarning
	for _, w := range l.warns {
		if w.msg == msg {
			out = append(out, w)
		}
	}
	return out
}

const (
	sweepKeepsFailingMsg = "execution keeps failing to expire past its deadline"
	sweepSaturatedMsg    = "execution deadline scan is saturated by executions that keep failing to expire"
)

type sweepHarness struct {
	*DeadlineSweep
	index   *sweepFakeIndex
	expirer *sweepFakeExpirer
	log     *sweepWarnLog
	backoff *ExpireBackoff
	now     time.Time
}

func newSweepHarness(batch int, mutate func(*DeadlineSweepConfig)) *sweepHarness {
	h := &sweepHarness{index: &sweepFakeIndex{}, log: &sweepWarnLog{}, backoff: NewExpireBackoff(), now: sweepEpoch}
	h.expirer = newSweepFakeExpirer(h.index)
	cfg := DeadlineSweepConfig{
		Lister:  h.index,
		Expirer: h.expirer,
		Backoff: h.backoff,
		Batch:   batch,
		Clock:   func() time.Time { return h.now },
		Logger:  h.log,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h.DeadlineSweep = NewDeadlineSweep(cfg)
	return h
}

func (h *sweepHarness) sweep(t *testing.T) DeadlineSweepResult {
	t.Helper()
	result, err := h.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	return result
}

func (h *sweepHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

func TestDeadlineSweepMapsEveryOutcome(t *testing.T) {
	var reported []DeadlineExpiredExecution
	var reportedNS []namespace.Namespace
	h := newSweepHarness(0, func(cfg *DeadlineSweepConfig) {
		cfg.OnExpired = func(ctx context.Context, candidate DeadlineExpiredExecution) {
			reported = append(reported, candidate)
			reportedNS = append(reportedNS, namespace.FromContext(ctx))
		}
	})
	base := sweepEpoch.Add(-time.Hour)
	h.index.entries = []DeadlineExpiredExecution{
		{ExecutionID: "exec-expired", Namespace: "tenant-a", Deadline: base},
		{ExecutionID: "exec-resumed", Deadline: base.Add(time.Second)},
		{ExecutionID: "exec-not-due", Deadline: base.Add(2 * time.Second)},
		{ExecutionID: "exec-terminal", Deadline: base.Add(3 * time.Second)},
		{ExecutionID: "exec-canceling", Deadline: base.Add(4 * time.Second)},
		{ExecutionID: "exec-gone", Deadline: base.Add(5 * time.Second)},
		{ExecutionID: "exec-inactive", Deadline: base.Add(6 * time.Second)},
		{ExecutionID: "exec-broken", Deadline: base.Add(7 * time.Second)},
	}
	h.expirer.outcomes["exec-resumed"] = ExpireOutcomeResumed
	h.expirer.outcomes["exec-not-due"] = ExpireOutcomeNotDue
	h.expirer.outcomes["exec-terminal"] = ExpireOutcomeAlreadyTerminal
	h.expirer.outcomes["exec-canceling"] = ExpireOutcomeCanceling
	h.expirer.outcomes["exec-gone"] = ExpireOutcomeGone
	h.expirer.errs["exec-inactive"] = fmt.Errorf("execution %q: %w", "exec-inactive", ErrExecutionInactive)
	h.expirer.errs["exec-broken"] = errSweepStoreDown

	got := h.sweep(t)

	want := DeadlineSweepResult{Listed: 8, Attempted: 8, Released: 2, Expired: 1, Failed: 1, Deferred: 2}
	if got != want {
		t.Fatalf("Sweep() = %+v, want %+v", got, want)
	}
	if h.index.lastLimit != DefaultDeadlineSweepBatch {
		t.Fatalf("scan limit = %d, want the default batch %d", h.index.lastLimit, DefaultDeadlineSweepBatch)
	}
	if ns := h.expirer.nsSeen["exec-expired"]; ns != "tenant-a" {
		t.Fatalf("exec-expired ran under namespace %q, want tenant-a", ns)
	}
	if ns, want := h.expirer.nsSeen["exec-resumed"], namespace.FromContext(context.Background()); ns != want {
		t.Fatalf("namespace-less candidate ran under %q, want the sweep context's %q", ns, want)
	}
	if len(reported) != 1 || reported[0].ExecutionID != "exec-expired" || reportedNS[0] != "tenant-a" {
		t.Fatalf("OnExpired reported %+v under %v, want only exec-expired under tenant-a (a resumed cascade is not a new expiry)", reported, reportedNS)
	}
	for _, id := range []types.ExecutionID{"exec-not-due", "exec-terminal", "exec-gone", "exec-inactive", "exec-canceling"} {
		if f := h.backoff.Failures(DeadlineSweepKey(DeadlineExpiredExecution{ExecutionID: id})); f != 0 {
			t.Fatalf("Failures(%s) = %d, want a benign result not to count as a failure", id, f)
		}
	}
	if f := h.backoff.Failures(DeadlineSweepKey(DeadlineExpiredExecution{ExecutionID: "exec-broken"})); f != 1 {
		t.Fatalf("Failures(exec-broken) = %d, want 1", f)
	}
	if w := h.log.withMsg(sweepKeepsFailingMsg); len(w) != 0 {
		t.Fatalf("failure warnings = %+v, want none below the first threshold", w)
	}
}

// TestDeadlineSweepFailuresDoNotStarveNewEntries is the starvation regression.
// The index lists the oldest deadlines first and keeps a failing execution
// listed, so without per-execution backoff the first batch entries that fail
// occupy every page and an execution that became due later is never reached.
// Here 40 permanently failing entries sit ahead of one fresh one with a batch of
// 10: the fresh one must still be expired once the failing ones are backing off.
func TestDeadlineSweepFailuresDoNotStarveNewEntries(t *testing.T) {
	const batch = 10
	h := newSweepHarness(batch, nil)
	base := sweepEpoch.Add(-time.Hour)
	for _, id := range h.index.add("broken", batch+30, base) {
		h.expirer.setErr(id, errSweepStoreDown)
	}
	fresh := h.index.add("fresh", 1, base.Add(time.Minute))[0]

	for round := 0; round < 12 && h.expirer.callsFor(fresh) == 0; round++ {
		h.sweep(t)
		h.advance(10 * time.Millisecond)
	}

	if got := h.expirer.callsFor(fresh); got != 1 {
		t.Fatalf("the fresh execution was expired %d times, want exactly 1: failing entries starved it", got)
	}
	if got := h.index.listed(); got != batch+30 {
		t.Fatalf("entries still listed = %d, want only the broken ones (%d)", got, batch+30)
	}
}

// TestDeadlineSweepCancelingDoesNotStarveNewEntries is the same regression for
// executions that are canceling: ExpireExecution leaves their entry listed until
// the cancel concludes and reports no error, so only a deferral keeps 40 of them
// from holding every slot of a 10-wide page ahead of a fresh one.
func TestDeadlineSweepCancelingDoesNotStarveNewEntries(t *testing.T) {
	const batch = 10
	h := newSweepHarness(batch, nil)
	base := sweepEpoch.Add(-time.Hour)
	for _, id := range h.index.add("canceling", batch+30, base) {
		h.expirer.outcomes[id] = ExpireOutcomeCanceling
	}
	fresh := h.index.add("fresh", 1, base.Add(time.Minute))[0]

	var deferred int
	for round := 0; round < 12 && h.expirer.callsFor(fresh) == 0; round++ {
		result := h.sweep(t)
		deferred += result.Deferred
		if result.Failed != 0 {
			t.Fatalf("round %d: Failed = %d, want canceling not to count as a failure", round, result.Failed)
		}
		h.advance(10 * time.Millisecond)
	}

	if got := h.expirer.callsFor(fresh); got != 1 {
		t.Fatalf("the fresh execution was expired %d times, want exactly 1: canceling entries starved it", got)
	}
	if deferred != batch+30 {
		t.Fatalf("deferred %d executions, want each of the %d canceling ones set aside once", deferred, batch+30)
	}
	if got := h.index.listed(); got != batch+30 {
		t.Fatalf("entries still listed = %d, want only the canceling ones (%d)", got, batch+30)
	}
}

// TestDeadlineSweepGoneDoesNotStarveNewEntries is the same regression for
// entries whose execution keys are missing: ExpireExecution reports Gone without
// acknowledging the entry (the keys may only be a replica lag away from
// reappearing), so only a deferral keeps 40 of them from holding every slot of a
// 10-wide page ahead of a fresh one, and from being retried on every round.
func TestDeadlineSweepGoneDoesNotStarveNewEntries(t *testing.T) {
	const batch = 10
	h := newSweepHarness(batch, nil)
	base := sweepEpoch.Add(-time.Hour)
	goneIDs := h.index.add("gone", batch+30, base)
	for _, id := range goneIDs {
		h.expirer.outcomes[id] = ExpireOutcomeGone
	}
	fresh := h.index.add("fresh", 1, base.Add(time.Minute))[0]

	var deferred, released int
	for round := 0; round < 12 && h.expirer.callsFor(fresh) == 0; round++ {
		result := h.sweep(t)
		deferred += result.Deferred
		released += result.Released
		if result.Failed != 0 {
			t.Fatalf("round %d: Failed = %d, want gone not to count as a failure", round, result.Failed)
		}
		h.advance(10 * time.Millisecond)
	}

	if got := h.expirer.callsFor(fresh); got != 1 {
		t.Fatalf("the fresh execution was expired %d times, want exactly 1: gone entries starved it", got)
	}
	if released != 1 {
		t.Fatalf("released %d executions, want only the fresh one: gone must not count as released", released)
	}
	if deferred != batch+30 {
		t.Fatalf("deferred %d executions, want each of the %d gone ones set aside once", deferred, batch+30)
	}
	for _, id := range goneIDs {
		if got := h.expirer.callsFor(id); got != 1 {
			t.Fatalf("%s was attempted %d times, want 1: a gone entry must wait out its window", id, got)
		}
	}
	if got := h.index.listed(); got != batch+30 {
		t.Fatalf("entries still listed = %d, want only the gone ones (%d): the sweep must not acknowledge them", got, batch+30)
	}
}

// TestDeadlineSweepAlertsOnlyAtFailureThresholds pins the failure-count alert: a
// stuck execution warns when it reaches 5, 20 and 100 consecutive failures and
// not on the attempts in between.
func TestDeadlineSweepAlertsOnlyAtFailureThresholds(t *testing.T) {
	h := newSweepHarness(5, nil)
	broken := h.index.add("broken", 1, sweepEpoch.Add(-time.Hour))[0]
	h.expirer.setErr(broken, errSweepStoreDown)

	for attempt := 1; attempt <= 120; attempt++ {
		h.sweep(t)
		// Past the longest backoff, so every sweep is an attempt.
		h.advance(10 * time.Minute)
	}

	if got := h.expirer.callsFor(broken); got != 120 {
		t.Fatalf("ExpireExecution calls = %d, want 120", got)
	}
	var counts []int
	for _, w := range h.log.withMsg(sweepKeepsFailingMsg) {
		n, _ := w.field("consecutive_failures").(int)
		counts = append(counts, n)
		if exec := w.field("exec"); exec != string(broken) {
			t.Fatalf("warning exec = %v, want %s", exec, broken)
		}
		if err, _ := w.field("err").(error); !errors.Is(err, errSweepStoreDown) {
			t.Fatalf("warning err = %v, want the underlying failure", w.field("err"))
		}
	}
	if want := []int{5, 20, 100}; fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Fatalf("alerted at failure counts %v, want %v", counts, want)
	}
}

// TestDeadlineSweepCancelingNeverAlerts pins that a canceling execution, however
// many times it comes back, is not a failure and so never reaches the alert.
func TestDeadlineSweepCancelingNeverAlerts(t *testing.T) {
	h := newSweepHarness(5, nil)
	stuck := h.index.add("stuck", 1, sweepEpoch.Add(-time.Hour))[0]
	h.expirer.outcomes[stuck] = ExpireOutcomeCanceling

	for attempt := 1; attempt <= 120; attempt++ {
		h.sweep(t)
		h.advance(10 * time.Minute)
	}

	if got := h.expirer.callsFor(stuck); got != 120 {
		t.Fatalf("ExpireExecution calls = %d, want 120", got)
	}
	if w := h.log.withMsg(sweepKeepsFailingMsg); len(w) != 0 {
		t.Fatalf("failure warnings = %+v, want none for a canceling execution", w)
	}
}

// TestDeadlineSweepWarnsOnceWhenFailuresSaturateTheScan covers the bound on the
// over-fetch: with a batch of 2 the scan reads at most 20, so 30 executions that
// keep failing fill it, and the sweep says so once instead of on every pass.
func TestDeadlineSweepWarnsOnceWhenFailuresSaturateTheScan(t *testing.T) {
	const batch = 2
	h := newSweepHarness(batch, nil)
	for _, id := range h.index.add("broken", 30, sweepEpoch.Add(-time.Hour)) {
		h.expirer.setErr(id, errSweepStoreDown)
	}

	for round := 0; round < 16; round++ {
		h.sweep(t)
		h.advance(time.Millisecond)
	}

	if want := batch * DefaultDeadlineSweepFetchFactor; h.index.lastLimit != want {
		t.Fatalf("scan limit = %d, want it held at %d", h.index.lastLimit, want)
	}
	if got := len(h.log.withMsg(sweepSaturatedMsg)); got != 1 {
		t.Fatalf("saturation warnings = %d, want exactly 1 for the episode", got)
	}
}

// TestDeadlineSweepSaturationWarningDoesNotFlap pins the hysteresis: with more
// failing executions than the scan can read, the count of those backing off
// hovers around the cap as windows expire and renew. Re-arming the warning at
// the cap itself logged it again every few passes (25 times in these 200).
func TestDeadlineSweepSaturationWarningDoesNotFlap(t *testing.T) {
	h := newSweepHarness(100, nil)
	for _, id := range h.index.add("broken", 1200, sweepEpoch.Add(-time.Hour)) {
		h.expirer.setErr(id, errSweepStoreDown)
	}

	for round := 0; round < 200; round++ {
		h.sweep(t)
		h.advance(time.Second)
	}

	if got := len(h.log.withMsg(sweepSaturatedMsg)); got != 1 {
		t.Fatalf("saturation warnings = %d, want exactly 1 for the episode", got)
	}
}

// TestDeadlineSweepSaturationWarningRearmsOnceTheBackoffDrains pins the other
// half: when the executions set aside have mostly cleared, the next saturation is
// a new episode and warns again.
func TestDeadlineSweepSaturationWarningRearmsOnceTheBackoffDrains(t *testing.T) {
	const batch = 2
	h := newSweepHarness(batch, nil)
	first := h.index.add("first", 30, sweepEpoch.Add(-time.Hour))
	for _, id := range first {
		h.expirer.setErr(id, errSweepStoreDown)
	}
	for round := 0; round < 16; round++ {
		h.sweep(t)
		h.advance(time.Millisecond)
	}
	if got := len(h.log.withMsg(sweepSaturatedMsg)); got != 1 {
		t.Fatalf("saturation warnings in the first episode = %d, want 1", got)
	}

	// The store recovers: past the longest backoff every sweep attempts a batch,
	// and the 30 executions are expired two at a time.
	for _, id := range first {
		h.expirer.setErr(id, nil)
	}
	h.advance(10 * time.Minute)
	for h.index.listed() > 0 {
		h.sweep(t)
	}
	h.sweep(t)

	for _, id := range h.index.add("second", 30, sweepEpoch.Add(-time.Minute)) {
		h.expirer.setErr(id, errSweepStoreDown)
	}
	for round := 0; round < 16; round++ {
		h.sweep(t)
		h.advance(time.Millisecond)
	}
	if got := len(h.log.withMsg(sweepSaturatedMsg)); got != 2 {
		t.Fatalf("saturation warnings after a second episode = %d, want 2", got)
	}
}

func TestDeadlineSweepFetchesPastWhatIsBackingOff(t *testing.T) {
	const batch = 4
	h := newSweepHarness(batch, nil)
	for _, id := range h.index.add("broken", 3, sweepEpoch.Add(-time.Hour)) {
		h.expirer.setErr(id, errSweepStoreDown)
	}

	h.sweep(t)
	if h.index.lastLimit != batch {
		t.Fatalf("first scan limit = %d, want the batch %d: nothing is backing off yet", h.index.lastLimit, batch)
	}
	h.advance(10 * time.Millisecond)
	result := h.sweep(t)
	if want := batch + 3; h.index.lastLimit != want {
		t.Fatalf("scan limit = %d, want the batch plus the 3 backing off (%d)", h.index.lastLimit, want)
	}
	if result.SkippedBackoff != 3 {
		t.Fatalf("SkippedBackoff = %d, want 3", result.SkippedBackoff)
	}
	if got := len(h.log.withMsg(sweepSaturatedMsg)); got != 0 {
		t.Fatalf("saturation warnings = %d, want none: the bound did not clip the read", got)
	}
}

// TestDeadlineSweepAttemptsAtMostOneBatch pins the batch boundary. The scan reads
// the batch plus one slot per execution backing off, so a stale backoff record
// lets the page carry Batch+1 healthy candidates; the sweep must still attempt
// exactly Batch of them and leave the last one for the next round.
func TestDeadlineSweepAttemptsAtMostOneBatch(t *testing.T) {
	const batch = 4
	h := newSweepHarness(batch, nil)
	// A record for an execution that is no longer listed widens the read by one.
	h.backoff.Failure(DeadlineSweepKey(DeadlineExpiredExecution{ExecutionID: "stale"}), h.now)
	ids := h.index.add("exec", batch+1, sweepEpoch.Add(-time.Hour))

	got := h.sweep(t)

	if h.index.lastLimit != batch+1 {
		t.Fatalf("scan limit = %d, want %d: the test needs the page to carry one candidate too many", h.index.lastLimit, batch+1)
	}
	want := DeadlineSweepResult{Listed: batch + 1, Attempted: batch, Released: batch, Expired: batch}
	if got != want {
		t.Fatalf("Sweep() = %+v, want %+v", got, want)
	}
	if calls := h.expirer.callsFor(ids[batch]); calls != 0 {
		t.Fatalf("the candidate past the batch was attempted %d times, want 0", calls)
	}
	if calls := h.expirer.callsFor(ids[batch-1]); calls != 1 {
		t.Fatalf("the last candidate inside the batch was attempted %d times, want 1", calls)
	}
	if left := h.index.listed(); left != 1 {
		t.Fatalf("entries still listed = %d, want only the one past the batch", left)
	}
}

func TestDeadlineSweepBacksOffAFailingExecution(t *testing.T) {
	h := newSweepHarness(5, nil)
	broken := h.index.add("broken", 1, sweepEpoch.Add(-time.Hour))[0]
	h.expirer.setErr(broken, errSweepStoreDown)

	steps := []struct {
		advance time.Duration
		calls   int
	}{
		{0, 1},
		{500 * time.Millisecond, 1},
		{500 * time.Millisecond, 2},
		{1500 * time.Millisecond, 2},
		{500 * time.Millisecond, 3},
		{3999 * time.Millisecond, 3},
		{time.Millisecond, 4},
	}
	for i, step := range steps {
		h.advance(step.advance)
		h.sweep(t)
		if got := h.expirer.callsFor(broken); got != step.calls {
			t.Fatalf("step %d (+%v): ExpireExecution calls = %d, want %d", i, step.advance, got, step.calls)
		}
	}
}

func TestDeadlineSweepRecoversAfterABackoff(t *testing.T) {
	h := newSweepHarness(5, nil)
	flaky := h.index.add("flaky", 1, sweepEpoch.Add(-time.Hour))[0]
	h.expirer.setErr(flaky, errSweepStoreDown)

	h.sweep(t)
	if got := h.backoff.Failures(DeadlineSweepKey(h.index.entries[0])); got != 1 {
		t.Fatalf("Failures after the first attempt = %d, want 1", got)
	}
	h.expirer.setErr(flaky, nil)
	h.advance(time.Second)
	if got := h.sweep(t); got.Released != 1 {
		t.Fatalf("sweep after the store recovered = %+v, want the execution released", got)
	}
	if got := h.backoff.Len(); got != 0 {
		t.Fatalf("backoff records = %d, want the success to clear them", got)
	}
}

func TestDeadlineSweepBackoffIsPerNamespace(t *testing.T) {
	h := newSweepHarness(5, nil)
	base := sweepEpoch.Add(-time.Hour)
	h.index.entries = []DeadlineExpiredExecution{
		{ExecutionID: "shared", Namespace: "tenant-a", Deadline: base},
		{ExecutionID: "shared", Namespace: "tenant-b", Deadline: base.Add(time.Second)},
	}
	h.expirer.setErr("shared", errSweepStoreDown)

	h.sweep(t)
	if got := h.backoff.Len(); got != 2 {
		t.Fatalf("backoff records = %d, want one per namespace", got)
	}
}

func TestDeadlineSweepReturnsTheScanError(t *testing.T) {
	h := newSweepHarness(5, nil)
	h.index.err = errors.New("index unreadable")

	result, err := h.Sweep(context.Background())

	if !errors.Is(err, h.index.err) {
		t.Fatalf("Sweep() error = %v, want the scan's error", err)
	}
	if result != (DeadlineSweepResult{}) {
		t.Fatalf("Sweep() = %+v, want nothing attempted after a failed scan", result)
	}
}

func TestDeadlineSweepWithoutItsPartsReturnsAnError(t *testing.T) {
	index := &sweepFakeIndex{}
	for name, cfg := range map[string]DeadlineSweepConfig{
		"no lister":  {Expirer: newSweepFakeExpirer(index)},
		"no expirer": {Lister: index},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewDeadlineSweep(cfg).Sweep(context.Background()); err == nil {
				t.Fatal("Sweep() error = nil, want an error for a sweep built without its parts")
			}
		})
	}
}

func TestDeadlineSweepStopsWhenContinueSaysNo(t *testing.T) {
	const every = 4
	h := newSweepHarness(100, nil)
	h.index.add("exec", 3*every, sweepEpoch.Add(-time.Hour))
	checks := 0
	h.DeadlineSweep = NewDeadlineSweep(DeadlineSweepConfig{
		Lister:        h.index,
		Expirer:       h.expirer,
		Backoff:       h.backoff,
		Clock:         func() time.Time { return h.now },
		ContinueEvery: every,
		Continue: func() bool {
			checks++
			return checks < 1
		},
	})

	result := h.sweep(t)

	if result.Stop != DeadlineSweepStopLeadershipLost {
		t.Fatalf("Stop = %q, want %q", result.Stop, DeadlineSweepStopLeadershipLost)
	}
	if result.Attempted != every || result.Released != every {
		t.Fatalf("Sweep() = %+v, want it to stop at the first re-check after %d attempts", result, every)
	}
	if checks != 1 {
		t.Fatalf("Continue called %d times, want 1: the sweep must stop at the first no", checks)
	}
}

func TestDeadlineSweepAsksContinueEveryNCandidates(t *testing.T) {
	const every = 4
	h := newSweepHarness(100, nil)
	h.index.add("exec", 3*every, sweepEpoch.Add(-time.Hour))
	checks := 0
	h.DeadlineSweep = NewDeadlineSweep(DeadlineSweepConfig{
		Lister:        h.index,
		Expirer:       h.expirer,
		Backoff:       h.backoff,
		Clock:         func() time.Time { return h.now },
		ContinueEvery: every,
		Continue:      func() bool { checks++; return true },
	})

	result := h.sweep(t)

	if result.Stop != DeadlineSweepStopNone || result.Released != 3*every {
		t.Fatalf("Sweep() = %+v, want the whole page released", result)
	}
	if checks != 2 {
		t.Fatalf("Continue called %d times over %d candidates every %d, want 2", checks, 3*every, every)
	}
}

func TestDeadlineSweepStopsWhenTheContextEnds(t *testing.T) {
	h := newSweepHarness(100, nil)
	h.index.add("exec", 10, sweepEpoch.Add(-time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := h.Sweep(ctx)

	if err != nil {
		t.Fatalf("Sweep() error = %v, want a canceled context to end the sweep quietly", err)
	}
	if result.Stop != DeadlineSweepStopCanceled || result.Attempted != 0 {
		t.Fatalf("Sweep() = %+v, want it stopped before the first attempt", result)
	}
}

// TestDeadlineSweepShutdownIsNotAFailure pins that an ExpireExecution error that
// arrives because the context ended does not count against the execution.
func TestDeadlineSweepShutdownIsNotAFailure(t *testing.T) {
	h := newSweepHarness(100, nil)
	ids := h.index.add("exec", 1, sweepEpoch.Add(-time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	h.DeadlineSweep = NewDeadlineSweep(DeadlineSweepConfig{
		Lister: h.index,
		Expirer: expirerFunc(func(context.Context, types.ExecutionID) (ExpireOutcome, error) {
			cancel()
			return "", context.Canceled
		}),
		Backoff: h.backoff,
		Clock:   func() time.Time { return h.now },
	})

	result, err := h.Sweep(ctx)

	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	if result.Stop != DeadlineSweepStopCanceled || result.Failed != 0 {
		t.Fatalf("Sweep() = %+v, want a canceled stop with no failure", result)
	}
	if got := h.backoff.Failures(DeadlineSweepKey(DeadlineExpiredExecution{ExecutionID: ids[0]})); got != 0 {
		t.Fatalf("Failures = %d, want shutdown not to back the execution off", got)
	}
}

type expirerFunc func(context.Context, types.ExecutionID) (ExpireOutcome, error)

func (f expirerFunc) ExpireExecution(ctx context.Context, id types.ExecutionID) (ExpireOutcome, error) {
	return f(ctx, id)
}

func TestDeadlineSweepIsSafeForConcurrentUse(t *testing.T) {
	h := newSweepHarness(8, nil)
	base := sweepEpoch.Add(-time.Hour)
	for _, id := range h.index.add("broken", 20, base) {
		h.expirer.setErr(id, errSweepStoreDown)
	}
	for _, id := range h.index.add("canceling", 20, base.Add(time.Minute)) {
		h.expirer.outcomes[id] = ExpireOutcomeCanceling
	}
	h.index.add("ok", 20, base.Add(2*time.Minute))
	var clockMu sync.Mutex
	tick := 0
	h.DeadlineSweep = NewDeadlineSweep(DeadlineSweepConfig{
		Lister:  h.index,
		Expirer: h.expirer,
		Backoff: h.backoff,
		Batch:   8,
		Logger:  h.log,
		Clock: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			tick++
			return sweepEpoch.Add(time.Duration(tick) * 50 * time.Millisecond)
		},
	})

	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if _, err := h.Sweep(context.Background()); err != nil {
					t.Errorf("Sweep() error = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for i := 0; i < 20; i++ {
		id := types.ExecutionID(fmt.Sprintf("ok-%d", i))
		if h.expirer.callsFor(id) == 0 {
			t.Fatalf("%s was never attempted: concurrent sweeps starved it", id)
		}
	}
}

// TestDeadlineExpiredSourceEmbeddedIsPinned pins the label value itself. Every
// other assertion compares against the constant, which would follow the
// constant wherever it drifted; the value is part of the observable surface,
// because dashboards and alerts select on source="embedded".
func TestDeadlineExpiredSourceEmbeddedIsPinned(t *testing.T) {
	if DeadlineExpiredSourceEmbedded != "embedded" {
		t.Fatalf("DeadlineExpiredSourceEmbedded = %q, want %q: the label value is a published contract",
			DeadlineExpiredSourceEmbedded, "embedded")
	}
}
