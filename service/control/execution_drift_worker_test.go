package control

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// fakeDriftScanner is an in-memory store.ExecutionDriftScanner. It records the
// arguments of every scan so a test can assert the staleness window, the page
// bound, the owned scope and the cursor without a SQL backend, and it filters
// its fixture the way the SQL query does (strictly before the staleness bound,
// strictly after the cursor in (updated_at, id) order).
type fakeDriftScanner struct {
	mu         sync.Mutex
	candidates []store.ExecutionDriftCandidate
	err        error
	calls      int
	owned      []namespace.Namespace
	before     time.Time
	limit      int
	cursors    []store.ExecutionDriftCursor
}

var _ store.ExecutionDriftScanner = (*fakeDriftScanner)(nil)

func (f *fakeDriftScanner) ListStaleRunningExecutions(_ context.Context, owned []namespace.Namespace, before time.Time, after store.ExecutionDriftCursor, limit int) ([]store.ExecutionDriftCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.owned = append([]namespace.Namespace(nil), owned...)
	f.before = before
	f.limit = limit
	f.cursors = append(f.cursors, after)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]store.ExecutionDriftCandidate, 0, limit)
	for _, c := range f.candidates {
		if !c.UpdatedAt.Before(before) {
			continue
		}
		if after.UpdatedAt.IsZero() && after.RowID == 0 {
			// Start of the scan.
		} else if !c.UpdatedAt.After(after.UpdatedAt) &&
			!(c.UpdatedAt.Equal(after.UpdatedAt) && c.RowID > after.RowID) {
			continue
		}
		out = append(out, c)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeDriftScanner) scanCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeDriftScanner) scanCursors() []store.ExecutionDriftCursor {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.ExecutionDriftCursor(nil), f.cursors...)
}

// fakeDriftStatuses is the authoritative status reader. An id absent from
// statuses reports found=false (the TTL-expired case); errs simulates an
// authority outage for one id.
type fakeDriftStatuses struct {
	mu       sync.Mutex
	statuses map[types.ExecutionID]types.ExecutionStatus
	errs     map[types.ExecutionID]error
	// namespaces records the namespace each read was issued under. Redis
	// status keys are namespaced, so a read under the wrong namespace is a
	// guaranteed miss.
	namespaces map[types.ExecutionID]namespace.Namespace
	calls      int
}

func (f *fakeDriftStatuses) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.namespaces == nil {
		f.namespaces = map[types.ExecutionID]namespace.Namespace{}
	}
	f.namespaces[id] = namespace.FromContext(ctx)
	if err := f.errs[id]; err != nil {
		return "", false, err
	}
	status, ok := f.statuses[id]
	if !ok {
		return "", false, nil
	}
	return status, true, nil
}

func (f *fakeDriftStatuses) readNamespace(id types.ExecutionID) namespace.Namespace {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.namespaces[id]
}

// fakeDriftRecorder implements the conditional repair write
// (store.ExecutionDriftRepairer) and records what was written. It deliberately
// does NOT implement the unconditional Executions.UpdateExecutionStatus: the
// embedded store.Executions is nil, so a worker that fell back to the
// unconditional write would panic rather than silently pass.
type fakeDriftRecorder struct {
	store.Executions
	mu       sync.Mutex
	updates  []driftUpdate
	errs     map[types.ExecutionID]error
	advanced map[types.ExecutionID]bool
}

type driftUpdate struct {
	id     types.ExecutionID
	status types.ExecutionStatus
	ns     namespace.Namespace
}

var _ store.ExecutionDriftRepairer = (*fakeDriftRecorder)(nil)

func (f *fakeDriftRecorder) RepairRunningExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[id]; err != nil {
		return false, err
	}
	if f.advanced[id] {
		// The ordinary projection path advanced the row between the scan and
		// this write, so the conditional update matches nothing.
		return false, nil
	}
	f.updates = append(f.updates, driftUpdate{id: id, status: status, ns: namespace.FromContext(ctx)})
	return true, nil
}

func (f *fakeDriftRecorder) recorded() []driftUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]driftUpdate(nil), f.updates...)
}

// recordingDriftObserver captures every observation for assertions.
type recordingDriftObserver struct {
	mu       sync.Mutex
	scans    int
	scanned  int
	scanErr  error
	repaired []driftRepairEvent
	skipped  []string
	skipNs   []namespace.Namespace
	errors   []types.ExecutionID
}

type driftRepairEvent struct {
	id      types.ExecutionID
	status  types.ExecutionStatus
	age     time.Duration
	applied bool
	ns      namespace.Namespace
}

func (o *recordingDriftObserver) OnExecutionDriftScan(_ context.Context, candidates int, _ time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.scans++
	o.scanned += candidates
	o.scanErr = err
}

func (o *recordingDriftObserver) OnExecutionDriftRepaired(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, age time.Duration, applied bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.repaired = append(o.repaired, driftRepairEvent{id: id, status: status, age: age, applied: applied, ns: namespace.FromContext(ctx)})
}

func (o *recordingDriftObserver) OnExecutionDriftSkipped(ctx context.Context, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.skipped = append(o.skipped, reason)
	o.skipNs = append(o.skipNs, namespace.FromContext(ctx))
}

func (o *recordingDriftObserver) OnExecutionDriftError(_ context.Context, id types.ExecutionID, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errors = append(o.errors, id)
}

func (o *recordingDriftObserver) skipReasons() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.skipped...)
}

func (o *recordingDriftObserver) repairEvents() []driftRepairEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]driftRepairEvent(nil), o.repaired...)
}

func (o *recordingDriftObserver) errorIDs() []types.ExecutionID {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]types.ExecutionID(nil), o.errors...)
}

func driftCandidate(id types.ExecutionID, ns namespace.Namespace, updatedAt time.Time) store.ExecutionDriftCandidate {
	return store.ExecutionDriftCandidate{ID: id, Namespace: ns, UpdatedAt: updatedAt}
}

// newDriftWorkerForTest builds a worker with test-friendly timing and a fixed
// clock so age assertions are deterministic.
func newDriftWorkerForTest(scanner store.ExecutionDriftScanner, statuses *fakeDriftStatuses, recorder *fakeDriftRecorder, cfg ExecutionDriftConfig) *ExecutionDriftWorker {
	cfg.Period = time.Hour // Run is never used in these tests
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = 5 * time.Minute
	}
	if cfg.Batch == 0 {
		cfg.Batch = 64
	}
	w := NewExecutionDriftWorker(scanner, statuses, recorder, cfg)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	w.clock = func() time.Time { return now }
	return w
}

// A terminal authoritative status is the one case the repair exists for: the
// durable projection lost the transition, so it is rewritten from Redis.
func TestExecutionDriftRepairWritesTerminalStatus(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-1", "tenant-a", now.Add(-10*time.Minute)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-1": types.ExecutionStatusSuccess,
	}}
	recorder := &fakeDriftRecorder{}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 1 {
		t.Fatalf("ReconcileOnce repaired %d rows, want 1", repaired)
	}
	updates := recorder.recorded()
	if len(updates) != 1 {
		t.Fatalf("recorded %d updates, want 1", len(updates))
	}
	if updates[0].id != "exec-1" || updates[0].status != types.ExecutionStatusSuccess {
		t.Fatalf("update = %+v, want exec-1 -> success", updates[0])
	}
	// The repair copies a status, never a message: there is no errMsg argument
	// on the conditional write, and the SQL only clears error_msg on a row that
	// still reads running (pinned in sqlstore's query-shape test).
	// The authoritative read must address the candidate's own namespace: Redis
	// keys are namespaced, and namespace.Default would miss every tenant row.
	if got := statuses.readNamespace("exec-1"); got != "tenant-a" {
		t.Fatalf("authoritative read namespace = %q, want tenant-a", got)
	}
	if got := updates[0].ns; got != "tenant-a" {
		t.Fatalf("write ctx namespace = %q, want tenant-a", got)
	}
	events := obs.repairEvents()
	if len(events) != 1 || !events[0].applied || events[0].status != types.ExecutionStatusSuccess {
		t.Fatalf("repaired observations = %+v, want one applied success", events)
	}
	if events[0].age != 10*time.Minute {
		t.Fatalf("reported age = %v, want 10m", events[0].age)
	}
}

// Every terminal status is copied verbatim. A worker that only recognized
// success would leave failed/canceled/timeout runs reading "running" forever.
func TestExecutionDriftRepairCopiesEveryTerminalStatus(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	terminal := []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout,
	}
	for _, want := range terminal {
		t.Run(string(want), func(t *testing.T) {
			scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
				driftCandidate("exec-1", "default", now.Add(-time.Hour)),
			}}
			statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{"exec-1": want}}
			recorder := &fakeDriftRecorder{}
			w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{})

			if repaired := w.ReconcileOnce(context.Background()); repaired != 1 {
				t.Fatalf("ReconcileOnce repaired %d rows, want 1", repaired)
			}
			updates := recorder.recorded()
			if len(updates) != 1 || updates[0].status != want {
				t.Fatalf("updates = %+v, want exactly one %q", updates, want)
			}
		})
	}
}

// A non-terminal authoritative status means the execution may still be alive:
// the durable row is behind, not wrong, and must not be touched.
func TestExecutionDriftRepairLeavesNonTerminalStatusAlone(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		status     types.ExecutionStatus
		skipReason string
	}{
		{types.ExecutionStatusRunning, ExecutionDriftSkipRunning},
		{types.ExecutionStatusQueued, ExecutionDriftSkipNonTerminal},
		{types.ExecutionStatusPending, ExecutionDriftSkipNonTerminal},
		{types.ExecutionStatusPaused, ExecutionDriftSkipNonTerminal},
		{types.ExecutionStatusCanceling, ExecutionDriftSkipNonTerminal},
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
				driftCandidate("exec-1", "default", now.Add(-time.Hour)),
			}}
			statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{"exec-1": tc.status}}
			recorder := &fakeDriftRecorder{}
			obs := &recordingDriftObserver{}
			w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

			if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
				t.Fatalf("ReconcileOnce repaired %d rows for a %q authority status, want 0", repaired, tc.status)
			}
			if updates := recorder.recorded(); len(updates) != 0 {
				t.Fatalf("wrote %+v for a %q authority status; a live execution is not drift", updates, tc.status)
			}
			if got := obs.skipReasons(); len(got) != 1 || got[0] != tc.skipReason {
				t.Fatalf("skip reasons = %v, want [%s]", got, tc.skipReason)
			}
		})
	}
}

// A vanished status key (retention elapsed) carries no outcome at all. Writing
// anything here would permanently fabricate a terminal result.
func TestExecutionDriftRepairLeavesExpiredAuthorityAlone(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-gone", "default", now.Add(-time.Hour)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{}}
	recorder := &fakeDriftRecorder{}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows with no authoritative status, want 0", repaired)
	}
	if updates := recorder.recorded(); len(updates) != 0 {
		t.Fatalf("wrote %+v with no authoritative status", updates)
	}
	if got := obs.skipReasons(); len(got) != 1 || got[0] != ExecutionDriftSkipNotFound {
		t.Fatalf("skip reasons = %v, want [%s]", got, ExecutionDriftSkipNotFound)
	}
}

// The scan and the conditional write are not atomic: the ordinary projection
// path may advance the row between them, with the very terminal status (and
// error_msg) the repair would have copied. The conditional write must match no
// row, so that real transition and its message survive.
func TestExecutionDriftRepairLeavesAdvancedProjectionAlone(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-advanced", "default", now.Add(-time.Minute*10)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-advanced": types.ExecutionStatusFailed,
	}}
	// The simulated race: between the scan and the repair the row was
	// projected as failed WITH an error_msg, so the conditional update matches
	// nothing.
	recorder := &fakeDriftRecorder{advanced: map[types.ExecutionID]bool{"exec-advanced": true}}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows whose row had already advanced, want 0", repaired)
	}
	if updates := recorder.recorded(); len(updates) != 0 {
		t.Fatalf("wrote %+v over a row the ordinary path already advanced; its error_msg belongs to the real transition", updates)
	}
	if got := obs.skipReasons(); len(got) != 1 || got[0] != ExecutionDriftSkipAlreadyAdvanced {
		t.Fatalf("skip reasons = %v, want [%s]", got, ExecutionDriftSkipAlreadyAdvanced)
	}
	if got := obs.errorIDs(); len(got) != 0 {
		t.Fatalf("a benign lost race was reported as an error: %v", got)
	}
}

// An unrepairable row (its Redis status has expired) sits at the head of the
// scan order forever. A fixed LIMIT would re-read the same batch every sweep
// and never reach the repairable row behind it; the cursor pages past the
// expired rows within one sweep, and reaching the end resets the cursor so
// they are retried next time.
func TestExecutionDriftRepairPagesPastUnrepairableCandidates(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-3 * time.Hour)
	secondAt := now.Add(-2 * time.Hour)
	repairableAt := now.Add(-time.Hour)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		{ID: "expired-1", Namespace: "default", UpdatedAt: expiredAt, RowID: 11},
		{ID: "expired-2", Namespace: "default", UpdatedAt: secondAt, RowID: 12},
		{ID: "repairable", Namespace: "default", UpdatedAt: repairableAt, RowID: 13},
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"repairable": types.ExecutionStatusSuccess,
	}}
	recorder := &fakeDriftRecorder{}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Batch: 2, Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 1 {
		t.Fatalf("ReconcileOnce repaired %d rows, want 1: the page after the expired head holds the repairable row", repaired)
	}
	updates := recorder.recorded()
	if len(updates) != 1 || updates[0].id != "repairable" {
		t.Fatalf("updates = %+v, want [repairable]", updates)
	}
	if got := obs.skipReasons(); len(got) != 2 ||
		got[0] != ExecutionDriftSkipNotFound || got[1] != ExecutionDriftSkipNotFound {
		t.Fatalf("skip reasons = %v, want two not_found: the expired rows still count as examined", got)
	}
	if calls := scanner.scanCalls(); calls != 2 {
		t.Fatalf("scanner calls = %d, want 2 (a full page, then a short one)", calls)
	}
	cursors := scanner.scanCursors()
	if len(cursors) != 2 {
		t.Fatalf("recorded %d cursors, want 2", len(cursors))
	}
	if !cursors[0].UpdatedAt.IsZero() || cursors[0].RowID != 0 {
		t.Fatalf("first page cursor = %+v, want the zero start", cursors[0])
	}
	if !cursors[1].UpdatedAt.Equal(secondAt) || cursors[1].RowID != 12 {
		t.Fatalf("second page cursor = %+v, want (updated_at=%v, id=12)", cursors[1], secondAt)
	}
	if !w.cursor.UpdatedAt.IsZero() || w.cursor.RowID != 0 {
		t.Fatalf("cursor after reaching the end = %+v, want the zero start", w.cursor)
	}

	// The next sweep starts from the head again, so the rows skipped this time
	// are re-examined rather than abandoned.
	_ = w.ReconcileOnce(context.Background())
	cursors = scanner.scanCursors()
	if len(cursors) != 4 {
		t.Fatalf("recorded %d cursors after two sweeps, want 4", len(cursors))
	}
	if !cursors[2].UpdatedAt.IsZero() || cursors[2].RowID != 0 {
		t.Fatalf("second sweep's first cursor = %+v, want the zero start", cursors[2])
	}
}

// A sweep whose page budget runs out keeps its cursor, so the next sweep
// resumes where it stopped instead of rescanning the head it just examined.
func TestExecutionDriftRepairKeepsCursorWhenPageBudgetRunsOut(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	secondAt := now.Add(-2 * time.Hour)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		{ID: "expired-1", Namespace: "default", UpdatedAt: now.Add(-3 * time.Hour), RowID: 11},
		{ID: "expired-2", Namespace: "default", UpdatedAt: secondAt, RowID: 12},
		{ID: "repairable", Namespace: "default", UpdatedAt: now.Add(-time.Hour), RowID: 13},
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"repairable": types.ExecutionStatusSuccess,
	}}
	recorder := &fakeDriftRecorder{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Batch: 2})
	w.maxPages = 1

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("first sweep repaired %d rows, want 0: its one page is the expired head", repaired)
	}
	if !w.cursor.UpdatedAt.Equal(secondAt) || w.cursor.RowID != 12 {
		t.Fatalf("cursor after an exhausted page budget = %+v, want (updated_at=%v, id=12)", w.cursor, secondAt)
	}

	// The next sweep resumes after the cursor and reaches the repairable row.
	if repaired := w.ReconcileOnce(context.Background()); repaired != 1 {
		t.Fatalf("resumed sweep repaired %d rows, want 1", repaired)
	}
	cursors := scanner.scanCursors()
	if len(cursors) != 2 || !cursors[1].UpdatedAt.Equal(secondAt) || cursors[1].RowID != 12 {
		t.Fatalf("resumed sweep cursor = %+v, want the saved (updated_at=%v, id=12)", cursors, secondAt)
	}
	if !w.cursor.UpdatedAt.IsZero() || w.cursor.RowID != 0 {
		t.Fatalf("cursor after the resumed sweep reached the end = %+v, want the zero start", w.cursor)
	}
}

// A store can advertise the scan without the conditional write. The sweep
// still classifies candidates — that is observable and honest — but it must
// write nothing, and it must not fall back to the unconditional update path
// that would erase an error_msg.
func TestExecutionDriftRepairWithoutConditionalWriteSkips(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-1", "default", now.Add(-time.Hour)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-1": types.ExecutionStatusSuccess,
	}}
	obs := &recordingDriftObserver{}
	// The embedded interface is nil, so any call to UpdateExecutionStatus
	// panics: the worker's only allowed write path here is the conditional one,
	// which this type does not implement.
	w := NewExecutionDriftWorker(scanner, statuses, struct{ store.Executions }{}, ExecutionDriftConfig{Observer: obs})
	w.clock = func() time.Time { return now }

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows without the conditional write, want 0", repaired)
	}
	if got := statuses.calls; got != 1 {
		t.Fatalf("authority reads = %d, want 1: classification still happens", got)
	}
	if got := obs.skipReasons(); len(got) != 1 || got[0] != ExecutionDriftSkipRepairUnsupported {
		t.Fatalf("skip reasons = %v, want [%s]", got, ExecutionDriftSkipRepairUnsupported)
	}
}

// Only the leader may read authority and write projections; followers must not
// even scan (that is the cost the gate exists to remove).
func TestExecutionDriftRepairRequiresLeadership(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-1", "default", now.Add(-time.Hour)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-1": types.ExecutionStatusSuccess,
	}}
	recorder := &fakeDriftRecorder{}
	gate := &toggleLeaderGate{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Elector: gate})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("a follower repaired %d rows, want 0", repaired)
	}
	if calls := scanner.scanCalls(); calls != 0 {
		t.Fatalf("a follower scanned %d times, want 0", calls)
	}
	if updates := recorder.recorded(); len(updates) != 0 {
		t.Fatalf("a follower wrote %+v", updates)
	}

	gate.leader.Store(true)
	if repaired := w.ReconcileOnce(context.Background()); repaired != 1 {
		t.Fatalf("the leader repaired %d rows, want 1", repaired)
	}
	if calls := scanner.scanCalls(); calls != 1 {
		t.Fatalf("the leader scanned %d times, want 1", calls)
	}
}

// Leadership is re-checked every leaderRecheckInterval candidates, not just at
// entry: a replica demoted mid-sweep stops writing. The cursor it leaves behind
// points at the last row it classified, so a later sweep resumes instead of
// redoing that page.
func TestExecutionDriftRepairStopsWhenLeadershipIsLostMidPass(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	const total = 40
	candidates := make([]store.ExecutionDriftCandidate, total)
	statuses := &fakeDriftStatuses{statuses: make(map[types.ExecutionID]types.ExecutionStatus, total)}
	for i := range candidates {
		id := types.ExecutionID("exec-" + strconv.Itoa(i))
		candidates[i] = store.ExecutionDriftCandidate{
			ID:        id,
			Namespace: "default",
			UpdatedAt: now.Add(-time.Hour),
			RowID:     uint64(i + 1),
		}
		statuses.statuses[id] = types.ExecutionStatusSuccess
	}
	scanner := &fakeDriftScanner{candidates: candidates}
	recorder := &fakeDriftRecorder{}
	// allow=1 passes the entry gate and fails the first mid-pass re-check.
	gate := &flipAfterGate{allow: 1}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Elector: gate})

	if repaired := w.ReconcileOnce(context.Background()); repaired != leaderRecheckInterval {
		t.Fatalf("ReconcileOnce repaired %d rows, want %d before the mid-pass leadership re-check",
			repaired, leaderRecheckInterval)
	}
	if got := len(recorder.recorded()); got != leaderRecheckInterval {
		t.Fatalf("writes = %d, want %d: the remaining candidates must be left for the new leader",
			got, leaderRecheckInterval)
	}
	if calls := scanner.scanCalls(); calls != 1 {
		t.Fatalf("scanner calls = %d, want 1: a lost leader must not start another page", calls)
	}
	last := candidates[leaderRecheckInterval-1]
	if !w.cursor.UpdatedAt.Equal(last.UpdatedAt) || w.cursor.RowID != last.RowID {
		t.Fatalf("cursor after the mid-pass stop = %+v, want the last classified row (id=%d)", w.cursor, last.RowID)
	}
}

// A page bound smaller than leaderRecheckInterval must not disable the
// mid-sweep re-check: the count has to carry across pages. A page-local index
// never reaches the interval when Batch < leaderRecheckInterval, so a demoted
// replica would keep repairing to the end of the sweep.
func TestExecutionDriftRepairRechecksLeadershipAcrossPages(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	const total = 80
	const batch = 20 // below leaderRecheckInterval: no page-local index reaches it
	candidates := make([]store.ExecutionDriftCandidate, total)
	statuses := &fakeDriftStatuses{statuses: make(map[types.ExecutionID]types.ExecutionStatus, total)}
	for i := range candidates {
		id := types.ExecutionID("exec-" + strconv.Itoa(i))
		candidates[i] = store.ExecutionDriftCandidate{
			ID:        id,
			Namespace: "default",
			UpdatedAt: now.Add(-time.Hour),
			RowID:     uint64(i + 1),
		}
		statuses.statuses[id] = types.ExecutionStatusSuccess
	}
	scanner := &fakeDriftScanner{candidates: candidates}
	recorder := &fakeDriftRecorder{}
	// allow=1 passes the entry gate and fails the first mid-sweep re-check,
	// which falls in the second page because the examined count is cumulative.
	gate := &flipAfterGate{allow: 1}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Elector: gate, Batch: batch})

	if repaired := w.ReconcileOnce(context.Background()); repaired != leaderRecheckInterval {
		t.Fatalf("ReconcileOnce repaired %d rows, want %d before the cross-page leadership re-check",
			repaired, leaderRecheckInterval)
	}
	if calls := scanner.scanCalls(); calls != 2 {
		t.Fatalf("scanner calls = %d, want 2: the re-check must fire while reading the second page", calls)
	}
	if got := len(recorder.recorded()); got != leaderRecheckInterval {
		t.Fatalf("writes = %d, want %d: the remaining candidates must be left for the new leader",
			got, leaderRecheckInterval)
	}
	if got := gate.callCount(); got != 2 {
		t.Fatalf("leadership checks = %d, want 2 (entry + one cumulative re-check)", got)
	}
	last := candidates[leaderRecheckInterval-1]
	if !w.cursor.UpdatedAt.Equal(last.UpdatedAt) || w.cursor.RowID != last.RowID {
		t.Fatalf("cursor after the mid-page stop = %+v, want the last classified row (id=%d)", w.cursor, last.RowID)
	}
}

// DryRun classifies exactly like a real sweep and writes nothing.
func TestExecutionDriftRepairDryRunWritesNothing(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-1", "default", now.Add(-time.Hour)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-1": types.ExecutionStatusFailed,
	}}
	recorder := &fakeDriftRecorder{}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{DryRun: true, Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("dry run repaired %d rows, want 0", repaired)
	}
	if updates := recorder.recorded(); len(updates) != 0 {
		t.Fatalf("dry run wrote %+v", updates)
	}
	events := obs.repairEvents()
	if len(events) != 1 || events[0].applied || events[0].status != types.ExecutionStatusFailed {
		t.Fatalf("dry-run observations = %+v, want one unapplied failed classification", events)
	}
}

// One bad row must not abort the sweep: the rows behind it are older and
// would never be repaired if the first transient error stopped the pass.
func TestExecutionDriftRepairContinuesAfterOneError(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-read-error", "default", now.Add(-5*time.Hour)),
		driftCandidate("exec-ok-1", "default", now.Add(-4*time.Hour)),
		driftCandidate("exec-write-error", "default", now.Add(-3*time.Hour)),
		driftCandidate("exec-ok-2", "default", now.Add(-2*time.Hour)),
	}}
	statuses := &fakeDriftStatuses{
		statuses: map[types.ExecutionID]types.ExecutionStatus{
			"exec-read-error":  types.ExecutionStatusSuccess,
			"exec-ok-1":        types.ExecutionStatusSuccess,
			"exec-write-error": types.ExecutionStatusCanceled,
			"exec-ok-2":        types.ExecutionStatusTimeout,
		},
		errs: map[types.ExecutionID]error{"exec-read-error": errors.New("redis unavailable")},
	}
	recorder := &fakeDriftRecorder{errs: map[types.ExecutionID]error{"exec-write-error": errors.New("mysql unavailable")}}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 2 {
		t.Fatalf("ReconcileOnce repaired %d rows, want 2", repaired)
	}
	if calls := statuses.calls; calls != 4 {
		t.Fatalf("authority reads = %d, want 4: the sweep stopped early", calls)
	}
	updates := recorder.recorded()
	if len(updates) != 2 || updates[0].id != "exec-ok-1" || updates[1].id != "exec-ok-2" {
		t.Fatalf("updates = %+v, want exec-ok-1 and exec-ok-2", updates)
	}
	gotErrs := obs.errorIDs()
	if len(gotErrs) != 2 || gotErrs[0] != "exec-read-error" || gotErrs[1] != "exec-write-error" {
		t.Fatalf("error observations = %v, want [exec-read-error exec-write-error]", gotErrs)
	}
}

// The durable table is shared like the rest of the infrastructure: a scoped
// worker must hand the scanner its owned set and address every authoritative
// read under the candidate's own namespace.
func TestExecutionDriftRepairScopesNamespaces(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	scanner := &fakeDriftScanner{candidates: []store.ExecutionDriftCandidate{
		driftCandidate("exec-a", "tenant-a", now.Add(-time.Hour)),
		driftCandidate("exec-b", "tenant-b", now.Add(-time.Hour)),
	}}
	statuses := &fakeDriftStatuses{statuses: map[types.ExecutionID]types.ExecutionStatus{
		"exec-a": types.ExecutionStatusSuccess,
		"exec-b": types.ExecutionStatusFailed,
	}}
	recorder := &fakeDriftRecorder{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{
		Namespaces: []namespace.Namespace{"tenant-a", "", "tenant-b"},
	})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 2 {
		t.Fatalf("ReconcileOnce repaired %d rows, want 2", repaired)
	}
	gotOwned := scanner.owned
	if len(gotOwned) != 2 || gotOwned[0] != "tenant-a" || gotOwned[1] != "tenant-b" {
		t.Fatalf("scanner owned = %v, want [tenant-a tenant-b] (empty entries dropped)", gotOwned)
	}
	for _, id := range []types.ExecutionID{"exec-a", "exec-b"} {
		want := namespace.Namespace("tenant-" + string(id[len(id)-1:]))
		if got := statuses.readNamespace(id); got != want {
			t.Fatalf("authority read for %s used namespace %q, want %q", id, got, want)
		}
	}
}

// A failed scan is reported, costs one cycle, and writes nothing.
func TestExecutionDriftRepairScanErrorWritesNothing(t *testing.T) {
	scanner := &fakeDriftScanner{err: errors.New("mysql unavailable")}
	statuses := &fakeDriftStatuses{}
	recorder := &fakeDriftRecorder{}
	obs := &recordingDriftObserver{}
	w := newDriftWorkerForTest(scanner, statuses, recorder, ExecutionDriftConfig{Observer: obs})

	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows after a scan error, want 0", repaired)
	}
	if updates := recorder.recorded(); len(updates) != 0 {
		t.Fatalf("wrote %+v after a scan error", updates)
	}
	if obs.scanErr == nil {
		t.Fatal("scan error was not reported to the observer")
	}
}

// The constructor's defaults must be the worker's actual behavior, and a
// non-positive batch must not reach the scanner as "unbounded".
func TestNewExecutionDriftWorkerDefaults(t *testing.T) {
	w := NewExecutionDriftWorker(&fakeDriftScanner{}, &fakeDriftStatuses{}, &fakeDriftRecorder{}, ExecutionDriftConfig{})
	if w.period != DefaultExecutionDriftPeriod {
		t.Fatalf("period = %v, want %v", w.period, DefaultExecutionDriftPeriod)
	}
	if w.staleAfter != DefaultExecutionDriftStaleAfter {
		t.Fatalf("staleAfter = %v, want %v", w.staleAfter, DefaultExecutionDriftStaleAfter)
	}
	if w.batch != DefaultExecutionDriftBatch {
		t.Fatalf("batch = %d, want %d", w.batch, DefaultExecutionDriftBatch)
	}
	if w.maxPages != DefaultExecutionDriftMaxPages {
		t.Fatalf("maxPages = %d, want %d", w.maxPages, DefaultExecutionDriftMaxPages)
	}
	if w.owned != nil {
		t.Fatalf("owned = %v, want nil (legacy unscoped scan)", w.owned)
	}
}

// A mistyped batch is clamped rather than sent to the scanner: one page is one
// SQL read plus one authoritative read per row, and a page budget that is not
// bounded is the long-sweep problem the page cap exists to prevent.
func TestNewExecutionDriftWorkerClampsBatch(t *testing.T) {
	w := NewExecutionDriftWorker(&fakeDriftScanner{}, &fakeDriftStatuses{}, &fakeDriftRecorder{}, ExecutionDriftConfig{
		Batch: MaxExecutionDriftBatch + 1,
	})
	if w.batch != MaxExecutionDriftBatch {
		t.Fatalf("batch = %d, want the clamp to %d", w.batch, MaxExecutionDriftBatch)
	}
}

// The staleness window and batch bound reach the scanner exactly; a worker
// that scanned everything would repair rows whose projection write is still
// in flight.
func TestExecutionDriftRepairPassesStalenessWindowAndBatch(t *testing.T) {
	scanner := &fakeDriftScanner{}
	w := newDriftWorkerForTest(scanner, &fakeDriftStatuses{}, &fakeDriftRecorder{}, ExecutionDriftConfig{
		StaleAfter: 7 * time.Minute,
		Batch:      9,
	})
	w.clock = func() time.Time { return time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC) }

	w.ReconcileOnce(context.Background())
	if scanner.limit != 9 {
		t.Fatalf("scanner limit = %d, want 9", scanner.limit)
	}
	want := time.Date(2026, 10, 21, 11, 53, 0, 0, time.UTC)
	if !scanner.before.Equal(want) {
		t.Fatalf("scanner updatedBefore = %v, want %v", scanner.before, want)
	}
}

// A worker without its dependencies is inert: it must not panic on the nil
// path (the dev/in-memory case NewServer leaves it unconstructed anyway).
func TestExecutionDriftWorkerNilDependenciesAreInert(t *testing.T) {
	w := NewExecutionDriftWorker(nil, nil, nil, ExecutionDriftConfig{})
	if repaired := w.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows without dependencies, want 0", repaired)
	}
}
