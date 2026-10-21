package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// fakeDriftScanner is an in-memory store.ExecutionDriftScanner. It records the
// arguments of every scan so a test can assert the staleness window, the batch
// bound and the owned scope without a SQL backend.
type fakeDriftScanner struct {
	mu         sync.Mutex
	candidates []store.ExecutionDriftCandidate
	err        error
	calls      int
	owned      []namespace.Namespace
	before     time.Time
	limit      int
}

var _ store.ExecutionDriftScanner = (*fakeDriftScanner)(nil)

func (f *fakeDriftScanner) ListStaleRunningExecutions(_ context.Context, owned []namespace.Namespace, before time.Time, limit int) ([]store.ExecutionDriftCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.owned = append([]namespace.Namespace(nil), owned...)
	f.before = before
	f.limit = limit
	if f.err != nil {
		return nil, f.err
	}
	return append([]store.ExecutionDriftCandidate(nil), f.candidates...), nil
}

func (f *fakeDriftScanner) scanCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
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

// fakeDriftRecorder implements store.Executions by embedding the interface (the
// unused methods are never reached) and records UpdateExecutionStatus calls.
type fakeDriftRecorder struct {
	store.Executions
	mu      sync.Mutex
	updates []driftUpdate
	errs    map[types.ExecutionID]error
}

type driftUpdate struct {
	id     types.ExecutionID
	status types.ExecutionStatus
	errMsg string
	ns     namespace.Namespace
}

func (f *fakeDriftRecorder) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[id]; err != nil {
		return err
	}
	f.updates = append(f.updates, driftUpdate{id: id, status: status, errMsg: errMsg, ns: namespace.FromContext(ctx)})
	return nil
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
	if updates[0].errMsg != "" {
		t.Fatalf("errMsg = %q, want empty: the authority reader reports a status, not an error message, "+
			"and inventing one would write a reason the run never gave", updates[0].errMsg)
	}
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
	if w.owned != nil {
		t.Fatalf("owned = %v, want nil (legacy unscoped scan)", w.owned)
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
