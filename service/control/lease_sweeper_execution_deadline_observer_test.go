package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// SweepMetrics is what controlplane.go wires as the sweeper's Observer. Asserting
// the mirror here makes a drift between ExecutionDeadlineObserver and the
// metrics package's local copy a compile failure, instead of a type assertion
// that quietly stops matching and a counter that quietly stops being emitted.
var (
	_ ExecutionDeadlineObserver = metrics.SweepMetrics{}
	_ ExecutionDeadlineObserver = (*recordingDeadlineObserver)(nil)
)

type deadlineExpiredEvent struct {
	source string
	ns     namespace.Namespace
}

// recordingDeadlineObserver adds the deadline hook to the candidate recorder, so
// one value captures the pass events and the expiry events of a pass.
type recordingDeadlineObserver struct {
	*recordingCandidateObserver
	mu       sync.Mutex
	expired  []deadlineExpiredEvent
	panicOn  bool
	panicked int
}

func (o *recordingDeadlineObserver) OnExecutionDeadlineExpired(ctx context.Context, source string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.panicOn {
		o.panicked++
		panic("observer failure")
	}
	o.expired = append(o.expired, deadlineExpiredEvent{source: source, ns: namespace.FromContext(ctx)})
}

func (o *recordingDeadlineObserver) snapshotExpired() []deadlineExpiredEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]deadlineExpiredEvent(nil), o.expired...)
}

func newDeadlineObserverHarness(observer *recordingDeadlineObserver) *deadlineHarness {
	state := &fakeDeadlineLister{fakeLeaseLister: &fakeLeaseLister{}}
	expirer := &fakeExpirer{
		fakeReclaimer: &fakeReclaimer{},
		outcomes:      map[types.ExecutionID]engine.ExpireOutcome{},
		errs:          map[types.ExecutionID]error{},
		ack:           func(id types.ExecutionID) { _ = state.AckDeadlineExpired(context.Background(), id) },
	}
	elector := &fakeElector{}
	elector.leader.Store(true)
	sw := NewLeaseSweeper(state, expirer, LeaseSweeperConfig{Elector: elector, Observer: observer})
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	sw.clock = func() time.Time { return now }
	return &deadlineHarness{
		LeaseSweeper: sw,
		state:        state,
		expirer:      expirer,
		elector:      elector,
		observer:     observer.recordingCandidateObserver,
		logger:       &recordingLogger{},
		now:          &now,
	}
}

func newRecordingDeadlineObserver() *recordingDeadlineObserver {
	return &recordingDeadlineObserver{recordingCandidateObserver: newRecordingCandidateObserver()}
}

// TestLeaseSweeperCountsOnlyTheTransitionItMadeAsExpired pins what the counter
// means: the call that wrote the timeout counts, with the execution's own
// namespace and the literal source; a resumed cascade, a not-due execution and
// the other benign outcomes do not.
func TestLeaseSweeperCountsOnlyTheTransitionItMadeAsExpired(t *testing.T) {
	observer := newRecordingDeadlineObserver()
	h := newDeadlineObserverHarness(observer)
	base := h.now.Add(-time.Hour)
	h.state.entries = []engine.DeadlineExpiredExecution{
		{ExecutionID: "exec-expired", Namespace: "tenant-a", Deadline: base},
		{ExecutionID: "exec-resumed", Namespace: "tenant-b", Deadline: base.Add(time.Second)},
		{ExecutionID: "exec-not-due", Deadline: base.Add(2 * time.Second)},
		{ExecutionID: "exec-terminal", Deadline: base.Add(3 * time.Second)},
		{ExecutionID: "exec-expired-2", Deadline: base.Add(4 * time.Second)},
	}
	h.expirer.outcomes["exec-resumed"] = engine.ExpireOutcomeResumed
	h.expirer.outcomes["exec-not-due"] = engine.ExpireOutcomeNotDue
	h.expirer.outcomes["exec-terminal"] = engine.ExpireOutcomeAlreadyTerminal

	if got := h.pass(); got != 3 {
		t.Fatalf("pass released %d, want 3 (expired x2 and resumed)", got)
	}

	want := []deadlineExpiredEvent{
		{source: "sweeper", ns: "tenant-a"},
		{source: "sweeper", ns: namespace.FromContext(context.Background())},
	}
	got := observer.snapshotExpired()
	if len(got) != len(want) {
		t.Fatalf("expired events = %+v, want %+v (the resumed call is not a second expiry)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expired event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestLeaseSweeperExpiryObserverPanicDoesNotStopThePass(t *testing.T) {
	observer := newRecordingDeadlineObserver()
	observer.panicOn = true
	h := newDeadlineObserverHarness(observer)
	h.list("exec", 3, h.now.Add(-time.Hour))

	if got := h.pass(); got != 3 {
		t.Fatalf("pass released %d, want all 3 despite the panicking observer", got)
	}
	if observer.panicked != 3 {
		t.Fatalf("observer called %d times, want 3", observer.panicked)
	}
}
