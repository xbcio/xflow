package control

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// unreleasedOutcomes are the outcomes ExpireExecution reports without an error
// that the pass must not count as released. Gone, AlreadyTerminal and NotDue are
// benign: someone else owns or finished the execution, so the pass retries it at
// its next opportunity. Canceling is also not a failure, but its entry stays
// listed until the cancel concludes, so the pass sets it aside for a window.
var unreleasedOutcomes = []struct {
	outcome  engine.ExpireOutcome
	setAside bool
}{
	{engine.ExpireOutcomeCanceling, true},
	{engine.ExpireOutcomeGone, false},
	{engine.ExpireOutcomeAlreadyTerminal, false},
	{engine.ExpireOutcomeNotDue, false},
}

// TestLeaseSweeperExecutionDeadlineOutcomesThatAreNotReleases pins, per outcome,
// what the pass records: nothing released, nothing expired, no failure and no
// failure alert; the benign ones are not backed off at all, and Canceling is set
// aside for a window without counting as a failure.
func TestLeaseSweeperExecutionDeadlineOutcomesThatAreNotReleases(t *testing.T) {
	for _, tc := range unreleasedOutcomes {
		t.Run(string(tc.outcome), func(t *testing.T) {
			observer := newRecordingDeadlineObserver()
			h := newDeadlineObserverHarness(observer)
			h.deadlinePeriod = time.Nanosecond
			id := h.list("exec", 1, h.now.Add(-time.Hour))[0]
			h.expirer.outcomes[id] = tc.outcome
			// The entry stays listed whatever the outcome, so a second pass
			// sees it again and shows whether it was set aside.
			h.expirer.ack = func(types.ExecutionID) {}
			key := executionDeadlineKey(h.state.entries[0])

			if got := h.pass(); got != 0 {
				t.Fatalf("first pass released %d, want 0 for %s", got, tc.outcome)
			}
			wantEvents := []passEvent{{pass: SweepPassExecutionDeadline, outcome: SweepPassOutcomeRan}}
			if got := h.observer.snapshot(); !equalPassEvents(got, wantEvents) {
				t.Fatalf("pass events = %+v, want %+v: nothing was released", got, wantEvents)
			}
			if got := observer.snapshotExpired(); len(got) != 0 {
				t.Fatalf("expired events = %+v, want none for %s", got, tc.outcome)
			}
			if got := h.deadlineBackoff.Failures(key); got != 0 {
				t.Fatalf("Failures = %d, want %s not to count as a failure", got, tc.outcome)
			}
			// Past the cadence gate, well inside the first deferral window (1s).
			*h.now = h.now.Add(time.Millisecond)
			h.pass()
			wantCalls := 2
			if tc.setAside {
				wantCalls = 1
			}
			if got := h.expirer.callsFor(id); got != wantCalls {
				t.Fatalf("calls after an immediate second pass = %d, want %d (set aside = %v)", got, wantCalls, tc.setAside)
			}
			if got := h.deadlineBackoff.Waiting(*h.now); (got == 1) != tc.setAside {
				t.Fatalf("executions waiting = %d, want set aside = %v", got, tc.setAside)
			}
		})
	}
}
