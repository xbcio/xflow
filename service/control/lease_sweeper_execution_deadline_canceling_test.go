package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// leaveCancelingListed makes the harness's expirer behave like the engine for an
// execution that is canceling: it reports ExpireOutcomeCanceling and leaves the
// deadline entry listed for the cancel to withdraw.
func (h *deadlineHarness) leaveCancelingListed(ids ...types.ExecutionID) {
	for _, id := range ids {
		h.expirer.outcomes[id] = engine.ExpireOutcomeCanceling
	}
	h.expirer.ack = func(id types.ExecutionID) {
		if h.expirer.outcomes[id] == engine.ExpireOutcomeCanceling {
			return
		}
		_ = h.state.AckDeadlineExpired(context.Background(), id)
	}
}

// TestLeaseSweeperExecutionDeadlineCancelingDoesNotStarveNewEntries is the
// starvation regression for executions stuck in canceling. ExpireExecution
// reports no error for them and leaves their entry listed until the canceling
// reaper concludes the cancel, so a pass that treated Canceling as plain success
// would hand the same oldest entries every slot of every page. Here 40 of them
// sit ahead of one fresh entry with a batch of 10.
func TestLeaseSweeperExecutionDeadlineCancelingDoesNotStarveNewEntries(t *testing.T) {
	const batch = 10
	h := newDeadlineHarness(everyPass(batch))
	base := h.now.Add(-time.Hour)
	h.leaveCancelingListed(h.list("canceling", batch+30, base)...)
	fresh := h.list("fresh", 1, base.Add(time.Minute))[0]

	for round := 0; round < 12 && h.expirer.callsFor(fresh) == 0; round++ {
		h.pass()
		*h.now = h.now.Add(10 * time.Millisecond)
	}

	if got := h.expirer.callsFor(fresh); got != 1 {
		t.Fatalf("the fresh execution was expired %d times, want exactly 1: canceling entries starved it", got)
	}
	if got := h.state.listed(); got != batch+30 {
		t.Fatalf("entries still listed = %d, want only the canceling ones (%d)", got, batch+30)
	}
}

// TestLeaseSweeperExecutionDeadlineCancelingIsNotAFailure pins that setting a
// canceling execution aside counts as neither a failure nor a release: no
// warning, no failure count, nothing reported as expired.
func TestLeaseSweeperExecutionDeadlineCancelingIsNotAFailure(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	stuck := h.list("stuck", 1, h.now.Add(-time.Hour))[0]
	h.leaveCancelingListed(stuck)

	for attempt := 0; attempt < 120; attempt++ {
		if got := h.pass(); got != 0 {
			t.Fatalf("pass %d released %d, want 0 for a canceling execution", attempt, got)
		}
		*h.now = h.now.Add(10 * time.Minute)
	}

	if got := h.expirer.callsFor(stuck); got != 120 {
		t.Fatalf("ExpireExecution calls = %d, want it retried on every pass once its window elapsed", got)
	}
	if got := h.deadlineBackoff.Failures(executionDeadlineKey(h.state.entries[0])); got != 0 {
		t.Fatalf("Failures = %d, want a canceling execution not to count as failing", got)
	}
	if got := len(h.warnings(keepsFailingMsg)); got != 0 {
		t.Fatalf("failure warnings = %d, want none for a canceling execution", got)
	}
}

// TestLeaseSweeperExecutionDeadlineCancelingIsRetriedAfterItsWindow pins that
// the deferral is a window, not an abandonment: the entry is not attempted
// inside it, and once the cancel concluded the next attempt after it settles it.
func TestLeaseSweeperExecutionDeadlineCancelingIsRetriedAfterItsWindow(t *testing.T) {
	h := newDeadlineHarness(everyPass(5))
	id := h.list("slow-cancel", 1, h.now.Add(-time.Hour))[0]
	h.leaveCancelingListed(id)

	h.pass()
	*h.now = h.now.Add(500 * time.Millisecond)
	h.pass()
	if got := h.expirer.callsFor(id); got != 1 {
		t.Fatalf("calls inside the window = %d, want 1", got)
	}

	delete(h.expirer.outcomes, id)
	*h.now = h.now.Add(500 * time.Millisecond)
	if got := h.pass(); got != 1 {
		t.Fatalf("pass after the window = %d, want the execution expired", got)
	}
	if got := h.state.listed(); got != 0 {
		t.Fatalf("entries still listed = %d, want 0", got)
	}
	if got := h.deadlineBackoff.Len(); got != 0 {
		t.Fatalf("backoff records = %d, want the success to clear the deferral", got)
	}
}
