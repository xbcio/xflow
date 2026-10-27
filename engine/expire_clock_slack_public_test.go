package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// The public ExpireExecution is what the deadline sweeper calls. The sweeper
// reads the same wall clock the deadline was stamped from, so unlike a node that
// a remote timer cut off it has no drift to forgive: the execution is due when
// the clock has reached the deadline, not executionDeadlineClockSlack before it.
// These cases pin that by reading the real clock, because the slack hides in the
// engine's own time.Now and no injected instant reaches it. Nothing sleeps and
// no margin depends on how fast the machine is: every attempt is judged only
// when the call provably returned before the deadline.

// maxExactEntryAttempts bounds the rigs one case may discard. An attempt is
// discarded when the call itself outlasted its margin, which on a loaded machine
// can happen to any single attempt but not to all of them.
const maxExactEntryAttempts = 100

// publicEntryInsideSlack pins a fresh execution's deadline margin ahead of the
// real clock and calls the public ExpireExecution right away. It returns the
// rig and the answer of the first attempt whose call returned before that
// deadline. Such an attempt reads the clock strictly before
// the deadline too, so the exact comparison has only one answer for it: not due.
func publicEntryInsideSlack(t *testing.T, margin time.Duration) (*guardRig, ExpireOutcome) {
	t.Helper()
	if margin <= 0 || margin >= executionDeadlineClockSlack {
		t.Fatalf("margin = %v, want a margin inside (0, %v): the case is about the slack window", margin, executionDeadlineClockSlack)
	}
	for attempt := 0; attempt < maxExactEntryAttempts; attempt++ {
		rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(time.Hour))
		deadline := time.Now().Add(margin)
		setStoredDeadline(rig.state.fakeState, rig.eng, rig.id, deadline)
		got, err := rig.eng.ExpireExecution(context.Background(), rig.id)
		returned := time.Now()
		if err != nil {
			t.Fatalf("ExpireExecution() error = %v", err)
		}
		if returned.Before(deadline) {
			return rig, got
		}
	}
	t.Fatalf("no ExpireExecution call returned within %v of its start in %d attempts: the machine is too slow to judge the exact comparison", margin, maxExactEntryAttempts)
	return nil, ""
}

// TestPublicExpireExecutionKeepsTheExactDeadline: an execution whose deadline is
// still ahead, but by less than the clock slack, is not due for the sweeper. It
// stays running and nothing is announced. Next to it, a node timeout from a
// clamped lease at the same margin is the execution's timeout, which makes the
// slack visibly a property of that one path and not of the execution.
func TestPublicExpireExecutionKeepsTheExactDeadline(t *testing.T) {
	for _, margin := range []time.Duration{40 * time.Millisecond, 20 * time.Millisecond} {
		t.Run(margin.String()+" before the deadline", func(t *testing.T) {
			t.Run("the public entry answers not due and writes nothing", func(t *testing.T) {
				rig, got := publicEntryInsideSlack(t, margin)
				if got != ExpireOutcomeNotDue {
					t.Fatalf("ExpireExecution() = %q, want %q: the sweeper must not expire an execution before its deadline", got, ExpireOutcomeNotDue)
				}
				snap := rig.snapshot(t)
				if snap.Status != types.ExecutionStatusRunning || snap.Error != "" {
					t.Fatalf("execution = (%q, %q), want it running and untouched", snap.Status, snap.Error)
				}
				if got := rig.hooks.completions(); len(got) != 0 {
					t.Fatalf("OnExecutionComplete statuses = %v, want none", got)
				}
			})
			t.Run("a clamped node timeout at the same margin is the execution's timeout", func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(time.Hour))
				if got := commitTimeoutBefore(t, rig, margin, clampedLease); got != CommitOutcomeExecutionInactive {
					t.Fatalf("commit outcome = %q, want %q: a lease cut off at the deadline gets the slack", got, CommitOutcomeExecutionInactive)
				}
				rig.requireExpired(t)
			})
		})
	}
}
