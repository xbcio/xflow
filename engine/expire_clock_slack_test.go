package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// The guard compares the server's wall clock with the stored execution
// deadline, but a node that was cut off at that deadline was cut off by a timer
// on another clock. These tests pin the slack the guard grants such a node, and
// that nothing else gets it. None of them sleeps: the instant the guard judges
// is a parameter, so "3ms before the deadline" is an exact value, not a race.

// setStoredDeadline pins an execution's stored deadline to an exact instant and
// drops the engine's memo of it, so that the next guard read sees the new value.
// A rig that is built with a far deadline and re-pinned right before the commit
// keeps the margin the case names, whatever the cost of building the rig was.
func setStoredDeadline(f *fakeState, e *Engine, id types.ExecutionID, deadline time.Time) {
	editExecution(f, id, func(s *ExecutionSnapshot) { s.Deadline = deadline })
	forgetExecutionDeadline(e, id)
}

// TestClockSlackIsABoundedConstant fixes the size of the slack from outside the
// implementation: it has to cover clock drift (a few milliseconds up to a slew
// window) and has to stay below anything that could pass for a separate budget.
func TestClockSlackIsABoundedConstant(t *testing.T) {
	if executionDeadlineClockSlack <= 0 {
		t.Fatalf("executionDeadlineClockSlack = %v, want a positive slack", executionDeadlineClockSlack)
	}
	if executionDeadlineClockSlack > 100*time.Millisecond {
		t.Fatalf("executionDeadlineClockSlack = %v, want at most 100ms", executionDeadlineClockSlack)
	}
}

// TestLeaseClampedToExecutionDeadline pins the verdict on its own: a lease is
// clamped when its effective deadline is the execution deadline, which only the
// two server-held values can say.
func TestLeaseClampedToExecutionDeadline(t *testing.T) {
	exec := time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		lease time.Time
		exec  time.Time
		want  bool
	}{
		{"a lease that ends at the execution deadline", exec, exec, true},
		{"a lease past the execution deadline", exec.Add(time.Second), exec, true},
		{"a lease a nanosecond short of the execution deadline", exec.Add(-time.Nanosecond), exec, false},
		{"a node budget well short of the execution deadline", exec.Add(-30 * time.Millisecond), exec, false},
		{"a lease without a deadline carries no evidence", time.Time{}, exec, false},
		{"an execution without a deadline has nothing to be clamped to", exec, time.Time{}, false},
		{"neither deadline", time.Time{}, time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := leaseClampedToExecutionDeadline(tc.lease, tc.exec); got != tc.want {
				t.Fatalf("leaseClampedToExecutionDeadline(%v, %v) = %v, want %v", tc.lease, tc.exec, got, tc.want)
			}
		})
	}
}

// guardAt runs the lease-aware guard against a fresh rig whose stored deadline
// is far in the real past, so that ExpireExecution's own clock check is always
// satisfied and the injected instant alone decides whether the guard calls it.
func guardAt(t *testing.T, now func(deadline time.Time) time.Time, lease func(deadline time.Time) time.Time) (handled bool, rig *guardRig) {
	t.Helper()
	deadline := time.Now().Add(-time.Hour)
	rig = newGuardRig(t, guardSpec{onError: "continue", bounded: true}, deadline)
	g, err := rig.eng.loadGraph(context.Background(), rig.id)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	handled, err = rig.eng.expireIfLeaseTimeoutReached(context.Background(), rig.id, g, now(deadline), lease(deadline))
	if err != nil {
		t.Fatalf("guard error = %v", err)
	}
	return handled, rig
}

func requireGuardVerdict(t *testing.T, handled bool, rig *guardRig, want bool) {
	t.Helper()
	if handled != want {
		t.Fatalf("handled = %v, want %v", handled, want)
	}
	wantStatus := types.ExecutionStatusRunning
	if want {
		wantStatus = types.ExecutionStatusTimeout
	}
	if got := rig.snapshot(t).Status; got != wantStatus {
		t.Fatalf("execution status = %q, want %q", got, wantStatus)
	}
}

// TestClampedLeaseTimeoutGetsTheClockSlack: a node cut off at the execution
// deadline reports from a clock that may be a few milliseconds ahead of the
// server's, so a report that lands inside the slack is still the execution's
// timeout. The edge is exact: the whole slack counts, one nanosecond more does
// not.
func TestClampedLeaseTimeoutGetsTheClockSlack(t *testing.T) {
	slack := executionDeadlineClockSlack
	for _, tc := range []struct {
		name string
		at   time.Duration // the guard's now, relative to the stored deadline
		want bool
	}{
		{"just outside the slack", -slack - time.Nanosecond, false},
		{"exactly one slack early", -slack, true},
		{"20ms early", -20 * time.Millisecond, true},
		{"3ms early", -3 * time.Millisecond, true},
		{"a nanosecond early", -time.Nanosecond, true},
		{"exactly at the deadline", 0, true},
		{"a nanosecond after the deadline", time.Nanosecond, true},
		{"well after the deadline", time.Minute, true},
		{"200ms early is a real early timeout, not drift", -200 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handled, rig := guardAt(t,
				func(d time.Time) time.Time { return d.Add(tc.at) },
				func(d time.Time) time.Time { return d },
			)
			requireGuardVerdict(t, handled, rig, tc.want)
		})
	}
}

// TestUnclampedLeaseTimeoutKeepsTheExactComparison: a node whose own budget ran
// out first really did time out before its execution did. It gets no slack, or
// a failure on_error exists to handle would become the execution's timeout. The
// tie-break stays: a node and an execution budget that end together are the
// execution's timeout.
func TestUnclampedLeaseTimeoutKeepsTheExactComparison(t *testing.T) {
	for _, lease := range []struct {
		name string
		at   func(deadline time.Time) time.Time
	}{
		{"a node budget 30ms short of the execution deadline", func(d time.Time) time.Time { return d.Add(-30 * time.Millisecond) }},
		{"a node budget a nanosecond short of the execution deadline", func(d time.Time) time.Time { return d.Add(-time.Nanosecond) }},
		{"a lease with no deadline of its own", func(time.Time) time.Time { return time.Time{} }},
	} {
		for _, tc := range []struct {
			name string
			at   time.Duration
			want bool
		}{
			{"3ms early", -3 * time.Millisecond, false},
			{"a nanosecond early", -time.Nanosecond, false},
			{"exactly at the deadline", 0, true},
			{"after the deadline", time.Nanosecond, true},
		} {
			t.Run(lease.name+"/"+tc.name, func(t *testing.T) {
				handled, rig := guardAt(t,
					func(d time.Time) time.Time { return d.Add(tc.at) },
					lease.at,
				)
				requireGuardVerdict(t, handled, rig, tc.want)
			})
		}
	}
}

// TestUnboundedGraphLeaseTimeoutNeverReadsTheExecutionSnapshot is the control
// for the lease-aware guard: a graph without settings.timeout has no execution
// deadline, so whatever lease deadline comes with the report, the guard answers
// from the graph alone.
func TestUnboundedGraphLeaseTimeoutNeverReadsTheExecutionSnapshot(t *testing.T) {
	rig := newGuardRig(t, guardSpec{}, time.Time{})
	g, err := rig.eng.loadGraph(context.Background(), rig.id)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	rig.state.resetReads()
	far := time.Now().AddDate(100, 0, 0)
	handled, err := rig.eng.expireIfLeaseTimeoutReached(context.Background(), rig.id, g, far, far)
	if err != nil || handled {
		t.Fatalf("guard = (%v, %v), want (false, nil)", handled, err)
	}
	if got := rig.state.readCount(); got != 0 {
		t.Fatalf("GetExecution calls = %d, want 0", got)
	}
}

// TestExpireExecutionSlackWidensOnlyThePrivateEntry: the slack reaches
// ExpireExecution's own deadline check, so that a guard which widened its
// comparison is not then turned back by the stricter check inside, while the
// public entry and every other caller keep the exact one.
func TestExpireExecutionSlackWidensOnlyThePrivateEntry(t *testing.T) {
	ctx := context.Background()
	notDue := func(t *testing.T, rig *guardRig) {
		t.Helper()
		if got := rig.snapshot(t).Status; got != types.ExecutionStatusRunning {
			t.Fatalf("execution status = %q, want %q: nothing may be written for a deadline that is not due", got, types.ExecutionStatusRunning)
		}
	}
	newRig := func(t *testing.T) *guardRig {
		t.Helper()
		return newGuardRig(t, guardSpec{bounded: true}, time.Now().Add(time.Hour))
	}
	t.Run("the public entry compares exactly", func(t *testing.T) {
		rig := newRig(t)
		got, err := rig.eng.ExpireExecution(ctx, rig.id)
		if err != nil || got != ExpireOutcomeNotDue {
			t.Fatalf("ExpireExecution() = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeNotDue)
		}
		notDue(t, rig)
	})
	t.Run("zero slack is the public entry", func(t *testing.T) {
		rig := newRig(t)
		got, err := rig.eng.expireExecution(ctx, rig.id, 0)
		if err != nil || got != ExpireOutcomeNotDue {
			t.Fatalf("expireExecution(0) = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeNotDue)
		}
		notDue(t, rig)
	})
	t.Run("a slack short of the deadline is not due", func(t *testing.T) {
		rig := newRig(t)
		got, err := rig.eng.expireExecution(ctx, rig.id, 59*time.Minute)
		if err != nil || got != ExpireOutcomeNotDue {
			t.Fatalf("expireExecution(59m) = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeNotDue)
		}
		notDue(t, rig)
	})
	t.Run("a slack that reaches the deadline expires the execution", func(t *testing.T) {
		rig := newRig(t)
		got, err := rig.eng.expireExecution(ctx, rig.id, 61*time.Minute)
		if err != nil || got != ExpireOutcomeExpired {
			t.Fatalf("expireExecution(61m) = (%q, %v), want (%q, nil)", got, err, ExpireOutcomeExpired)
		}
		if snap := rig.snapshot(t); snap.Status != types.ExecutionStatusTimeout || snap.Error != ExecutionDeadlineExceededError {
			t.Fatalf("execution = (%q, %q), want timeout for its deadline", snap.Status, snap.Error)
		}
	})
}
