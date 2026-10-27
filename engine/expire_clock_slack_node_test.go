package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// A node cut off at the execution deadline reports its timeout from a clock that
// can be a few milliseconds ahead of the server's. These cases drive that report
// through the real commit path of a runner-reported node timeout. The engine
// reads the wall clock itself there, so the margin is set the only way it can
// be: the stored deadline (and the lease's, for an unclamped one) is pinned to
// "now plus the margin" immediately before the commit. Every margin that matters
// is either far from the slack or on the side where both readings agree, so the
// few microseconds between the pin and the guard's own read decide nothing.

// leaseAt places the lease's own deadline relative to the stored execution
// deadline d.
type leaseAt func(d time.Time) time.Time

var (
	// clampedLease is a lease the engine clamped to the execution deadline:
	// the node had no shorter budget of its own.
	clampedLease leaseAt = func(d time.Time) time.Time { return d }
	// shorterBudgetLease is a node whose own budget ends 30ms before the
	// execution's, which is the scenario a lease-blind slack gets wrong.
	shorterBudgetLease leaseAt = func(d time.Time) time.Time { return d.Add(-30 * time.Millisecond) }
)

// commitTimeoutBefore reports a node timeout whose arrival is margin before the
// stored execution deadline (a negative margin is an arrival after it) and
// returns what the commit answered.
func commitTimeoutBefore(t *testing.T, rig *guardRig, margin time.Duration, lease leaseAt) CommitOutcome {
	t.Helper()
	deadline := time.Now().Add(margin)
	setStoredDeadline(rig.state.fakeState, rig.eng, rig.id, deadline)
	rig.lease.ExecutionDeadline = lease(deadline)
	result := TaskResult{Error: types.NewPermanentError(types.NodeTimeoutErrorCode, "node execution exceeded its 1s timeout")}
	outcome, err := rig.eng.CommitTaskResultWithOutcome(context.Background(), rig.lease, result)
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	return outcome
}

// onErrorOutcomes is what each on_error strategy does with a node failure the
// execution has time to absorb, as the existing guard tests pin it.
var onErrorOutcomes = []struct {
	onError    string
	wantExec   types.ExecutionStatus
	wantNode   types.NodeStatus
	wantQueued string
}{
	{"", types.ExecutionStatusFailed, types.NodeStatusFailed, ""},
	{"continue", types.ExecutionStatusRunning, types.NodeStatusContinued, "next"},
	{"error_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "onerr"},
	{"main_output", types.ExecutionStatusRunning, types.NodeStatusSuccess, "next"},
}

// TestClampedNodeTimeoutArrivingEarlyExpiresTheExecution is the bug itself: a
// clamped node's timer fires a few milliseconds before the server's clock
// reaches the execution deadline. That is the execution's timeout, whichever
// on_error strategy the node has. Before the slack, stop ended the execution as
// failed for good and continue ran it on past its budget.
func TestClampedNodeTimeoutArrivingEarlyExpiresTheExecution(t *testing.T) {
	for _, margin := range []time.Duration{20 * time.Millisecond, 3 * time.Millisecond} {
		for _, onError := range guardOnErrors {
			t.Run(margin.String()+"/"+guardName(onError), func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: onError, bounded: true}, time.Now().Add(time.Hour))
				if got := commitTimeoutBefore(t, rig, margin, clampedLease); got != CommitOutcomeExecutionInactive {
					t.Fatalf("commit outcome = %q, want %q: the execution's timeout settles the commit", got, CommitOutcomeExecutionInactive)
				}
				rig.requireExpired(t)
			})
		}
	}
}

// TestClampedNodeTimeoutArrivingEarlyExpiresACyclicExecution is the same cut-off
// on a cyclic graph, whose commit runs through the leased-node committer.
func TestClampedNodeTimeoutArrivingEarlyExpiresACyclicExecution(t *testing.T) {
	rig := newGuardRig(t, guardSpec{cyclic: true, bounded: true}, time.Now().Add(time.Hour))
	if got := commitTimeoutBefore(t, rig, 3*time.Millisecond, clampedLease); got != CommitOutcomeExecutionInactive {
		t.Fatalf("commit outcome = %q, want %q", got, CommitOutcomeExecutionInactive)
	}
	rig.requireExpired(t)
}

// TestClampedNodeTimeoutArrivingAfterTheDeadlineExpiresTheExecution: the slack
// widens the guard towards earlier arrivals only. A report after the deadline
// was the execution's timeout before it and stays so.
func TestClampedNodeTimeoutArrivingAfterTheDeadlineExpiresTheExecution(t *testing.T) {
	for _, margin := range []time.Duration{-time.Millisecond, -time.Minute} {
		for _, onError := range guardOnErrors {
			t.Run(margin.String()+"/"+guardName(onError), func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: onError, bounded: true}, time.Now().Add(time.Hour))
				if got := commitTimeoutBefore(t, rig, margin, clampedLease); got != CommitOutcomeExecutionInactive {
					t.Fatalf("commit outcome = %q, want %q", got, CommitOutcomeExecutionInactive)
				}
				rig.requireExpired(t)
			})
		}
	}
}

// TestClampedNodeTimeoutFarBeforeTheDeadlineStillAppliesOnError proves the slack
// is a bound: a clamped lease that times out 200ms (four times the slack) before
// the execution deadline cannot be clock drift, so it is an ordinary node
// failure and on_error decides.
func TestClampedNodeTimeoutFarBeforeTheDeadlineStillAppliesOnError(t *testing.T) {
	for _, tc := range onErrorOutcomes {
		t.Run(guardName(tc.onError), func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: tc.onError, bounded: true}, time.Now().Add(time.Hour))
			if got := commitTimeoutBefore(t, rig, 200*time.Millisecond, clampedLease); got == CommitOutcomeExecutionInactive {
				t.Fatalf("commit outcome = %q: a timeout 200ms early is not the execution's", got)
			}
			requireOnErrorApplied(t, rig, tc.wantExec, tc.wantNode, tc.wantQueued)
		})
	}
}

// TestUnclampedNodeTimeoutKeepsTheExactComparison is the scenario a lease-blind
// slack gets wrong. The node's own budget ends 30ms before the execution's, so
// its timeout is a node failure however close to the execution deadline it is:
// treating it as the execution's would turn a failure on_error exists to handle
// into the execution's timeout. The 30ms is inside the slack, which is exactly
// why only the lease can tell the two apart.
func TestUnclampedNodeTimeoutKeepsTheExactComparison(t *testing.T) {
	for _, tc := range onErrorOutcomes {
		t.Run(guardName(tc.onError), func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: tc.onError, bounded: true}, time.Now().Add(time.Hour))
			// The node timer fires at its own deadline, 30ms before the
			// execution's: the arrival is the moment the margin is taken.
			if got := commitTimeoutBefore(t, rig, 30*time.Millisecond, shorterBudgetLease); got == CommitOutcomeExecutionInactive {
				t.Fatalf("commit outcome = %q: a node budget that ran out first is not the execution's timeout", got)
			}
			requireOnErrorApplied(t, rig, tc.wantExec, tc.wantNode, tc.wantQueued)
		})
	}
}

// TestUnclampedNodeTimeoutAfterTheDeadlineStillExpiresTheExecution: a node with
// a shorter budget that reports only once the execution deadline is reached is
// still caught by it. Nothing about the slack changes this.
func TestUnclampedNodeTimeoutAfterTheDeadlineStillExpiresTheExecution(t *testing.T) {
	for _, onError := range guardOnErrors {
		t.Run(guardName(onError), func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: onError, bounded: true}, time.Now().Add(time.Hour))
			if got := commitTimeoutBefore(t, rig, -time.Millisecond, shorterBudgetLease); got != CommitOutcomeExecutionInactive {
				t.Fatalf("commit outcome = %q, want %q", got, CommitOutcomeExecutionInactive)
			}
			rig.requireExpired(t)
		})
	}
}

// TestOtherErrorsEarlyOnAClampedLeaseStillApplyOnError: the slack belongs to the
// node timeout code and nothing else. Any other failure on a clamped lease, even
// one that lands inside the slack, keeps its on_error semantics.
func TestOtherErrorsEarlyOnAClampedLeaseStillApplyOnError(t *testing.T) {
	rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(time.Hour))
	setStoredDeadline(rig.state.fakeState, rig.eng, rig.id, time.Now().Add(3*time.Millisecond))
	rig.lease.ExecutionDeadline = rig.snapshot(t).Deadline
	err := rig.eng.CommitTaskResult(context.Background(), rig.lease, TaskResult{Error: types.NewPermanentError("node.http.status", "bad gateway")})
	if err != nil {
		t.Fatalf("CommitTaskResult() error = %v", err)
	}
	requireOnErrorApplied(t, rig, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
}
