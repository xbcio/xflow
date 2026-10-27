package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// The group call site: a group the runner cut off at the execution deadline
// reports GroupOutcomeTimeout from a clock that may be a few milliseconds ahead
// of the server's. Nothing in the TaskLease says which budget the group ran
// out of (its bound travels in a payload the runner echoes), so the verdict is
// rebuilt from the graph, the lease's issue time and the stored deadline.

// groupSlackRig leases a group unit whose own timeout is groupTimeout (zero for
// none) over an execution whose stored deadline starts an hour out; each case
// then pins the deadline relative to the moment it commits.
func groupSlackRig(t *testing.T, groupTimeout time.Duration) (*Engine, *groupDeadlineState, *TaskLease, *expireHooks) {
	t.Helper()
	_, state, task := groupClampRig(t, true, groupTimeout, time.Now().Add(time.Hour))
	g := state.fakeState.executions[task.ExecutionID].Graph
	if g == nil {
		t.Fatal("the stored execution carries no graph")
	}
	log := &expireLog{track: task.ExecutionID}
	hooks := &expireHooks{log: log}
	eng := New(&groupReasonState{groupDeadlineState: state}, &fakeQueue{}, WithHooks(hooks))
	eng.cacheExecutionGraph(task.ExecutionID, g)
	lease, _, err := eng.BuildGroupLease(context.Background(), task)
	if err != nil {
		t.Fatalf("BuildGroupLease() error = %v", err)
	}
	return eng, state, lease, hooks
}

// commitGroupTimeoutBefore reports a group timeout arriving margin before the
// stored execution deadline (a negative margin is an arrival after it).
func commitGroupTimeoutBefore(t *testing.T, eng *Engine, state *groupDeadlineState, lease *TaskLease, margin time.Duration) CommitOutcome {
	t.Helper()
	setStoredDeadline(state.fakeState, eng, lease.Task.ExecutionID, time.Now().Add(margin))
	outcome, err := eng.CommitGroupResult(context.Background(), lease, GroupResult{Outcome: GroupOutcomeTimeout, Error: "deadline exceeded"})
	if err != nil {
		t.Fatalf("CommitGroupResult() error = %v", err)
	}
	return outcome
}

func requireGroupExpired(t *testing.T, state *groupDeadlineState, hooks *expireHooks, id types.ExecutionID) {
	t.Helper()
	snap := groupExecution(t, state, id)
	if snap.Status != types.ExecutionStatusTimeout || snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("execution = (%q, %q), want timeout for its deadline", snap.Status, snap.Error)
	}
	if state.committed {
		t.Fatal("the group was committed: an expired execution must not take a group commit")
	}
	want := []types.ExecutionStatus{types.ExecutionStatusTimeout}
	if got := hooks.completions(); !slices.Equal(got, want) {
		t.Fatalf("OnExecutionComplete statuses = %v, want %v", got, want)
	}
}

func requireGroupFailedFatally(t *testing.T, state *groupDeadlineState, hooks *expireHooks, id types.ExecutionID) {
	t.Helper()
	if !state.committed || !state.lastCommit.Fatal {
		t.Fatalf("group committed = %v, fatal = %v, want a fatal commit", state.committed, state.lastCommit.Fatal)
	}
	if got := groupExecution(t, state, id).Status; got != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %q, want %q", got, types.ExecutionStatusFailed)
	}
	want := []types.ExecutionStatus{types.ExecutionStatusFailed}
	if got := hooks.completions(); !slices.Equal(got, want) {
		t.Fatalf("OnExecutionComplete statuses = %v, want %v", got, want)
	}
}

// TestClampedGroupTimeoutArrivingEarlyExpiresTheExecution: a group whose lease
// ends at the execution deadline, with or without a timeout of its own that is
// longer, reports its timeout inside the slack. That is the execution's
// timeout, not the group failure that would end it as failed.
func TestClampedGroupTimeoutArrivingEarlyExpiresTheExecution(t *testing.T) {
	for _, groupTimeout := range []time.Duration{0, time.Hour} {
		for _, margin := range []time.Duration{20 * time.Millisecond, 3 * time.Millisecond, -time.Millisecond} {
			t.Run(groupTimeout.String()+"/"+margin.String(), func(t *testing.T) {
				eng, state, lease, hooks := groupSlackRig(t, groupTimeout)
				if got := commitGroupTimeoutBefore(t, eng, state, lease, margin); got != CommitOutcomeExecutionInactive {
					t.Fatalf("outcome = %q, want %q: the execution's timeout settles the commit", got, CommitOutcomeExecutionInactive)
				}
				requireGroupExpired(t, state, hooks, lease.Task.ExecutionID)
			})
		}
	}
}

// TestClampedGroupTimeoutFarBeforeTheDeadlineStillFailsTheExecution: the slack
// is a bound. A clamped group that times out 200ms before the execution
// deadline is a group failure like any other.
func TestClampedGroupTimeoutFarBeforeTheDeadlineStillFailsTheExecution(t *testing.T) {
	eng, state, lease, hooks := groupSlackRig(t, 0)
	if got := commitGroupTimeoutBefore(t, eng, state, lease, 200*time.Millisecond); got != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q: a timeout 200ms early is not the execution's", got, CommitOutcomeAccepted)
	}
	requireGroupFailedFatally(t, state, hooks, lease.Task.ExecutionID)
}

// TestUnclampedGroupTimeoutKeepsTheExactComparison: the group's own budget ran
// out first. The execution deadline is 30ms away, inside the slack, but the
// group lease did not end there, so the slack does not apply and the timeout
// stays a group failure.
func TestUnclampedGroupTimeoutKeepsTheExactComparison(t *testing.T) {
	eng, state, lease, hooks := groupSlackRig(t, time.Millisecond)
	if got := commitGroupTimeoutBefore(t, eng, state, lease, 30*time.Millisecond); got != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q: the group budget ran out before the execution's", got, CommitOutcomeAccepted)
	}
	requireGroupFailedFatally(t, state, hooks, lease.Task.ExecutionID)
}

// TestUnclampedGroupTimeoutAfterTheDeadlineStillExpiresTheExecution: the group
// has a shorter budget but reports only after the execution deadline.
func TestUnclampedGroupTimeoutAfterTheDeadlineStillExpiresTheExecution(t *testing.T) {
	eng, state, lease, hooks := groupSlackRig(t, time.Millisecond)
	if got := commitGroupTimeoutBefore(t, eng, state, lease, -time.Millisecond); got != CommitOutcomeExecutionInactive {
		t.Fatalf("outcome = %q, want %q", got, CommitOutcomeExecutionInactive)
	}
	requireGroupExpired(t, state, hooks, lease.Task.ExecutionID)
}

// TestCommittedGroupDeadlineIsTheDeadlineTheLeaseWasIssuedWith: the verdict is
// only as good as the bound it rebuilds. For a lease as BuildGroupLease issued
// it, and as RecoverGroupLease replays it, the rebuilt bound is the payload
// deadline the runner was given.
func TestCommittedGroupDeadlineIsTheDeadlineTheLeaseWasIssuedWith(t *testing.T) {
	ctx := context.Background()
	for _, tc := range groupClampCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, task := groupClampRig(t, tc.bounded, tc.groupTimeout, tc.execDeadline)
			lease, payload, err := eng.BuildGroupLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildGroupLease() error = %v", err)
			}
			g, err := eng.loadGraph(ctx, task.ExecutionID)
			if err != nil || g == nil {
				t.Fatalf("loadGraph() = (%v, %v)", g, err)
			}
			got, err := eng.committedGroupDeadline(ctx, lease, g)
			if err != nil {
				t.Fatalf("committedGroupDeadline() error = %v", err)
			}
			if !got.Equal(payload.Deadline) {
				t.Fatalf("rebuilt deadline = %v, want the issued payload deadline %v", got, payload.Deadline)
			}

			state.issuedAt = lease.IssuedAt
			restarted := New(state, &fakeQueue{})
			recoveredLease, recovered, err := restarted.RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx)
			if err != nil {
				t.Fatalf("RecoverGroupLease() error = %v", err)
			}
			got, err = restarted.committedGroupDeadline(ctx, recoveredLease, g)
			if err != nil {
				t.Fatalf("committedGroupDeadline(recovered) error = %v", err)
			}
			if !got.Equal(recovered.Deadline) {
				t.Fatalf("rebuilt deadline after recovery = %v, want the replayed payload deadline %v", got, recovered.Deadline)
			}
		})
	}
}

// TestCommittedGroupDeadlineOfAnUnusableUnitIsZero: a lease that names no group
// unit has no bound to judge by. The guard then stays exact, and the commit
// itself rejects the unit afterwards.
func TestCommittedGroupDeadlineOfAnUnusableUnitIsZero(t *testing.T) {
	ctx := context.Background()
	eng, _, lease, _ := groupSlackRig(t, 0)
	g, err := eng.loadGraph(ctx, lease.Task.ExecutionID)
	if err != nil || g == nil {
		t.Fatalf("loadGraph() = (%v, %v)", g, err)
	}
	for _, unitIdx := range []int{-1, g.UnitCount()} {
		bad := *lease
		bad.Task.UnitIdx = unitIdx
		got, err := eng.committedGroupDeadline(ctx, &bad, g)
		if err != nil || !got.IsZero() {
			t.Fatalf("committedGroupDeadline(unit %d) = (%v, %v), want (zero, nil)", unitIdx, got, err)
		}
	}
}
