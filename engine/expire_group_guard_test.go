package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// groupReasonState is the group deadline fake with the status write both real
// backends have and fakeState does not: the execution-level reason is recorded
// for any status (fakeState keeps it for failed only), and a terminal status is
// never overwritten. ExpireExecution reads the reason back to tell its own
// timeout from another one, so without this it would never see its write.
type groupReasonState struct {
	*groupDeadlineState
}

func (s *groupReasonState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	f := s.fakeState
	f.mu.Lock()
	defer f.mu.Unlock()
	exec, ok := f.executions[id]
	if !ok || types.IsTerminalExecutionStatus(exec.Status) {
		return nil
	}
	exec.Status = status
	if errMsg != "" {
		exec.Error = errMsg
	}
	f.releaseConcurrencySlotLocked(id, status)
	f.publishLocked(ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

// groupGuardRig is a running execution with one leased group unit, over the
// group lease fake the other group deadline tests use. execDeadline is written
// into the stored snapshot before the lease is built, because the engine
// memoizes its first read of it.
func groupGuardRig(t *testing.T, bounded bool, execDeadline time.Time) (*Engine, *groupDeadlineState, *TaskLease, *expireHooks) {
	t.Helper()
	_, state, task := groupClampRig(t, bounded, 0, execDeadline)
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

func groupExecution(t *testing.T, state *groupDeadlineState, id types.ExecutionID) ExecutionSnapshot {
	t.Helper()
	snap, err := state.fakeState.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = (%v, %v)", snap, err)
	}
	return *snap
}

// TestGroupTimeoutPastTheExecutionDeadlineExpiresTheExecution: a group that
// reports timeout once the execution's own budget is gone ends the execution as
// timeout for its deadline, not as the failed a group timeout otherwise is. The
// group is never committed: the expiry owns the execution.
func TestGroupTimeoutPastTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	eng, state, lease, hooks := groupGuardRig(t, true, time.Now().Add(-time.Minute))
	ctx := context.Background()

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{Outcome: GroupOutcomeTimeout, Error: "deadline exceeded"})
	if err != nil {
		t.Fatalf("CommitGroupResult() error = %v", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("outcome = %q, want %q: the execution was already over", outcome, CommitOutcomeExecutionInactive)
	}
	snap := groupExecution(t, state, lease.Task.ExecutionID)
	if snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("execution status = %q, want %q", snap.Status, types.ExecutionStatusTimeout)
	}
	if snap.Error != ExecutionDeadlineExceededError {
		t.Fatalf("execution error = %q, want %q", snap.Error, ExecutionDeadlineExceededError)
	}
	if state.committed {
		t.Fatal("the group was committed: an expired execution must not take a group commit")
	}
	want := []types.ExecutionStatus{types.ExecutionStatusTimeout}
	if got := hooks.completions(); !slices.Equal(got, want) {
		t.Fatalf("OnExecutionComplete statuses = %v, want %v", got, want)
	}
}

// TestGroupTimeoutBeforeTheExecutionDeadlineStillFailsTheExecution is the
// control: the group's own budget ran out while the execution still has time, so
// the group commit is the fatal one it has always been.
func TestGroupTimeoutBeforeTheExecutionDeadlineStillFailsTheExecution(t *testing.T) {
	eng, state, lease, hooks := groupGuardRig(t, true, time.Now().Add(time.Hour))
	ctx := context.Background()

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{Outcome: GroupOutcomeTimeout, Error: "deadline exceeded"})
	if err != nil {
		t.Fatalf("CommitGroupResult() error = %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeAccepted)
	}
	if !state.committed || !state.lastCommit.Fatal {
		t.Fatalf("group committed = %v, fatal = %v, want a fatal commit", state.committed, state.lastCommit.Fatal)
	}
	if got := groupExecution(t, state, lease.Task.ExecutionID).Status; got != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %q, want %q", got, types.ExecutionStatusFailed)
	}
	// The fatal group commit announces the failure itself, as it always has.
	want := []types.ExecutionStatus{types.ExecutionStatusFailed}
	if got := hooks.completions(); !slices.Equal(got, want) {
		t.Fatalf("OnExecutionComplete statuses = %v, want %v", got, want)
	}
}

// TestGroupOtherOutcomesPastTheExecutionDeadlineAreNotExpired: the guard is
// about the timeout outcome and nothing else. A failed or canceled group keeps
// its commit semantics after the execution budget is gone.
func TestGroupOtherOutcomesPastTheExecutionDeadlineAreNotExpired(t *testing.T) {
	for _, outcome := range []GroupOutcome{GroupOutcomeFailed, GroupOutcomeCanceled} {
		t.Run(string(outcome), func(t *testing.T) {
			eng, state, lease, hooks := groupGuardRig(t, true, time.Now().Add(-time.Minute))
			got, err := eng.CommitGroupResult(context.Background(), lease, GroupResult{Outcome: outcome, Error: "boom"})
			if err != nil {
				t.Fatalf("CommitGroupResult() error = %v", err)
			}
			if got != CommitOutcomeAccepted || !state.committed {
				t.Fatalf("outcome = %q, committed = %v, want an accepted group commit", got, state.committed)
			}
			if status := groupExecution(t, state, lease.Task.ExecutionID).Status; status == types.ExecutionStatusTimeout {
				t.Fatalf("execution status = %q: only the timeout outcome may expire the execution", status)
			}
			if c := hooks.completions(); slices.Contains(c, types.ExecutionStatusTimeout) {
				t.Fatalf("OnExecutionComplete statuses = %v, want no timeout", c)
			}
		})
	}
}

// TestUnboundedGroupTimeoutNeverReadsTheExecutionSnapshot: a graph without
// settings.timeout has no execution deadline, so a group timeout adds not one
// snapshot read to its commit and stays the fatal failure it has always been.
// The store here has no narrow status read, so the activeness check already
// reads the snapshot; the claim is about the DIFFERENCE between a timeout
// outcome (which asks the guard) and a failed one (which does not).
func TestUnboundedGroupTimeoutNeverReadsTheExecutionSnapshot(t *testing.T) {
	reads := func(outcome GroupOutcome) (int64, *groupDeadlineState) {
		t.Helper()
		eng, state, lease, _ := groupGuardRig(t, false, time.Time{})
		before := state.getExecution.Load()
		if _, err := eng.CommitGroupResult(context.Background(), lease, GroupResult{Outcome: outcome, Error: "boom"}); err != nil {
			t.Fatalf("CommitGroupResult(%s) error = %v", outcome, err)
		}
		return state.getExecution.Load() - before, state
	}
	failedReads, _ := reads(GroupOutcomeFailed)
	timeoutReads, state := reads(GroupOutcomeTimeout)
	if timeoutReads != failedReads {
		t.Fatalf("GetExecution calls: timeout outcome %d, failed outcome %d, want the same: an unbounded graph pays nothing for the guard",
			timeoutReads, failedReads)
	}
	if !state.committed || !state.lastCommit.Fatal {
		t.Fatalf("group committed = %v, fatal = %v, want a fatal commit", state.committed, state.lastCommit.Fatal)
	}
}
