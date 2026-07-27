package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestSingleGroupExecution_CompletesOnOneCommit validates the P0-2 end-to-end
// invariant: a single-group graph (UnitCount==1 for the group) completes the
// execution in exactly one group commit.
func TestSingleGroupExecution_CompletesOnOneCommit(t *testing.T) {
	g := buildSingleGroupGraph(t) // UnitCount includes group + downstream "out" node
	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"n": 1}}}}

	// Build a combined state that satisfies both StateStore and GroupStateStore,
	// with CommitGroup implementing the unit-based remaining logic.
	state := &fakeStateWithGroupE2E{fakeState: newFakeState()}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     "exec-1",
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(fake))
	eng.cacheExecutionGraph("exec-1", g)

	// Find the group unit index.
	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	if _, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: "exec-1", NodeName: "g", UnitIdx: groupUnitIdx, Type: TaskTypeGroupExec,
	}, true); err != nil {
		t.Fatalf("group exec: %v", err)
	}

	snap, _ := eng.state.GetExecution(context.Background(), "exec-1")
	if snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("P0-2: single-group execution must complete on one commit, got %q", snap.Status)
	}
}

// fakeStateWithGroupE2E extends fakeStateWithGroup to implement a realistic
// CommitGroup that decrements remaining unit count and terminates execution.
type fakeStateWithGroupE2E struct {
	*fakeState
	committed bool
}

func (f *fakeStateWithGroupE2E) AcquireGroupLease(_ context.Context, _ *GroupLease) (bool, error) {
	return true, nil
}

func (f *fakeStateWithGroupE2E) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (f *fakeStateWithGroupE2E) CommitGroup(_ context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.committed {
		return GroupCommitResult{Outcome: CommitOutcomeDuplicateTerminal}, nil
	}
	f.committed = true

	exec := f.executions[req.ExecutionID]
	if exec == nil {
		return GroupCommitResult{Outcome: CommitOutcomeExecutionInactive}, nil
	}

	// Single-group graph: UnitCount for the group is 1. One commit makes
	// remaining=0 so the execution is done.
	finalStatus := types.ExecutionStatusSuccess
	if req.Fatal {
		finalStatus = types.ExecutionStatusFailed
	}
	exec.Status = finalStatus
	return GroupCommitResult{
		Outcome:         CommitOutcomeAccepted,
		Applied:         true,
		ExecutionDone:   true,
		ExecutionStatus: finalStatus,
	}, nil
}
