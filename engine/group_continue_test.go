package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// buildGroupContinueGraph compiles [group g (g.source->g.sink)] with
// group-level on_error=continue, alongside the ordinary success path
// g.sink -main-> "ok". UnitCount==2: group unit, ok unit.
func buildGroupContinueGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-on-error-continue",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "ok", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "ok", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{
			Name:    "g",
			Members: []string{"g.source", "g.sink"},
			OnError: string(types.OnErrorContinue),
		}},
	})
	if err != nil {
		t.Fatalf("buildGroupContinueGraph: %v", err)
	}
	return g
}

// TestGroupOnErrorContinue_MemberFailureCompletesExecution is the engine-level
// reproduction for the documented latent bug (NODE-GROUP-COLOCATION.md §12.2):
// a group configured with on_error=continue whose member fails must NOT fail
// the whole execution, mirroring node-level on_error=continue
// (ApplyOnError's NodeStatusContinued/ExecFatal=false contract,
// engine/errorpolicy.go). Before the fix, every backend's failed-unit counter
// incremented on GroupOutcomeFailed regardless of Fatal, so a non-fatal
// "continue" commit still finalized the execution as Failed once the
// remaining-unit counter reached zero.
func TestGroupOnErrorContinue_MemberFailureCompletesExecution(t *testing.T) {
	g := buildGroupContinueGraph(t)
	fake := &failingGroupExecutor{err: errors.New("member boom"), fatal: false}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")

	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}

// TestGroupOnErrorContinue_RealFailureStillObservable proves the real failure
// is not swallowed by the fix: the group's commit still carries Fatal=false
// and the original error message, matching error_output's "handled, not
// lost" contract (commitGroup's execErr/errMsg plumbing).
func TestGroupOnErrorContinue_RealFailureStillObservable(t *testing.T) {
	eng, g, execID := setupGroupLeaseTestWithContinue(t)
	ctx := context.Background()

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID:  execID,
		NodeName:     gm.Name,
		NodeIdx:      gm.EntryIdx,
		UnitIdx:      gm.UnitIdx,
		Type:         TaskTypeGroupExec,
		ActivationID: 0,
	}
	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcomeFailed,
		Error:   "member boom",
	})
	if err != nil {
		t.Fatalf("CommitGroupResult: %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want accepted", outcome)
	}

	state := eng.state.(*fakeGroupLeaseState)
	state.mu.Lock()
	fatal := state.lastCommit.Fatal
	errMsg := state.lastCommit.Error
	finalStatus := state.executions[execID].Status
	state.mu.Unlock()
	if fatal {
		t.Error("continue group commit Fatal = true, want false")
	}
	if errMsg != "member boom" {
		t.Errorf("commit error message = %q, want %q", errMsg, "member boom")
	}
	if finalStatus != types.ExecutionStatusSuccess {
		t.Errorf("final execution status = %q, want %q (continue must not fail the execution)", finalStatus, types.ExecutionStatusSuccess)
	}
}

// TestGroupOnErrorContinue_CancellationStaysFatal proves cancellation remains
// fatal under on_error=continue, mirroring
// TestCommitGroupResult_CanceledOutcomeIsFatalDespiteErrorOutput for the
// error_output policy: a canceled/timeout GroupOutcome is caller-classified
// before groupOnErrorFatal is ever consulted (engine/group_lease.go's outcome
// switch), so continue must not downgrade it to non-fatal.
func TestGroupOnErrorContinue_CancellationStaysFatal(t *testing.T) {
	eng, g, execID := setupGroupLeaseTestWithContinue(t)
	ctx := context.Background()

	gm := g.Groups()[0]
	task := &Task{
		ExecutionID:  execID,
		NodeName:     gm.Name,
		NodeIdx:      gm.EntryIdx,
		UnitIdx:      gm.UnitIdx,
		Type:         TaskTypeGroupExec,
		ActivationID: 0,
	}
	lease, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcomeCanceled,
		Error:   "context canceled",
	})
	if err != nil {
		t.Fatalf("CommitGroupResult: %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %q, want accepted", outcome)
	}

	state := eng.state.(*fakeGroupLeaseState)
	state.mu.Lock()
	fatal := state.lastCommit.Fatal
	finalStatus := state.executions[execID].Status
	state.mu.Unlock()
	if !fatal {
		t.Error("canceled group commit Fatal = false, want true despite on_error=continue")
	}
	if finalStatus != types.ExecutionStatusFailed {
		t.Errorf("final execution status = %q, want %q (cancellation fails the execution)", finalStatus, types.ExecutionStatusFailed)
	}
}

// setupGroupLeaseTestWithContinue mirrors setupGroupLeaseTestWithErrorOutput
// but compiles a group with on_error=continue instead.
func setupGroupLeaseTestWithContinue(t *testing.T) (*Engine, *graph.Graph, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "test-grouped-continue",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
		},
		Groups: []types.GroupDef{
			{
				Name:    "grp1",
				Members: []string{"A", "B"},
				OnError: string(types.OnErrorContinue),
			},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	execID := types.ExecutionID("exec-group-lease-continue-1")
	state := &fakeGroupLeaseState{fakeState: newFakeState()}
	if err := state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	q := &fakeQueue{}
	eng := New(state, q)
	eng.cacheExecutionGraph(execID, g)

	return eng, g, execID
}
