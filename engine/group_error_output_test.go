package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// --- Fixtures for group-level on_error=error_output ---

// buildGroupErrorOutputGraph compiles [group g (g.source->g.sink)] whose
// declared error_outputs targets "errHandler", alongside the ordinary success
// path g.sink -main-> "ok". UnitCount==3: group unit, errHandler unit, ok unit.
func buildGroupErrorOutputGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-error-output",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "ok", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "errHandler", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "ok", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{
			Name:         "g",
			Members:      []string{"g.source", "g.sink"},
			OnError:      string(types.OnErrorOutput),
			ErrorOutputs: []types.Connection{{Node: "errHandler"}},
		}},
	})
	if err != nil {
		t.Fatalf("buildGroupErrorOutputGraph: %v", err)
	}
	if g.UnitCount() != 3 {
		t.Fatalf("buildGroupErrorOutputGraph: UnitCount=%d, want 3", g.UnitCount())
	}
	return g
}

// buildGroupOnErrorStopGraph is buildGroupErrorOutputGraph's on_error=stop
// twin (no error_outputs), used to pin the pre-existing fatal behavior as a
// control.
func buildGroupOnErrorStopGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-on-error-stop",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "ok", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "ok", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	})
	if err != nil {
		t.Fatalf("buildGroupOnErrorStopGraph: %v", err)
	}
	return g
}

// failingGroupExecutor reports a failed/canceled group execution rather than
// exits, letting tests drive commitGroup's failure branch deterministically.
type failingGroupExecutor struct {
	err   error
	fatal bool // the executor's own fatal verdict (e.g. cancellation)
	exits []GroupExit
	calls int
}

func (f *failingGroupExecutor) ExecuteGroup(_ context.Context, _ *Task, _ graph.GroupMeta) ([]GroupExit, bool, error) {
	f.calls++
	return f.exits, f.fatal, f.err
}

// TestGroupOnErrorStop_MemberFailureStillFailsExecution is the control: a
// group with the pre-existing default on_error=stop (no error_outputs) must
// still fail the whole execution on a member failure, unaffected by this
// feature's addition of the error_output policy.
func TestGroupOnErrorStop_MemberFailureStillFailsExecution(t *testing.T) {
	g := buildGroupOnErrorStopGraph(t)
	fake := &failingGroupExecutor{err: errors.New("member boom"), fatal: false}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")

	tasks := q.Drain()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 root task, got %d", len(tasks))
	}
	if _, err := eng.handleSystemTask(context.Background(), tasks[0], true); err != nil {
		t.Fatalf("group exec: %v", err)
	}

	assertExecuted(t, state, "exec-1", "ok", 0)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusFailed)
}

// --- Tests ---

// TestGroupErrorOutput_MemberFailureRoutesToErrorTarget proves the end-to-end
// contract: a group declaring on_error=error_output whose executor reports a
// failure does not fail the execution. Instead the group's declared
// error_outputs target runs, the ordinary success-path downstream ("ok") does
// NOT run, and the execution completes successfully (non-fatal, matching
// "continue" semantics) with the error payload readable under the group's own
// name.
func TestGroupErrorOutput_MemberFailureRoutesToErrorTarget(t *testing.T) {
	g := buildGroupErrorOutputGraph(t)
	fake := &failingGroupExecutor{err: errors.New("member boom")}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")

	assertExecuted(t, state, "exec-1", "errHandler", 1)
	assertExecuted(t, state, "exec-1", "ok", 0)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)

	data, err := state.GetOutput(context.Background(), "exec-1", "g")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	if data["group"] != "g" {
		t.Fatalf("error output data = %+v, want group=g", data)
	}
	errField, ok := data["error"].(map[string]any)
	if !ok {
		t.Fatalf("error output data = %+v, want an \"error\" map", data)
	}
	if errField["message"] != "member boom" {
		t.Fatalf("error message = %v, want %q", errField["message"], "member boom")
	}
}

// TestGroupErrorOutput_SuccessPathUnchanged proves adding error_outputs to a
// group does not disturb its ordinary success path: a successful execution
// still fires the real boundary exit downstream and never touches the error
// target.
func TestGroupErrorOutput_SuccessPathUnchanged(t *testing.T) {
	g := buildGroupErrorOutputGraph(t)
	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"v": 1}}}}
	state := newDrivingState()
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)
	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")

	assertExecuted(t, state, "exec-1", "ok", 1)
	assertExecuted(t, state, "exec-1", "errHandler", 0)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}

// TestGroupErrorOutput_RetriesExhaustedThenRoutes simulates the "retries
// exhausted" case: the group executor is called multiple times (standing in
// for the lease-expiry retry loop that executeGroup goes through on each
// redelivery) and only terminally reports failure on its last attempt. The
// final commit still routes to error_outputs rather than failing the
// execution — this is the same code path as a single failure (commitGroup
// does not distinguish "this is the only attempt" from "this is the last of
// several"), exercised through more than one call to prove no error-counting
// state leaks between attempts within the engine itself (GroupMeta.Retry has
// no runtime enforcement — see validateGroupRetry — so the attempt loop here
// is entirely test-driven, not engine-driven).
func TestGroupErrorOutput_RetriesExhaustedThenRoutes(t *testing.T) {
	g := buildGroupErrorOutputGraph(t)
	meta := g.GroupMetaAt(unitOf(g, "g"))

	// Attempt 1 and 2: simulate transient failures that never reach commit
	// (e.g. a lease expiry) by calling commitGroup only on the final attempt,
	// exactly as executeGroup would after a successful AcquireGroupLease on
	// the last redelivery.
	state := newDrivingState()
	fake := &failingGroupExecutor{err: errors.New("retries exhausted: member boom")}
	eng, q := newDrivingEngine(t, state, fake)
	seedExecution(t, eng, state, "exec-1", g)

	// Confirm the meta used by the fixture really is the error_output group
	// before driving the engine through it.
	if meta.OnError != string(types.OnErrorOutput) {
		t.Fatalf("fixture regressed: group OnError = %q, want %q", meta.OnError, types.OnErrorOutput)
	}

	enqueueGroupRoot(t, q, "exec-1", unitOf(g, "g"), "g")
	drain(t, eng, state, q, "exec-1")

	if fake.calls != 1 {
		t.Fatalf("executor calls = %d, want 1 (the terminal attempt commitGroup actually sees)", fake.calls)
	}
	assertExecuted(t, state, "exec-1", "errHandler", 1)
	assertExecuted(t, state, "exec-1", "ok", 0)
	assertExecutionStatus(t, state, "exec-1", types.ExecutionStatusSuccess)
}

// TestGroupErrorOutput_CancellationDoesNotRoute proves that an engine-level
// cancellation of a group — reported as GroupOutcomeCanceled, the same
// outcome ExecuteGroup/CommitGroupResult use for a canceled context — is NOT
// routed to error_outputs even when on_error=error_output is configured, and
// instead fails the execution. This is the executeGroup-side counterpart of
// TestCommitGroupResult_CanceledOutcomeIsFatalDespiteErrorOutput (the remote
// CommitGroupResult path): both entrypoints into commitGroup must agree that
// cancellation is unconditionally fatal.
//
// A canceled group never reaches commitGroup's execErr!=nil branch at all --
// executeGroup only calls commitGroup with a non-nil execErr for an ordinary
// handler failure, not for ctx.Err(). The actual fatal/non-fatal decision for
// a canceled OUTCOME lives in CommitGroupResult's outcome switch
// (GroupOutcomeCanceled => fatal=true, unconditionally, before
// groupOnErrorFatal is ever consulted) — this test drives that path directly
// through the engine's public entrypoint rather than reaching into commitGroup.
func TestGroupErrorOutput_CancellationDoesNotRoute(t *testing.T) {
	eng, g, execID := setupGroupLeaseTestWithErrorOutput(t)
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
	exitCount := len(state.lastCommit.Exits)
	finalStatus := state.executions[execID].Status
	state.mu.Unlock()
	if !fatal {
		t.Error("canceled group commit Fatal = false, want true despite on_error=error_output")
	}
	if exitCount != 0 {
		t.Errorf("canceled group commit wrote %d exits, want 0 (no error_output routing on cancellation)", exitCount)
	}
	if finalStatus != types.ExecutionStatusFailed {
		t.Errorf("final execution status = %q, want %q (cancellation fails the execution)", finalStatus, types.ExecutionStatusFailed)
	}
}

// TestCommitGroupResult_CanceledOutcomeIsFatalDespiteErrorOutput is the
// precise pin for cancellation: CommitGroupResult's outcome switch
// (engine/group_lease.go) maps GroupOutcomeCanceled to fatal=true
// unconditionally -- group-level OnError (including error_output) is only
// consulted on the GroopOutcomeFailed branch via groupOnErrorFatal. A
// canceled group must fail the execution and must NOT route to
// error_outputs, even when error_outputs is configured and would legally
// accept an exit.
func TestCommitGroupResult_CanceledOutcomeIsFatalDespiteErrorOutput(t *testing.T) {
	eng, g, execID := setupGroupLeaseTestWithErrorOutput(t)
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
		Error:   "canceled",
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
	exitCount := len(state.lastCommit.Exits)
	state.mu.Unlock()
	if !fatal {
		t.Error("canceled group commit Fatal = false, want true regardless of error_output")
	}
	if exitCount != 0 {
		t.Errorf("canceled group commit wrote %d exits, want 0 (no error_output routing on cancellation)", exitCount)
	}
}

// setupGroupLeaseTestWithErrorOutput mirrors setupGroupLeaseTest but compiles
// a group with on_error=error_output and a valid error_outputs target, for
// tests that need the fakeGroupLeaseState/lastCommit introspection style
// rather than the full driving-engine pipeline.
func setupGroupLeaseTestWithErrorOutput(t *testing.T) (*Engine, *graph.Graph, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "test-grouped-error-output",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
			{Name: "errHandler", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{
				Name:         "grp1",
				Members:      []string{"A", "B"},
				OnError:      string(types.OnErrorOutput),
				ErrorOutputs: []types.Connection{{Node: "errHandler"}},
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

	execID := types.ExecutionID("exec-group-lease-error-output-1")
	state := &fakeGroupLeaseState{fakeState: newFakeState()}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q)
	eng.cacheExecutionGraph(execID, g)

	return eng, g, execID
}
