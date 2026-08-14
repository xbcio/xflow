package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestBuildGroupLease_FillsIdentity(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	lease, payload, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildGroupLease: %v", err)
	}

	if lease.LeaseID == "" {
		t.Error("LeaseID empty")
	}
	if lease.LeaseToken == "" {
		t.Error("LeaseToken empty")
	}
	if lease.NodeType != "xflow.group" {
		t.Errorf("NodeType = %q, want xflow.group", lease.NodeType)
	}
	if lease.Task.UnitIdx != task.UnitIdx {
		t.Errorf("task UnitIdx = %d, want %d", lease.Task.UnitIdx, task.UnitIdx)
	}
	// TaskLease.Input must be nil — group payload is authoritative.
	if lease.Input != nil {
		t.Error("TaskLease.Input should be nil for group lease")
	}

	if payload == nil {
		t.Fatal("payload is nil")
	}
	if payload.GroupID == "" {
		t.Error("GroupID empty")
	}
	if payload.PackageHash == "" {
		t.Error("PackageHash empty")
	}
	if payload.Package == nil {
		t.Error("Package is nil")
	}
	if payload.Input == nil {
		t.Error("Input is nil")
	}
	if payload.IdempotencyKey == "" {
		t.Error("IdempotencyKey empty")
	}
	if payload.GroupUnitIdx != task.UnitIdx {
		t.Errorf("payload UnitIdx = %d, want %d", payload.GroupUnitIdx, task.UnitIdx)
	}
}

func TestBuildGroupLease_DoubleAcquireReturnsError(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	_, _, err := eng.BuildGroupLease(ctx, task)
	if err != nil {
		t.Fatalf("first BuildGroupLease: %v", err)
	}

	_, _, err = eng.BuildGroupLease(ctx, task)
	if err == nil {
		t.Fatal("second BuildGroupLease should fail")
	}
	if err != ErrGroupLeaseAlreadyActive {
		t.Errorf("error = %v, want ErrGroupLeaseAlreadyActive", err)
	}
}

func TestCommitGroupResult_InvalidExitPort(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	_, err = eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcomeSuccess,
		Exits: []GroupExitResult{{
			NodeName: "A",
			Port:     "nonexistent_port",
			Data:     map[string]any{"x": 1},
		}},
	})
	if err == nil {
		t.Fatal("CommitGroupResult should reject invalid exit port")
	}
}

// An outcome the engine does not recognize must be rejected, not committed.
// GroupResult.Outcome arrives as a bare JSON string from a remote runner, so an
// unknown value is reachable input rather than an impossible state.
//
// Before the fatality switch grew a default arm, such a value fell through to
// fatal=false and committed as a non-fatal failure, releasing downstream on a
// result nothing understood. Only "suspended" was guarded, and only because
// durable group suspend was unimplemented; removing that subsystem is what
// surfaced the wider hole.
func TestCommitGroupResult_UnknownOutcomeRejected(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	_, err = eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome: GroupOutcome("garbage"),
	})
	if err == nil {
		t.Fatal("CommitGroupResult accepted an unknown outcome")
	}
	// Name the reason: without this the test would also pass if the commit
	// failed for an unrelated cause (bad lease, missing graph).
	if !strings.Contains(err.Error(), "unknown group outcome") {
		t.Fatalf("failed for the wrong reason: %v", err)
	}
}

func TestRenewGroupLease_Success(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	renewed, err := eng.RenewGroupLease(ctx, lease, 30*time.Second)
	if err != nil {
		t.Fatalf("RenewGroupLease: %v", err)
	}
	if !renewed {
		t.Error("RenewGroupLease returned false, want true")
	}
}

func TestCommitGroupResult_ValidExitAccepted(t *testing.T) {
	eng, g, execID := setupGroupLeaseTest(t)
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

	// The fixture's crossing member edge is what makes this test meaningful: a
	// group with no boundary outputs has no valid exit to accept, so an empty
	// list means the fixture regressed, not that there is nothing to check.
	// Fail rather than skip — a silent skip here would hide the regression.
	boundaryOutputs := gm.BoundaryOutputs
	if len(boundaryOutputs) == 0 {
		t.Fatalf("fixture regressed: group %q has no boundary outputs, so no exit can be accepted", gm.Name)
	}
	bo := boundaryOutputs[0]
	srcName := g.NodeName(bo.Src.NodeIdx)

	outcome, err := eng.CommitGroupResult(ctx, lease, GroupResult{
		Outcome:     GroupOutcomeSuccess,
		GroupExecID: "test-exec",
		Attempt:     1,
		Exits: []GroupExitResult{{
			NodeName: srcName,
			Port:     bo.Src.Port,
			Data:     map[string]any{"result": "ok"},
		}},
	})
	if err != nil {
		t.Fatalf("CommitGroupResult: %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Errorf("outcome = %q, want accepted", outcome)
	}
}

// setupGroupLeaseTest creates an engine with a local group-capable state
// and a grouped workflow execution in running state.
func setupGroupLeaseTest(t *testing.T) (*Engine, *graph.Graph, types.ExecutionID) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "test-grouped",
		Version: "1",
		Context: &types.WorkflowContext{
			Vars:   map[string]any{"env": "test"},
			Config: map[string]any{"timeout": 30},
		},
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
			{Name: "C", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://c"}},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B", "C"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "C", Input: "main"}}}},
			"C": {"result": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	execID := types.ExecutionID("exec-group-lease-1")
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

// fakeGroupLeaseState implements StateStore + GroupStateStore for group lease
// tests. Tracks acquire/renew/commit.
type fakeGroupLeaseState struct {
	*fakeState
	acquired  bool
	committed bool
	leaseID   LeaseID
	token     LeaseToken
	attempt   int
}

func (f *fakeGroupLeaseState) AcquireGroupLease(_ context.Context, lease *GroupLease) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acquired {
		return false, nil
	}
	f.acquired = true
	f.leaseID = lease.LeaseID
	f.token = lease.LeaseToken
	f.attempt = lease.Attempt
	return true, nil
}

func (f *fakeGroupLeaseState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, token LeaseToken, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.acquired || token != f.token {
		return false, nil
	}
	return true, nil
}

func (f *fakeGroupLeaseState) CommitGroup(_ context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.committed {
		return GroupCommitResult{Outcome: CommitOutcomeDuplicateTerminal}, nil
	}
	if req.LeaseToken != f.token {
		return GroupCommitResult{Outcome: CommitOutcomeStaleToken}, nil
	}
	f.committed = true

	exec := f.executions[req.ExecutionID]
	if exec == nil {
		return GroupCommitResult{Outcome: CommitOutcomeExecutionInactive}, nil
	}

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

func (f *fakeGroupLeaseState) GetGroupLease(_ context.Context, _ types.ExecutionID, _ int) (*GroupLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.acquired || f.committed {
		return nil, nil
	}
	return &GroupLease{
		LeaseID:    f.leaseID,
		LeaseToken: f.token,
		Attempt:    f.attempt,
	}, nil
}
