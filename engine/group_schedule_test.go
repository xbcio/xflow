package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// buildLinearGraph compiles a linear DAG from the given node names (a->b->c...).
func buildLinearGraph(t *testing.T, names ...string) *graph.Graph {
	t.Helper()
	nodes := make([]types.NodeDef, len(names))
	for i, name := range names {
		nodes[i] = types.NodeDef{Name: name, Type: "test.action", Kind: types.NodeKindAction}
	}
	conns := make(types.Connections, len(names)-1)
	for i := 0; i < len(names)-1; i++ {
		conns[names[i]] = map[string]types.PortConnections{
			"main": {Targets: []types.Connection{{Node: names[i+1], Input: "main"}}},
		}
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name:        "linear",
		Nodes:       nodes,
		Connections: conns,
	})
	if err != nil {
		t.Fatalf("buildLinearGraph: %v", err)
	}
	return g
}

// buildSingleGroupGraph compiles a graph with a single group "g" containing
// members "g.source" and "g.sink" (g.source->g.sink), with an external
// downstream "out" (g.sink->out). The group has UnitInDegree==0 so it is a root.
func buildSingleGroupGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "grouped",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.action", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.action", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	})
	if err != nil {
		t.Fatalf("buildSingleGroupGraph: %v", err)
	}
	return g
}

// --- fakeGroupExecutor ---

type fakeGroupExecutor struct {
	exits []GroupExit
	calls int
}

func (f *fakeGroupExecutor) ExecuteGroup(_ context.Context, _ *Task, _ graph.GroupMeta) ([]GroupExit, bool, error) {
	f.calls++
	return f.exits, false, nil
}

// --- fakeGroupStateStore ---

type fakeGroupState struct {
	acquired bool
}

func (f *fakeGroupState) AcquireGroupLease(_ context.Context, _ *GroupLease) (bool, error) {
	f.acquired = true
	return true, nil
}

func (f *fakeGroupState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (f *fakeGroupState) CommitGroup(_ context.Context, _ GroupCommitRequest) (GroupCommitResult, error) {
	return GroupCommitResult{Outcome: CommitOutcomeAccepted, Applied: true}, nil
}

// --- Tests ---

func TestSubmitInitialTasks_NoGroupUnchanged(t *testing.T) {
	g := buildLinearGraph(t, "a", "b", "c")
	tasks := submitInitialTasks("exec-1", g)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 root task, got %d: %+v", len(tasks), tasks)
	}
	if tasks[0].task.Type != TaskTypeNodeExec {
		t.Fatalf("expected TaskTypeNodeExec, got %d", tasks[0].task.Type)
	}
	if tasks[0].task.NodeName != "a" {
		t.Fatalf("expected root node 'a', got %q", tasks[0].task.NodeName)
	}
}

func TestSubmitInitialTasks_GroupEmitsGroupExec(t *testing.T) {
	g := buildSingleGroupGraph(t)
	tasks := submitInitialTasks("exec-1", g)
	// The group is a root unit (in-degree 0), so exactly one TaskTypeGroupExec
	// should be emitted for it.
	var groupTasks []initialTask
	for _, it := range tasks {
		if it.task.Type == TaskTypeGroupExec {
			groupTasks = append(groupTasks, it)
		}
	}
	if len(groupTasks) != 1 {
		t.Fatalf("expected 1 group exec task, got %d from %+v", len(groupTasks), tasks)
	}
	gt := groupTasks[0].task
	if gt.NodeName != "g" {
		t.Fatalf("expected group task NodeName='g', got %q", gt.NodeName)
	}
	// UnitIdx must point to the group unit
	if g.UnitKindAt(gt.UnitIdx) != graph.UnitGroup {
		t.Fatalf("UnitIdx %d is not a UnitGroup", gt.UnitIdx)
	}
}

func TestHandleSystemTask_GroupExecDispatch(t *testing.T) {
	g := buildSingleGroupGraph(t)

	// Find the group unit index
	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	fake := &fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}}}
	fgs := &fakeGroupState{}

	// Build a combined state that satisfies both StateStore and GroupStateStore.
	state := &fakeStateWithGroup{fakeState: newFakeState(), groupState: fgs}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     "exec-1",
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q, WithGroupExecutor(fake))
	eng.cacheExecutionGraph("exec-1", g)

	handled, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: "exec-1",
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	if err != nil {
		t.Fatalf("handleSystemTask returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true for TaskTypeGroupExec")
	}
	if fake.calls != 1 {
		t.Fatalf("expected executor called once, got %d", fake.calls)
	}
	if !fgs.acquired {
		t.Fatal("expected group lease to be acquired")
	}
}

func TestHandleSystemTask_GroupExecNilExecutor(t *testing.T) {
	g := buildSingleGroupGraph(t)

	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	state := &fakeStateWithGroup{fakeState: newFakeState(), groupState: &fakeGroupState{}}
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     "exec-1",
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(state, q) // no GroupExecutor
	eng.cacheExecutionGraph("exec-1", g)

	handled, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: "exec-1",
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	// With no GroupExecutor, the task is NOT handled locally — the Dispatcher
	// must route it to a remote runner via TaskRouting → EnqueueAssignment.
	if handled {
		t.Fatal("expected handled=false when group executor is nil (remote dispatch)")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// fakeStateWithGroup wraps fakeState and adds GroupStateStore support.
type fakeStateWithGroup struct {
	*fakeState
	groupState *fakeGroupState
}

func (f *fakeStateWithGroup) AcquireGroupLease(ctx context.Context, lease *GroupLease) (bool, error) {
	return f.groupState.AcquireGroupLease(ctx, lease)
}

func (f *fakeStateWithGroup) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (f *fakeStateWithGroup) CommitGroup(ctx context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	return f.groupState.CommitGroup(ctx, req)
}

// attemptTrackingGroupState simulates the backend contract: AcquireGroupLease
// writes the real attempt back into the lease (just as both the local memory
// and Redis backends do), and CommitGroup records the attempt it received so
// callers can assert the fence value was correct.
//
// This is a probe for the fact that the hardcoded seed of 1 in executeGroup is
// overwritten by AcquireGroupLease before commitGroup is called. The seed
// value is irrelevant; what matters is that commitGroup always uses
// lease.Attempt as returned by the backend.
type attemptTrackingGroupState struct {
	currentAttempt   int // simulates the persisted attempt counter
	committedAttempt int
}

func (s *attemptTrackingGroupState) AcquireGroupLease(_ context.Context, lease *GroupLease) (bool, error) {
	// Mirror the local/Redis contract: if the persisted attempt is >= the
	// incoming seed, use persisted+1; otherwise use the seed.
	attempt := lease.Attempt
	if s.currentAttempt >= attempt {
		attempt = s.currentAttempt + 1
	}
	s.currentAttempt = attempt
	lease.Attempt = attempt // write the real attempt back, as both backends do
	return true, nil
}

func (s *attemptTrackingGroupState) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (s *attemptTrackingGroupState) CommitGroup(_ context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	s.committedAttempt = req.Attempt
	return GroupCommitResult{Outcome: CommitOutcomeAccepted, Applied: true}, nil
}

// fakeStateWithAttemptTracking wires attemptTrackingGroupState into the
// combined state used by Engine.
type fakeStateWithAttemptTracking struct {
	*fakeState
	groupState *attemptTrackingGroupState
}

func (f *fakeStateWithAttemptTracking) AcquireGroupLease(ctx context.Context, lease *GroupLease) (bool, error) {
	return f.groupState.AcquireGroupLease(ctx, lease)
}

func (f *fakeStateWithAttemptTracking) RenewGroupLease(_ context.Context, _ types.ExecutionID, _ int, _ LeaseToken, _ time.Time) (bool, error) {
	return true, nil
}

func (f *fakeStateWithAttemptTracking) CommitGroup(ctx context.Context, req GroupCommitRequest) (GroupCommitResult, error) {
	return f.groupState.CommitGroup(ctx, req)
}

// TestGroupExec_AttemptFromBackend is a probe that verifies lease.Attempt is
// read from AcquireGroupLease (the backend's authoritative value) before being
// passed to CommitGroup, not taken from the hardcoded seed of 1.
//
// The engine seeds attempt=1 unconditionally; the backend overwrites it with
// the real counter. On the first run the backend returns 1 (seed matches),
// and on a simulated second run it returns 2 (prev >= seed triggers bump).
// CommitGroup must receive the attempt the backend returned.
func TestGroupExec_AttemptFromBackend(t *testing.T) {
	g := buildSingleGroupGraph(t)

	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == graph.UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	fake := &fakeGroupExecutor{exits: []GroupExit{
		{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}},
	}}
	tracker := &attemptTrackingGroupState{}

	// Seed the tracker to simulate that the group has already been attempted
	// once and its lease expired (attempt counter is now 1 in the backend).
	// The next acquire with seed=1 must bump it to 2.
	tracker.currentAttempt = 1

	combined := &fakeStateWithAttemptTracking{fakeState: newFakeState(), groupState: tracker}
	combined.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     "exec-probe-attempt",
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	q := &fakeQueue{}
	eng := New(combined, q, WithGroupExecutor(fake))
	eng.cacheExecutionGraph("exec-probe-attempt", g)

	_, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: "exec-probe-attempt",
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true)
	if err != nil {
		t.Fatalf("handleSystemTask error: %v", err)
	}

	// The backend bumped attempt from 1 to 2; CommitGroup must have received 2.
	if tracker.committedAttempt != 2 {
		t.Fatalf("CommitGroup received Attempt=%d, want 2 — engine is not using the attempt returned by AcquireGroupLease", tracker.committedAttempt)
	}
}
