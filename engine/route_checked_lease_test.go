package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// checkedLeaseState is fakeState plus engine.RouteCheckedLeaseAcquirer, with
// call counters for every read and write the lease path can make.
//
// The counters are the point: the saving this capability buys is invisible in
// the returned lease — an arm that still issued the two pre-reads would produce
// the same answer — so only the call counts can tell the two paths apart.
type checkedLeaseState struct {
	*fakeState
	checkedAcquires  int
	legacyAcquires   int
	statusReads      int
	getNodeCalls     int
	executionMissing bool
	acquiredVerdict  *bool
	lastOptions      LeaseAcquireOptions
}

func (s *checkedLeaseState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.legacyAcquires++
	return s.fakeState.AcquireTaskLease(ctx, lease)
}

func (s *checkedLeaseState) AcquireTaskLeaseChecked(ctx context.Context, lease *TaskLease, opts LeaseAcquireOptions) (LeaseAcquireResult, error) {
	s.checkedAcquires++
	s.lastOptions = opts
	prev, acquired, err := s.fakeState.AcquireTaskLease(ctx, lease)
	if err != nil {
		return LeaseAcquireResult{}, err
	}
	if s.acquiredVerdict != nil {
		acquired = *s.acquiredVerdict
	}
	res := LeaseAcquireResult{Acquired: acquired, Previous: prev}
	if !s.executionMissing {
		if snap, err := s.fakeState.GetExecution(ctx, lease.Task.ExecutionID); err != nil {
			return LeaseAcquireResult{}, err
		} else if snap != nil {
			res.ExecutionFound = true
			res.ExecutionStatus = snap.Status
		}
	}
	return res, nil
}

func (s *checkedLeaseState) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.statusReads++
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return "", false, err
	}
	return snap.Status, true, nil
}

func (s *checkedLeaseState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.getNodeCalls++
	return s.fakeState.GetNode(ctx, id, name)
}

func (s *checkedLeaseState) resetCounters() {
	s.checkedAcquires = 0
	s.legacyAcquires = 0
	s.statusReads = 0
	s.getNodeCalls = 0
}

// legacyLeaseCountingState is fakeState with the reads the legacy lease path
// makes counted. It deliberately does NOT implement RouteCheckedLeaseAcquirer,
// so BuildTaskLease must take the read-then-write path.
type legacyLeaseCountingState struct {
	*fakeState
	acquires         int
	getNodeCalls     int
	getExecutionCall int
}

func (s *legacyLeaseCountingState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.acquires++
	return s.fakeState.AcquireTaskLease(ctx, lease)
}

func (s *legacyLeaseCountingState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.getNodeCalls++
	return s.fakeState.GetNode(ctx, id, name)
}

func (s *legacyLeaseCountingState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.getExecutionCall++
	return s.fakeState.GetExecution(ctx, id)
}

func compileRouteCheckedTestGraph(t *testing.T, cyclic bool) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:  "route-checked-lease",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	}
	if cyclic {
		def = &types.WorkflowDef{
			Name:    "route-checked-lease-cyclic",
			Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 3},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "next", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
				"next":  {"main": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
			},
		}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestBuildTaskLeaseRouteCheckedUsesOneAcquire pins the whole point of the
// capability: on a warm graph cache the lease path issues exactly one store
// call — the acquire itself — and zero execution-status or node reads.
//
// Before the capability, the same call paid three round trips on Redis: the
// activeness read, the node-route read, and the acquire write.
func TestBuildTaskLeaseRouteCheckedUsesOneAcquire(t *testing.T) {
	for _, cyclic := range []bool{false, true} {
		name := "Acyclic"
		if cyclic {
			name = "Cyclic"
		}
		t.Run(name, func(t *testing.T) {
			state := &checkedLeaseState{fakeState: newFakeState()}
			queue := &fakeQueue{}
			eng := New(state, queue)
			ctx := context.Background()

			g := compileRouteCheckedTestGraph(t, cyclic)
			id, err := eng.Submit(ctx, g, nil)
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			tasks := queue.Drain()
			if len(tasks) != 1 {
				t.Fatalf("task count = %d, want 1", len(tasks))
			}

			state.resetCounters()
			lease, err := eng.BuildTaskLease(ctx, tasks[0])
			if err != nil {
				t.Fatalf("BuildTaskLease() error = %v", err)
			}
			if lease == nil || lease.Task.ExecutionID != id {
				t.Fatalf("lease = %+v, want a lease for %q", lease, id)
			}
			if state.checkedAcquires != 1 {
				t.Fatalf("AcquireTaskLeaseChecked calls = %d, want exactly 1", state.checkedAcquires)
			}
			if state.legacyAcquires != 0 {
				t.Fatalf("legacy AcquireTaskLease calls = %d, want 0: the capability was declared", state.legacyAcquires)
			}
			if state.statusReads != 0 {
				t.Fatalf("GetExecutionStatus calls = %d, want 0: the acquire reports the status itself", state.statusReads)
			}
			if state.getNodeCalls != 0 {
				t.Fatalf("GetNode calls = %d, want 0: the acquire re-decides the route itself", state.getNodeCalls)
			}
			if state.lastOptions.AllowCycles != cyclic {
				t.Fatalf("AllowCycles = %v, want %v: the option must come from the compiled graph", state.lastOptions.AllowCycles, cyclic)
			}
		})
	}
}

// TestBuildTaskLeaseWithoutCapabilityKeepsReadThenWrite pins the fallback: a
// store without the capability still pays the activeness read, the node-route
// read, and the legacy acquire, exactly as before.
func TestBuildTaskLeaseWithoutCapabilityKeepsReadThenWrite(t *testing.T) {
	if _, ok := any(newFakeState()).(RouteCheckedLeaseAcquirer); ok {
		t.Fatal("fakeState now implements RouteCheckedLeaseAcquirer: the fallback arm would stop covering the read-then-write path")
	}
	state := &legacyLeaseCountingState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	g := compileRouteCheckedTestGraph(t, false)
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}

	state.acquires, state.getNodeCalls, state.getExecutionCall = 0, 0, 0
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if lease == nil {
		t.Fatal("BuildTaskLease() = nil lease on the fallback path")
	}
	if state.acquires != 1 {
		t.Fatalf("AcquireTaskLease calls = %d, want 1", state.acquires)
	}
	if state.getExecutionCall < 1 {
		t.Fatalf("GetExecution calls = %d, want at least 1: the fallback confirms the execution is still active", state.getExecutionCall)
	}
	if state.getNodeCalls != 1 {
		t.Fatalf("GetNode calls = %d, want 1: the fallback checks the node's route before acquiring", state.getNodeCalls)
	}
}

// TestBuildTaskLeaseRouteCheckedRefusesTerminalExecution pins the race the
// capability exists to close: the acquire itself reports the terminal status,
// and the engine must evict the cached graph and report the execution inactive
// instead of handing the runner a lease.
func TestBuildTaskLeaseRouteCheckedRefusesTerminalExecution(t *testing.T) {
	state := &checkedLeaseState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, compileRouteCheckedTestGraph(t, false), nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	id := tasks[0].ExecutionID

	// The graph is cached by Submit; the terminal write alone must be enough to
	// refuse the lease, because the acquire observes the status itself.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	state.resetCounters()

	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if !errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("BuildTaskLease() on a canceled execution = (%v, %v), want ErrExecutionInactive", lease, err)
	}
	if state.checkedAcquires != 1 {
		t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1: the status verdict must come from the acquire", state.checkedAcquires)
	}
	if state.statusReads != 0 {
		t.Fatalf("GetExecutionStatus calls = %d, want 0", state.statusReads)
	}
	eng.mu.RLock()
	_, cached := eng.graphs[id]
	eng.mu.RUnlock()
	if cached {
		t.Fatalf("graph for %q is still cached after the acquire reported a terminal execution", id)
	}
}

// TestBuildTaskLeaseRouteCheckedRefusesMissingExecution pins the other half of
// the acquire's execution verdict: execution_found=false evicts and reports
// inactive, for the same reason terminal does.
func TestBuildTaskLeaseRouteCheckedRefusesMissingExecution(t *testing.T) {
	state := &checkedLeaseState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	g := compileRouteCheckedTestGraph(t, false)
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}

	// The graph stays cached; the execution record goes away underneath it,
	// which is exactly what the engine cannot detect from its own cache.
	state.mu.Lock()
	delete(state.executions, id)
	state.mu.Unlock()
	state.resetCounters()

	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if !errors.Is(err, ErrExecutionInactive) {
		t.Fatalf("BuildTaskLease() for a vanished execution = (%v, %v), want ErrExecutionInactive", lease, err)
	}
	if state.checkedAcquires != 1 {
		t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1", state.checkedAcquires)
	}
	eng.mu.RLock()
	_, cached := eng.graphs[id]
	eng.mu.RUnlock()
	if cached {
		t.Fatalf("graph for %q is still cached after the acquire reported the execution missing", id)
	}
}

// TestBuildTaskLeaseRouteCheckedClassifiesAcquireRefusals pins that a refusal
// the execution verdict does not explain still goes through the existing
// classifier — here a live lease, which must surface as ErrLeaseAlreadyActive
// rather than as a generic inactive execution.
func TestBuildTaskLeaseRouteCheckedClassifiesAcquireRefusals(t *testing.T) {
	state := &checkedLeaseState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, compileRouteCheckedTestGraph(t, false), nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	if _, err := eng.BuildTaskLease(ctx, tasks[0]); err != nil {
		t.Fatalf("first BuildTaskLease() error = %v", err)
	}

	state.resetCounters()
	if _, err := eng.BuildTaskLease(ctx, tasks[0]); !errors.Is(err, ErrLeaseAlreadyActive) {
		t.Fatalf("second BuildTaskLease() error = %v, want ErrLeaseAlreadyActive: the owner's lease is live", err)
	}
	if state.checkedAcquires != 1 {
		t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1", state.checkedAcquires)
	}
}
