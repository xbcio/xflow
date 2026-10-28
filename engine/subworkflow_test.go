package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// stubSubWorkflowResolver serves one fixed target for every reference. The
// admission gate only needs the engine to hold a resolver; what it answers is
// the dispatch path's business.
type stubSubWorkflowResolver struct{ target graph.SubWorkflowTarget }

func (s stubSubWorkflowResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return s.target, nil
}

// subWorkflowCapableState decorates any StateStore with the sub-workflow link
// capability so the admission gate can be exercised without a backend that
// really implements it. The gate only asserts the interface; these methods must
// never be reached from a test that only submits, so they fail loudly rather
// than quietly returning a zero link.
type subWorkflowCapableState struct{ StateStore }

var errSubWorkflowDoubleUnused = errors.New("subWorkflowCapableState: the link capability is asserted, not exercised, by these tests")

func (s *subWorkflowCapableState) BeginSubWorkflowWait(context.Context, *TaskLease, SubWorkflowLink, []OutboxEntry) (bool, error) {
	return false, errSubWorkflowDoubleUnused
}

func (s *subWorkflowCapableState) GetSubWorkflowLink(context.Context, types.ExecutionID, string) (SubWorkflowLink, bool, error) {
	return SubWorkflowLink{}, false, errSubWorkflowDoubleUnused
}

func (s *subWorkflowCapableState) RecordSubWorkflowConclusion(context.Context, types.ExecutionID, string, SubWorkflowConclusion) error {
	return errSubWorkflowDoubleUnused
}

func (s *subWorkflowCapableState) GetSubWorkflowConclusion(context.Context, types.ExecutionID, string, types.ExecutionID) (SubWorkflowConclusion, bool, error) {
	return SubWorkflowConclusion{}, false, errSubWorkflowDoubleUnused
}

// subWorkflowChildGraph compiles the minimal child target: start -> work.
func subWorkflowChildGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "child",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "work"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile child graph: %v", err)
	}
	return g
}

func subWorkflowStubResolver(t *testing.T) stubSubWorkflowResolver {
	t.Helper()
	return stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          subWorkflowChildGraph(t),
	}}
}

// subWorkflowParentDef is a parent graph with one sub-workflow root node.
func subWorkflowParentDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "parent",
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: map[string]any{"workflow": "child"},
		}},
	}
}

// resolvedSubWorkflowParent compiles the parent WITH a resolver, so its node
// carries the pinned binding the gate requires.
func resolvedSubWorkflowParent(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.CompileWithResolver(context.Background(), subWorkflowParentDef(), subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

func TestSubmitRefusesSubWorkflowWithoutResolver(t *testing.T) {
	state := newFakeState()
	queue := &fakeQueue{}
	eng := newTestEngine(t, state, queue, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	_, err := eng.Submit(context.Background(), resolvedSubWorkflowParent(t), nil)
	if !errors.Is(err, ErrSubWorkflowUnsupported) {
		t.Fatalf("Submit error = %v, want ErrSubWorkflowUnsupported for an engine without a resolver", err)
	}
	if len(state.executions) != 0 {
		t.Fatalf("a refused submission persisted %d executions", len(state.executions))
	}
	if len(queue.tasks) != 0 {
		t.Fatalf("a refused submission enqueued %d tasks", len(queue.tasks))
	}
}

func TestSubmitRefusesSubWorkflowWithoutStoreCapability(t *testing.T) {
	state := newFakeState() // not a SubWorkflowLinkStore
	queue := &fakeQueue{}
	eng := New(state, queue, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	t.Cleanup(func() { testRegistries.Delete(eng) })

	_, err := eng.Submit(context.Background(), resolvedSubWorkflowParent(t), nil)
	if !errors.Is(err, ErrSubWorkflowUnsupported) {
		t.Fatalf("Submit error = %v, want ErrSubWorkflowUnsupported for a store without the link capability", err)
	}
	if len(state.executions) != 0 {
		t.Fatalf("a refused submission persisted %d executions", len(state.executions))
	}
}

func TestSubmitRefusesUnresolvedSubWorkflowNode(t *testing.T) {
	// Compiled without a resolver: the node has no pinned target revision, so
	// even a fully capable engine must refuse to run it.
	unresolved, err := graph.Compile(subWorkflowParentDef())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	state := &subWorkflowCapableState{StateStore: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	t.Cleanup(func() { testRegistries.Delete(eng) })

	_, err = eng.Submit(context.Background(), unresolved, nil)
	if !errors.Is(err, ErrSubWorkflowUnresolved) {
		t.Fatalf("Submit error = %v, want ErrSubWorkflowUnresolved for an unresolvable node", err)
	}
}

func TestSubmitAdmitsResolvedSubWorkflowGraph(t *testing.T) {
	inner := newFakeState()
	state := &subWorkflowCapableState{StateStore: inner}
	queue := &fakeQueue{}
	eng := New(state, queue, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	t.Cleanup(func() { testRegistries.Delete(eng) })

	id, err := eng.Submit(context.Background(), resolvedSubWorkflowParent(t), nil)
	if err != nil {
		t.Fatalf("Submit: %v; a capable engine must admit a graph whose references are pinned", err)
	}
	if _, ok := inner.executions[id]; !ok {
		t.Fatal("admitted submission did not persist its execution")
	}
}

// TestSubmitAdmitsOrdinaryGraphWithoutSubWorkflowSupport pins the gate's scope:
// a graph with no sub-workflow nodes is unaffected by a missing resolver or a
// store without the link capability.
func TestSubmitAdmitsOrdinaryGraphWithoutSubWorkflowSupport(t *testing.T) {
	state := newFakeState()
	queue := &fakeQueue{}
	eng := newTestEngine(t, state, queue, &fakeRegistry{handlers: map[string]types.ActionHandler{}})

	if _, err := eng.Submit(context.Background(), subWorkflowChildGraph(t), nil); err != nil {
		t.Fatalf("Submit of an ordinary graph: %v; the sub-workflow gate must not see it", err)
	}
}

// TestRerunRefusesSubWorkflowWithoutResolver pins the gate on the rerun
// creation path: a rerun re-creates an execution of the parent's graph, so it
// needs exactly what a submission needs, and a refusal must land before the
// seeding transition (a claim or a partially seeded execution would be worse
// than the refusal).
func TestRerunRefusesSubWorkflowWithoutResolver(t *testing.T) {
	ctx := context.Background()
	g := resolvedSubWorkflowParent(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, map[string]rerunNode{
		"sub": {status: types.NodeStatusFailed, port: "error"},
	})
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "sub"})
	if !errors.Is(err, ErrSubWorkflowUnsupported) {
		t.Fatalf("Rerun error = %v, want ErrSubWorkflowUnsupported", err)
	}
	if len(seeder.requests) != 0 {
		t.Fatal("a rerun an engine cannot run seeded an execution anyway")
	}
}

// subWorkflowSchedulingEngine builds a legacy (non-atomic) capable engine so
// the scheduled task lands directly in the fake queue, where its type is
// readable without driving the execution.
func subWorkflowSchedulingEngine(t *testing.T) (*Engine, *fakeState, *fakeQueue) {
	t.Helper()
	inner := newFakeState()
	eng := New(&subWorkflowCapableState{StateStore: inner}, &fakeQueue{}, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	queue := eng.queue.(*fakeQueue)
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{}})
	t.Cleanup(func() { testRegistries.Delete(eng) })
	return eng, inner, queue
}

// TestScheduledSubWorkflowNodeGetsItsOwnTaskType pins the scheduler half: the
// root task of a sub-workflow node is stamped TaskTypeSubWorkflow, which is
// what keeps it off the runner routes.
func TestScheduledSubWorkflowNodeGetsItsOwnTaskType(t *testing.T) {
	eng, _, queue := subWorkflowSchedulingEngine(t)

	if _, err := eng.Submit(context.Background(), resolvedSubWorkflowParent(t), nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued %d tasks, want the one root task", len(tasks))
	}
	if tasks[0].Type != TaskTypeSubWorkflow {
		t.Fatalf("root task type = %d, want TaskTypeSubWorkflow", tasks[0].Type)
	}
}

// TestDownstreamArrivalOfSubWorkflowNodeGetsItsOwnTaskType pins the downstream
// half: a node reached by an advance is stamped by its own type, not by the
// default.
func TestDownstreamArrivalOfSubWorkflowNodeGetsItsOwnTaskType(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "advance-to-sub",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "sub", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "child"}},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "sub"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	startIdx, _ := g.NodeIndex("start")
	arrivals := downstreamArrivals(g, startIdx, "main")
	if len(arrivals) != 1 {
		t.Fatalf("downstreamArrivals returned %d arrivals, want 1", len(arrivals))
	}
	if arrivals[0].ExecTaskType != TaskTypeSubWorkflow {
		t.Fatalf("arrival task type = %d, want TaskTypeSubWorkflow", arrivals[0].ExecTaskType)
	}
}

// TestTaskRoutingRefusesEngineOwnedTasks is the architecture pin: no runner
// may ever be offered a task type that only the engine knows how to consume,
// and the refusal must be permanent so a retry-capable transport dead-letters
// instead of replaying it forever.
func TestTaskRoutingRefusesEngineOwnedTasks(t *testing.T) {
	ctx := context.Background()
	eng, _, _ := subWorkflowSchedulingEngine(t)

	for _, taskType := range []TaskType{TaskTypeSubWorkflow, TaskTypeNodeAdvance, TaskTypeNodeSkip} {
		_, err := eng.TaskRouting(ctx, &Task{
			ExecutionID: "exec-1",
			NodeName:    "sub",
			Type:        taskType,
		})
		if err == nil {
			t.Fatalf("TaskRouting(type %d) was accepted; no runner can run it", taskType)
		}
		if !errors.Is(err, types.ErrPermanent) {
			t.Fatalf("TaskRouting(type %d) error = %v, want a permanent refusal", taskType, err)
		}
	}
}

// TestScheduleRetryKeepsSubWorkflowTaskType pins the retry half: the retry of a
// sub-workflow node re-enters the engine-owned path, not the runner path.
func TestScheduleRetryKeepsSubWorkflowTaskType(t *testing.T) {
	ctx := context.Background()
	eng, state, queue := subWorkflowSchedulingEngine(t)

	task := &Task{ExecutionID: "exec-retry", NodeName: "sub", NodeIdx: 0, Type: TaskTypeSubWorkflow, ActivationID: 1}
	leaseID, token := newLeaseCredentials()
	if _, acquired, err := state.AcquireTaskLease(ctx, &TaskLease{
		LeaseID: leaseID, LeaseToken: token, Task: *task,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease: acquired=%v err=%v", acquired, err)
	}

	scheduled, err := eng.scheduleRetry(ctx, task, 1, &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1}, token)
	if err != nil || !scheduled {
		t.Fatalf("scheduleRetry: scheduled=%v err=%v", scheduled, err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued %d retry tasks, want 1", len(tasks))
	}
	if tasks[0].Type != TaskTypeSubWorkflow {
		t.Fatalf("retry task type = %d, want TaskTypeSubWorkflow", tasks[0].Type)
	}
}
