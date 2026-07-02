package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// echoHandler returns its input data unchanged.
type echoHandler struct{}

func (h *echoHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.echo"}
}

func (h *echoHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}

// taskNames extracts node names from a task slice for readable assertions.
func taskNames(tasks []*Task) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.NodeName
	}
	return names
}

var testRegistries sync.Map

func newTestEngine(state StateStore, queue TaskQueue, reg HandlerRegistry) *Engine {
	eng := New(state, queue)
	testRegistries.Store(eng, reg)
	return eng
}

// runAll drains the queue and executes every task synchronously.
func runAll(t *testing.T, eng *Engine, queue *fakeQueue, state *fakeState, id types.ExecutionID, g *graph.Graph) {
	t.Helper()
	for {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			executeTask(t, eng, task)
		}
	}
}

func executeTask(t *testing.T, eng *Engine, task *Task) {
	t.Helper()
	ctx := context.Background()

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease(%s): %v", task.NodeName, err)
	}
	registry, ok := testRegistries.Load(eng)
	if !ok {
		t.Fatalf("missing test registry for %s", task.NodeName)
	}
	handler, err := registry.(HandlerRegistry).Get(lease.Task.ExecutionID, lease.Task.NodeName, lease.NodeType, lease.NodeVersion)
	if err != nil {
		t.Fatalf("registry.Get(%s): %v", task.NodeName, err)
	}

	var result TaskResult
	if sh, ok := handler.(types.SuspendingHandler); ok {
		result = executeSuspendingTask(t, sh, lease)
	} else {
		output, execErr := handler.Execute(ctx, lease.Input)
		result = TaskResult{Output: output, Error: execErr}
	}

	if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
		t.Fatalf("CommitTaskResult(%s): %v", task.NodeName, err)
	}
}

func executeSuspendingTask(t *testing.T, sh types.SuspendingHandler, lease *TaskLease) TaskResult {
	t.Helper()
	ctx := context.Background()
	if lease.Task.Type == TaskTypeNodeResume {
		output, err := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
		if err != nil {
			return TaskResult{Output: output, Error: err}
		}
		if output != nil && output.Resuspend {
			input := lease.Input
			if output.Data != nil {
				cp := *lease.Input
				cp.Data = output.Data
				input = &cp
			}
			spec, err := sh.PrepareSuspend(ctx, input)
			if err != nil {
				return TaskResult{Output: output, Error: err}
			}
			return TaskResult{Output: output, Suspend: spec}
		}
		return TaskResult{Output: output}
	}

	spec, err := sh.PrepareSuspend(ctx, lease.Input)
	if err != nil {
		return TaskResult{Error: err}
	}
	return TaskResult{Suspend: spec}
}

func TestScheduler_LinearChain(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "linear",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.echo"},
			{Name: "B", Type: "test.echo"},
			{Name: "C", Type: "test.echo"},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "C", Input: "main"}}},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	// Only A should be enqueued (in-degree 0).
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "A" {
		t.Fatalf("expected [A], got %v", taskNames(tasks))
	}

	// Execute A → should enqueue B.
	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "B" {
		t.Fatalf("expected [B], got %v", taskNames(tasks))
	}

	// Execute B → should enqueue C.
	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "C" {
		t.Fatalf("expected [C], got %v", taskNames(tasks))
	}

	// Execute C → execution completes.
	executeTask(t, eng, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusSuccess {
		t.Errorf("expected success, got %s", snap.Status)
	}
}

func TestEngineBuildTaskLeaseKeepsStaticVarsAndRuntimeVarsSeparate(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runtime-context",
		Context: &types.WorkflowContext{
			Vars: map[string]any{
				"region":    "static-region",
				"tenant_id": "static-tenant",
			},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	_, err = eng.Submit(ctx, g, map[string]any{"ticket": "VULN-1"}, &types.Runtime{
		Vars: map[string]any{"tenant_id": "runtime-tenant"},
	})
	if err != nil {
		t.Fatal(err)
	}

	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	if got := lease.Input.Vars["region"]; got != "static-region" {
		t.Fatalf("Input.Vars[region] = %v, want static-region", got)
	}
	if got := lease.Input.Vars["tenant_id"]; got != "runtime-tenant" {
		t.Fatalf("Input.Vars[tenant_id] = %v, want runtime-tenant", got)
	}
	if lease.Input.Runtime == nil {
		t.Fatal("Input.Runtime = nil, want runtime context")
	}
	if got := lease.Input.Runtime.Vars["tenant_id"]; got != "runtime-tenant" {
		t.Fatalf("Input.Runtime.Vars[tenant_id] = %v, want runtime-tenant", got)
	}
	if got := lease.Input.Data["ticket"]; got != "VULN-1" {
		t.Fatalf("Input.Data[ticket] = %v, want VULN-1", got)
	}
}

func TestEngineBuildTaskLeaseIncludesSubmittedTraceMetadata(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "trace-context",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(state, queue, reg)
	ctx := WithSpanID(WithTraceID(context.Background(), "trace-123"), "span-456")

	_, err = eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}

	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	lease, err := eng.BuildTaskLease(context.Background(), tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if lease.Input.TraceID != "trace-123" {
		t.Fatalf("Input.TraceID = %q, want trace-123", lease.Input.TraceID)
	}
	if lease.Input.SpanID != "span-456" {
		t.Fatalf("Input.SpanID = %q, want span-456", lease.Input.SpanID)
	}
}

func TestInvokeStartsOnlyNamedEntryAndDoesNotBlockSharedDownstream(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "multi-trigger",
		Nodes: []types.NodeDef{
			{Name: "kafka", Type: "xflow.trigger.kafka", Kind: types.NodeKindTrigger},
			{Name: "webhook", Type: "xflow.trigger.webhook", Kind: types.NodeKindTrigger},
			{Name: "normalize", Type: "test.echo"},
		},
		Connections: types.Connections{
			"kafka":   {"main": []types.Connection{{Node: "normalize", Input: "main"}}},
			"webhook": {"main": []types.Connection{{Node: "normalize", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(state, queue, reg)

	id, err := eng.Invoke(context.Background(), g, "kafka", map[string]any{"id": "e1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(queue.tasks) != 1 || queue.tasks[0].NodeName != "kafka" {
		t.Fatalf("queued = %+v, want kafka only", queue.tasks)
	}
	if id == "" {
		t.Fatal("empty execution id")
	}
}

func TestScheduler_FanOutFanIn(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "left", Type: "test.echo"},
			{Name: "right", Type: "test.echo"},
			{Name: "join", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{
				{Node: "left", Input: "main"},
				{Node: "right", Input: "main"},
			}},
			"left":  {"main": []types.Connection{{Node: "join", Input: "main"}}},
			"right": {"main": []types.Connection{{Node: "join", Input: "main"}}},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute start → left + right enqueued.
	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks after start, got %d: %v", len(tasks), taskNames(tasks))
	}

	// Execute left → join not yet ready (still waiting for right).
	executeTask(t, eng, tasks[0])
	partial := queue.Drain()
	if len(partial) != 0 {
		t.Fatalf("join should not be enqueued yet, got %v", taskNames(partial))
	}

	// Execute right → join now ready.
	executeTask(t, eng, tasks[1])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "join" {
		t.Fatalf("expected [join], got %v", taskNames(tasks))
	}

	// Execute join → execution completes.
	executeTask(t, eng, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusSuccess {
		t.Errorf("expected success, got %s", snap.Status)
	}
}

func TestScheduler_SkipCascade(t *testing.T) {
	// check routes to "main" → ok executes, fail is skipped.
	def := &types.WorkflowDef{
		Name: "skip",
		Nodes: []types.NodeDef{
			{Name: "check", Type: "test.echo"},
			{Name: "ok", Type: "test.echo"},
			{Name: "fail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"check": {
				"main":  []types.Connection{{Node: "ok", Input: "main"}},
				"error": []types.Connection{{Node: "fail", Input: "main"}},
			},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo": &echoHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute check → routes to "main" port.
	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	// ok should be enqueued; fail should be skipped immediately.
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "ok" {
		t.Fatalf("expected [ok], got %v", taskNames(tasks))
	}

	ns, _ := state.GetNode(ctx, id, "fail")
	if ns == nil || ns.Status != types.NodeStatusSkipped {
		t.Errorf("fail node should be skipped, got %v", ns)
	}

	// Execute ok → execution completes.
	executeTask(t, eng, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusSuccess {
		t.Errorf("expected success, got %s", snap.Status)
	}
}

func TestScheduler_ErrorFatal(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "fatal",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.fail"}, // no OnError → defaults to stop
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.fail": &failHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusFailed {
		t.Errorf("expected failed, got %s", snap.Status)
	}
}

func TestScheduler_CyclicSubmitStartsAtStartNodeWithIncomingEdge(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cyclic-start",
		Options: &types.WorkflowOptions{AllowCycles: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	if _, err := eng.Submit(context.Background(), g, map[string]any{"ticket": "v-1"}); err != nil {
		t.Fatal(err)
	}

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("expected cyclic start task, got %v", taskNames(tasks))
	}
	if tasks[0].ActivationID != 1 || tasks[0].AutoDepth != 0 {
		t.Fatalf("task activation/depth = %d/%d, want 1/0", tasks[0].ActivationID, tasks[0].AutoDepth)
	}
}

func TestScheduler_CyclicReturnReentersTerminalNodeAndOverwritesLatestOutput(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "cyclic-return",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		t.Fatal(err)
	}
	startTask := queue.Drain()[0]
	startLease, err := eng.BuildTaskLease(ctx, startTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 1}}}); err != nil {
		t.Fatal(err)
	}

	reviewTask := queue.Drain()[0]
	reviewLease, err := eng.BuildTaskLease(ctx, reviewTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, reviewLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 2}, Port: "reject"}}); err != nil {
		t.Fatal(err)
	}

	reenteredStart := queue.Drain()[0]
	if reenteredStart.NodeName != "start" || reenteredStart.ActivationID <= startTask.ActivationID {
		t.Fatalf("reentered task = %+v, original activation %d", reenteredStart, startTask.ActivationID)
	}
	reenteredLease, err := eng.BuildTaskLease(ctx, reenteredStart)
	if err != nil {
		t.Fatal(err)
	}
	if reenteredLease.Input.Data["round"] != 2 {
		t.Fatalf("reentered input = %v, want latest review output", reenteredLease.Input.Data)
	}
	if err := eng.CommitTaskResult(ctx, reenteredLease, TaskResult{Output: &types.Output{Data: map[string]any{"round": 3}}}); err != nil {
		t.Fatal(err)
	}

	out, err := state.GetOutput(ctx, id, "start")
	if err != nil {
		t.Fatal(err)
	}
	if out["round"] != 3 {
		t.Fatalf("start output = %v, want latest reentry output", out)
	}
}

func TestScheduler_CyclicAutomaticDepthLimitFailsExecution(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "depth",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 1},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "loop", Type: "test.loop"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "loop", Input: "main"}}},
			"loop":  {"main": []types.Connection{{Node: "loop", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	startLease, err := eng.BuildTaskLease(ctx, queue.Drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"step": "start"}}}); err != nil {
		t.Fatal(err)
	}
	loopLease, err := eng.BuildTaskLease(ctx, queue.Drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, loopLease, TaskResult{Output: &types.Output{Data: map[string]any{"step": "loop"}}}); err != nil {
		t.Fatal(err)
	}

	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed", snap.Status)
	}
	if tasks := queue.Drain(); len(tasks) != 0 {
		t.Fatalf("expected no task after depth failure, got %v", taskNames(tasks))
	}
}

func TestScheduler_CyclicStaleActivationTaskCannotReacquireNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "stale-activation",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "step", Type: "test.step"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "step", Input: "main"}}},
			"step":  {"main": []types.Connection{{Node: "step", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatal(err)
	}
	startLease, err := eng.BuildTaskLease(ctx, queue.Drain()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{"step": 0}}}); err != nil {
		t.Fatal(err)
	}

	staleTask := queue.Drain()[0]
	firstLease, err := eng.BuildTaskLease(ctx, staleTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, firstLease, TaskResult{Output: &types.Output{Data: map[string]any{"step": 1}}}); err != nil {
		t.Fatal(err)
	}
	newerTask := queue.Drain()[0]
	newerLease, err := eng.BuildTaskLease(ctx, newerTask)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, newerLease, TaskResult{Output: &types.Output{Data: map[string]any{"step": 2}}}); err != nil {
		t.Fatal(err)
	}

	if _, err := eng.BuildTaskLease(ctx, staleTask); err == nil {
		t.Fatal("expected stale activation task to be rejected")
	}
}

// failHandler always returns an error.
type failHandler struct{}

func (h *failHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.fail"}
}

func (h *failHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, fmt.Errorf("intentional failure")
}
