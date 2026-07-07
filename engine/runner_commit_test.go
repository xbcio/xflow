package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

type recordingHooks struct {
	started []string
}

func (h *recordingHooks) OnNodeStart(_ context.Context, _ types.ExecutionID, name string) {
	h.started = append(h.started, name)
}

func (h *recordingHooks) OnNodeComplete(context.Context, types.ExecutionID, string, types.NodeStatus) {
}
func (h *recordingHooks) OnNodeSuspended(context.Context, types.ExecutionID, string) {}
func (h *recordingHooks) OnExecutionComplete(context.Context, types.ExecutionID, types.ExecutionStatus) {
}
func (h *recordingHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *recordingHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}
func (h *recordingHooks) OnNodeTimeout(context.Context, types.ExecutionID, string)   {}
func (h *recordingHooks) OnNodeRetry(context.Context, types.ExecutionID, string, int, time.Duration) {
}

func TestEngine_BuildTaskLeaseAndCommitTaskResult_RunnerStyleFlow(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-style",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "next", Input: "main"}}},
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

	id, err := eng.Submit(ctx, g, map[string]any{"claim_id": "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("expected start task, got %v", taskNames(tasks))
	}

	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	input := lease.Input
	if input.Data["claim_id"] != "c-1" {
		t.Fatalf("expected root input params, got %v", input.Data)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"parsed": true}},
	})
	if err != nil {
		t.Fatal(err)
	}

	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "next" {
		t.Fatalf("expected next task after commit, got %v", taskNames(tasks))
	}

	lease, err = eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}
	input = lease.Input
	if input.Data["parsed"] != true {
		t.Fatalf("expected upstream output as input, got %v", input.Data)
	}
}

func TestEngine_BuildTaskLeaseIncludesRunnerRoutingMetadata(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-lease",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", Version: 2},
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

	id, err := eng.Submit(ctx, g, map[string]any{"claim_id": "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]

	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ExecutionID != id || lease.Task.NodeName != "start" {
		t.Fatalf("unexpected lease task: %+v", lease.Task)
	}
	if lease.NodeType != "test.echo" || lease.NodeVersion != 2 {
		t.Fatalf("unexpected routing metadata: type=%q version=%d", lease.NodeType, lease.NodeVersion)
	}
	if lease.Input.Data["claim_id"] != "c-1" {
		t.Fatalf("expected root params in lease input, got %v", lease.Input.Data)
	}
}

func TestEngine_TaskRoutingIncludesEffectiveRunnerSelector(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-selector-routing",
		RunnerSelector: &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeDefault,
			MatchLabels: map[string]string{"mode": "remote", "env": "prod"},
		},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{
				Name: "approve",
				Type: "xflow.function",
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{"mode": "local"},
				},
			},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "approve", Input: "main"}}},
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
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(tasks))
	}
	routing, err := eng.TaskRouting(ctx, tasks[0])
	if err != nil {
		t.Fatalf("TaskRouting(start) error = %v", err)
	}
	if got := routing.RunnerSelector.MatchLabels["mode"]; got != "remote" {
		t.Fatalf("start selector mode = %q, want remote", got)
	}

	startLease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, TaskResult{Output: &types.Output{Data: map[string]any{}}}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	tasks = queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued tasks after start = %d, want 1", len(tasks))
	}
	routing, err = eng.TaskRouting(ctx, tasks[0])
	if err != nil {
		t.Fatalf("TaskRouting(approve) error = %v", err)
	}
	if got := routing.RunnerSelector.MatchLabels["mode"]; got != "local" {
		t.Fatalf("approve selector mode = %q, want local override", got)
	}
	if _, ok := routing.RunnerSelector.MatchLabels["env"]; ok {
		t.Fatalf("approve selector inherited env in default mode: %+v", routing.RunnerSelector.MatchLabels)
	}
}

func TestEngine_BuildTaskLeaseMarksNodeRunningAndFiresStartHook(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runner-start",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	hooks := &recordingHooks{}
	eng := New(state, queue, WithHooks(hooks))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]

	if _, err := eng.BuildTaskLease(ctx, task); err != nil {
		t.Fatal(err)
	}

	snap, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.Status != types.NodeStatusRunning {
		t.Fatalf("node status = %+v, want running", snap)
	}
	if len(hooks.started) != 1 || hooks.started[0] != "start" {
		t.Fatalf("started hooks = %v, want [start]", hooks.started)
	}
}

func TestEngine_CommitTaskResult_IgnoresDuplicateTerminalResult(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "duplicate-result",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "next", Input: "main"}}},
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
	state.InitInDegrees(id, g)
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	result := TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}
	if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
		t.Fatal(err)
	}
	if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
		t.Fatal(err)
	}

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "next" {
		t.Fatalf("duplicate result should enqueue next once, got %v", taskNames(tasks))
	}
}

func TestEngine_CommitTaskResultRejectsStaleLeaseToken(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "lease-fencing",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
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
	task := queue.Drain()[0]

	first, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseToken == "" || second.LeaseToken == "" || first.LeaseToken == second.LeaseToken {
		t.Fatalf("expected distinct non-empty lease tokens, got first=%q second=%q", first.LeaseToken, second.LeaseToken)
	}

	err = eng.CommitTaskResult(ctx, first, TaskResult{Output: &types.Output{Data: map[string]any{"stale": true}}})
	if err == nil {
		t.Fatal("expected stale lease commit to fail")
	}
	if !errors.Is(err, ErrInvalidLeaseToken) {
		t.Fatalf("CommitTaskResult() error = %v, want ErrInvalidLeaseToken", err)
	}

	if err := eng.CommitTaskResult(ctx, second, TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}}); err != nil {
		t.Fatalf("fresh lease commit failed: %v", err)
	}

	out, err := state.GetOutput(ctx, task.ExecutionID, "start")
	if err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true || out["stale"] == true {
		t.Fatalf("output = %v, want fresh result only", out)
	}
}

func TestEngine_CommitTaskResultParksSuspendRequestWithoutHandlerExecution(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runtime-suspend",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	hooks := &recordingHooks{}
	eng := New(state, queue, WithHooks(hooks))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Suspend: &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ns, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		t.Fatalf("node state = %+v, want suspended", ns)
	}
	if tasks := queue.Drain(); len(tasks) != 0 {
		t.Fatalf("expected no queued tasks after parking, got %v", taskNames(tasks))
	}
}

func TestEngine_CommitTaskResultFailsSuspendWhenDisabled(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "runtime-suspend-disabled",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithSuspendDisabled(ErrSuspendUnsupported))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	err = eng.CommitTaskResult(ctx, lease, TaskResult{
		Suspend: &types.SuspendSpec{Mode: node.ModeSignal, Signals: []string{"approval"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ns, err := state.GetNode(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if ns == nil || ns.Status != types.NodeStatusFailed {
		t.Fatalf("node state = %+v, want failed", ns)
	}
	if ns.Error != ErrSuspendUnsupported.Error() {
		t.Fatalf("node error = %q, want %q", ns.Error, ErrSuspendUnsupported.Error())
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %q, want failed", snap.Status)
	}
}
