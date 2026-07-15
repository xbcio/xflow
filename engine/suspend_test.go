package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// fakeSuspendHandler implements SuspendingHandler — waits for "approval" signal.
type fakeSuspendHandler struct{}

func (h *fakeSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.suspend"}
}

func (h *fakeSuspendHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *fakeSuspendHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{"approval"},
	}, nil
}

func (h *fakeSuspendHandler) OnResume(_ context.Context, _ *types.Input, sig *types.SignalPayload) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"approved": true, "signal": sig.Name}}, nil
}

type fakeMultiSignalHandler struct{}

func (h *fakeMultiSignalHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.multi_signal"}
}

func (h *fakeMultiSignalHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *fakeMultiSignalHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{
		Mode:    types.ModeMultiSignal,
		Signals: []string{"sec", "app", "ops"},
		Quorum:  2,
	}, nil
}

func (h *fakeMultiSignalHandler) OnResume(_ context.Context, _ *types.Input, sig *types.SignalPayload) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"trigger": sig.Name,
		"count":   len(sig.All),
		"all":     sig.All,
	}}, nil
}

func TestSuspend_SignalAfterSuspend(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "suspend-test",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "wait", Type: "test.suspend"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "wait", Input: "main"}}},
			"wait":  {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":    &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute start → enqueues wait.
	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	// Execute wait → suspends (no signal pre-delivered).
	tasks = queue.Drain()
	executeTask(t, eng, tasks[0])

	// No new tasks — node is parked.
	tasks = queue.Drain()
	if len(tasks) != 0 {
		t.Fatalf("expected no tasks after suspend, got %v", taskNames(tasks))
	}

	// Verify node status is suspended.
	ns, _ := state.GetNode(ctx, id, "wait")
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		t.Fatalf("wait node should be suspended, got %v", ns)
	}

	// Deliver signal → should enqueue a resume task for "wait".
	if err := eng.DeliverSignal(ctx, id, "approval", map[string]any{"by": "manager"}); err != nil {
		t.Fatal(err)
	}
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "wait" || tasks[0].Type != TaskTypeNodeResume {
		t.Fatalf("expected resume task for wait, got %v", tasks)
	}

	// Execute resume → wait completes and enqueues end.
	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "end" {
		t.Fatalf("expected [end], got %v", taskNames(tasks))
	}

	// Execute end → execution completes.
	executeTask(t, eng, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusSuccess {
		t.Errorf("expected success, got %s", snap.Status)
	}
}

func TestSuspend_SignalBeforeSuspend(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "pre-signal",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"wait": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":    &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Pre-deliver the signal before the node executes.
	state.mu.Lock()
	state.signals[string(id)+"/approval"] = map[string]any{"by": "early"}
	state.mu.Unlock()

	// Execute wait → SuspendOrConsume finds the pre-delivered signal and enqueues a resume task.
	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "wait" || tasks[0].Type != TaskTypeNodeResume {
		t.Fatalf("expected resume task for wait, got %v", tasks)
	}

	// Execute resume → end should be enqueued.
	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "end" {
		t.Fatalf("expected [end], got %v", taskNames(tasks))
	}

	// Node should not be in suspended state.
	ns, _ := state.GetNode(ctx, id, "wait")
	if ns != nil && ns.Status == types.NodeStatusSuspended {
		t.Error("wait node should not be suspended when signal was pre-delivered")
	}
}

func TestSuspend_Cancel(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "cancel-test",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.suspend"},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute wait → suspends.
	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	// Cancel the execution.
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}

	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.ExecutionStatusCanceled {
		t.Errorf("expected canceled, got %s", snap.Status)
	}

	// Delivering a signal after cancel should not enqueue anything
	// (graph is removed from cache).
	if err := eng.DeliverSignal(ctx, id, "approval", nil); err != nil {
		t.Fatal(err)
	}
	tasks = queue.Drain()
	if len(tasks) != 0 {
		t.Errorf("expected no tasks after cancel, got %v", taskNames(tasks))
	}
}

func TestSuspend_MultiSignalWaitsForQuorumAndPassesAllSignals(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "multi-signal",
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "test.multi_signal"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"wait": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}

	g, _ := graph.Compile(def)
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":         &echoHandler{},
		"test.multi_signal": &fakeMultiSignalHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	executeTask(t, eng, tasks[0])

	if err := eng.DeliverSignal(ctx, id, "sec", map[string]any{"by": "sec"}); err != nil {
		t.Fatal(err)
	}
	if tasks = queue.Drain(); len(tasks) != 0 {
		t.Fatalf("first signal should not resume before quorum, got %v", tasks)
	}

	if err := eng.DeliverSignal(ctx, id, "app", map[string]any{"by": "app"}); err != nil {
		t.Fatal(err)
	}
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "wait" || tasks[0].Payload == nil {
		t.Fatalf("second signal should resume wait, got %v", tasks)
	}
	if got := len(tasks[0].Payload.All); got != 2 {
		t.Fatalf("resume payload signal count = %d, want 2", got)
	}

	executeTask(t, eng, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "end" {
		t.Fatalf("expected [end], got %v", taskNames(tasks))
	}

	executeTask(t, eng, tasks[0])
	out, err := state.GetOutput(ctx, id, "wait")
	if err != nil {
		t.Fatal(err)
	}
	if out["count"] != 2 {
		t.Fatalf("wait output count = %v, want 2", out["count"])
	}
}

type resumeBaseSuspendHandler struct{}

func (h *resumeBaseSuspendHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.resume_base"}
}

func (h *resumeBaseSuspendHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *resumeBaseSuspendHandler) PrepareSuspend(context.Context, *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}, nil
}

func (h *resumeBaseSuspendHandler) OnResume(_ context.Context, input *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	data := cloneMap(input.Data)
	data["approved"] = true
	return &types.Output{Data: data}, nil
}

func TestSuspendPersistsInitialInputAsResumeBase(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "suspend-resume-base",
		Nodes: []types.NodeDef{{Name: "wait", Type: "test.resume_base"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.resume_base": &resumeBaseSuspendHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{
		"vuln_id":  "VULN-2026-001",
		"severity": "critical",
	})
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("initial task count = %d, want 1", len(tasks))
	}
	executeTask(t, eng, tasks[0])

	base, err := state.GetOutput(ctx, id, "wait")
	if err != nil {
		t.Fatalf("GetOutput() after initial suspend error = %v", err)
	}
	if base["vuln_id"] != "VULN-2026-001" || base["severity"] != "critical" {
		t.Fatalf("persisted resume base = %v, want original workflow input", base)
	}

	if err := eng.DeliverSignal(ctx, id, "approval", map[string]any{"action": "approve"}); err != nil {
		t.Fatal(err)
	}
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].Type != TaskTypeNodeResume {
		t.Fatalf("resume tasks = %v, want one resume task", tasks)
	}
	executeTask(t, eng, tasks[0])

	out, err := state.GetOutput(ctx, id, "wait")
	if err != nil {
		t.Fatalf("GetOutput() after resume error = %v", err)
	}
	if out["vuln_id"] != "VULN-2026-001" || out["severity"] != "critical" || out["approved"] != true {
		t.Fatalf("resume output = %v, want preserved original input plus decision", out)
	}
}
