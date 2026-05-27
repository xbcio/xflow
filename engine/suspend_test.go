package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// fakeSuspendHandler implements SuspendingHandler — waits for "approval" signal.
type fakeSuspendHandler struct{}

func (h *fakeSuspendHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *fakeSuspendHandler) PrepareSuspend(_ context.Context, _ *node.Input) (*node.SuspendSpec, error) {
	return &node.SuspendSpec{
		Mode:    node.ModeSignal,
		Signals: []string{"approval"},
	}, nil
}

func (h *fakeSuspendHandler) OnResume(_ context.Context, _ *node.Input, sig *node.SignalPayload) (*node.Output, error) {
	return &node.Output{Data: map[string]any{"approved": true, "signal": sig.Name}}, nil
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{
		"test.echo":    &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := NewEngine(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute start → enqueues wait.
	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	// Execute wait → suspends (no signal pre-delivered).
	tasks = queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	// No new tasks — node is parked.
	tasks = queue.Drain()
	if len(tasks) != 0 {
		t.Fatalf("expected no tasks after suspend, got %v", taskNames(tasks))
	}

	// Verify node status is suspended.
	ns, _ := state.GetNode(ctx, id, "wait")
	if ns == nil || ns.Status != "suspended" {
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
	eng.ExecuteNode(ctx, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "end" {
		t.Fatalf("expected [end], got %v", taskNames(tasks))
	}

	// Execute end → execution completes.
	eng.ExecuteNode(ctx, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusSuccess {
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{
		"test.echo":    &echoHandler{},
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := NewEngine(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Pre-deliver the signal before the node executes.
	state.mu.Lock()
	state.signals[string(id)+"/approval"] = map[string]any{"by": "early"}
	state.mu.Unlock()

	// Execute wait → SuspendOrConsume finds the pre-delivered signal → immediate resume.
	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	// end should be enqueued (wait completed without parking).
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "end" {
		t.Fatalf("expected [end], got %v", taskNames(tasks))
	}

	// Node should not be in suspended state.
	ns, _ := state.GetNode(ctx, id, "wait")
	if ns != nil && ns.Status == "suspended" {
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{
		"test.suspend": &fakeSuspendHandler{},
	}}
	eng := NewEngine(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute wait → suspends.
	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	// Cancel the execution.
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}

	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusCanceled {
		t.Errorf("expected canceled, got %s", snap.Status)
	}

	// Delivering a signal after cancel should not enqueue anything
	// (graph is removed from cache).
	eng.DeliverSignal(ctx, id, "approval", nil)
	tasks = queue.Drain()
	if len(tasks) != 0 {
		t.Errorf("expected no tasks after cancel, got %v", taskNames(tasks))
	}
}
