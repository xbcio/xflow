package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// echoHandler returns its input data unchanged.
type echoHandler struct{}

func (h *echoHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	return &node.Output{Data: input.Data}, nil
}

// taskNames extracts node names from a task slice for readable assertions.
func taskNames(tasks []*Task) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.NodeName
	}
	return names
}

// runAll drains the queue and executes every task synchronously.
func runAll(t *testing.T, eng *Engine, queue *fakeQueue, state *fakeState, id types.ExecutionID, g *graph.Graph) {
	t.Helper()
	ctx := context.Background()
	for {
		tasks := queue.Drain()
		if len(tasks) == 0 {
			break
		}
		for _, task := range tasks {
			if err := eng.ExecuteNode(ctx, task); err != nil {
				t.Fatalf("ExecuteNode(%s): %v", task.NodeName, err)
			}
		}
	}
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{"test.echo": &echoHandler{}}}
	eng := New(state, queue, WithRegistry(reg))
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
	eng.ExecuteNode(ctx, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "B" {
		t.Fatalf("expected [B], got %v", taskNames(tasks))
	}

	// Execute B → should enqueue C.
	eng.ExecuteNode(ctx, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "C" {
		t.Fatalf("expected [C], got %v", taskNames(tasks))
	}

	// Execute C → execution completes.
	eng.ExecuteNode(ctx, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusSuccess {
		t.Errorf("expected success, got %s", snap.Status)
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{"test.echo": &echoHandler{}}}
	eng := New(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute start → left + right enqueued.
	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])
	tasks = queue.Drain()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks after start, got %d: %v", len(tasks), taskNames(tasks))
	}

	// Execute left → join not yet ready (still waiting for right).
	eng.ExecuteNode(ctx, tasks[0])
	partial := queue.Drain()
	if len(partial) != 0 {
		t.Fatalf("join should not be enqueued yet, got %v", taskNames(partial))
	}

	// Execute right → join now ready.
	eng.ExecuteNode(ctx, tasks[1])
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "join" {
		t.Fatalf("expected [join], got %v", taskNames(tasks))
	}

	// Execute join → execution completes.
	eng.ExecuteNode(ctx, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusSuccess {
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	// Execute check → routes to "main" port.
	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	// ok should be enqueued; fail should be skipped immediately.
	tasks = queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "ok" {
		t.Fatalf("expected [ok], got %v", taskNames(tasks))
	}

	ns, _ := state.GetNode(ctx, id, "fail")
	if ns == nil || ns.Status != "skipped" {
		t.Errorf("fail node should be skipped, got %v", ns)
	}

	// Execute ok → execution completes.
	eng.ExecuteNode(ctx, tasks[0])
	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusSuccess {
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
	reg := &fakeRegistry{handlers: map[string]node.TaskHandler{
		"test.fail": &failHandler{},
	}}
	eng := New(state, queue, WithRegistry(reg))
	ctx := context.Background()

	id, _ := eng.Submit(ctx, g, nil)
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	eng.ExecuteNode(ctx, tasks[0])

	snap, _ := state.GetExecution(ctx, id)
	if snap.Status != types.StatusFailed {
		t.Errorf("expected failed, got %s", snap.Status)
	}
}

// failHandler always returns an error.
type failHandler struct{}

func (h *failHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	return nil, fmt.Errorf("intentional failure")
}
