package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// portHandler returns output on a specific port.
type portHandler struct {
	port string
	data map[string]any
}

func (h *portHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.port"}
}

func (h *portHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: h.data, Port: h.port}, nil
}

func TestScheduler_MergeWaitAny_TriggersOnFirstActive(t *testing.T) {
	// Graph: A --main--> merge <--main-- B
	// merge is wait_any, so it should trigger when A completes (before B).
	def := &types.WorkflowDef{
		Name: "merge-wait-any",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.echo"},
			{Name: "B", Type: "test.echo"},
			{Name: "merge", Type: "xflow.merge", Parameters: map[string]any{"mode": "wait_any"}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"A":     {"main": []types.Connection{{Node: "merge", Input: "main"}}},
			"B":     {"main": []types.Connection{{Node: "merge", Input: "main"}}},
			"merge": {"main": []types.Connection{{Node: "done", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	// Verify merge mode was extracted.
	mergeIdx := g.Index["merge"]
	if g.Nodes[mergeIdx].MergeMode != "wait_any" {
		t.Fatalf("expected MergeMode=wait_any, got %q", g.Nodes[mergeIdx].MergeMode)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":   &echoHandler{},
		"xflow.merge": &echoHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Root nodes A and B are enqueued.
	tasks := queue.Drain()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 root tasks, got %d", len(tasks))
	}

	// Execute only A.
	for _, task := range tasks {
		if task.NodeName == "A" {
			executeTask(t, eng, task)
			break
		}
	}

	// Merge should be enqueued (wait_any triggers on first active).
	mergeQueued := queue.Drain()
	found := false
	for _, task := range mergeQueued {
		if task.NodeName == "merge" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected merge to be enqueued after first active input (wait_any)")
	}

	_ = id
}

func TestScheduler_MergeWaitAll_WaitsForAll(t *testing.T) {
	// Graph: A --main--> merge <--main-- B
	// merge is wait_all (default), so it should NOT trigger until both A and B complete.
	def := &types.WorkflowDef{
		Name: "merge-wait-all",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.echo"},
			{Name: "B", Type: "test.echo"},
			{Name: "merge", Type: "xflow.merge", Parameters: map[string]any{"mode": "wait_all"}},
		},
		Connections: types.Connections{
			"A": {"main": []types.Connection{{Node: "merge", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "merge", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo":   &echoHandler{},
		"xflow.merge": &echoHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	_, err = eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Execute only A.
	tasks := queue.Drain()
	for _, task := range tasks {
		if task.NodeName == "A" {
			executeTask(t, eng, task)
			break
		}
	}

	// Merge should NOT be enqueued yet.
	afterA := queue.Drain()
	for _, task := range afterA {
		if task.NodeName == "merge" {
			t.Fatal("merge should not be enqueued before all inputs arrive (wait_all)")
		}
	}

	// Execute B.
	for _, task := range tasks {
		if task.NodeName == "B" {
			executeTask(t, eng, task)
			break
		}
	}

	// Now merge should be enqueued.
	afterB := queue.Drain()
	found := false
	for _, task := range afterB {
		if task.NodeName == "merge" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected merge to be enqueued after all inputs arrive (wait_all)")
	}
}

// loopHandler simulates a loop node output.
type loopHandler struct{}

func (h *loopHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "xflow.loop"}
}

func (h *loopHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{
		Data: map[string]any{
			"_loop":       true,
			"items":       []any{"a", "b", "c"},
			"batches":     [][]any{{"a"}, {"b"}, {"c"}},
			"batch_size":  1,
			"total":       3,
			"batch_count": 3,
		},
	}, nil
}

func TestScheduler_LoopExpansion_CreatesSubExecutions(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "loop-test",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.loop"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {"main": []types.Connection{{Node: "done", Input: "main"}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.loop": &loopHandler{},
		"test.echo":  &echoHandler{},
	}}
	eng := newTestEngine(state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Execute root (loop node).
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "loop" {
		t.Fatalf("expected 1 root task 'loop', got %v", taskNames(tasks))
	}
	executeTask(t, eng, tasks[0])

	// Loop should have created 3 batch tasks.
	batchTasks := queue.Drain()
	if len(batchTasks) != 3 {
		t.Fatalf("expected 3 batch tasks, got %d", len(batchTasks))
	}

	// Execute all batch tasks.
	for _, bt := range batchTasks {
		if err := eng.ExecuteBatch(ctx, bt); err != nil {
			t.Fatalf("ExecuteBatch: %v", err)
		}
	}

	// After all batches complete, "done" should be enqueued.
	finalTasks := queue.Drain()
	found := false
	for _, task := range finalTasks {
		if task.NodeName == "done" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'done' to be enqueued after loop completes, got %v", taskNames(finalTasks))
	}

	_ = id
}
