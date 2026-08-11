package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// markerEchoHandler 是一个普通节点，它的输出里恰好带着扇出描述符长得一样的键。
// 这不是假想：`_loop` / `batches` / `items` 都是任何 handler 都能自由产出的
// 普通字段名，而扇出与否是**图的结构性质**，不是某个 payload 键的性质。
type markerEchoHandler struct{}

func (h *markerEchoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.echo"}
}

func (h *markerEchoHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"_loop":   true,
		"_split":  true,
		"batches": []any{[]any{"a"}, []any{"b"}},
		"items":   []any{"a", "b"},
	}}, nil
}

// bodylessMapHandler 是 body 形态 map 的 handler，但它**不发任何标记键**。
// 扇出判据下沉到编译期之后，标记键就没有存在理由了：引擎从
// `g.BodyAt(nodeIdx) != nil` 就能知道这个节点要扩展。
type bodylessMapHandler struct{}

func (h *bodylessMapHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *bodylessMapHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"items":       []any{"a", "b"},
		"batches":     []any{[]any{"a"}, []any{"b"}},
		"batch_size":  1,
		"total":       2,
		"batch_count": 2,
	}}, nil
}

// 一个不声明 body 的节点，无论它的输出里有什么键，都不得触发扇出。
//
// 修复前的判据嗅 payload 的 `_loop`/`_split` 键，于是任何 handler 只要在输出里
// 用了这两个名字，就能把自己变成扇出节点——而它没有 body，扩展出的批次
// 撞 `ErrNoMapBody`，整条执行以 deadline 挂死。判据是「这个节点在编译期被投影了
// 子图 body 吗」，payload 无权回答这个问题。
func TestANodeWithoutABodyDoesNotExpandNoMatterWhatItsOutputContains(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "marker-in-ordinary-output",
		Nodes: []types.NodeDef{
			{Name: "n", Type: "test.echo"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"n": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, _ := g.NodeIndex("n")
	if g.BodyAt(idx) != nil {
		t.Fatal("premise broken: a test.echo node was projected a body")
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.echo": &markerEchoHandler{},
	}}
	eng := New(state, queue)
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "n" {
		t.Fatalf("root tasks = %v, want one \"n\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	next := queue.Drain()
	for _, task := range next {
		if task.Type == TaskTypeNodeBatch {
			t.Fatalf("node %q expanded into batch task %q: the criterion read the payload "+
				"instead of the graph, so any handler can turn itself into a fan-out node "+
				"by naming a field \"_loop\"", "n", task.NodeName)
		}
	}
	if len(next) != 1 || next[0].NodeName != "done" {
		t.Fatalf("downstream tasks = %v, want one \"done\"", taskNames(next))
	}

	out, err := state.GetOutput(ctx, execID, "n")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	if out == nil {
		t.Fatal("node produced no committed output; it was treated as a fan-out parent")
	}
	if _, ok := out["_loop"]; !ok {
		t.Errorf("committed output = %v, want the handler's data verbatim", out)
	}
}

// expression 形态的 map 不得扩展——判据不是节点类型，是有没有投影出 body。
//
// 这条与上一条守的不是同一件事：上一条的节点类型压根不在 fanOutNodeTypes 里，
// 而这条的节点**就是 xflow.map**，只是走的 expression 形态（没有 body 可投影，
// 逐项求值在 handler 里就地做完）。把判据写成 `Type == "xflow.map"` 能让上一条
// 全绿，却会在这里把已经算完的结果当成扇出描述符去扩展——而没有 body 可跑。
//
// handler 用的是 bodylessMapHandler（发扇出描述符的那个），不是生产 handler：
// 生产的 expression 分支本来就不发描述符，用它测不出「引擎有没有去扩展」，
// 只能测出「handler 没给它东西扩展」。要分辨这两者，得让 handler 把描述符递上去。
func TestAMapInExpressionFormDoesNotExpand(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "expression-form-map",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items":      "$input.items",
				"expression": "$item",
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, _ := g.NodeIndex("m")
	if g.BodyAt(idx) != nil {
		t.Fatal("premise broken: the expression form projected a body")
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &bodylessMapHandler{},
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	next := queue.Drain()
	for _, task := range next {
		if task.Type == TaskTypeNodeBatch {
			t.Fatalf("the expression form expanded into batch task %q: it has no body to run, "+
				"so every batch would fail — the criterion read the node's type instead of "+
				"whether a body was projected", task.NodeName)
		}
	}
	if len(next) != 1 || next[0].NodeName != "done" {
		t.Fatalf("downstream tasks = %v, want one \"done\"", taskNames(next))
	}
}

// 失败的 map 节点走的是普通的错误路径：重试预算跑满、节点终态是 failed。
//
// 判据从「嗅 payload」换成「读图」时，这条性质是白捡又白丢的：失败的任务没有
// 输出可嗅，所以旧判据天然把失败排除在外；而 `BodyAt != nil` 只描述节点，与本次
// 执行成功与否无关，要靠 taskResultExpands 显式收窄回来。
//
// 诚实交代这条测试守什么：实测把收窄摘掉，全仓一条不红，本测试也不红。失败此时
// 改走 commitLegacyTaskResult，而它的错误分支跑同一套重试与 OnError，提交时又在
// `!AllowCycles` 处折回 commitAcyclicNode——两条路在失败上是收敛的。所以这条守的
// 是**失败路径本身的语义**（重试预算 + 终态 failed），不是收窄这一行代码；一旦
// legacy 与 acyclic 两条路在失败处理上分叉，它才会开始承重。
func TestAFailingMapKeepsTheOrdinaryErrorPathInsteadOfExpanding(t *testing.T) {
	params := mapBodyParamsForTest()
	def := &types.WorkflowDef{
		Name: "failing-map",
		Settings: &types.WorkflowSettings{
			Retry: &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1},
		},
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: params},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, _ := g.NodeIndex("m")
	if g.BodyAt(idx) == nil {
		t.Fatal("premise broken: without a projected body this node never reaches the " +
			"expansion decision at all, so the test would pass for the wrong reason")
	}

	state := newFakeState()
	queue := &fakeQueue{}
	handler := &alwaysFailHandler{err: errors.New("transient")}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": handler}}
	// 装上 body executor：少了它，一次误扩展会以 ErrNoBatchBodyExecutor 失败，
	// 那不是本测试要区分的东西——有它才能问出「误扩展会不会静静跑完批次」。
	eng := New(state, queue, WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	runFakeTasksWithOutbox(t, eng, queue, state, id, 10)

	if handler.calls != 3 {
		t.Fatalf("handler calls = %d, want 3 (MaxAttempts): the retry budget did not apply "+
			"to a failing map node", handler.calls)
	}
	node, err := state.GetNode(ctx, id, "m")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	// 默认 OnError（fail）下终态是 failed。用 error_output 的话终态是 success
	// （沿 error 端口走），断言会红在与本判据无关的地方。
	if node.Status != types.NodeStatusFailed {
		t.Errorf("node status = %q, want failed", node.Status)
	}
}

// 反过来：真正声明了 body 的 map 必须扩展，哪怕它的输出里一个标记键都没有。
//
// 没有这条，一个恒 false 的判据能让上面那条全绿。
func TestAMapWithAProjectedBodyExpandsWithoutAnyMarkerKey(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "body-form-map",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: mapBodyParamsForTest()},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, _ := g.NodeIndex("m")
	if g.BodyAt(idx) == nil {
		t.Fatal("premise broken: the map node declares a body but none was projected")
	}

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &bodylessMapHandler{},
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	batches := queue.Drain()
	if len(batches) != 2 {
		t.Fatalf("batch tasks = %v, want 2: a map declaring a body must expand from the "+
			"graph alone, without the handler having to stamp a marker key", taskNames(batches))
	}
	for _, bt := range batches {
		if bt.Type != TaskTypeNodeBatch {
			t.Errorf("task %q type = %v, want TaskTypeNodeBatch", bt.NodeName, bt.Type)
		}
	}
}
