package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// echoBodyExecutor stands in for the real body sub-graph execution: it returns
// the injected roots verbatim, one result per item. It replaces a real inner
// execution so these tests can assert the BATCH-to-ITEM relationship without
// re-testing sub-graph execution itself (which execution/subgraph covers).
type echoBodyExecutor struct {
	mu sync.Mutex
	// seen records every request in arrival order, so a test can assert what
	// the engine actually passed rather than only what came back.
	seen []BatchBodyRequest
	// failAt makes the item at this GLOBAL index fail. -1 disables.
	failAt int
}

func newEchoBodyExecutor() *echoBodyExecutor { return &echoBodyExecutor{failAt: -1} }

func (x *echoBodyExecutor) ExecuteBatchBody(_ context.Context, req BatchBodyRequest) ([]BatchItemResult, error) {
	x.mu.Lock()
	x.seen = append(x.seen, req)
	x.mu.Unlock()

	out := make([]BatchItemResult, 0, len(req.Items))
	for i, item := range req.Items {
		idx := globalItemIndex(req.BatchIndex, req.BatchSize, i)
		if idx == x.failAt {
			out = append(out, BatchItemResult{Index: idx, Err: errors.New("body blew up")})
			if !req.ContinueOnError {
				// Stop the batch's remaining items. Already-executed items keep
				// their side effects; there is no rollback.
				break
			}
			continue
		}
		out = append(out, BatchItemResult{Index: idx, Data: map[string]any{
			"id":    item,
			"index": idx,
		}})
	}
	return out, nil
}

func (x *echoBodyExecutor) requests() []BatchBodyRequest {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]BatchBodyRequest(nil), x.seen...)
}

// sizedMapHandler produces the map node's fan-out description. batchSize comes
// from the constructor and items are fixed at 10: only the batching changes, the
// input set is identical.
type sizedMapHandler struct{ batchSize int }

func (h *sizedMapHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *sizedMapHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	items := make([]any, 10)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	var batches [][]any
	for i := 0; i < len(items); i += h.batchSize {
		end := i + h.batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return &types.Output{Data: map[string]any{
		"items":       items,
		"batches":     batches,
		"batch_size":  h.batchSize,
		"total":       len(items),
		"batch_count": len(batches),
	}}, nil
}

// mapBodyParamsForTest is the minimum body a map node needs to be runnable. What
// actually runs it in these tests is echoBodyExecutor, so the body's contents
// only have to compile.
func mapBodyParamsForTest() map[string]any {
	return map[string]any{
		"items": "$input.items",
		"body": map[string]any{
			"type": "xflow.subgraph",
			"parameters": map[string]any{
				"nodes": []any{
					map[string]any{"name": "echo", "type": "test.echo"},
				},
			},
		},
	}
}

// mapNodeWithEchoBody builds the map node these tests expand. The body is the
// minimum shape the compiler accepts; what runs it here is echoBodyExecutor, not
// a real sub-graph execution, so the body's contents only have to be valid.
func mapNodeWithEchoBody(batchSize int) types.NodeDef {
	return types.NodeDef{Name: "m", Type: "xflow.map", Parameters: map[string]any{
		"items":      "$input.items",
		"batch_size": batchSize,
		"body": map[string]any{
			"type": "xflow.subgraph",
			"parameters": map[string]any{
				"nodes": []any{
					map[string]any{"name": "echo", "type": "test.echo"},
				},
			},
		},
	}}
}

// runMapWorkflow drives a 10-item map node to completion and returns the map
// node's final output. Shape follows TestScheduler_LoopExpansion_CreatesSubExecutions:
// submit → drain root → executeTask → drain batches → ExecuteBatch each.
func runMapWorkflow(t *testing.T, batchSize int, exec BatchBodyExecutor) map[string]any {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "map-shape",
		Nodes: []types.NodeDef{
			mapNodeWithEchoBody(batchSize),
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

	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &sizedMapHandler{batchSize: batchSize},
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, WithBatchBodyExecutor(exec))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	batchTasks := queue.Drain()
	wantBatches := (10 + batchSize - 1) / batchSize
	if len(batchTasks) != wantBatches {
		t.Fatalf("batch_size=%d: got %d batch tasks, want %d", batchSize, len(batchTasks), wantBatches)
	}
	for _, bt := range batchTasks {
		if err := eng.ExecuteBatch(ctx, bt); err != nil {
			t.Fatalf("ExecuteBatch: %v", err)
		}
	}

	out, err := state.GetOutput(ctx, execID, "m")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	if out == nil {
		t.Fatal("map node produced no output after all batches completed")
	}
	return out
}

// A batch is a durability unit, an item is an execution unit. The same items run
// under different batch_size values must give downstream identical results and
// count — otherwise batch_size stops being a durability policy and becomes a
// semantic parameter.
func TestExecuteBatchBatchSizeDoesNotChangeDownstreamShape(t *testing.T) {
	for _, size := range []int{1, 3, 10} {
		out := runMapWorkflow(t, size, newEchoBodyExecutor())

		if count, _ := out["count"].(int); count != 10 {
			t.Errorf("batch_size=%d: count = %v, want 10 (items, not batches)", size, out["count"])
		}
		results, _ := out["results"].([]any)
		if len(results) != 10 {
			t.Errorf("batch_size=%d: results length = %d, want 10 (flattened per item)", size, len(results))
		}
	}
}

// $index is the item's global position, not its position within its batch. With
// batch_size=3 the 4th item (batch 2, position 0) must carry index 3, not 0.
func TestExecuteBatchIndexIsGlobalNotPerBatch(t *testing.T) {
	out := runMapWorkflow(t, 3, newEchoBodyExecutor())
	results, _ := out["results"].([]any)
	if len(results) != 10 {
		t.Fatalf("results length = %d, want 10", len(results))
	}
	for i, r := range results {
		row, _ := r.(map[string]any)
		got, _ := row["index"].(int)
		if got != i {
			t.Errorf("results[%d] carries index=%v, want %d; a per-batch index would restart at 0 every 3 items",
				i, row["index"], i)
		}
	}
}

// The engine, not the executor, knows the map node's batch_size and items: they
// live in the node's parameters and output, which the batch task's payload does
// not carry. If the engine passed batch_size=0 the executor could not compute a
// global index at all, and $items would be missing.
func TestExecuteBatchPassesTheMapNodesBatchingContextToTheBody(t *testing.T) {
	exec := newEchoBodyExecutor()
	runMapWorkflow(t, 3, exec)

	reqs := exec.requests()
	if len(reqs) != 4 {
		t.Fatalf("body executor saw %d batches, want 4 (10 items at batch_size=3)", len(reqs))
	}
	for _, req := range reqs {
		if req.BatchSize != 3 {
			t.Errorf("batch %d carried BatchSize=%d, want 3: without it the executor cannot compute a global index",
				req.BatchIndex, req.BatchSize)
		}
		if len(req.AllItems) != 10 {
			t.Errorf("batch %d carried %d AllItems, want all 10 for $items", req.BatchIndex, len(req.AllItems))
		}
		if req.ParentNode != "m" {
			t.Errorf("batch %d carried ParentNode=%q, want \"m\"", req.BatchIndex, req.ParentNode)
		}
	}
}

// expandOneBatch submits a single-node map workflow, runs the map node, and
// returns the engine plus the one batch task it expanded to. Used by the two
// misconfiguration tests, which care about what ExecuteBatch does rather than
// about the expansion.
func expandOneBatch(t *testing.T, nd types.NodeDef, opts ...Option) (*Engine, *Task) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{Name: "map-one-batch", Nodes: []types.NodeDef{nd}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return expandOneBatchOf(t, g, opts...)
}

// expandOneBatchOf is expandOneBatch for a graph the caller compiled itself —
// the only way to reach the runtime with a shape graph.Compile rejects.
func expandOneBatchOf(t *testing.T, g *graph.Graph, opts ...Option) (*Engine, *Task) {
	t.Helper()
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &sizedMapHandler{batchSize: 10},
	}}
	eng := New(state, queue, opts...)
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })

	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	executeTask(t, eng, roots[0])
	batches := queue.Drain()
	if len(batches) != 1 {
		t.Fatalf("batch tasks = %d, want 1", len(batches))
	}
	return eng, batches[0]
}

// An engine that executes batches in process but was assembled without a body
// executor must say so. Silently reporting the batch's items back — what the
// pass-through stub did — makes a map node with a real body look like it ran
// while nothing in the body ever executed.
func TestExecuteBatchWithoutABodyExecutorFailsInsteadOfPassingItemsThrough(t *testing.T) {
	eng, batch := expandOneBatch(t, mapNodeWithEchoBody(10)) // no BatchBodyExecutor

	err := eng.ExecuteBatch(context.Background(), batch)
	if err == nil {
		t.Fatal("ExecuteBatch succeeded without a body executor, so a map node with a real body would report success having run nothing")
	}
	if !errors.Is(err, ErrNoBatchBodyExecutor) {
		t.Errorf("ExecuteBatch error = %v, want ErrNoBatchBodyExecutor", err)
	}
}

// 无 body 无 expression 的 map 现在**两条**编译路径都拒绝，包括受信的那条。
//
// 这条测试守的缺陷仍是本仓库真实出现过的那个：compileTrusted（投影出的 group
// 包走的路径）是 Compile 的 pass 列表的手写平行实现，已经漂移过一次——它曾不跑
// projectNodeBodies，于是成员 map 编译干净通过但 Body == nil。变的是拦截点。
//
// 扩展判据下沉到 `BodyAt != nil` 之前，那次漂移在运行期以 ErrNoMapBody 响亮
// 失败（每批次一次）。判据下沉后它不会再失败了：没有 body 就不扩展，批次根本
// 不产生，handler 发的扇出描述符会被当成节点的普通输出提交下去，body 跑零次
// 且无任何诊断——比原先的失败更糟。所以守卫必须上移到编译期，且必须落在
// projectNodeBodies 这个**两条路径共用**的 pass 上（assertFanOutNodesResolved），
// 而不是只在 Compile 侧的 validateNodeBody 里。
func TestTheTrustedCompilePathAlsoRejectsAFanOutNodeWithNoBody(t *testing.T) {
	_, err := graph.CompileProjectedPackage(&graph.SubgraphPackage{
		Def: &types.WorkflowDef{
			Name:  "map-one-batch",
			Nodes: []types.NodeDef{{Name: "m", Type: "xflow.map"}},
		},
	})
	if err == nil {
		t.Fatal("the trusted path compiled a map node with neither a body nor an expression; " +
			"at run time it does not fail — it commits the handler's fan-out descriptor as " +
			"the node's output and never runs a body")
	}
	if !strings.Contains(err.Error(), `"m"`) || !strings.Contains(err.Error(), "xflow.map") {
		t.Errorf("rejection %q does not name both the offending node and its type", err)
	}
}
