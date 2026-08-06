package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// bodyItemHandler is the body sub-graph's only node. It records the $item and
// $index roots it received: the evidence that the body EXECUTED rather than the
// runtime reporting the batch's own items straight back.
type bodyItemHandler struct {
	mu    sync.Mutex
	items []any
	idx   []any
}

func (h *bodyItemHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *bodyItemHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.items = append(h.items, in.Data["$item"])
	h.idx = append(h.idx, in.Data["$index"])
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"seen": in.Data["$item"], "at": in.Data["$index"]}}, nil
}

func (h *bodyItemHandler) recorded() ([]any, []any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.items...), append([]any(nil), h.idx...)
}

// bodyPackageForTest builds the minimal one-node body package: the member plus
// its boundary collector.
func bodyPackageForTest() *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "m/body",
		EntryNode: "step",
		Def: &types.WorkflowDef{
			Name: "m/body",
			Nodes: []types.NodeDef{
				{Name: "step", Type: "test.body_item", Version: 1},
				{Name: "__collector_step_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"step": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_step_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_step_main", SrcNode: "step", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.body_item", NodeVersion: 1},
		},
	}
}

// batchBodyLease builds the lease a control plane hands a runner for ONE batch
// that carries a real body: batch 1 of a batch_size=2 expansion over 4 items.
func batchBodyLease(t *testing.T, pkg *graph.SubgraphPackage) *engine.TaskLease {
	t.Helper()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	all := []any{"a", "b", "c", "d"}
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-parent"),
		LeaseToken: engine.LeaseToken("token-parent"),
		Attempt:    1,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-1"),
			NodeName:    "m/_batch/1",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeBatch,
		},
		NodeType: "xflow.map",
		SubgraphPayload: &engine.SubgraphLeasePayload{
			ProtocolVersion: 1,
			ParentNode:      "m",
			ParentNodeIdx:   0,
			BatchIndex:      1,
			ChildExecID:     types.ExecutionID("exec-1/sub/m/lease-parent/1"),
			Items:           []any{"c", "d"},
			AllItems:        all,
			BatchSize:       2,
			Package:         pkg,
			PackageHash:     hash,
			Deadline:        time.Now().Add(10 * time.Second),
		},
	}
}

// The runner path must run the body, not report the batch's items back. A
// pass-through reaches the same "count" the real path does, so counting alone
// proves nothing: what distinguishes them is whether the body's node ran and
// what roots it saw.
func TestSubgraphRuntimeRunsTheBodyOncePerItem(t *testing.T) {
	reg := execution.NewRegistry()
	body := &bodyItemHandler{}
	reg.RegisterGlobal("test.body_item", body)
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}))

	result, err := rt.Execute(context.Background(), batchBodyLease(t, bodyPackageForTest()))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Error != nil {
		t.Fatalf("batch failed: %v", result.Error)
	}

	seen, at := body.recorded()
	if len(seen) != 2 {
		t.Fatalf("body ran %d time(s) with items %v, want 2 — one per item in this batch", len(seen), seen)
	}
	if seen[0] != "c" || seen[1] != "d" {
		t.Errorf("body saw items %v, want [c d] — this batch's own items, in order", seen)
	}
	// batch 1 at batch_size 2 holds global positions 2 and 3. A per-batch index
	// would restart at 0 here, which is exactly what must not happen.
	if at[0] != 2 || at[1] != 3 {
		t.Errorf("body saw $index %v, want [2 3]: a per-batch index would report [0 1]", at)
	}

	if result.Output == nil || result.Output.Data == nil {
		t.Fatalf("batch reported no output: %+v", result)
	}
	if count, _ := result.Output.Data["count"].(int); count != 2 {
		t.Errorf("batch count = %v, want 2", result.Output.Data["count"])
	}
	items, _ := result.Output.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("batch items = %v, want 2 per-item results", result.Output.Data["items"])
	}
	first, _ := items[0].(map[string]any)
	if first["seen"] != "c" {
		t.Errorf("results[0] = %v, want the body's output for item \"c\"; the batch's own "+
			"item echoed back would carry no \"seen\" key at all", items[0])
	}
}

// $items is the whole map node's array, not this batch's slice. A body that
// reads $items to correlate against its neighbours must see all 4.
func TestSubgraphRuntimeExposesTheWholeItemsArray(t *testing.T) {
	reg := execution.NewRegistry()
	var got []any
	var mu sync.Mutex
	reg.RegisterGlobal("test.body_item", &recordingItemsHandler{fn: func(in *types.Input) {
		mu.Lock()
		defer mu.Unlock()
		if all, ok := in.Data["$items"].([]any); ok && got == nil {
			got = all
		}
	}})
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}))

	if _, err := rt.Execute(context.Background(), batchBodyLease(t, bodyPackageForTest())); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Errorf("body saw $items = %v, want all 4 items; this batch holds only 2, so a "+
			"length of 2 means the batch's slice leaked in as $items", got)
	}
}

// recordingItemsHandler observes its input and echoes it.
type recordingItemsHandler struct{ fn func(*types.Input) }

func (h *recordingItemsHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *recordingItemsHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.fn(in)
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// A batch whose lease carries no body cannot run anything. Reporting its items
// back — what the pass-through did — makes a misrouted or truncated lease look
// like a successful iteration.
func TestSubgraphRuntimeWithoutABodyFailsInsteadOfPassingItemsThrough(t *testing.T) {
	rt := NewSubgraphRuntime(execution.NewRegistry(), NewPackageCache(PackageCacheConfig{MaxEntries: 4}))
	lease := batchBodyLease(t, bodyPackageForTest())
	lease.SubgraphPayload.Package = nil
	lease.SubgraphPayload.PackageHash = ""

	result, err := rt.Execute(context.Background(), lease)
	if err == nil && result.Error == nil {
		t.Fatalf("bodyless batch reported success: %+v", result)
	}
}

// The whole benefit of projecting the body at COMPILE time is that its hash
// does not move between batches: the package cache is keyed on that hash, so N
// batches of one map node compile the body once instead of N times.
//
// The cache adds one entry per MISS, and a hit returns before compiling
// (execution/subgraph/cache.go). So "exactly one entry after N batches" is
// exactly "compiled once" — provided the batches really did differ, which the
// distinct items below ensure.
func TestSubgraphRuntimeCompilesOneBodyForAllBatches(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.body_item", &bodyItemHandler{})
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 16})
	rt := NewSubgraphRuntime(reg, cache)

	pkg := bodyPackageForTest()
	for batch := 0; batch < 4; batch++ {
		lease := batchBodyLease(t, pkg)
		lease.SubgraphPayload.BatchIndex = batch
		lease.SubgraphPayload.Items = []any{batch * 2, batch*2 + 1}
		if _, err := rt.Execute(context.Background(), lease); err != nil {
			t.Fatalf("batch %d: Execute() error = %v", batch, err)
		}
	}

	if known := cache.Known(); len(known) != 1 {
		t.Errorf("package cache holds %d entries after 4 batches (%v), want 1: a hash that "+
			"moved between batches would recompile the body every time", len(known), known)
	}
}
