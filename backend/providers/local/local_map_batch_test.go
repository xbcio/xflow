package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// An in-process backend has no runner directory and no remote runner: a batch
// task that escapes to be routed somewhere is routed nowhere, and the map node
// waits on a child generation that never reports. Every embedded deployment
// (sdk.NewLocal, every inner sub-graph execution) runs on exactly this shape,
// so the escape must be something the control plane opts INTO rather than the
// default every engine inherits.
type mapFanoutHandler struct{}

func (h *mapFanoutHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *mapFanoutHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	items := []any{map[string]any{"id": 1}, map[string]any{"id": 2}}
	return &types.Output{Data: map[string]any{
		"_loop":       true,
		"items":       items,
		"batches":     [][]any{{items[0]}, {items[1]}},
		"batch_size":  1,
		"total":       2,
		"batch_count": 2,
	}}, nil
}

type batchEchoHandler struct{}

func (h *batchEchoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.echo"}
}

func (h *batchEchoHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// bodyItemHandler is the body sub-graph's only node. It records the $item roots
// it was handed, which is the evidence that the body actually EXECUTED: the
// expansion completing proves only that the batches were routed somewhere.
type bodyItemHandler struct {
	mu   sync.Mutex
	seen []any
}

func (h *bodyItemHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *bodyItemHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen = append(h.seen, in.Data["$item"])
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{
		"id":    in.Data["$item"],
		"index": in.Data["$index"],
	}}, nil
}

func (h *bodyItemHandler) items() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.seen...)
}

func TestLocalBackendCompletesAMapExpansion(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &bodyItemHandler{}
	reg.RegisterGlobal("test.body_item", body)
	b := New(WithConcurrency(2), WithRegistry(reg))
	// A batch runs its body in this same process, so an in-process engine needs
	// a body executor as much as a runner does. Each body attempt gets its own
	// local backend: the inner execution must not share the outer backend's
	// queue, or its tasks would be drained by the outer scheduler.
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true)
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "local-map",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "echo", "type": "test.body_item"},
						},
					},
				},
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v — the map node never finished, so its batches were routed nowhere", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}
	// Two items, two batches, one body node each: the body must have run twice
	// with each item's own content. A pass-through batch would reach success
	// with this list empty; an injection that lost $item would leave it nil.
	ran := body.items()
	if len(ran) != 2 {
		t.Fatalf("body node ran %d time(s) with items %v, want 2 — the expansion "+
			"succeeded without executing the body", len(ran), ran)
	}
	gotIDs := map[any]bool{}
	for _, item := range ran {
		row, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("body saw $item = %#v, want the map the fan-out produced", item)
		}
		gotIDs[row["id"]] = true
	}
	if !gotIDs[1] || !gotIDs[2] {
		t.Errorf("body saw ids %v, want both 1 and 2 — each item must reach the body once", gotIDs)
	}
}
