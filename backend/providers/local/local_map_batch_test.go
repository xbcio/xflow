package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
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

func TestLocalBackendCompletesAMapExpansion(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	b := New(WithConcurrency(2), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "local-map",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map"},
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
}
