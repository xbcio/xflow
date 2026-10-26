package local

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// batchBlockHandler is the map body's only node. It holds until its context
// ends, so the only thing that can stop a batch is a deadline.
type batchBlockHandler struct{}

func (batchBlockHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.batch_block"}
}

func (batchBlockHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// batchRunCounter counts how often a downstream node ran.
type batchRunCounter struct{ runs atomic.Int64 }

func (c *batchRunCounter) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.batch_count"}
}

func (c *batchRunCounter) Execute(context.Context, *types.Input) (*types.Output, error) {
	c.runs.Add(1)
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// A map batch whose body is stopped by the execution deadline is the
// execution's timeout, whatever the map node's on_error says.
func TestMapBatchPastTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	cases := []struct {
		onError    types.OnError
		downstream bool
	}{
		{"", true},
		{types.OnErrorContinue, true},
		{types.OnErrorOutput, true},
		// A terminal map node has no later lease to notice the spent budget.
		{"", false},
		{types.OnErrorContinue, false},
		{types.OnErrorOutput, false},
	}
	for _, tc := range cases {
		onError := tc.onError
		name := "on_error=" + string(onError)
		if !tc.downstream {
			name += "/terminal"
		}
		t.Run(name, func(t *testing.T) {
			reg := execution.NewRegistry()
			reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
			reg.RegisterGlobal("test.batch_block", batchBlockHandler{})
			downstream := &batchRunCounter{}
			reg.RegisterGlobal("test.batch_count", downstream)
			b := New(WithConcurrency(2), WithRegistry(reg))
			bodies := subgraph.NewMapBodyExecutor(
				subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
					func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
				true, time.Time{})
			eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
			stop := b.Bind(eng)
			defer stop()

			def := &types.WorkflowDef{
				Name:     "map-batch-exec-deadline",
				Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: 700 * time.Millisecond},
				Nodes: []types.NodeDef{
					{Name: "m", Type: "xflow.map", OnError: string(onError), Parameters: map[string]any{
						"items": "$input.items",
						"body": map[string]any{
							"type": "xflow.subgraph",
							"parameters": map[string]any{
								"nodes": []any{
									map[string]any{"name": "block", "type": "test.batch_block"},
								},
							},
						},
					}},
				},
			}
			if tc.downstream {
				def.Nodes = append(def.Nodes,
					types.NodeDef{Name: "done", Type: "test.batch_count"},
					types.NodeDef{Name: "onerr", Type: "test.batch_count"})
				def.Connections = types.Connections{
					"m": {
						"main":  {Targets: []types.Connection{{Node: "done", Input: "main"}}},
						"error": {Targets: []types.Connection{{Node: "onerr", Input: "main"}}},
					},
				}
			}
			g, err := graph.Compile(def)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			id, err := eng.Submit(ctx, g, nil)
			if err != nil {
				t.Fatalf("submit: %v", err)
			}
			res, err := b.WaitDone(ctx, id)
			if err != nil {
				t.Fatalf("WaitDone: %v", err)
			}
			if res.Status != types.ExecutionStatusTimeout {
				t.Errorf("status = %q (error %q), want %q", res.Status, res.Error, types.ExecutionStatusTimeout)
			}
			if res.Error != engine.ExecutionDeadlineExceededError {
				t.Errorf("error = %q, want %q", res.Error, engine.ExecutionDeadlineExceededError)
			}
			// Give a wrongly routed on_error branch time to run before counting.
			time.Sleep(300 * time.Millisecond)
			if n := downstream.runs.Load(); n != 0 {
				t.Errorf("downstream nodes ran %d time(s), want 0 past the execution deadline", n)
			}
		})
	}
}
