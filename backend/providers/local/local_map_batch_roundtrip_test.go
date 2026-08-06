package local

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// C0: a compiled graph is what production actually resubmits after a reload
// from Redis (rstate.Store.LoadGraph unmarshals straight into a fresh
// *graph.Graph whenever the in-memory per-process cache misses — a second
// server replica, or the same server after a restart). If BodyAt does not
// survive that round-trip, BuildSubgraphLease ships an empty Package on every
// batch and the runner rejects it with ErrPackageMissing. This test proves the
// body still runs after exactly that round-trip, not just that BodyAt
// returns non-nil in isolation (engine/graph/snapshot_mapbody_test.go covers
// that unit-level claim already).
func TestLocalBackendCompletesAMapExpansionAfterGraphRoundTrip(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &bodyItemHandler{}
	reg.RegisterGlobal("test.body_item", body)
	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true)
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "local-map-roundtrip",
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
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// The step production takes between compile and (re)submit: serialize to
	// JSON (what rstate.Store persists to Redis) and deserialize into a FRESH
	// Graph (what a cache miss on LoadGraph produces). Submitting the ORIGINAL
	// compiled graph would not exercise the bug at all — g.mapBodies would
	// still be the in-memory map that was never round-tripped.
	wire, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	var reloaded graph.Graph
	if err := json.Unmarshal(wire, &reloaded); err != nil {
		t.Fatalf("unmarshal graph: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, &reloaded, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v — the map node never finished on the reloaded graph", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}
	ran := body.items()
	if len(ran) != 2 {
		t.Fatalf("body node ran %d time(s) on the reloaded graph, want 2 — "+
			"BodyAt did not survive the JSON round-trip, so the batch had no "+
			"body to run", len(ran))
	}
}
