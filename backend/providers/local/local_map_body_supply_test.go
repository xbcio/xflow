package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// C1: the real-run test design §11.1 probe (5) requires for the body case --
// a body member actually reading a supply's content and getting it, driven
// through a real Compile()+Submit(), not a direct call into the projection
// internals (engine/graph/map_body_supply_test.go already covers that
// unit-level claim).
//
// supplyReaderHandler is the body's only node. It reads $supplies.rules off
// the process-wide supply.Default registry, exactly the way
// exprx.BuildExprEnv's default does for xflow.function/xflow.script --
// this test does not go through the expr engine itself (that machinery is
// exercised elsewhere) but the plumbing under test (ProjectNodeBodyPackage's
// VisibleSupplies, CompileProjectedPackage's validateSupplyUsage) is identical
// whether the read happens via expr or a direct handler.
type supplyReaderHandler struct {
	resultCh   chan string
	supplyName string
}

func (h *supplyReaderHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.supply_reader"}
}

func (h *supplyReaderHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	snap, ok := supply.Default.Get(h.supplyName)
	if !ok {
		return &types.Output{Data: map[string]any{"content": nil}}, nil
	}
	h.resultCh <- string(snap.Content)
	return &types.Output{Data: map[string]any{"content": string(snap.Content)}}, nil
}

func TestMapBodyMemberReadsSupplyContent(t *testing.T) {
	const supplyName = "rules-c1-realrun"
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name: supplyName, Content: []byte(`{"threshold":7}`), Hash: "h1", Revision: 1,
	}); err != nil {
		t.Fatalf("seed supply: %v", err)
	}
	t.Cleanup(func() {
		// supply.Default is a process-wide singleton (see
		// supply-default-global-test-pollution project memory): leaving this
		// name cached would leak into any other test that happens to read it.
		_ = supply.Default.Apply(context.Background(), supply.Snapshot{Name: supplyName, Content: nil, Hash: ""})
	})

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	resultCh := make(chan string, 2)
	reg.RegisterGlobal("test.supply_reader", &supplyReaderHandler{resultCh: resultCh, supplyName: supplyName})

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-supply",
		Nodes: []types.NodeDef{
			{Name: supplyName, Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "reader", "type": "test.supply_reader", "parameters": map[string]any{
								// This is what actually exercises validateSupplyUsage / the
								// compile-time gate C1 fixes: the handler below reads
								// supply.Default directly rather than through this
								// parameter, but the parameter is what makes
								// ProjectNodeBodyPackage's VisibleSupplies matter at all --
								// without it CompileProjectedPackage never even looks at
								// the visible-supply list, and the fix's absence would go
								// undetected.
								"rule": "$supplies." + supplyName,
							}},
						},
					},
				},
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
			supplyName: {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "m"}},
			}},
		},
	}
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v (a body member reading a supply the parent map node "+
			"declares a dependency edge to must compile at deploy time)", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, compiled, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success", res.Status)
	}

	select {
	case got := <-resultCh:
		if got != `{"threshold":7}` {
			t.Fatalf("body member read supply content = %q, want the seeded content", got)
		}
	default:
		t.Fatal("body member never read any supply content -- the batch completed " +
			"but the body's own node never saw $supplies.rules")
	}
}
