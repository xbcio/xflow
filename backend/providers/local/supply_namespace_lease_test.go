package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// These two tests are the live-fire finding of 2026-10-16 reduced to a unit:
// an embedded engine whose deployment namespace is NOT Default resolved every
// $supplies expression against Default. The namespace is dropped three times
// on the way down -- the local queue's worker rebuilds ctx from
// context.Background() (twice: outer queue and batch execution), the inner
// engine of a map body is a fresh engine with no namespace of its own -- and
// Default-namespace deployments never noticed because every fallback happened
// to equal the truth.
//
// The resolution is that the namespace rides the lease as data at every
// boundary: engine.WithNamespace stamps it (BuildTaskLease, ExecuteBatch's
// re-anchor), subgraph re-encodes the ctx it received as an engine option,
// and execution/runner.go binds it onto Input unconditionally. Each of the
// three edits has a mutation that turns exactly one of these tests red.
//
// Both tests seed BOTH namespaces with DIFFERENT content on purpose: a test
// that seeded only the dedicated namespace would pass on the broken build
// whenever the fallback path happened to find nothing (failing closed with a
// nil-fetch error), which is not the same assertion as "read the right
// tenant's value". With Default seeded to 1 and the tenant to 7, a regression
// that silently falls back to Default reads 1 and fails the value assertion.

const nsSupplyName = "ns-drag"

// nsSupplyReader captures the boundary-evaluated "rule" parameter. The
// expression lives in the workflow definition (see the two defs below), so
// the value arrives here only if exprx resolved $supplies in the executing
// input's namespace.
type nsSupplyReader struct {
	resultCh chan any
}

func (h *nsSupplyReader) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.ns_supply_reader"}
}

func (h *nsSupplyReader) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.resultCh <- in.Params["rule"]
	return &types.Output{Data: map[string]any{"rule": in.Params["rule"]}}, nil
}

// seedNamespaceSupplies seeds Default with threshold 1 and tenant-b with
// threshold 7, and registers cleanup for both (supply.Default is a
// process-wide singleton; leaked names poison other tests in this package).
func seedNamespaceSupplies(t *testing.T) {
	t.Helper()
	content := func(threshold string) []byte { return []byte(`{"threshold":` + threshold + `}`) }
	for _, seed := range []struct {
		ns        namespace.Namespace
		threshold string
		hash      string
	}{
		{namespace.Default, "1", "d1"},
		{"tenant-b", "7", "t1"},
	} {
		if err := supply.Default.Apply(context.Background(), supply.Snapshot{
			Namespace: seed.ns, Name: nsSupplyName,
			Content: content(seed.threshold), Hash: seed.hash, Revision: 1,
		}); err != nil {
			t.Fatalf("seed supply in %s: %v", seed.ns, err)
		}
	}
	t.Cleanup(func() {
		for _, ns := range []namespace.Namespace{namespace.Default, "tenant-b"} {
			_ = supply.Default.Apply(context.Background(), supply.Snapshot{Namespace: ns, Name: nsSupplyName})
		}
	})
}

func assertReadTheTenantsThreshold(t *testing.T, got any) {
	t.Helper()
	if got != float64(7) {
		t.Fatalf("$supplies[%q].threshold evaluated to %v; want 7 (tenant-b). "+
			"1 (or a nil-fetch failure) means the expression resolved against "+
			"Default, i.e. the namespace was dropped on the way to the input", nsSupplyName, got)
	}
}

// TestNodeReadsSupplyInTheEnginesNamespace covers the direct node path: one
// root node whose parameter is a bare $supplies expression, evaluated by the
// boundary (execution/params.go) against exprx.BuildExprEnv, which reads
// supply.Default.Decoded(input.Namespace()).
//
// Mutation: drop engine.WithNamespace from the engine construction — the
// outer lease carries no namespace, execution/runner.go binds Default, the
// expression reads 1, and the value assertion fails. Dropping the
// unconditional SetNamespace in execution/runner.go has the same effect: the
// embedded dispatcher installs no credential resolver, so the old
// conditional never bound the input at all.
func TestNodeReadsSupplyInTheEnginesNamespace(t *testing.T) {
	seedNamespaceSupplies(t)

	reg := execution.NewRegistry()
	resultCh := make(chan any, 1)
	reg.RegisterGlobal("test.ns_supply_reader", &nsSupplyReader{resultCh: resultCh})

	b := New(WithConcurrency(1), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue(), engine.WithNamespace(namespace.Namespace("tenant-b")))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "ns-supply-direct",
		Nodes: []types.NodeDef{
			{Name: nsSupplyName, Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "reader", Type: "test.ns_supply_reader", Parameters: map[string]any{
				"rule": `${{ $supplies["` + nsSupplyName + `"].threshold }}`,
			}},
		},
		Connections: types.Connections{
			nsSupplyName: {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "reader"}},
			}},
		},
	}
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
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
		t.Fatalf("execution status = %v, want success (error: %v)", res.Status, res.Error)
	}
	select {
	case got := <-resultCh:
		assertReadTheTenantsThreshold(t, got)
	default:
		t.Fatal("reader node never ran — the boundary evaluation of its rule parameter must have failed")
	}
}

// TestMapBodyMemberReadsSupplyInTheEnginesNamespace covers the in-process
// batch path: a map node expands into a batch task, the batch is consumed by
// the engine itself (no remote batch execution), and the body member's
// $supplies expression is evaluated by the INNER execution's boundary.
//
// This path crosses two more context resets than the direct one, and each
// has its own hop of the fix:
//   - ExecuteBatch re-anchors lease.Namespace (stamped by WithNamespace onto
//     the parent lease) onto the batch ctx — the task came off the local
//     queue, whose worker starts from context.Background();
//   - subgraph.Executor re-encodes that ctx namespace as engine.WithNamespace
//     before building the inner engine, whose queue will drop ctx again.
//
// Mutation: drop any of the three (WithNamespace at construction, the
// ExecuteBatch re-anchor, the subgraph ctx-to-option conversion) — the body
// member resolves against Default, reads 1, and the value assertion fails.
func TestMapBodyMemberReadsSupplyInTheEnginesNamespace(t *testing.T) {
	seedNamespaceSupplies(t)

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	resultCh := make(chan any, 2)
	reg.RegisterGlobal("test.ns_supply_reader", &nsSupplyReader{resultCh: resultCh})

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(),
		engine.WithNamespace(namespace.Namespace("tenant-b")),
		engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "ns-supply-map-body",
		Nodes: []types.NodeDef{
			{Name: nsSupplyName, Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "reader", "type": "test.ns_supply_reader", "parameters": map[string]any{
								"rule": `${{ $supplies["` + nsSupplyName + `"].threshold }}`,
							}},
						},
					},
				},
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
			nsSupplyName: {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "m"}},
			}},
		},
	}
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
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
		t.Fatalf("execution status = %v, want success (error: %v)", res.Status, res.Error)
	}
	select {
	case got := <-resultCh:
		assertReadTheTenantsThreshold(t, got)
	default:
		t.Fatal("body member never ran — the inner boundary evaluation of its rule parameter must have failed")
	}
}
