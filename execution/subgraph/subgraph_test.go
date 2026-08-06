package subgraph

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// echoHandler returns the input Data as output on the "main" port.
type echoHandler struct{}

func (echoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.echo"}
}

func (echoHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}

// testRegistry returns a registry with the handlers buildTwoNodeChainPackage's
// nodes need.
func testRegistry(t *testing.T) *execution.Registry {
	t.Helper()
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	return reg
}

// buildTwoNodeChainPackage builds a plain two-node-chain sub-graph package:
// a -> b -> collector. Nothing here names "group" or "map" -- the layering
// test below is exactly the point: this package is caller-agnostic.
func buildTwoNodeChainPackage(t *testing.T) *graph.SubgraphPackage {
	t.Helper()
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "chain",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "chain",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.echo", Version: 1},
				{Name: "b", Type: "test.echo", Version: 1},
				{Name: "__collector_b_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "b"}}}},
				"b": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_b_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_b_main", SrcNode: "b", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.echo", NodeVersion: 1},
		},
	}
}

// TestExecutor_RunsAPackageWithNoKnowledgeOfItsCaller is the layering
// criterion for this package: the executor cannot tell whether a body came
// from a node group or a map. This test builds no group or map concept at
// all -- if it needed to mention either one, the boundary would be drawn
// wrong. Likewise, Request carries no Items or BatchIndex.
func TestExecutor_RunsAPackageWithNoKnowledgeOfItsCaller(t *testing.T) {
	pkg := buildTwoNodeChainPackage(t) // pure sub-graph package, no group/map wording
	// The brief's Step 1 snippet builds the closure as
	// `func() Backend { return local.New(local.WithConcurrency(1)) }`, i.e. a
	// backend with its own fresh, empty registry. That can't dispatch "a"/"b"
	// (registered below on reg, not on the backend's internal registry) or the
	// package's collector node (registered by Executor.Execute onto reg too),
	// so it would hang forever waiting for an execution that can never
	// progress. reg is captured by the closure instead, exactly as the real
	// adapter (service/runner/group_runtime.go) injects the outer registry
	// into local.New via WithRegistry so member/collector dispatch resolves
	// against the same registry Executor registers into.
	reg := testRegistry(t)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })

	// The brief's Step 1 snippet hardcodes PackageHash: "h1"; PackageCache's
	// (pre-existing, unrelated-to-this-task) hash verification rejects any
	// hash that doesn't match the recomputed one, so a literal placeholder
	// hash can never resolve. Compute the real hash instead, exactly like
	// every other test in this codebase that builds a *graph.SubgraphPackage
	// (e.g. service/runner/group_runtime_test.go's buildTestLease).
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	res, err := ex.Execute(context.Background(), Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v, want success (error: %s)", res.Outcome, res.Error)
	}
	if len(res.Exits) == 0 {
		t.Error("expected exit results")
	}
}

// I1: Executor.Execute registers per-execution collector handlers (Register in
// collector.go) but, before this fix, never unregistered them -- every call
// leaked its entries into the outer registry for the life of the process.
// This matters far more for a map body than for a group: MapBodyExecutor calls
// Execute once per ITEM (not once per execution), so an unbounded items array
// means unbounded growth. Assert on the actual registry size, not "no panic":
// a leak that merely wastes memory would pass a not-panicking assertion just
// as easily as a correct implementation.
func TestExecutor_UnregistersCollectorHandlersAfterEachExecution(t *testing.T) {
	pkg := buildTwoNodeChainPackage(t)
	reg := testRegistry(t)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	before := reg.ExecutionHandlerCount()
	const n = 20
	for i := 0; i < n; i++ {
		res, err := ex.Execute(context.Background(), Request{
			Package:     pkg,
			PackageHash: hash,
			Input:       &types.Input{Data: map[string]any{"seed": i}},
		})
		if err != nil {
			t.Fatalf("execute %d: %v", i, err)
		}
		if res.Outcome != OutcomeSuccess {
			t.Fatalf("execute %d: outcome = %v, want success (error: %s)", i, res.Outcome, res.Error)
		}
	}

	if got := reg.ExecutionHandlerCount(); got != before {
		t.Fatalf("registry holds %d execution-scoped handler entries after %d executions, want %d "+
			"(back to the count before any of them ran) -- collector registrations are leaking, "+
			"which for a map body (one Execute call per ITEM) is unbounded growth on a long-running runner",
			got, n, before)
	}
}

// A name-scoped handler (sdk.LocalNode, execution.Registry.RegisterNodeHandler)
// carries the synthetic node type "__direct__/<node-name>". Nothing is ever
// registered under that string as a TYPE, so a type-only inventory reports the
// handler missing and the package is rejected before it runs — which is what a
// map body built from LocalNode members hits.
func TestInventoryResolvesNameScopedHandlers(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterNodeHandler("double", echoHandler{})
	inv := (&Executor{registry: reg}).inventoryFromRegistry()

	if !inv.Has("__direct__/double", 0) {
		t.Error("inventory reports __direct__/double unavailable, but a handler is registered " +
			"for node name \"double\"; a body of LocalNode members can never pass validation")
	}
	if inv.Has("__direct__/absent", 0) {
		t.Error("inventory reports __direct__/absent available with no handler registered: " +
			"the name-scoped path must still fail closed")
	}
}

