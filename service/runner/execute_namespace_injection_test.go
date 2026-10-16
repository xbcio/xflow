package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// These tests cover the runner-side hop of the namespace chain: the three-line
// injection in executeAndReport that threads lease.Namespace onto the execute
// ctx. Everything downstream of it — SubgraphRuntime re-encoding the ctx
// namespace as an engine option, the inner engine stamping its leases,
// execution/runner.go binding Input.Namespace() — is covered by the local
// backend's end-to-end supply tests; what had no regression test was this hop
// itself, on the path production actually runs (a remote runner executing a
// map batch).
//
// Both namespaces are seeded with DIFFERENT content on purpose: Default=1,
// tenant-b=7. A regression that silently falls back to Default reads 1, which
// is distinguishable from a correct read of the tenant's 7 — and from a
// nil-fetch failure, which fails closed.

const runnerNSSupplyName = "ns-runner-drag"

// runnerNSProbeHandler captures the boundary-evaluated "rule" parameter of the
// body member. The expression lives in the body package's node definition, so
// the value arrives here only if the inner execution resolved $supplies in the
// namespace the lease named.
type runnerNSProbeHandler struct {
	resultCh chan any
}

func (h *runnerNSProbeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.ns_probe"}
}

func (h *runnerNSProbeHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.resultCh <- in.Params["rule"]
	return &types.Output{Data: map[string]any{"rule": in.Params["rule"]}}, nil
}

// runnerNSSupplyBodyPackage builds a map body whose only member evaluates a
// $supplies expression. Shape mirrors scriptMemberPackage in
// artifact_in_subgraph_test.go, plus the supply declaration node and its
// dependency edge, which the compiler requires for any $supplies reference.
func runnerNSSupplyBodyPackage() *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "m/body",
		EntryNode: "p",
		Def: &types.WorkflowDef{
			Name: "m/body",
			Nodes: []types.NodeDef{
				{Name: runnerNSSupplyName, Type: "xflow.supply.external", Kind: types.NodeKindSupply},
				{Name: "p", Type: "test.ns_probe", Version: 1, Parameters: map[string]any{
					"rule": `${{ $supplies["` + runnerNSSupplyName + `"].threshold }}`,
				}},
				{Name: "__collector_p_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"p": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_p_main"}}}},
				runnerNSSupplyName: {"supply": {
					Type:    types.ConnectionTypeDependency,
					Targets: []types.Connection{{Node: "p"}},
				}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_p_main", SrcNode: "p", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.ns_probe", NodeVersion: 1},
		},
	}
}

// seedRunnerNamespaceSupplies seeds Default with threshold 1 and tenant-b with
// threshold 7, and registers cleanup for both (supply.Default is a process-wide
// singleton; leaked names poison other tests in this package).
func seedRunnerNamespaceSupplies(t *testing.T) {
	t.Helper()
	content := func(threshold string) []byte { return []byte(`{"threshold":` + threshold + `}`) }
	for _, seed := range []struct {
		ns        namespace.Namespace
		threshold string
		hash      string
	}{
		{namespace.Default, "1", "rd1"},
		{"tenant-b", "7", "rt1"},
	} {
		if err := supply.Default.Apply(context.Background(), supply.Snapshot{
			Namespace: seed.ns, Name: runnerNSSupplyName,
			Content: content(seed.threshold), Hash: seed.hash, Revision: 1,
		}); err != nil {
			t.Fatalf("seed supply in %s: %v", seed.ns, err)
		}
	}
	t.Cleanup(func() {
		for _, ns := range []namespace.Namespace{namespace.Default, "tenant-b"} {
			_ = supply.Default.Apply(context.Background(), supply.Snapshot{Namespace: ns, Name: runnerNSSupplyName})
		}
	})
}

// TestExecuteAndReportThreadsLeaseNamespaceIntoSubgraphBody pins the injection:
// a batch lease carrying Namespace "tenant-b" must have that namespace on the
// execute ctx by the time SubgraphRuntime runs, so the body member's $supplies
// expression resolves against tenant-b and reads 7.
//
// Mutation: drop the `if lease.Namespace != "" { execCtx =
// namespace.WithNamespace(...) }` block in executeAndReport — the body falls
// back to Default, reads 1, and the value assertion fails.
func TestExecuteAndReportThreadsLeaseNamespaceIntoSubgraphBody(t *testing.T) {
	seedRunnerNamespaceSupplies(t)

	reg := execution.NewRegistry()
	resultCh := make(chan any, 1)
	reg.RegisterGlobal("test.ns_probe", &runnerNSProbeHandler{resultCh: resultCh})

	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}))

	pkg := runnerNSSupplyBodyPackage()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	items := []any{map[string]any{"n": 1.0}}
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-ns"),
		LeaseToken: engine.LeaseToken("token-ns"),
		Attempt:    1,
		Namespace:  namespace.Namespace("tenant-b"),
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-ns"),
			NodeName:    "m/_batch/0",
			Type:        engine.TaskTypeNodeBatch,
		},
		NodeType: "xflow.map",
		SubgraphPayload: &engine.SubgraphLeasePayload{
			ProtocolVersion: 1,
			ParentNode:      "m",
			PackageHash:     hash,
			Package:         pkg,
			BatchIndex:      0,
			BatchSize:       1,
			Items:           items,
			AllItems:        items,
		},
	}

	client := &fakeProtocolClient{cancel: func() {}}
	r := New(client, reg, Config{
		RunnerID:          "runner-1",
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		SubgraphRuntime:   rt,
	})

	r.executeAndReport(context.Background(), "session-ns", lease, &atomic.Int32{}, newActiveLeases(), func(error) {})

	if client.reported.Result.Error != nil {
		t.Fatalf("batch report carries error %v, want none", client.reported.Result.Error)
	}
	select {
	case got := <-resultCh:
		if got != float64(7) {
			t.Fatalf("body member's $supplies expression evaluated to %v (type %T); want 7 (tenant-b). "+
				"1 (or a nil-fetch) means the expression resolved against Default — the lease namespace "+
				"never reached the body's ctx, and executeAndReport's injection is the only hop that carries it",
				got, got)
		}
	default:
		t.Fatal("body member never ran — the inner boundary evaluation of its rule parameter must have failed")
	}
}
