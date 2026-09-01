package subgraph

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// BenchmarkExecutorTwoNode measures the fixed framework cost paid by every map
// item before any business handler work. The production group runtime uses the
// same fresh local backend per Execute call.
func BenchmarkExecutorTwoNode(b *testing.B) {
	pkg := &graph.SubgraphPackage{
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
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		b.Fatal(err)
	}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	executor := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend {
		return local.New(
			local.WithRegistry(reg),
			local.WithConcurrency(1),
			local.WithQueueCapacity(16),
		)
	})
	req := Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := executor.Execute(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		if res.Outcome != OutcomeSuccess {
			b.Fatalf("outcome = %s, error = %s", res.Outcome, res.Error)
		}
	}
}
