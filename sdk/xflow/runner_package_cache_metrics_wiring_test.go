package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// TestNewRunnerObservesGroupPackageCache is the wiring counterpart of
// TestNewRunnerObservesGroupMemberNodes (runner_group_hooks_test.go), read
// first for the pattern this follows.
//
// It deliberately does not construct a subgraph.PackageCacheConfig and stuff
// an observer into it directly — that would prove the field exists, not that
// WithRunnerMetrics reaches it (see probe-that-stuffs-the-field-proves-nothing).
// Instead it drives a real group lease through buildRunnerServiceConfig's
// cfg.GroupRuntime, the same real-assembly path production runners build, and
// reads the outcome back off the metrics registry's own Gather() output —
// the strongest assertion available from this package, since sdk/xflow has no
// exported access to the runtime's internal PackageCache to inspect directly.
func TestNewRunnerObservesGroupPackageCache(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.group_echo", groupEchoHandler{})

	m := metrics.New()
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{engine.GroupNodeType},
	}, WithRunnerMetrics(m), WithRunnerNodeRegistry(reg))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.GroupRuntime == nil {
		t.Fatal("cfg.GroupRuntime is nil")
	}

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "member",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "member", Type: "test.group_echo", Version: 1},
				{Name: "__collector_member_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"member": {"main": types.PortConnections{
					Targets: []types.Connection{{Node: "__collector_member_main"}},
				}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_member_main", SrcNode: "member", Port: "main"},
		},
		Requirements: []graph.Requirement{{NodeType: "test.group_echo", NodeVersion: 1}},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("ComputePackageHash: %v", err)
	}

	lease := func(attempt int) *engine.TaskLease {
		return &engine.TaskLease{
			LeaseID:    "lease-1",
			LeaseToken: "token-1",
			Attempt:    attempt,
			GroupPayload: &engine.GroupLeasePayload{
				ProtocolVersion: 1,
				GroupExecID:     "gexec-1",
				PackageHash:     hash,
				Package:         pkg,
				Input:           &types.Input{Data: map[string]any{"x": 1}},
				Deadline:        time.Now().Add(10 * time.Second),
			},
		}
	}

	// First Resolve of this hash: the package is not in the runtime's cache
	// yet, so it compiles and caches it. That is a "miss" — a real package
	// transfer was needed.
	result, err := cfg.GroupRuntime.Execute(context.Background(), lease(1))
	if err != nil {
		t.Fatalf("GroupRuntime.Execute (first): %v", err)
	}
	if result.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("group outcome = %s (error %q); the counts below would be zero for "+
			"the wrong reason", result.Outcome, result.Error)
	}

	// Second Execute with the same PackageHash: the runtime's PackageCache
	// already holds the compiled graph, so this is a "hit".
	result, err = cfg.GroupRuntime.Execute(context.Background(), lease(2))
	if err != nil {
		t.Fatalf("GroupRuntime.Execute (second): %v", err)
	}
	if result.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("group outcome = %s (error %q) on the second attempt", result.Outcome, result.Error)
	}

	if got := counterFor(t, m, "xflow_group_package_cache_total", "result", "miss"); got != 1 {
		t.Errorf("xflow_group_package_cache_total{result=\"miss\"} = %v, want 1 — "+
			"WithRunnerMetrics does not reach the group runtime's PackageCache", got)
	}
	if got := counterFor(t, m, "xflow_group_package_cache_total", "result", "hit"); got != 1 {
		t.Errorf("xflow_group_package_cache_total{result=\"hit\"} = %v, want 1 — "+
			"the second Execute on the same PackageHash should have hit the cache", got)
	}
}
