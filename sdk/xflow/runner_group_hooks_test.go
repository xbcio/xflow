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

type groupEchoHandler struct{}

func (groupEchoHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.group_echo"}
}
func (groupEchoHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	return &types.Output{Data: in.Data}, nil
}

// A group's members run on an engine that service/runner builds per attempt.
// WithRunnerMetrics has to reach that engine, and it is two hops away from the
// registry the caller hands in: buildRunnerServiceConfig -> NewGroupRuntime ->
// subgraph.NewExecutor. A missing link at either hop leaves every member node of
// every group silently uncounted, which is what the runner did before this.
//
// This drives a real group lease through cfg.GroupRuntime and reads the result
// out of the CALLER's registry, so it fails on a hook that was never installed
// and on one installed against some other registry. Asserting the option exists
// would prove neither.
func TestNewRunnerObservesGroupMemberNodes(t *testing.T) {
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

	result, err := cfg.GroupRuntime.Execute(context.Background(), &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Attempt:    1,
		GroupPayload: &engine.GroupLeasePayload{
			ProtocolVersion: 1,
			GroupExecID:     "gexec-1",
			PackageHash:     hash,
			Package:         pkg,
			Input:           &types.Input{Data: map[string]any{"x": 1}},
			Deadline:        time.Now().Add(10 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("GroupRuntime.Execute: %v", err)
	}
	// Guard the premise: an outcome other than success leaves the counters at
	// zero for reasons unrelated to the hooks.
	if result.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("group outcome = %s (error %q); the count below would be zero for "+
			"the wrong reason", result.Outcome, result.Error)
	}

	if got := counterFor(t, m, "xflow_subgraph_node_started_total", "node", "member"); got != 1 {
		t.Errorf("xflow_subgraph_node_started_total{node=\"member\"} = %v, want 1 — "+
			"WithRunnerMetrics does not reach the engine a group member runs on, "+
			"so every member node on this runner is invisible", got)
	}
}
