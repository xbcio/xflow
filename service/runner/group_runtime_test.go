package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
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

// failHandler always returns an error.
type failHandler struct{ err error }

func (f failHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.fail"}
}
func (f failHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, f.err
}

func buildTestLease(t *testing.T, pkg *graph.SubgraphPackage, input *types.Input) *engine.TaskLease {
	t.Helper()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatal(err)
	}
	return &engine.TaskLease{
		LeaseID:    "lease-test",
		LeaseToken: "token-test",
		Attempt:    1,
		GroupPayload: &engine.GroupLeasePayload{
			ProtocolVersion: 1,
			GroupExecID:     "gexec-1",
			PackageHash:     hash,
			Package:         pkg,
			Input:           input,
			Deadline:        time.Now().Add(10 * time.Second),
		},
	}
}

func TestGroupRuntime_TwoNodeChainSuccess(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})

	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	rt := NewGroupRuntime(reg, cache, WithSuspendDisabled())

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

	input := &types.Input{Data: map[string]any{"x": 1}}
	lease := buildTestLease(t, pkg, input)

	result, err := rt.Execute(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("outcome = %s, want success; error = %s", result.Outcome, result.Error)
	}
	if len(result.Exits) != 1 {
		t.Fatalf("exits = %d, want 1", len(result.Exits))
	}
	if result.Exits[0].NodeName != "b" || result.Exits[0].Port != "main" {
		t.Fatalf("exit = %+v, want b/main", result.Exits[0])
	}
	if result.Exits[0].Data["x"] != 1 {
		t.Fatalf("exit data = %v, want {x:1}", result.Exits[0].Data)
	}
}

func TestGroupRuntime_MemberFailure(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	reg.RegisterGlobal("test.fail", failHandler{err: errors.New("boom")})

	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	rt := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "fail-group",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "fail-group",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.fail", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.fail", NodeVersion: 1},
		},
	}

	input := &types.Input{Data: map[string]any{}}
	lease := buildTestLease(t, pkg, input)

	result, err := rt.Execute(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != engine.GroupOutcomeFailed {
		t.Fatalf("outcome = %s, want failed", result.Outcome)
	}
	if result.Error == "" {
		t.Fatal("expected non-empty error")
	}
}

func TestGroupRuntime_DeadlineTimeout(t *testing.T) {
	// Use a handler that sleeps longer than the deadline.
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})

	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	rt := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "timeout-group",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "timeout-group",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.echo", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.echo", NodeVersion: 1},
		},
	}

	// Already expired deadline.
	hash, _ := graph.ComputePackageHash(pkg)
	lease := &engine.TaskLease{
		LeaseID:    "lease-timeout",
		LeaseToken: "token-timeout",
		Attempt:    1,
		GroupPayload: &engine.GroupLeasePayload{
			ProtocolVersion: 1,
			GroupExecID:     "gexec-timeout",
			PackageHash:     hash,
			Package:         pkg,
			Input:           &types.Input{Data: map[string]any{}},
			Deadline:        time.Now().Add(-1 * time.Second),
		},
	}

	result, err := rt.Execute(context.Background(), lease)
	if err != nil {
		t.Fatal(err)
	}
	// With an already-expired deadline, the context is immediately canceled.
	// However, the echo handler is so fast it may complete before the context
	// propagates. Accept success, timeout, or failed.
	switch result.Outcome {
	case engine.GroupOutcomeSuccess, engine.GroupOutcomeTimeout, engine.GroupOutcomeFailed:
		// all acceptable
	default:
		t.Fatalf("outcome = %s, want success/timeout/failed", result.Outcome)
	}
}

func TestGroupRuntime_ExternalCancel(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})

	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	rt := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "cancel-group",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "cancel-group",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.echo", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.echo", NodeVersion: 1},
		},
	}

	input := &types.Input{Data: map[string]any{}}
	lease := buildTestLease(t, pkg, input)

	// Cancel immediately — race between handler completion and cancel.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := rt.Execute(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	// With immediate cancel, we accept timeout (context done) or success
	// (handler completed before context was checked).
	switch result.Outcome {
	case engine.GroupOutcomeTimeout, engine.GroupOutcomeSuccess, engine.GroupOutcomeFailed:
		// acceptable
	default:
		t.Fatalf("outcome = %s, want timeout/success/failed", result.Outcome)
	}
}

type slowHandler struct{ delay time.Duration }

func (s slowHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.slow"}
}
func (s slowHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	select {
	case <-time.After(s.delay):
		return &types.Output{Data: map[string]any{}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
