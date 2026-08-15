package subgraph

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// sentinelPermanentHandler fails the way the wasm reactor marks a host trap:
// the original error's text is the message, and types.ErrPermanent rides in the
// unwrap chain. Deliberately NOT a *types.ClassifiedError -- that is the shape
// engine/buildEffectiveClassification handles in its first branch, and the
// branch this test exercises is the second one.
type sentinelPermanentHandler struct{}

func (sentinelPermanentHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.permfail"}
}

func (sentinelPermanentHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, &sentinelFault{err: fmt.Errorf("wasm reactor: eval: wasm error: unreachable")}
}

type sentinelFault struct{ err error }

func (e *sentinelFault) Error() string   { return e.err.Error() }
func (e *sentinelFault) Unwrap() []error { return []error{e.err, types.ErrPermanent} }

// transientHandler fails without classifying itself, standing in for an
// ordinary node error or an environmental fault.
type transientHandler struct{}

func (transientHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.transientfail"}
}

func (transientHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, fmt.Errorf("connection refused")
}

// buildSingleNodePackage builds a one-node sub-graph package around nodeType.
func buildSingleNodePackage(nodeType string) *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "failing",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "failing",
			Nodes: []types.NodeDef{
				{Name: "a", Type: nodeType, Version: 1},
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
			{NodeType: nodeType, NodeVersion: 1},
		},
	}
}

// TestExecutor_SentinelPermanentFailureSetsResultPermanent traces a member
// node's permanence marker across the layers that stand between the failure and
// the Kafka commit decision.
//
// The marker starts at the node (for the real case, a wasm host trap on a
// malformed message) and has to survive: engine's buildEffectiveClassification,
// which reads types.IsPermanent when the error carries no *types.ClassifiedError;
// ObservedNodeFailure.Permanent; and finally Result.Permanent here. One layer
// dropping it is enough -- service/runner reads Result.Permanent straight into
// GroupExecResult.Deterministic, and node/trigger/kafka/entryseed.go turns that
// into "commit the offset and skip" versus "refuse and let the broker
// redeliver". Unmarked, the same bytes are redelivered forever: the partition
// stops advancing and every message behind it stalls too.
//
// Asserting on Result.Permanent rather than on the engine's internals is the
// point -- this is the value the next layer up actually reads.
func TestExecutor_SentinelPermanentFailureSetsResultPermanent(t *testing.T) {
	pkg := buildSingleNodePackage("test.permfail")
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.permfail", sentinelPermanentHandler{})
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
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
	if res.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %v, want failed", res.Outcome)
	}
	if !res.Permanent {
		t.Errorf("a member failure carrying types.ErrPermanent did not set "+
			"Result.Permanent, so GroupExecResult.Deterministic stays false and "+
			"the Kafka batch is redelivered forever. res.Error = %q", res.Error)
	}
}

// TestExecutor_UnclassifiedFailureLeavesResultTransient is the counter-case.
// An unclassified failure may well succeed on a retry, so marking it permanent
// would commit the offset and silently discard real messages. If this test and
// the one above ever agree, Result.Permanent has stopped carrying information.
func TestExecutor_UnclassifiedFailureLeavesResultTransient(t *testing.T) {
	pkg := buildSingleNodePackage("test.transientfail")
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.transientfail", transientHandler{})
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
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
	if res.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %v, want failed", res.Outcome)
	}
	if res.Permanent {
		t.Error("an unclassified failure was marked permanent: the Kafka batch " +
			"would be committed and its messages discarded, though a retry " +
			"could have succeeded")
	}
}
