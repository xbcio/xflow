package runner

import (
	"context"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
)

// GroupRuntimeOption configures a GroupRuntime.
type GroupRuntimeOption func(*GroupRuntime)

// WithSuspendDisabled makes the inner engine reject suspend nodes.
func WithSuspendDisabled() GroupRuntimeOption {
	return func(r *GroupRuntime) { r.suspendDisabled = true }
}

// GroupRuntime adapts execution/subgraph.Executor -- the caller-agnostic
// sub-graph execution layer -- to the runner's group-lease shape: it unpacks
// engine.TaskLease.GroupPayload into a subgraph.Request, and maps the
// resulting subgraph.Result back onto engine.GroupResult for the
// control-plane wire protocol. A node group is just one caller of the
// executor; the executor itself has no notion of "group" (see
// execution/subgraph).
type GroupRuntime struct {
	suspendDisabled bool
	executor        *subgraph.Executor
}

// NewGroupRuntime creates a group runtime that uses the given registry for
// handler resolution and the cache for package validation. The registry is
// also injected into each attempt's embedded local backend (via
// local.WithRegistry) so member/collector node dispatch resolves against the
// same registry the outer runner registered its handlers on.
func NewGroupRuntime(reg *execution.Registry, cache *PackageCache, opts ...GroupRuntimeOption) *GroupRuntime {
	r := &GroupRuntime{}
	for _, o := range opts {
		o(r)
	}
	r.executor = subgraph.NewExecutor(reg, cache, func() subgraph.Backend {
		return local.New(local.WithRegistry(reg), local.WithConcurrency(1))
	})
	return r
}

// Execute runs the group subgraph defined by the lease's GroupPayload.
// It returns a GroupResult suitable for reporting to the control plane.
func (r *GroupRuntime) Execute(ctx context.Context, lease *engine.TaskLease) (engine.GroupResult, error) {
	if lease == nil || lease.GroupPayload == nil {
		return engine.GroupResult{Outcome: engine.GroupOutcomeFailed, Error: "nil lease or payload"}, nil
	}
	payload := lease.GroupPayload

	res, err := r.executor.Execute(ctx, subgraph.Request{
		Package:         payload.Package,
		PackageHash:     payload.PackageHash,
		Input:           payload.Input,
		Deadline:        payload.Deadline,
		SuspendDisabled: r.suspendDisabled,
	})
	if err != nil {
		return engine.GroupResult{}, err
	}

	result := engine.GroupResult{
		ProtocolVersion: payload.ProtocolVersion,
		GroupExecID:     payload.GroupExecID,
		Attempt:         lease.Attempt,
		Outcome:         engine.GroupOutcome(res.Outcome),
		Error:           res.Error,
	}
	// res.Exits carries no outer-graph NodeIdx (see graph.SubgraphExitResult's
	// doc) -- it was already always the zero value on this remote-runner path
	// even before this move: the collector never set it, and the wire type
	// (service/protocol.GroupExitResultWire) has no NodeIdx field to carry it
	// across anyway. CommitGroupResult (engine/group_lease.go) independently
	// recomputes the outer-graph node index by name once the result reaches
	// the control plane, so nothing here needs to reconstruct it.
	if len(res.Exits) > 0 {
		result.Exits = make([]engine.GroupExitResult, len(res.Exits))
		for i, ex := range res.Exits {
			result.Exits[i] = engine.GroupExitResult{NodeName: ex.NodeName, Port: ex.Port, Data: ex.Data}
		}
	}
	return result, nil
}
