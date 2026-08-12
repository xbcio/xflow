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

// ExecuteRequest runs req directly against the underlying subgraph executor
// and maps the result to engine.GroupResult, without unwrapping an
// engine.TaskLease. This is the entry point for callers that have no lease at
// all — a trigger-group's per-Kafka-batch local execution (see
// node/trigger/kafka/kafka.go's group-exec path) runs once per flushed
// batch, never through the lease-based task queue, so there is no
// LeaseID/Attempt/GroupExecID to unwrap. Execute (below) is now a thin
// wrapper over this for the lease-bearing callers (the batch task-queue
// path, runner.go:363).
func (r *GroupRuntime) ExecuteRequest(ctx context.Context, req subgraph.Request) (engine.GroupResult, error) {
	// r.suspendDisabled is a floor, not an override: a caller that already
	// wants suspend disabled (e.g. the group-exec trigger adapter, which
	// always sets this true) keeps that; a caller relying on the runtime's
	// own construction-time setting inherits it too.
	req.SuspendDisabled = req.SuspendDisabled || r.suspendDisabled

	res, err := r.executor.Execute(ctx, req)
	if err != nil {
		return engine.GroupResult{}, err
	}

	result := engine.GroupResult{
		Outcome: engine.GroupOutcome(res.Outcome),
		Error:   res.Error,
	}
	// See the doc comment on the equivalent conversion in Execute below for
	// why res.Exits carries no outer-graph NodeIdx.
	if len(res.Exits) > 0 {
		result.Exits = make([]engine.GroupExitResult, len(res.Exits))
		for i, ex := range res.Exits {
			result.Exits[i] = engine.GroupExitResult{NodeName: ex.NodeName, Port: ex.Port, Data: ex.Data}
		}
	}
	return result, nil
}

// Execute runs the group subgraph defined by the lease's GroupPayload.
// It returns a GroupResult suitable for reporting to the control plane.
func (r *GroupRuntime) Execute(ctx context.Context, lease *engine.TaskLease) (engine.GroupResult, error) {
	if lease == nil || lease.GroupPayload == nil {
		return engine.GroupResult{Outcome: engine.GroupOutcomeFailed, Error: "nil lease or payload"}, nil
	}
	payload := lease.GroupPayload

	result, err := r.ExecuteRequest(ctx, subgraph.Request{
		Package:     payload.Package,
		PackageHash: payload.PackageHash,
		Input:       payload.Input,
		Deadline:    payload.Deadline,
	})
	if err != nil {
		return engine.GroupResult{}, err
	}
	// res.Exits carries no outer-graph NodeIdx (see graph.SubgraphExitResult's
	// doc) -- it was already always the zero value on this remote-runner path
	// even before this move: the collector never set it, and the wire type
	// (service/protocol.GroupExitResultWire) has no NodeIdx field to carry it
	// across anyway. CommitGroupResult (engine/group_lease.go) independently
	// recomputes the outer-graph node index by name once the result reaches
	// the control plane, so nothing here needs to reconstruct it.
	result.ProtocolVersion = payload.ProtocolVersion
	result.GroupExecID = payload.GroupExecID
	result.Attempt = lease.Attempt
	return result, nil
}
