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

// WithGroupArtifactCodeResolver installs the digest -> script bytes resolver on
// every per-attempt inner backend, so a member node that is an xflow.script
// referencing an artifact_digest can fetch its code.
//
// Without it the resolver chain ends at the group boundary: Config.
// ArtifactCodeResolver reaches only the runner's top-level dispatcher
// (runner.go, execution.WithArtifactCodeResolver), never the embedded backend
// this runtime builds per attempt. A member script then reads a nil resolver
// (types.Input.ArtifactCode returns nil, nil) and fails permanently with
// script.artifact_unavailable — which is exactly the shape a node group whose
// members are wasm scripts takes.
func WithGroupArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) GroupRuntimeOption {
	return func(r *GroupRuntime) { r.artifactCode = fn }
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
	artifactCode    func(ctx context.Context, digest string) ([]byte, error)
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
		backendOpts := []local.Option{local.WithRegistry(reg), local.WithConcurrency(1)}
		// Read r.artifactCode inside the closure, not at construction: the
		// closure is what every attempt (and every nested map body, which reuses
		// this same executor) calls, so the resolver must travel with it.
		if r.artifactCode != nil {
			backendOpts = append(backendOpts, local.WithArtifactCodeResolver(r.artifactCode))
		}
		return local.New(backendOpts...)
	})
	return r
}

// ExecuteSubgraph runs req against the underlying executor and returns the
// executor's OWN result, undegraded.
//
// ExecuteRequest below is this plus a mapping onto engine.GroupResult, which is
// the control plane's wire shape and therefore carries only what the wire
// carries. A caller that stays in this process — the trigger-group's per-Kafka-
// batch path — reads this instead, so a member's failure classification
// (subgraph.Result.Permanent) reaches it rather than being dropped at a
// conversion it never needed.
func (r *GroupRuntime) ExecuteSubgraph(ctx context.Context, req subgraph.Request) (subgraph.Result, error) {
	// r.suspendDisabled is a floor, not an override: a caller that already
	// wants suspend disabled (e.g. the group-exec trigger adapter, which
	// always sets this true) keeps that; a caller relying on the runtime's
	// own construction-time setting inherits it too.
	req.SuspendDisabled = req.SuspendDisabled || r.suspendDisabled
	return r.executor.Execute(ctx, req)
}

// ExecuteRequest runs req directly against the underlying subgraph executor
// and maps the result to engine.GroupResult, without unwrapping an
// engine.TaskLease. This is the entry point for callers that have no lease at
// all. Execute (below) is now a thin wrapper over this for the lease-bearing
// callers (the batch task-queue path, runner.go:363).
func (r *GroupRuntime) ExecuteRequest(ctx context.Context, req subgraph.Request) (engine.GroupResult, error) {
	res, err := r.ExecuteSubgraph(ctx, req)
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
