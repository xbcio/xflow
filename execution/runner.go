package execution

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Runner executes task leases using a handler registry.
type Runner struct {
	registry           engine.HandlerRegistry
	pool               types.ResourcePool
	credentialResolver func(namespace namespace.Namespace, name string) map[string]any
	artifactCode       func(ctx context.Context, digest string) ([]byte, error)
}

// RunnerOption customizes a Runner.
type RunnerOption func(*Runner)

// WithResourcePool installs a ResourcePool. The Runner attaches it to the
// per-call context so resource-aware nodes (DatabaseNode, GRPCNode) can pool
// their connections. nil pool = no injection; resource-aware nodes will
// error at runtime.
func WithResourcePool(p types.ResourcePool) RunnerOption {
	return func(r *Runner) { r.pool = p }
}

// WithCredentialResolver installs a credential resolver that the Runner applies
// to each Input before invoking the handler. nil = no resolver; nodes calling
// input.Credential(name) will get nil (existing behavior). The resolver is a
// pure, idempotent closure keyed by namespace and credential name; it is applied to
// the shared lease.Input in place (same pattern as the parity test wrappers).
func WithCredentialResolver(fn func(namespace namespace.Namespace, name string) map[string]any) RunnerOption {
	return func(r *Runner) { r.credentialResolver = fn }
}

// WithArtifactCodeResolver installs a resolver that fetches script artifact
// bytes by content-addressable digest. The Runner applies it to each Input
// before invoking the handler so ScriptNode can load code from the artifact
// store instead of requiring it inline in parameters.
func WithArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) RunnerOption {
	return func(r *Runner) { r.artifactCode = fn }
}

// NewRunner creates an in-process task runner.
func NewRunner(registry engine.HandlerRegistry, opts ...RunnerOption) *Runner {
	r := &Runner{registry: registry}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Execute runs a task lease. The returned handled flag is false when the task
// requires engine-managed compatibility behavior.
func (r *Runner) Execute(ctx context.Context, lease *engine.TaskLease) (engine.TaskResult, error) {
	handler, err := r.registry.Get(
		types.ExecutionID(lease.Task.ExecutionID),
		lease.Task.NodeName,
		lease.NodeType,
		lease.NodeVersion,
	)
	if err != nil {
		return engine.TaskResult{}, err
	}
	if r.pool != nil {
		ctx = types.WithResourcePool(ctx, r.pool)
	}
	// Apply the credential resolver to the input the handler sees. This covers
	// both the non-suspending Execute path and the suspending path
	// (OnResume/PrepareSuspend). The resolver is a pure, idempotent closure;
	// applying it in place on the shared lease.Input mirrors what the parity
	// test wrappers do and is safe because the same resolver applies to every
	// call. cloneInputWithData preserves it via the shallow struct copy.
	if r.credentialResolver != nil && lease.Input != nil {
		lease.Input.SetNamespace(namespace.FromContext(ctx))
		lease.Input.SetCredentialResolver(r.credentialResolver)
	}
	if r.artifactCode != nil && lease.Input != nil {
		lease.Input.SetArtifactCodeResolver(r.artifactCode)
	}
	if sh, ok := handler.(types.SuspendingHandler); ok {
		return r.executeSuspending(ctx, lease, sh)
	}

	output, sysErr := handler.Execute(ctx, lease.Input)
	return engine.TaskResult{Output: output, Error: sysErr}, nil
}

func (r *Runner) executeSuspending(ctx context.Context, lease *engine.TaskLease, sh types.SuspendingHandler) (engine.TaskResult, error) {
	if lease.Task.Type == engine.TaskTypeNodeResume {
		output, err := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
		if err != nil {
			return engine.TaskResult{Output: output, Error: err}, nil
		}
		if output != nil && output.Resuspend {
			input := lease.Input
			if output.Data != nil {
				input = cloneInputWithData(lease.Input, output.Data)
			}
			spec, err := sh.PrepareSuspend(ctx, input)
			if err != nil {
				return engine.TaskResult{Output: output, Error: err}, nil
			}
			return engine.TaskResult{Output: output, Suspend: spec}, nil
		}
		return engine.TaskResult{Output: output}, nil
	}

	spec, err := sh.PrepareSuspend(ctx, lease.Input)
	if err != nil {
		return engine.TaskResult{Error: err}, nil
	}
	return engine.TaskResult{Suspend: spec}, nil
}

func cloneInputWithData(input *types.Input, data map[string]any) *types.Input {
	if input == nil {
		return &types.Input{Data: data}
	}
	cp := *input
	cp.Data = data
	return &cp
}
