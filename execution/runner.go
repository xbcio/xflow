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
	// Evaluate ${{ }} and {{ }} templates in non-exempt parameters. This runs
	// BEFORE the SuspendingHandler branch so both the normal Execute path and
	// the suspending path (PrepareSuspend / OnResume, which consume the same
	// lease.Input) see evaluated values. Credentials and supplies are already
	// resolved at this point (SetCredentialResolver above), so expressions
	// referencing $supplies resolve correctly.
	//
	// On failure: return the error wrapped in NodeFailure, so the dispatcher
	// commits it through the engine and the node's retry / on_error policy
	// applies. A plain (unclassified) error is NOT usable here: the dispatcher
	// reads one as ExecutorFailureUnknown and deliberately leaves the lease
	// fenced for its expiry path, because an unclassified failure might have
	// started the handler and releasing it could double-execute a side effect.
	// The boundary runs before the handler, so nothing started -- but the
	// dispatcher cannot know that from a bare error, and the execution simply
	// stalls. Measured with a syntactically invalid "${{ $params.x + }}" on a
	// local backend: handler invocations 0, status stuck at "running", no
	// terminal state (the LeaseSweeper that would eventually reclaim it is only
	// wired in the control plane, not in the local backend).
	//
	// NodeFailure is the honest-enough classification: it means "this task
	// failed and the engine owns the outcome". It does slightly overstate
	// things -- the handler never ran -- but every alternative is worse.
	// Marking the failure permanent would require deciding that an expression
	// can NEVER succeed, and no such discriminator exists here: compilation
	// sees env KEYS, and both the $input Data spread and the mutable $supplies
	// root change between attempts, so the same expression genuinely compiles
	// on a later attempt (measured: "bare_upstream_key + 1" fails to compile
	// before the upstream commits and compiles after). Retry policy gives the
	// bound instead -- a real typo exhausts its attempts and terminates, a
	// not-yet-available value gets the retries it needs.
	if lease.Input != nil {
		if err := evaluateParams(lease.Input, lease.NodeType); err != nil {
			return engine.TaskResult{}, NodeFailure(err)
		}
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
