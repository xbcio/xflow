package execution

import (
	"context"
	"fmt"
	"time"

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

	// --- node-level deadline enforcement ---
	deadline := nodeExecutionDeadline(lease, time.Now())
	var budget time.Duration
	if lease.Input != nil {
		budget = lease.Input.Timeout
	}
	if !deadline.IsZero() {
		// An ALREADY-EXPIRED deadline must terminate the task, not wave it
		// through. Do NOT copy subgraph.go's `!IsZero() && After(now)` guard:
		// its second half means an expired deadline builds no context at all,
		// i.e. runs unbounded. A lease can legitimately arrive expired --
		// queueing, poll interval, and redelivery all sit between issue and
		// execution -- and running it unbounded is precisely the hole this
		// feature closes.
		if !deadline.After(time.Now()) {
			return engine.TaskResult{Error: newNodeTimeoutError(budget)}, nil
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	if sh, ok := handler.(types.SuspendingHandler); ok {
		return r.executeSuspending(ctx, lease, sh, budget, deadline)
	}

	if !deadline.IsZero() {
		// Run handler in a goroutine so a non-cooperative handler that ignores
		// ctx does not hold the slot indefinitely. If the deadline fires, we
		// return the timeout error and the goroutine leaks (Go cannot kill a
		// goroutine; the alternative -- waiting -- is the permanent slot
		// occupancy this exists to end).
		type handlerResult struct {
			output *types.Output
			err    error
		}
		ch := make(chan handlerResult, 1)
		go func() {
			output, sysErr := handler.Execute(ctx, lease.Input)
			ch <- handlerResult{output, sysErr}
		}()

		select {
		case hr := <-ch:
			hr.err = reclassifyTimeout(ctx, hr.err, budget, deadline)
			return engine.TaskResult{Output: hr.output, Error: hr.err}, nil
		case <-ctx.Done():
			// Deadline fired before handler returned.
			return engine.TaskResult{Error: newNodeTimeoutError(budget)}, nil
		}
	}

	output, sysErr := handler.Execute(ctx, lease.Input)
	return engine.TaskResult{Output: output, Error: sysErr}, nil
}

func (r *Runner) executeSuspending(ctx context.Context, lease *engine.TaskLease, sh types.SuspendingHandler, budget time.Duration, deadline time.Time) (engine.TaskResult, error) {
	if lease.Task.Type == engine.TaskTypeNodeResume {
		output, err := r.callOnResume(ctx, sh, lease, budget, deadline)
		if err != nil {
			return engine.TaskResult{Output: output, Error: err}, nil
		}
		if output != nil && output.Resuspend {
			input := lease.Input
			if output.Data != nil {
				input = cloneInputWithData(lease.Input, output.Data)
			}
			spec, prepErr := r.callPrepareSuspend(ctx, sh, input, budget, deadline)
			if prepErr != nil {
				return engine.TaskResult{Output: output, Error: prepErr}, nil
			}
			return engine.TaskResult{Output: output, Suspend: spec}, nil
		}
		return engine.TaskResult{Output: output}, nil
	}

	spec, err := r.callPrepareSuspend(ctx, sh, lease.Input, budget, deadline)
	if err != nil {
		return engine.TaskResult{Error: err}, nil
	}
	return engine.TaskResult{Suspend: spec}, nil
}

// callOnResume wraps sh.OnResume in a goroutine+select when a deadline is set,
// so a non-cooperative handler does not hold the slot.
func (r *Runner) callOnResume(ctx context.Context, sh types.SuspendingHandler, lease *engine.TaskLease, budget time.Duration, deadline time.Time) (*types.Output, error) {
	if !deadline.IsZero() {
		type result struct {
			output *types.Output
			err    error
		}
		ch := make(chan result, 1)
		go func() {
			o, e := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
			ch <- result{o, e}
		}()
		select {
		case r := <-ch:
			r.err = reclassifyTimeout(ctx, r.err, budget, deadline)
			return r.output, r.err
		case <-ctx.Done():
			return nil, newNodeTimeoutError(budget)
		}
	}
	output, err := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
	return output, err
}

// callPrepareSuspend wraps sh.PrepareSuspend in a goroutine+select when a
// deadline is set.
func (r *Runner) callPrepareSuspend(ctx context.Context, sh types.SuspendingHandler, input *types.Input, budget time.Duration, deadline time.Time) (*types.SuspendSpec, error) {
	if !deadline.IsZero() {
		type result struct {
			spec *types.SuspendSpec
			err  error
		}
		ch := make(chan result, 1)
		go func() {
			s, e := sh.PrepareSuspend(ctx, input)
			ch <- result{s, e}
		}()
		select {
		case r := <-ch:
			r.err = reclassifyTimeout(ctx, r.err, budget, deadline)
			return r.spec, r.err
		case <-ctx.Done():
			return nil, newNodeTimeoutError(budget)
		}
	}
	return sh.PrepareSuspend(ctx, input)
}

func cloneInputWithData(input *types.Input, data map[string]any) *types.Input {
	if input == nil {
		return &types.Input{Data: data}
	}
	cp := *input
	cp.Data = data
	return &cp
}

// nodeExecutionDeadline picks the effective absolute deadline for one handler
// invocation. The lease's stamped deadline wins: it is anchored at issue time,
// so dispatch latency counts against the budget rather than resetting it. Input
// .Timeout is the fallback for a lease built by an older control plane that
// does not stamp one.
func nodeExecutionDeadline(lease *engine.TaskLease, now time.Time) time.Time {
	if !lease.ExecutionDeadline.IsZero() {
		return lease.ExecutionDeadline
	}
	if lease.Input != nil && lease.Input.Timeout > 0 {
		return now.Add(lease.Input.Timeout)
	}
	return time.Time{}
}

// newNodeTimeoutError builds the terminal error for an execution that ran past
// its deadline. It is Permanent so the retry short-circuit in
// engine/atomic_commit.go declines to re-run it and the queue layers
// (memory_queue, asynq/retry) decline to redeliver it: a timeout is not a
// transient failure to be retried, it is a verdict.
//
// The message carries the CONFIGURED budget and nothing else. Not the absolute
// deadline (runtime data), and never node output, Input, or params: an upstream
// node's output is routinely an HTTP response body carrying a token.
func newNodeTimeoutError(budget time.Duration) error {
	if budget <= 0 {
		return types.NewPermanentError("node.timeout",
			"node execution exceeded its deadline")
	}
	return types.NewPermanentError("node.timeout",
		fmt.Sprintf("node execution exceeded its %s timeout", budget))
}

// reclassifyTimeout replaces err with a permanent timeout error when the
// context's deadline fired. A cooperative handler surfaces ctx.Err() as its own
// error -- which is not Permanent and would be retried. This reclassifies it so
// the queue layers decline to redeliver.
//
// No-ops: err == nil, ctx has no error, deadline was never set (zero).
func reclassifyTimeout(ctx context.Context, err error, budget time.Duration, deadline time.Time) error {
	if err == nil {
		return nil
	}
	if deadline.IsZero() {
		return err
	}
	if ctx.Err() == nil {
		return err
	}
	return newNodeTimeoutError(budget)
}
