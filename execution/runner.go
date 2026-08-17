package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// abandonGrace bounds how long the ctx.Done() path waits for a handler that
// has already unblocked to deliver its result before abandoning it. A handler
// blocked on a ctx-aware final IO unblocks on cancellation and returns a
// verdict it computed earlier; without a wait, that verdict lands in the
// buffered channel after Execute has already returned a synthesized error, and
// is discarded -- measured at 2.5% of runs on one machine, up to ~49% on a
// loaded one. This cannot be closed completely: a bounded wait cannot
// distinguish a handler that returns in a microsecond from one that never
// returns. It is bounded by a constant, never by the handler, so the
// slot-release guarantee this mechanism exists to provide still holds.
const abandonGrace = 2 * time.Millisecond

// TimeoutObserver receives node execution timeout and duration events from the
// bounded-handler path. Implementations MUST be non-blocking and MUST avoid
// high-cardinality labels: the only label dimensions are node_type and source
// — never node name, execution ID, params, or any node output (an upstream
// node's output is routinely an HTTP response body carrying a token).
//
// This is the runner-side mirror of the engine.Hooks surface: the runner owns
// the goroutine+select that detects a handler exceeding its deadline, so it owns
// the events. The concrete adapter lives in observability/metrics.
type TimeoutObserver interface {
	// OnNodeExecutionTimeout records a terminal execution timeout. source is
	// "runner" when the runner detected it (this package) or "server" when the
	// control plane's lease-renewal backstop did.
	OnNodeExecutionTimeout(ctx context.Context, nodeType, source string)
	// OnHandlerAbandoned adjusts the abandoned gauge by delta (+1 when a handler
	// is left running past its deadline, -1 when it finally returns). The gauge
	// must be able to fall back to zero: a persistently non-zero value means a
	// node type ignores ctx and is leaking goroutines.
	OnHandlerAbandoned(ctx context.Context, nodeType string, delta float64)
	// OnHandlerDuration records the wall-clock duration of one handler
	// invocation.
	OnHandlerDuration(ctx context.Context, nodeType string, elapsed time.Duration)
}

// noopTimeoutObserver discards all events. It is the zero value of Runner so
// production wiring stays optional and a nil observer is safe.
type noopTimeoutObserver struct{}

func (noopTimeoutObserver) OnNodeExecutionTimeout(context.Context, string, string) {}
func (noopTimeoutObserver) OnHandlerAbandoned(context.Context, string, float64)     {}
func (noopTimeoutObserver) OnHandlerDuration(context.Context, string, time.Duration) {}

// Runner executes task leases using a handler registry.
type Runner struct {
	registry           engine.HandlerRegistry
	pool               types.ResourcePool
	credentialResolver func(namespace namespace.Namespace, name string) map[string]any
	artifactCode       func(ctx context.Context, digest string) ([]byte, error)
	timeoutObserver    TimeoutObserver
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

// WithTimeoutObserver installs an observer for node execution timeout and
// duration events. nil or unset leaves the runner with a no-op observer so
// existing behaviour (no metrics) is byte-identical. The observer must be
// non-blocking and avoid high-cardinality labels (see TimeoutObserver).
func WithTimeoutObserver(obs TimeoutObserver) RunnerOption {
	return func(r *Runner) {
		if obs != nil {
			r.timeoutObserver = obs
		}
	}
}

// NewRunner creates an in-process task runner.
func NewRunner(registry engine.HandlerRegistry, opts ...RunnerOption) *Runner {
	r := &Runner{registry: registry, timeoutObserver: noopTimeoutObserver{}}
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
			r.observeTimeout(ctx, lease.NodeType)
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
		started := time.Now()
		go func() {
			// Record the wall-clock duration of this invocation when the handler
			// returns, regardless of outcome: a successful run, a cooperative
			// ctx.Err, and an abandoned-but-eventually-returning handler all
			// land here. The abandon case records asynchronously (after Execute
			// returns), which is honest -- the handler's true duration is only
			// known once it finishes. The closure captures started so
			// time.Since runs at defer execution, not at registration.
			defer func() { r.observeDuration(ctx, lease.NodeType, time.Since(started)) }()
			output, sysErr := handler.Execute(ctx, lease.Input)
			ch <- handlerResult{output, sysErr}
		}()

		select {
		case hr := <-ch:
			hr.err = reclassifyTimeout(ctx, hr.err, budget, deadline)
			if isNodeTimeoutError(hr.err) {
				r.observeTimeout(ctx, lease.NodeType)
			}
			return engine.TaskResult{Output: hr.output, Error: hr.err}, nil
		case <-ctx.Done():
			// Deadline fired before handler returned. Check once more: the
			// handler may have raced to completion at the same instant. A
			// handler blocked on a ctx-aware final IO unblocks on cancellation
			// and returns a verdict it computed earlier; abandonGrace bounds
			// how long we wait for that verdict before abandoning the goroutine.
			// Beyond testability, this has independent correctness value:
			// when the handler and deadline complete at the same instant,
			// using the handler's real result (with reclassification) is
			// preferable to a synthetic timeout error — it preserves any
			// partial output the handler produced.
			select {
			case hr := <-ch:
				hr.err = reclassifyTimeout(ctx, hr.err, budget, deadline)
				if isNodeTimeoutError(hr.err) {
					r.observeTimeout(ctx, lease.NodeType)
				}
				return engine.TaskResult{Output: hr.output, Error: hr.err}, nil
			case <-time.After(abandonGrace):
			}
			// Abandon: the handler goroutine is still running and Go cannot kill
			// it. The slot is released (Execute returns), the terminal error is
			// recorded, and the abandoned gauge is incremented. A watcher
			// goroutine waits on ch (buffered, size 1) so that when the leaked
			// handler finally writes its result and exits, the gauge decrements
			// back toward zero. A persistently non-zero gauge is the operational
			// signal that some node type ignores ctx and is leaking goroutines.
			//
			// The cause distinguishes a genuine deadline expiry (permanent
			// node.timeout, metric fires) from a parent cancellation (transient
			// node.cancelled, metric does NOT fire). The abandoned gauge
			// increments either way: a handler that ignores ctx leaks a
			// goroutine regardless of why its context was done.
			cause := classifyCancelCause(ctx, deadline)
			if cause == cancelTimeout {
				r.observeTimeout(ctx, lease.NodeType)
			}
			r.observeAbandoned(ctx, lease.NodeType, 1)
			go func() { <-ch; r.observeAbandoned(ctx, lease.NodeType, -1) }()
			return engine.TaskResult{Error: newCancelError(cause, budget)}, nil
		}
	}

	started := time.Now()
	output, sysErr := handler.Execute(ctx, lease.Input)
	r.observeDuration(ctx, lease.NodeType, time.Since(started))
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
			spec, prepErr := r.callPrepareSuspend(ctx, sh, input, lease.NodeType, budget, deadline)
			if prepErr != nil {
				return engine.TaskResult{Output: output, Error: prepErr}, nil
			}
			return engine.TaskResult{Output: output, Suspend: spec}, nil
		}
		return engine.TaskResult{Output: output}, nil
	}

	spec, err := r.callPrepareSuspend(ctx, sh, lease.Input, lease.NodeType, budget, deadline)
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
		started := time.Now()
		go func() {
			defer func() { r.observeDuration(ctx, lease.NodeType, time.Since(started)) }()
			o, e := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
			ch <- result{o, e}
		}()
		select {
		case res := <-ch:
			res.err = reclassifyTimeout(ctx, res.err, budget, deadline)
			if isNodeTimeoutError(res.err) {
				r.observeTimeout(ctx, lease.NodeType)
			}
			return res.output, res.err
		case <-ctx.Done():
			select {
			case res := <-ch:
				res.err = reclassifyTimeout(ctx, res.err, budget, deadline)
				if isNodeTimeoutError(res.err) {
					r.observeTimeout(ctx, lease.NodeType)
				}
				return res.output, res.err
			case <-time.After(abandonGrace):
			}
			cause := classifyCancelCause(ctx, deadline)
			if cause == cancelTimeout {
				r.observeTimeout(ctx, lease.NodeType)
			}
			r.observeAbandoned(ctx, lease.NodeType, 1)
			go func() { <-ch; r.observeAbandoned(ctx, lease.NodeType, -1) }()
			return nil, newCancelError(cause, budget)
		}
	}
	output, err := sh.OnResume(ctx, lease.Input, lease.Task.Payload)
	return output, err
}

// callPrepareSuspend wraps sh.PrepareSuspend in a goroutine+select when a
// deadline is set. nodeType is the resolved node type from the lease, passed in
// so the timeout/duration metrics carry the same node_type label the runner
// path uses (PrepareSuspend's input may be a cloned Input without the type).
func (r *Runner) callPrepareSuspend(ctx context.Context, sh types.SuspendingHandler, input *types.Input, nodeType string, budget time.Duration, deadline time.Time) (*types.SuspendSpec, error) {
	if !deadline.IsZero() {
		type result struct {
			spec *types.SuspendSpec
			err  error
		}
		ch := make(chan result, 1)
		started := time.Now()
		go func() {
			defer func() { r.observeDuration(ctx, nodeType, time.Since(started)) }()
			s, e := sh.PrepareSuspend(ctx, input)
			ch <- result{s, e}
		}()
		select {
		case res := <-ch:
			res.err = reclassifyTimeout(ctx, res.err, budget, deadline)
			if isNodeTimeoutError(res.err) {
				r.observeTimeout(ctx, nodeType)
			}
			return res.spec, res.err
		case <-ctx.Done():
			select {
			case res := <-ch:
				res.err = reclassifyTimeout(ctx, res.err, budget, deadline)
				if isNodeTimeoutError(res.err) {
					r.observeTimeout(ctx, nodeType)
				}
				return res.spec, res.err
			case <-time.After(abandonGrace):
			}
			cause := classifyCancelCause(ctx, deadline)
			if cause == cancelTimeout {
				r.observeTimeout(ctx, nodeType)
			}
			r.observeAbandoned(ctx, nodeType, 1)
			go func() { <-ch; r.observeAbandoned(ctx, nodeType, -1) }()
			return nil, newCancelError(cause, budget)
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

// observeTimeout records a runner-detected terminal execution timeout. The
// label set is exactly {node_type, source="runner"} -- never node name,
// execution ID, params, or output. nil-receiver-safe via the noop default.
func (r *Runner) observeTimeout(ctx context.Context, nodeType string) {
	if r == nil {
		return
	}
	r.timeoutObserver.OnNodeExecutionTimeout(ctx, nodeType, "runner")
}

// observeAbandoned raises the abandoned gauge by delta. +1 when a handler is
// left running past its deadline; -1 when it finally returns. The companion
// watcher goroutine in the abandon branch calls this with -1 once the leaked
// handler writes to its result channel, so the gauge can fall back to zero.
func (r *Runner) observeAbandoned(ctx context.Context, nodeType string, delta float64) {
	if r == nil {
		return
	}
	r.timeoutObserver.OnHandlerAbandoned(ctx, nodeType, delta)
}

// observeDuration records the wall-clock duration of one handler invocation.
func (r *Runner) observeDuration(ctx context.Context, nodeType string, elapsed time.Duration) {
	if r == nil {
		return
	}
	r.timeoutObserver.OnHandlerDuration(ctx, nodeType, elapsed)
}

// cancelCause classifies why a handler's context is done, distinguishing a
// genuine execution-deadline expiry from a parent cancellation (lease fenced,
// renewal exhausted, execution cancelled, runner shutdown). The distinction is
// load-bearing: a deadline that fired is a verdict (permanent, do not retry);
// a parent cancellation is a preemption (the server will redeliver, so a
// permanent error would suppress the retry the redelivery depends on).
type cancelCause int

const (
	cancelNone cancelCause = iota
	// cancelTimeout means the node's own execution deadline elapsed. This is a
	// terminal verdict: the task consumed its full budget and must not retry.
	cancelTimeout
	// cancelCancelled means the parent context was cancelled before the deadline
	// elapsed (lease fenced/lost, renewal failed MaxRetries times, execution
	// cancelled, or runner shutting down). The work was interrupted, not
	// over-budget — the server redelivers, so this is transient.
	cancelCancelled
)

// classifyCancelCause inspects ctx.Err() against the configured deadline. The
// deadline argument is a second signal that resolves the racy case where both
// causes are present.
//
// context.WithDeadline(parent, d) reports ctx.Err() == context.Canceled when
// the parent is cancelled, even if the deadline instant d has since passed —
// whichever canceller fires first wins the Err() value. So when ctx.Err() is
// Canceled we consult the clock: if the deadline instant has already elapsed,
// the node WAS over budget and the parent cancellation merely won the race to
// set Err(); we treat it as a genuine timeout (the honest verdict). Only a
// cancellation that arrives while the deadline is still in the future is a
// true preemption.
func classifyCancelCause(ctx context.Context, deadline time.Time) cancelCause {
	if deadline.IsZero() {
		return cancelNone
	}
	err := ctx.Err()
	if err == nil {
		return cancelNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return cancelTimeout
	}
	// ctx.Err() is context.Canceled (or a wrapped form). If the deadline has
	// elapsed, the budget was genuinely exhausted — classify as a timeout.
	if !time.Now().Before(deadline) {
		return cancelTimeout
	}
	return cancelCancelled
}

// isNodeTimeoutError reports whether err is the permanent node.timeout error
// produced by reclassifyTimeout / the abandon path. It gates the
// xflow_node_timeouts_total metric so the counter fires exactly when a terminal
// timeout error is produced — not when the deadline fired but the handler
// succeeded (reclassify is a no-op on nil err), and not when a parent
// cancellation produced a transient node.cancelled error.
func isNodeTimeoutError(err error) bool {
	var ce *types.ClassifiedError
	return errors.As(err, &ce) && ce.Code == "node.timeout"
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

// newNodeCancelledError builds a transient error for a handler whose context
// was cancelled by its parent before the execution deadline elapsed — i.e. the
// runner lost the lease (fenced, or renewal failed MaxRetries times), the
// execution was cancelled, or the runner is shutting down.
//
// It is transient (NOT permanent) on purpose: a permanent error would make the
// retry short-circuit in engine/atomic_commit.go decline to re-run the node and
// the queue layers decline to redeliver it. But a fenced/lost lease is exactly
// the case the redelivery path exists for — the server hands the lease to
// another runner, which produces the authoritative result. Marking the losing
// runner's interruption permanent would kill the retry the redelivery depends
// on, which is the regression this fixes.
//
// A transient ClassifiedError (rather than passing the bare ctx.Err() through)
// carries a stable code across the wire so the protocol and the commit
// classification (buildEffectiveClassification) see a retryable system error
// instead of an unclassified cancellation that the commit path reports as
// ErrorSourceUnclassified.
//
// The message carries no budget: a cancellation is not a timeout and the
// configured budget is irrelevant. It also carries no runtime data (absolute
// deadline, node output, params) — an upstream node's output is routinely an
// HTTP response body carrying a token.
func newNodeCancelledError() error {
	return types.NewTransientError("node.cancelled",
		"node execution cancelled before its deadline (lease lost or parent context cancelled)")
}

// newCancelError returns the terminal error for a handler whose context is
// done, keyed on the cancel cause: a permanent node.timeout when the deadline
// elapsed, a transient node.cancelled when the parent cancelled first.
func newCancelError(cause cancelCause, budget time.Duration) error {
	if cause == cancelTimeout {
		return newNodeTimeoutError(budget)
	}
	return newNodeCancelledError()
}

// reclassifyTimeout replaces err with the appropriate terminal error when the
// handler's error IS the cancellation itself, merely echoed back by a
// cooperative handler. A cooperative handler that does
// `<-ctx.Done(); return ctx.Err()` surfaces a bare context.Canceled /
// context.DeadlineExceeded, which is neither Permanent nor a stable
// ClassifiedError, so the retry/queue/commit layers mishandle it (the commit
// path reports it as ErrorSourceUnclassified and fences the lease for its
// expiry path). Reclassifying it to a stable node.timeout / node.cancelled
// ClassifiedError fixes that.
//
// Discriminator (a) vs (b):
//
//   - (a) the handler's error IS the cancellation, merely echoed -> reclassify.
//     Detected by errors.Is(err, context.Canceled) || errors.Is(err,
//     context.DeadlineExceeded). This matches:
//       * the bare sentinel (ctx.Err() returned directly)
//       * a %w-wrapped form (fmt.Errorf("node X: %w", ctx.Err()))
//   - (b) the handler's error is its OWN verdict, produced while the ctx was
//     cancelled -> PRESERVED verbatim. A *types.ClassifiedError never matches
//     the errors.Is probe: its Is() only returns true for ErrPermanent. So a
//     handler that computed types.NewPermanentError("biz.invalid_account",
//     "account is closed") keeps its verdict. A permanent business verdict
//     must not become a retryable node.cancelled, and retrying will never make
//     the account open. This holds symmetrically for cancelTimeout: a genuine
//     business verdict produced at/after the deadline instant is more truthful
//     and actionable for operators than the generic budget message, and a
//     handler that returned a verdict is not hung, so retry policy is
//     unaffected. The budget verdict is the platform's; the business verdict
//     is the node's, and the node's is more specific.
//
// Edge case: a handler that formats ctx.Err() with %v instead of %w
// (fmt.Errorf("...: %v", ctx.Err())) breaks the errors.Is chain and is NOT
// detected as an echo, so it is preserved as-is. This is acceptable: such a
// handler violates Go's error-wrapping convention, and the preserved error is
// treated the same as any other unclassified error the handler might return
// on the no-deadline path (cancelNone -> pass-through). The reclassify safety
// net is best-effort for cooperative handlers that follow the convention.
//
// Note on the preserved error string: when (b) preserves the handler's error,
// it preserves a string the runner did not author. This is the SAME behaviour
// as the no-deadline path (cancelNone returns err verbatim) and the pre-fix
// code; it is not introduced by this change. A handler that embeds node
// output / credentials in its error message violates security policy §7
// (handlers must not leak tokens/output into error strings) regardless of
// reclassification — the runner cannot sanitise an arbitrary handler string,
// and the budget-only node.timeout/node.cancelled messages it DOES author
// carry no runtime data.
//
// No-ops: err == nil (the handler succeeded — even if the deadline fired at
// the same instant, success is preserved and no timeout metric fires), or no
// deadline was set (zero deadline -> cancelNone -> err passes through).
func reclassifyTimeout(ctx context.Context, err error, budget time.Duration, deadline time.Time) error {
	if err == nil {
		return nil
	}
	cause := classifyCancelCause(ctx, deadline)
	if cause == cancelNone {
		return err
	}
	// Only reclassify when the handler's error IS the cancellation echo. A
	// handler's own verdict (any non-echo error, including a
	// *types.ClassifiedError business verdict) is preserved verbatim.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return newCancelError(cause, budget)
	}
	return err
}
