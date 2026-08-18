// Package execution is the reusable task-execution boundary that sits between
// the scheduler (engine) and the handler that performs the work. It occupies
// the centre-left of the main pipeline:
//
//	[engine] -- engine.Task --> [execution.Dispatcher] -- engine.TaskLease --> [Executor]
//	                                                                                |
//	                             engine.CommitTaskResult <-- engine.TaskResult <---+
//
// Upstream: engine produces tasks via its TaskQueue and hands them to
// Dispatcher.HandleTask. Downstream: Executor either runs the handler
// in-process (Runner) or forwards the lease through the remote runner protocol
// (service/protocol). Neither Dispatcher nor Runner imports redis, asynq, HTTP
// or any network package.
//
// # Core invariants
//
// Lease token fencing is the only commit credential. Dispatcher calls
// Engine.BuildTaskLease before executing and Engine.CommitTaskResult after.
// A lease that was never acquired cannot be committed; a stale token is silently
// rejected by the engine's Lua transition.
//
// ExecutorFailure classification governs what happens on error:
//   - ExecutorFailureNode   — handler ran and failed; committed through the engine
//     so node's retry/on_error policy applies.
//   - ExecutorFailureDispatch — delivery failed before execution began; the lease
//     is immediately released via Engine.ReleaseTaskLease.
//   - ExecutorFailurePermanentConfiguration — non-retryable setup error; committed
//     as a permanent failure (types.ErrPermanent), bypassing retry.
//   - ExecutorFailureUnknown — safe default; lease is left fenced for its normal
//     expiry/recovery path to avoid double-execution side effects.
//
// This package does NOT import redis, asynq, net/http, or any storage package.
// IO adapters belong in service/runner or service/control.
//
// # Key flow
//
//	Dispatcher.HandleTask(task)
//	  │
//	  ├─ Engine.BuildTaskLease        → engine.TaskLease (token + input + deadline)
//	  │
//	  ├─ Executor.Execute(lease)
//	  │    ├─ Runner (in-process)
//	  │    │    ├─ Registry.Get → handler (exec-scoped > name-scoped > type > global)
//	  │    │    ├─ evaluateParams (template expansion before handler sees input)
//	  │    │    ├─ nodeExecutionDeadline (lease deadline wins over Input.Timeout)
//	  │    │    └─ goroutine+select: deadline enforcement, abandonGrace (2ms)
//	  │    └─ remote Executor (service/protocol transport)
//	  │
//	  └─ Engine.CommitTaskResult(lease, result)
//
// # Traps for maintainers
//
// abandonGrace (2 ms) in runner.go is load-bearing for timing correctness.
// A cooperative handler that calls <-ctx.Done() then returns its verdict races
// against Execute returning a synthetic timeout error. The 2 ms window was
// measured to catch 2.5–49% of such completions depending on machine load
// (see the constant's doc comment). Shrinking it causes real verdicts to be
// discarded; removing it causes goroutine abandonment to never be diagnosed.
//
// reclassifyTimeout only reclassifies errors that ARE the cancellation echo
// (errors.Is(err, context.Canceled/DeadlineExceeded)). A handler that returns
// its own types.ClassifiedError while the context is already cancelled keeps
// its verdict verbatim — do not widen this check without re-reading the
// discriminator comment in runner.go.
//
// Registry.UnregisterExecution must be called after every sub-execution managed
// by execution/subgraph. Without it, execution-scoped entries registered by
// MapBodyExecutor (one per map item) accumulate unboundedly for the lifetime of
// the process. The defer is in subgraph.Executor.Execute, not in the caller.
//
// evaluateParams in runner.go runs before the handler and wraps failures as
// NodeFailure (not UnknownExecutionOutcome). An unclassified error here would
// leave the lease fenced for expiry recovery; NodeFailure routes it through the
// engine's retry/on_error policy, which is the correct bound.
package execution
