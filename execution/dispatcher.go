package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Engine is the scheduler surface required by the task execution boundary.
type Engine interface {
	BuildTaskLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, error)
	CommitTaskResult(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) error
	TaskRouting(ctx context.Context, t *engine.Task) (engine.TaskRouting, error)
	ReclaimLease(ctx context.Context, lease engine.ExpiredLease) (bool, error)
}

// Executor runs a leased task. Implementations may execute in-process or send
// the lease through a remote runner protocol.
type Executor interface {
	Execute(ctx context.Context, lease *engine.TaskLease) (engine.TaskResult, error)
}

// ErrLeaseReleaseUnsupported means a custom Engine implementation predates the
// immediate dispatch-failure recovery capability. The dispatcher leaves the
// lease fenced in this case rather than risking duplicate execution.
var ErrLeaseReleaseUnsupported = errors.New("engine does not support immediate task lease release")

type taskLeaseReleaser interface {
	ReleaseTaskLease(ctx context.Context, lease *engine.TaskLease) (bool, error)
}

// ExecutorFailureKind describes what is known about an Executor error. The
// dispatcher deliberately treats an unclassified error as an unknown outcome:
// the handler may have started, so releasing its lease could execute it twice.
type ExecutorFailureKind string

const (
	// ExecutorFailureUnknown means the caller cannot determine whether execution
	// began. This is also the safe default for plain errors.
	ExecutorFailureUnknown ExecutorFailureKind = "unknown"
	// ExecutorFailureNode means the handler ran and failed. The failure is
	// committed through the Engine so the node's error/retry policy applies.
	ExecutorFailureNode ExecutorFailureKind = "node"
	// ExecutorFailureDispatch means delivery failed before execution began. The
	// dispatcher can immediately release and requeue the fenced lease.
	ExecutorFailureDispatch ExecutorFailureKind = "dispatch"
	// ExecutorFailurePermanentConfiguration means the task cannot run with the
	// current configuration and should bypass retry policy.
	ExecutorFailurePermanentConfiguration ExecutorFailureKind = "permanent_configuration"
)

// ExecutorFailure attaches a durable execution-outcome classification to an
// Executor error. Remote executors must use DispatchFailure only when they can
// prove the runner did not receive or start the lease.
type ExecutorFailure struct {
	Kind ExecutorFailureKind
	Err  error
}

func (f *ExecutorFailure) Error() string {
	if f == nil {
		return "<nil executor failure>"
	}
	prefix := "executor " + string(f.Kind) + " failure"
	if f.Err == nil {
		return prefix
	}
	return prefix + ": " + f.Err.Error()
}

// Unwrap exposes the original executor error to errors.Is/errors.As.
func (f *ExecutorFailure) Unwrap() error {
	if f == nil {
		return nil
	}
	return f.Err
}

// NodeFailure reports a handler failure after execution began.
func NodeFailure(err error) *ExecutorFailure {
	return &ExecutorFailure{Kind: ExecutorFailureNode, Err: err}
}

// DispatchFailure reports a failure before the lease reached any executor.
func DispatchFailure(err error) *ExecutorFailure {
	return &ExecutorFailure{Kind: ExecutorFailureDispatch, Err: err}
}

// PermanentConfigurationFailure reports a non-retryable executor setup or
// capability error. It is committed as a permanent node failure.
func PermanentConfigurationFailure(err error) *ExecutorFailure {
	return &ExecutorFailure{Kind: ExecutorFailurePermanentConfiguration, Err: err}
}

// UnknownExecutionOutcome explicitly marks a result whose execution state is
// not known. It has the same safe behavior as an unwrapped Executor error.
func UnknownExecutionOutcome(err error) *ExecutorFailure {
	return &ExecutorFailure{Kind: ExecutorFailureUnknown, Err: err}
}

// ClassifyExecutorFailure returns the structured failure kind carried by err.
// Invalid or absent classifications conservatively become unknown outcomes.
func ClassifyExecutorFailure(err error) ExecutorFailureKind {
	var failure *ExecutorFailure
	if !errors.As(err, &failure) || failure == nil {
		return ExecutorFailureUnknown
	}
	switch failure.Kind {
	case ExecutorFailureNode, ExecutorFailureDispatch, ExecutorFailurePermanentConfiguration:
		return failure.Kind
	default:
		return ExecutorFailureUnknown
	}
}

// Dispatcher converts queued scheduler tasks into runner leases and commits
// completed results through the engine.
type Dispatcher struct {
	engine   Engine
	executor Executor
}

// NewDispatcher creates a dispatcher with an explicit task executor.
func NewDispatcher(eng Engine, executor Executor) *Dispatcher {
	return &Dispatcher{
		engine:   eng,
		executor: executor,
	}
}

// NewEmbeddedDispatcher creates a dispatcher backed by an in-process runner.
// RunnerOptions forward to the embedded Runner (e.g. WithResourcePool).
func NewEmbeddedDispatcher(eng Engine, registry engine.HandlerRegistry, opts ...RunnerOption) *Dispatcher {
	return NewDispatcher(eng, NewRunner(registry, opts...))
}

// HandleTask converts a queued engine task into a runner lease and commits the
// executor result back through the engine. It only releases a lease immediately
// when an Executor explicitly proves dispatch failed before execution began.
func (d *Dispatcher) HandleTask(ctx context.Context, t *engine.Task) error {
	lease, err := d.engine.BuildTaskLease(ctx, t)
	if err != nil {
		if errors.Is(err, engine.ErrSystemTaskHandled) || errors.Is(err, engine.ErrExecutionInactive) || errors.Is(err, engine.ErrLeaseAlreadyActive) {
			return nil
		}
		return err
	}

	result, executeErr := d.executor.Execute(ctx, lease)
	if executeErr == nil {
		return d.engine.CommitTaskResult(ctx, lease, result)
	}

	switch ClassifyExecutorFailure(executeErr) {
	case ExecutorFailureNode:
		result.Error = joinTaskErrors(result.Error, executeErr)
		return d.engine.CommitTaskResult(ctx, lease, result)
	case ExecutorFailurePermanentConfiguration:
		result.Error = errors.Join(types.ErrPermanent, joinTaskErrors(result.Error, executeErr))
		return d.engine.CommitTaskResult(ctx, lease, result)
	case ExecutorFailureDispatch:
		releaser, ok := d.engine.(taskLeaseReleaser)
		if !ok {
			return errors.Join(executeErr, ErrLeaseReleaseUnsupported)
		}
		if _, releaseErr := releaser.ReleaseTaskLease(ctx, lease); releaseErr != nil {
			return errors.Join(executeErr, fmt.Errorf("release task lease after dispatch failure: %w", releaseErr))
		}
		return nil
	default:
		// The executor may have started the handler but lost its response. Leave
		// the lease fenced and let its normal expiry/recovery path decide what
		// runs next rather than risking concurrent side effects.
		return executeErr
	}
}

func joinTaskErrors(existing, next error) error {
	if existing == nil {
		return next
	}
	return errors.Join(existing, next)
}
