package execution

import (
	"context"

	"github.com/xbcio/xflow/engine"
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
// executor result back through the engine.
func (d *Dispatcher) HandleTask(ctx context.Context, t *engine.Task) error {
	lease, err := d.engine.BuildTaskLease(ctx, t)
	if err != nil {
		if err == engine.ErrExecutionInactive {
			return nil
		}
		return err
	}

	result, err := d.executor.Execute(ctx, lease)
	if err != nil {
		return err
	}

	return d.engine.CommitTaskResult(ctx, lease, result)
}
