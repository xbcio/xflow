package control

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/engine"
)

// ErrNoMatchingRunner indicates no registered runner advertises the lease's
// node type. The task will be retried by the queue layer — adding a runner
// resolves the condition.
var ErrNoMatchingRunner = errors.New("no runner registered for task type")

// ErrNoCapacity indicates a capable runner exists but every runner is already
// at its concurrency limit. The task will be retried by the queue layer once
// any runner reports headroom.
var ErrNoCapacity = errors.New("no runner has capacity for task lease")

// ErrNoRunnerAvailable is kept for backwards compatibility with the
// HTTP/gRPC error mapping. New code should branch on the specific sentinels.
var ErrNoRunnerAvailable = ErrNoMatchingRunner

type LeaseBuilder interface {
	TaskRouting(ctx context.Context, t *engine.Task) (engine.TaskRouting, error)
	BuildTaskLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, error)
}

type Dispatcher struct {
	engine   LeaseBuilder
	runners  *RunnerPool
	observer DispatcherObserver
}

// DispatcherObserver receives retryable dispatch placement failures.
type DispatcherObserver interface {
	OnDispatchTransient(reason string)
}

// DispatcherOption configures a Dispatcher.
type DispatcherOption func(*Dispatcher)

// WithDispatcherObserver installs a non-blocking observer for placement
// failures that should be retried by the queue layer.
func WithDispatcherObserver(observer DispatcherObserver) DispatcherOption {
	return func(d *Dispatcher) {
		d.observer = observer
	}
}

func NewDispatcher(engine LeaseBuilder, runners *RunnerPool, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		engine:  engine,
		runners: runners,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *Dispatcher) observeTransient(err error) {
	if d.observer == nil {
		return
	}
	switch {
	case errors.Is(err, ErrNoCapacity):
		d.observer.OnDispatchTransient("no_capacity")
	case errors.Is(err, ErrNoMatchingRunner):
		d.observer.OnDispatchTransient("no_matching_runner")
	}
}

func (d *Dispatcher) HandleTask(ctx context.Context, task *engine.Task) error {
	routing, err := d.engine.TaskRouting(ctx, task)
	if err != nil {
		if err == engine.ErrExecutionInactive {
			return nil
		}
		return err
	}
	if d.runners == nil {
		err := ErrNoMatchingRunner
		d.observeTransient(err)
		return err
	}
	if err := d.runners.AssignRouted(routing, func() (*engine.TaskLease, error) {
		return d.engine.BuildTaskLease(ctx, task)
	}); err != nil {
		if err == engine.ErrExecutionInactive {
			return nil
		}
		d.observeTransient(err)
		return err
	}
	return nil
}
