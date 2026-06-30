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

// Transient implements queue-layer requeueing for dispatch backpressure. Any
// error returned from Dispatcher.HandleTask whose chain contains a Transient
// error becomes a requeue with exponential backoff; everything else lands in
// the dead-letter sink so real bugs are not silently retried forever.
type Transient struct{ Err error }

func (t *Transient) Error() string  { return t.Err.Error() }
func (t *Transient) Unwrap() error  { return t.Err }
func (t *Transient) Transient() bool { return true }

// IsTransient reports whether err (or any wrapped error) signals a transient
// dispatch failure that should be requeued rather than dead-lettered.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var t *Transient
	if errors.As(err, &t) {
		return true
	}
	return errors.Is(err, ErrNoMatchingRunner) || errors.Is(err, ErrNoCapacity)
}

type LeaseBuilder interface {
	BuildTaskLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, error)
}

type Dispatcher struct {
	engine  LeaseBuilder
	runners *RunnerPool
}

func NewDispatcher(engine LeaseBuilder, runners *RunnerPool) *Dispatcher {
	return &Dispatcher{
		engine:  engine,
		runners: runners,
	}
}

func (d *Dispatcher) HandleTask(ctx context.Context, task *engine.Task) error {
	lease, err := d.engine.BuildTaskLease(ctx, task)
	if err != nil {
		if err == engine.ErrExecutionInactive {
			return nil
		}
		return err
	}
	if lease == nil {
		return &Transient{Err: ErrNoMatchingRunner}
	}
	if d.runners == nil {
		return &Transient{Err: ErrNoMatchingRunner}
	}
	if err := d.runners.Assign(*lease); err != nil {
		return &Transient{Err: err}
	}
	return nil
}
