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

func (t *Transient) Error() string   { return t.Err.Error() }
func (t *Transient) Unwrap() error   { return t.Err }
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

// Router returns side-effect-free routing metadata for a queued task.
type Router interface {
	TaskRouting(ctx context.Context, t *engine.Task) (engine.TaskRouting, error)
}

// systemTaskHandler is intentionally optional so existing routing fakes and
// custom routers remain source-compatible. The concrete Engine consumes these
// durable scheduling tasks locally before any remote runner assignment.
type systemTaskHandler interface {
	HandleSystemTask(ctx context.Context, task *engine.Task) (bool, error)
}

type Dispatcher struct {
	engine  Router
	runners RunnerDirectory
}

func NewDispatcher(engine Router, runners RunnerDirectory) *Dispatcher {
	return &Dispatcher{engine: engine, runners: runners}
}

func (d *Dispatcher) HandleTask(ctx context.Context, task *engine.Task) error {
	if handler, ok := d.engine.(systemTaskHandler); ok {
		handled, err := handler.HandleSystemTask(ctx, task)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}

	routing, err := d.engine.TaskRouting(ctx, task)
	if err != nil {
		if errors.Is(err, engine.ErrExecutionInactive) {
			return nil
		}
		return err
	}
	if d.runners == nil {
		return &Transient{Err: ErrNoMatchingRunner}
	}
	_, err = d.runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(task),
		Task:         *task,
		Routing:      routing,
	})
	if err != nil {
		return &Transient{Err: err}
	}
	return nil
}
