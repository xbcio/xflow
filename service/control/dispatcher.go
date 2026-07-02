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

// LeaseBuilder builds a runner-facing task lease. The legacy *RunnerPool path
// uses it; RunnerDirectory dispatch defers lease construction to runner poll.
type LeaseBuilder interface {
	BuildTaskLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, error)
}

// Router returns side-effect-free routing metadata for a queued task.
type Router interface {
	TaskRouting(ctx context.Context, t *engine.Task) (engine.TaskRouting, error)
}

type Dispatcher struct {
	engine     Router
	runners    RunnerDirectory
	legacyPool *RunnerPool
	observer   DispatcherObserver
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

func NewDispatcher(engine Router, runners any, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{engine: engine}
	switch r := runners.(type) {
	case RunnerDirectory:
		d.runners = r
	case *RunnerPool:
		d.legacyPool = r
	case nil:
	default:
		panic("unsupported dispatcher runner target")
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
	if d.runners != nil {
		routing, err := d.engine.TaskRouting(ctx, task)
		if err != nil {
			if errors.Is(err, engine.ErrExecutionInactive) {
				return nil
			}
			return err
		}
		_, err = d.runners.EnqueueAssignment(ctx, Assignment{
			AssignmentID: BuildAssignmentID(task),
			Task:         *task,
			Routing:      routing,
		})
		if err != nil {
			d.observeTransient(err)
			return &Transient{Err: err}
		}
		return nil
	}

	routing, err := d.engine.TaskRouting(ctx, task)
	if err != nil {
		if errors.Is(err, engine.ErrExecutionInactive) {
			return nil
		}
		return err
	}
	if d.legacyPool == nil {
		err := ErrNoMatchingRunner
		d.observeTransient(err)
		return &Transient{Err: err}
	}
	builder, ok := d.engine.(LeaseBuilder)
	if !ok {
		return errors.New("dispatcher engine does not build task leases for legacy runner pool")
	}
	if err := d.legacyPool.AssignRouted(routing, func() (*engine.TaskLease, error) {
		return builder.BuildTaskLease(ctx, task)
	}); err != nil {
		if err == engine.ErrExecutionInactive || err == engine.ErrLeaseAlreadyActive {
			return nil
		}
		d.observeTransient(err)
		return err
	}
	return nil
}
