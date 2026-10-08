package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
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

// Dispatch-drop reason labels. This is the whole vocabulary of the dropped-task
// counter — keep it bounded; never use error text or an execution id as a
// label.
const (
	// dispatchDropExecutionGone is a loss claim bounded to the evidence
	// window: no live state, no terminal marker, and the delivery's measured
	// age — from the durable intent's creation, not a delayed intent's
	// availability — is inside the backend's evidence window, so had the
	// execution terminalized during that span its marker would still be
	// readable. On a TTL-backed store this is what a task looks like when the
	// execution expired under its queued work. It is a claim, not proof (the
	// marker write is best-effort), and deliveries whose age passed the window
	// land in dispatchDropExecutionUnattributed instead.
	dispatchDropExecutionGone = "execution_gone"
	// dispatchDropExecutionUnattributed is a drop the classifier could not
	// attribute: no live state and no terminal evidence exists for the
	// execution, but the delivery's measured age passed the backend's evidence
	// window (or it carried no stamp to measure), so a benign late duplicate
	// of a long-finished execution cannot be ruled out. During a backlog
	// longer than the retention this bucket holds both late duplicates and
	// true losses; it is neither proof of loss nor proof of health. The task
	// is still not acked as a success (there is no execution left to run it),
	// but the count must not be read or paged as confirmed loss.
	dispatchDropExecutionUnattributed = "execution_unattributed"
	// dispatchDropExecutionTerminal is a benign late or duplicate delivery for
	// an execution that already finished (or whose terminal marker outlived
	// its status record). Dropping it is correct; it must not page anyone.
	dispatchDropExecutionTerminal = "execution_terminal"
	// dispatchDropNodeStale is a node-level stale route on a live execution —
	// a duplicate delivery, a newer activation, a terminal node, or a node
	// mid-commit. Benign, and deliberately not classified by the engine.
	dispatchDropNodeStale = "node_stale"
	// dispatchDropClassifyError is an inactive verdict whose classification
	// read failed. It is neither proof of loss nor of health; the task is
	// returned unmarked so a retry-capable transport retries it. See the mode
	// note on HandleTask for what a fire-and-forget transport does instead.
	dispatchDropClassifyError = "classify_error"
	// dispatchDropBatchDuplicate is a map batch the runner directory refused
	// as a duplicate of one it already holds. The delivery is still acked; it
	// is counted because a refused batch that was not really a redelivery
	// leaves its expansion barrier one child short until the parent lease
	// expires.
	dispatchDropBatchDuplicate = "batch_duplicate"
)

// dispatcherDropLogInterval bounds how often the lost-task path writes a log
// line. The counter is exact; the log is a rate-limited sample, because a
// backlog past the TTL can drop thousands of tasks and one line each would be
// its own outage.
const dispatcherDropLogInterval = 30 * time.Second

// dispatchDroppedLogMessage is the message on the rate-limited lost-task
// record. It names the condition (queued work dropped because the execution no
// longer exists), not the mechanism, because it is the line an operator greps
// during a backlog incident.
const dispatchDroppedLogMessage = "dispatch dropped queued work: the execution no longer exists"

// dispatchUnattributedLogMessage is the message on the rate-limited record for
// a drop the classifier could not attribute (no evidence, and the wait is not
// provably inside the retention window — exceeded, unmeasurable, or the window
// itself unprovable). It is a warning, not the lost-task error: the same
// bucket holds benign late duplicates during a backlog, so it must not read as
// a confirmed loss.
const dispatchUnattributedLogMessage = "dispatch dropped queued work it cannot attribute: no execution evidence, delivery wait not provably inside the retention window"

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

// DispatcherObserver receives transient dispatch failures. Implementations
// must be non-blocking and must avoid high-cardinality labels.
type DispatcherObserver interface {
	OnDispatchTransient(ctx context.Context, reason string)
}

// DispatcherDropObserver is an optional extension of DispatcherObserver:
// observers that implement it also receive every task the dispatcher drops
// without assigning it, partitioned by reason (the dispatchDrop* constants).
//
// It is discovered by type assertion so every existing DispatcherObserver
// implementation stays source-compatible — the same pattern as
// systemTaskHandler. A custom observer that does not implement it leaves
// dropped tasks uncounted: the drop is still not a silent success (the gone
// case returns a permanent error), but nothing observes it. Hosts that install
// metrics get it for free through observability/metrics.DispatcherMetrics.
//
// Implementations must be non-blocking and must not use the reason as anything
// but a bounded label.
type DispatcherDropObserver interface {
	OnDispatchDropped(ctx context.Context, reason string)
}

// DeliveryLagObserver is an optional extension of DispatcherObserver: it
// receives, for every consumed task that carries a DeliverableAt stamp, how
// long the task waited between becoming deliverable and being consumed. That
// distribution is the pre-loss signal for queue residency: its tail approaching
// the execution TTL is the warning that tasks are about to outlive their
// execution, while the drop counters only fire after that happened.
//
// Implementations must be non-blocking.
type DeliveryLagObserver interface {
	OnTaskDeliveryLag(ctx context.Context, lag time.Duration)
}

type noopDispatcherObserver struct{}

func (noopDispatcherObserver) OnDispatchTransient(context.Context, string) {}

// DispatcherOption configures a Dispatcher.
type DispatcherOption func(*Dispatcher)

// WithDispatcherObserver installs an observer for transient dispatch failures
// and, when the value implements the optional extensions above, for dropped
// tasks and delivery latency as well.
func WithDispatcherObserver(obs DispatcherObserver) DispatcherOption {
	return func(d *Dispatcher) {
		if obs == nil {
			return
		}
		d.observer = obs
		if drop, ok := obs.(DispatcherDropObserver); ok {
			d.dropObserver = drop
		}
		if lag, ok := obs.(DeliveryLagObserver); ok {
			d.lagObserver = lag
		}
	}
}

// WithDispatcherLogger installs the logger used for the rate-limited lost-task
// record. Without one the counter still moves; only the log is skipped.
func WithDispatcherLogger(logger engine.Logger) DispatcherOption {
	return func(d *Dispatcher) {
		if logger != nil {
			d.logger = logger
		}
	}
}

type Dispatcher struct {
	engine       Router
	runners      RunnerDirectory
	observer     DispatcherObserver
	dropObserver DispatcherDropObserver
	lagObserver  DeliveryLagObserver
	logger       engine.Logger

	// dropLogInterval and the fields below implement the rate limit shared by
	// the lost-task and unattributed-task logs. HandleTask runs on concurrent
	// queue workers, so the timestamp and suppressed count are mutex-guarded;
	// the logger call itself happens outside the lock.
	dropLogInterval   time.Duration
	dropLogMu         sync.Mutex
	dropLogLast       time.Time
	dropLogSuppressed int
}

func NewDispatcher(engine Router, runners RunnerDirectory, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		engine:          engine,
		runners:         runners,
		observer:        noopDispatcherObserver{},
		dropLogInterval: dispatcherDropLogInterval,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// HandleTask routes one consumed queue task. The error contract is the queue
// layer's disposition:
//
//   - nil: the task was assigned, or dropped as benign (terminal late arrival,
//     node-level stale route).
//   - a permanent error (types.ErrPermanent in the chain): the task is
//     undeliverable — the execution is gone (provable loss) or left no
//     evidence at all and the delivery could not be attributed; a
//     retry-capable transport should archive it, a fire-and-forget transport
//     drops it. Its payload is the dead-letter artifact.
//   - any other error: transient or unexpected; retry.
//
// Classification read failures are returned unmarked, not as permanent. On a
// retry-capable transport (asynq durable mode) they are retried, which is the
// right answer for a store blip. A fire-and-forget transport treats every
// non-nil error as skip-retry, so there the task is archived rather than
// retried: a Redis outage archives a bounded sample of tasks instead of
// silently acking them, and the classify_error counter records it either way.
// The dispatcher cannot know the transport's mode, so it does not pretend to;
// this asymmetry is why classify_error is a distinct reason from
// execution_gone.
//
// System tasks (advance/skip intents) are consumed before routing and keep
// their own disposition, but an inactive execution is now classified and
// counted exactly like a routing drop: the engine returns a handled error
// carrying the classification, and this method runs it through the same
// reason mapping. (true, nil) from the system-task handler is a task that was
// actually processed — a success, never a drop.
func (d *Dispatcher) HandleTask(ctx context.Context, task *engine.Task) error {
	d.observeDeliveryLag(ctx, task)

	if handler, ok := d.engine.(systemTaskHandler); ok {
		handled, err := handler.HandleSystemTask(ctx, task)
		if err != nil {
			// A system task the engine owns but could not process is a drop,
			// not a success: route it through the same classification as the
			// routing path so an advance/skip intent for an inactive
			// execution is counted and logged like any other dropped
			// delivery instead of vanishing as (true, nil) did. Non-inactive
			// errors pass through unchanged.
			return d.handleRoutingError(ctx, task, err)
		}
		if handled {
			return nil
		}
	}

	routing, err := d.engine.TaskRouting(ctx, task)
	if err != nil {
		return d.handleRoutingError(ctx, task, err)
	}
	if d.runners == nil {
		d.observeTransient(ctx, "no_runner_directory")
		return &Transient{Err: ErrNoMatchingRunner}
	}
	enqueued, err := d.runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(task),
		Task:         *task,
		Routing:      routing,
		Namespace:    namespace.FromContext(ctx),
	})
	if err != nil {
		d.observeTransient(ctx, dispatchTransientReason(err))
		return &Transient{Err: err}
	}
	if !enqueued && task.Type == engine.TaskTypeNodeBatch {
		d.observeDrop(ctx, dispatchDropBatchDuplicate)
	}
	return nil
}

// handleRoutingError classifies a TaskRouting failure. Only
// ErrExecutionInactive is a drop; everything else is propagated unchanged.
func (d *Dispatcher) handleRoutingError(ctx context.Context, task *engine.Task, err error) error {
	var classify *engine.ExecutionInactiveClassifyError
	if errors.As(err, &classify) {
		d.observeDrop(ctx, dispatchDropClassifyError)
		return err
	}
	if !errors.Is(err, engine.ErrExecutionInactive) {
		return err
	}
	var inactive *engine.ExecutionInactiveError
	if errors.As(err, &inactive) {
		switch inactive.Kind {
		case engine.ExecutionInactiveTerminal:
			d.observeDrop(ctx, dispatchDropExecutionTerminal)
			return nil
		case engine.ExecutionInactiveGone:
			d.observeDrop(ctx, dispatchDropExecutionGone)
			d.logDroppedTask(ctx, task, err)
			// Not a success: the work never ran and never will, so the task
			// must not be acked as if it had. Marking it permanent keeps a
			// retry-capable transport from burning its retry budget on an
			// execution that has no state left to accept the work; the
			// transport's archive is the dead-letter sink for the payload.
			return fmt.Errorf("%w: %w", types.ErrPermanent, err)
		case engine.ExecutionInactiveUnattributed:
			d.observeDrop(ctx, dispatchDropExecutionUnattributed)
			d.logUnattributedTask(ctx, task, err)
			// Also not a success — there is no execution left to run the work —
			// but not a loss claim either: the classification cannot tell a
			// late duplicate of a long-finished execution from expired queued
			// work. Permanent keeps a retry-capable transport from retrying an
			// execution with no state, and the distinct reason keeps this
			// bucket out of the loss alert.
			return fmt.Errorf("%w: %w", types.ErrPermanent, err)
		}
	}
	// The bare sentinel: with engine.Engine this is only a node-level stale
	// route on a live execution (stale activation, terminal node, node
	// mid-commit) — a classification read failure arrives as
	// ExecutionInactiveClassifyError, not as the bare sentinel. A custom
	// Router may still return the bare sentinel for whatever it cannot
	// classify. Duplicate or delayed deliveries, not lost work — dropping is
	// correct, and it is no longer silent.
	d.observeDrop(ctx, dispatchDropNodeStale)
	return nil
}

func dispatchTransientReason(err error) string {
	switch {
	case errors.Is(err, ErrNoMatchingRunner):
		return "no_matching_runner"
	case errors.Is(err, ErrNoCapacity):
		return "no_capacity"
	default:
		return "enqueue_failed"
	}
}

func (d *Dispatcher) observeTransient(ctx context.Context, reason string) {
	defer func() { _ = recover() }()
	d.observer.OnDispatchTransient(ctx, reason)
}

func (d *Dispatcher) observeDrop(ctx context.Context, reason string) {
	if d.dropObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	d.dropObserver.OnDispatchDropped(ctx, reason)
}

// observeDeliveryLag reports how long the task waited in the queue when the
// producing side stamped it. An unstamped task (legacy payload, direct-enqueue
// path) produces no observation rather than a fabricated one.
func (d *Dispatcher) observeDeliveryLag(ctx context.Context, task *engine.Task) {
	if d.lagObserver == nil || task == nil || task.DeliverableAt.IsZero() {
		return
	}
	lag := time.Since(task.DeliverableAt)
	if lag < 0 {
		// A delayed intent's AvailableAt can legitimately sit in the future if
		// it was enqueued early; a negative age is not an observation.
		return
	}
	defer func() { _ = recover() }()
	d.lagObserver.OnTaskDeliveryLag(ctx, lag)
}

// logDroppedTask writes one rate-limited record for a drop claimed as lost
// work (the window-bounded execution_gone verdict).
func (d *Dispatcher) logDroppedTask(ctx context.Context, task *engine.Task, err error) {
	d.logDropRecord(ctx, task, dispatchDropExecutionGone, dispatchDroppedLogMessage, err, false)
}

// logUnattributedTask writes one rate-limited record for a drop the classifier
// could not attribute. It is a warning rather than an error because the same
// bucket holds benign late duplicates once the backlog outlives the retention.
func (d *Dispatcher) logUnattributedTask(ctx context.Context, task *engine.Task, err error) {
	d.logDropRecord(ctx, task, dispatchDropExecutionUnattributed, dispatchUnattributedLogMessage, err, true)
}

// logDropRecord writes one rate-limited record for a dropped task. Fields are
// bounded identifiers — execution id, node name, task type, namespace — never
// the task payload: a resume task carries execution input, which may contain
// credentials, and the point of this log is diagnosis, not data retention.
//
// Both drop kinds share one throttle: they are the same condition (queued work
// dropped because its execution left no state) seen with different
// provability, the counter distinguishes them exactly, and the suppressed
// count on the line says how many records of either kind it absorbed.
func (d *Dispatcher) logDropRecord(ctx context.Context, task *engine.Task, reason, message string, err error, warn bool) {
	if d.logger == nil {
		return
	}
	now := time.Now()
	d.dropLogMu.Lock()
	if d.dropLogInterval > 0 && !d.dropLogLast.IsZero() && now.Sub(d.dropLogLast) < d.dropLogInterval {
		d.dropLogSuppressed++
		d.dropLogMu.Unlock()
		return
	}
	suppressed := d.dropLogSuppressed
	d.dropLogSuppressed = 0
	d.dropLogLast = now
	d.dropLogMu.Unlock()

	fields := []any{
		"reason", reason,
		"namespace", string(namespace.FromContext(ctx)),
		"execution_id", string(task.ExecutionID),
		"node_name", task.NodeName,
		"task_type", int(task.Type),
	}
	if suppressed > 0 {
		fields = append(fields, "suppressed", suppressed)
	}
	fields = append(fields, "err", err)
	if warn {
		d.logger.Warn(message, fields...)
		return
	}
	d.logger.Error(message, fields...)
}
