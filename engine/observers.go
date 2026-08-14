package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// DefaultOutboxMaxDeliveryAttempts is the number of failed queue handoffs
// retained before a durable outbox entry is moved to dead-letter storage.
const DefaultOutboxMaxDeliveryAttempts = 10

// CommitObserver receives the final classification of each runner result
// commit. Implementations must be non-blocking and must not affect scheduling.
type CommitObserver interface {
	OnCommitOutcome(ctx context.Context, outcome CommitOutcome)
}

// OutboxObserver receives durable outbox delivery and backlog observations.
// Implementations must be non-blocking and must not affect scheduling.
type OutboxObserver interface {
	OnOutboxRetry(ctx context.Context, attempt int)
	OnOutboxDeadLetter(ctx context.Context)
	OnOutboxReplayed(ctx context.Context, outcome DeadLetterReplayOutcome)
	OnOutboxPending(ctx context.Context, pending int, deadLettered int, oldestAge time.Duration)
	OnOutboxError(ctx context.Context, operation string, err error)
}

// OutboxDeliveryFailure describes the durable state after a queue handoff
// failure was recorded.
type OutboxDeliveryFailure struct {
	Attempts     int
	DeadLettered bool
}

// OutboxMetricsSnapshot reports the aggregate durable outbox backlog. The
// oldest timestamp is zero when no pending delivery intent exists.
type OutboxMetricsSnapshot struct {
	Pending         int
	OldestPendingAt time.Time
	DeadLettered    int
}

// OutboxFailureRecorder is an optional StateStore capability that durably
// records a failed outbox handoff. Backends that implement it must move entries
// that reach maxAttempts to independent dead-letter storage, writing immutable
// node/activation metadata so later replay can guard against stale activations
// without parsing the entry body.
type OutboxFailureRecorder interface {
	RecordOutboxFailure(ctx context.Context, id types.ExecutionID, entry OutboxEntry, maxAttempts int) (OutboxDeliveryFailure, error)
}

// OutboxMetricsReader is an optional StateStore capability that reports the
// aggregate durable outbox backlog for periodic observability collection.
type OutboxMetricsReader interface {
	OutboxMetrics(ctx context.Context) (OutboxMetricsSnapshot, error)
}

// OutboxReleaser is an optional StateStore capability that hands a leased
// delivery intent back before its visibility timeout elapses.
//
// Without it, an entry the flush could not hand off — a full queue, a failed
// enqueue — stays invisible for the whole OutboxDeliveryLeaseTTL even though
// both conditions clear in milliseconds. A store that leases must implement it;
// one that does not lease has nothing to release.
type OutboxReleaser interface {
	ReleaseOutbox(ctx context.Context, id types.ExecutionID, entry OutboxEntry) error
}

// WithCommitObserver installs an observer for structured result-commit
// outcomes. A nil observer leaves commit observation disabled.
func WithCommitObserver(observer CommitObserver) Option {
	return func(e *Engine) {
		if observer != nil {
			e.commitObserver = observer
		}
	}
}

// WithOutboxObserver installs an observer for durable outbox delivery and
// backlog events. A nil observer leaves outbox observation disabled.
func WithOutboxObserver(observer OutboxObserver) Option {
	return func(e *Engine) {
		if observer != nil {
			e.outboxObserver = observer
		}
	}
}

// WithRuntimeEvidenceBuffer installs a read-only evidence sink. nil or absent
// buffer means zero behavior change. The same buffer must not be reused across
// topologies.
func WithRuntimeEvidenceBuffer(buf *RuntimeEvidenceBuffer) Option {
	return func(e *Engine) {
		e.evidenceBuffer = buf
	}
}

// ObservedNodeFailure carries context about a node failure sufficient for the
// group runtime to classify the group outcome without reverse-engineering the
// error string.
type ObservedNodeFailure struct {
	NodeName string
	Err      error
	Attempt  int
	Fatal    bool
}

// NodeFailureObserver receives per-node failure observations from the engine.
// Only Fatal==true observations are actionable for group outcome classification.
type NodeFailureObserver interface {
	ObserveNodeFailure(execID types.ExecutionID, f ObservedNodeFailure)
}

// WithNodeFailureObserver installs an observer for node-level failures. Used by
// the group runtime to capture classified errors from the inner engine.
func WithNodeFailureObserver(o NodeFailureObserver) Option {
	return func(e *Engine) {
		if o != nil {
			e.nodeFailureObserver = o
		}
	}
}

// ObservedItemFailures reports how many items of one batch had their body
// execution fail. It carries counts, not identities: the failing items' indices
// and content stay out because an observer is expected to turn this into a
// metric, where an index is unbounded cardinality and content is not an
// operator's to read.
type ObservedItemFailures struct {
	Workflow string
	NodeName string
	// Failed is the number of items in this batch whose body failed. Always
	// positive: a batch with no failures is not observed at all.
	Failed int
	// Total is the batch's item count, so a rate can be expressed against the
	// work attempted rather than only in absolute terms.
	Total int
}

// ItemFailureObserver receives per-batch counts of failed body items.
//
// This is what makes continue_on_error safe to turn on. With it set, a failed
// item leaves a placeholder the downstream filter drops, the batch commits, and
// the execution reports Success — so nothing else in the system distinguishes a
// node steadily losing records from a healthy one.
type ItemFailureObserver interface {
	ObserveItemFailures(f ObservedItemFailures)
}

// WithItemFailureObserver installs an observer for per-item body failures.
func WithItemFailureObserver(o ItemFailureObserver) Option {
	return func(e *Engine) {
		if o != nil {
			e.itemFailureObserver = o
		}
	}
}

// WithOutboxMaxDeliveryAttempts changes the number of failed queue handoffs
// allowed before a backend moves an outbox entry to dead-letter storage.
func WithOutboxMaxDeliveryAttempts(maxAttempts int) Option {
	return func(e *Engine) {
		if maxAttempts > 0 {
			e.outboxMaxDeliveryAttempt = maxAttempts
		}
	}
}

func (e *Engine) notifyCommitOutcome(ctx context.Context, outcome CommitOutcome) {
	if e.commitObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.commitObserver.OnCommitOutcome(observerCtx, outcome)
	})
}

func (e *Engine) notifyOutboxRetry(ctx context.Context, attempt int) {
	if e.outboxObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.outboxObserver.OnOutboxRetry(observerCtx, attempt)
	})
}

func (e *Engine) notifyOutboxDeadLetter(ctx context.Context) {
	if e.outboxObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.outboxObserver.OnOutboxDeadLetter(observerCtx)
	})
}

func (e *Engine) notifyOutboxReplayed(ctx context.Context, outcome DeadLetterReplayOutcome) {
	if e.outboxObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.outboxObserver.OnOutboxReplayed(observerCtx, outcome)
	})
}

func (e *Engine) notifyOutboxPending(ctx context.Context, pending int, deadLettered int, oldestAge time.Duration) {
	if e.outboxObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.outboxObserver.OnOutboxPending(observerCtx, pending, deadLettered, oldestAge)
	})
}

func (e *Engine) notifyOutboxError(ctx context.Context, operation string, err error) {
	if e.outboxObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.outboxObserver.OnOutboxError(observerCtx, operation, err)
	})
}

func (e *Engine) recordOutboxDeliveryFailure(ctx context.Context, state AtomicStateStore, id types.ExecutionID, entry OutboxEntry, deliveryErr error) {
	e.notifyOutboxError(ctx, "delivery", deliveryErr)
	// A failed handoff is a retry-now condition, so give the delivery lease
	// back rather than making the next attempt wait out the visibility timeout.
	// Release before recording the failure: once the attempt counter reaches
	// the limit the entry is dead-lettered and there is nothing left to release.
	e.releaseOutboxLease(ctx, state, id, entry)

	recorder, ok := state.(OutboxFailureRecorder)
	if !ok {
		return
	}
	result, err := recorder.RecordOutboxFailure(ctx, id, entry, e.outboxMaxDeliveryAttempt)
	if err != nil {
		e.notifyOutboxError(ctx, "record_failure", err)
		return
	}
	if result.DeadLettered {
		e.notifyOutboxDeadLetter(ctx)
		return
	}
	e.notifyOutboxRetry(ctx, result.Attempts)
}

// releaseOutboxLease returns one leased entry to the ready set. A store without
// the capability never leased it in the first place, so there is nothing to do.
func (e *Engine) releaseOutboxLease(ctx context.Context, state AtomicStateStore, id types.ExecutionID, entry OutboxEntry) {
	releaser, ok := state.(OutboxReleaser)
	if !ok {
		return
	}
	if err := releaser.ReleaseOutbox(ctx, id, entry); err != nil {
		e.notifyOutboxError(ctx, "release", err)
	}
}

func (e *Engine) observeOutboxMetrics(ctx context.Context, state AtomicStateStore) {
	reader, ok := state.(OutboxMetricsReader)
	if !ok {
		return
	}
	snapshot, err := reader.OutboxMetrics(ctx)
	if err != nil {
		e.notifyOutboxError(ctx, "metrics", err)
		return
	}
	oldestAge := time.Duration(0)
	if !snapshot.OldestPendingAt.IsZero() {
		oldestAge = time.Since(snapshot.OldestPendingAt)
		if oldestAge < 0 {
			oldestAge = 0
		}
	}
	e.notifyOutboxPending(ctx, snapshot.Pending, snapshot.DeadLettered, oldestAge)
}
