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

// OutboxDispatchObserver is an optional extension of OutboxObserver that
// receives one observation per dispatcher drain.
//
// It is a separate interface rather than another method on OutboxObserver so
// that an observer interested only in delivery outcomes — the dead-letter
// manager's, for instance — keeps compiling. The dispatcher type-asserts the
// installed OutboxObserver, so an observer that does not implement this simply
// receives no drain observations.
//
// Both numbers exist because neither alone answers "is dispatch keeping up".
// A drain that discovers nothing while ready work is visible in
// xflow_outbox_ready means discovery cannot see the backlog; a drain that
// discovers work and runs far longer than the configured interval means
// delivery is the bound. Operators previously had to count Redis keys by hand
// to tell those apart.
type OutboxDispatchObserver interface {
	// OnOutboxDrain reports how many executions this drain discovered with
	// ready outbox work, and how long the whole pass took.
	OnOutboxDrain(ctx context.Context, discovered int, duration time.Duration)
	// OnOutboxBacklog reports the dispatcher's throttled backlog scan. It
	// carries the whole snapshot, including the due-now count OutboxObserver's
	// OnOutboxPending has no field for; an observer that also implements
	// OutboxObserver still receives that older callback and should keep
	// reporting pending and dead-lettered from it rather than from here.
	OnOutboxBacklog(ctx context.Context, snapshot OutboxMetricsSnapshot)
}

// OutboxMetricsSnapshot reports the aggregate durable outbox backlog. The
// oldest timestamp is zero when no pending delivery intent exists.
type OutboxMetricsSnapshot struct {
	Pending         int
	OldestPendingAt time.Time
	DeadLettered    int
	// Ready is how many of the pending entries are due for delivery right now.
	//
	// Pending counts every entry sitting in an execution's ready index, which
	// includes entries a live deliverer currently holds a lease on and entries
	// waiting out a retry backoff — both scored in the future and both
	// undeliverable at this instant. Ready counts only the entries whose score
	// has passed, which is the number a dispatcher could hand to the queue in
	// this tick. A backlog that is large but entirely not-yet-ready is a
	// different state from one that is large and due, and Pending alone cannot
	// distinguish them.
	//
	// A store that cannot answer it reports zero.
	Ready int
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

// OutboxLeaser is an optional StateStore capability that claims ready delivery
// intents for exclusive delivery.
//
// It exists because multiple processes flush the same execution concurrently —
// every server runs its own OutboxDispatcher, ungated by leader election — and
// without a claim each of them delivers the same entry. An 800-item map at
// concurrency 4 ran its body 826-1110 times against 800 at concurrency 1.
//
// The claim is a lease rather than a removal so at-least-once survives: a
// deliverer that dies mid-flight must have its entries become deliverable again.
// RenewOutbox is what keeps that recovery prompt without stranding a live
// deliverer — see OutboxDeliveryLeaseTTL.
//
// A store that leases must also implement OutboxReleaser. A store that
// implements neither is delivered from without claiming, which is correct but
// duplicates under concurrency.
type OutboxLeaser interface {
	// LeaseOutbox claims up to limit ready entries and hides them from other
	// callers — including ListOutbox — until their leases lapse or are released.
	//
	// now is both the availability cutoff and the clock the lease is taken
	// against, so a test can drive lease expiry deterministically.
	LeaseOutbox(ctx context.Context, id types.ExecutionID, now time.Time, limit int) ([]OutboxEntry, error)
	// RenewOutbox extends the leases the caller holds, proving the deliverer is
	// still alive, and returns the entries whose renewal was granted — each
	// carrying the new deadline the next renewal must present.
	//
	// A caller renews by presenting the deadline it believes it holds, because
	// the lease has no other identity. An entry whose lease already lapsed and
	// was claimed elsewhere is refused rather than renewed, so a deliverer that
	// stalls and comes back cannot steal work another deliverer legitimately
	// took over. An entry absent from the result is one the caller no longer
	// holds and must stop renewing.
	RenewOutbox(ctx context.Context, id types.ExecutionID, entries []OutboxEntry, now time.Time) ([]OutboxEntry, error)
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
	// Permanent reports that retrying this node with the same input produces
	// the same failure — the node returned a types.ClassifiedError marked
	// permanent, or an error wrapping types.ErrPermanent.
	//
	// It is a separate field rather than something a consumer derives from Err
	// because Err does not survive the commit path: the observation is built
	// from the committed error MESSAGE (errors.New of it), so the sentinel and
	// the ClassifiedError type are both gone by the time an observer sees it.
	// The classification is recovered at the commit boundary instead, where the
	// original error is still in hand.
	//
	// False for an unclassified failure, which is the conservative reading: a
	// consumer that skips work on Permanent must not skip on a maybe.
	Permanent bool
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

// GroupObserver receives group unit lifecycle observations. Implementations
// must be non-blocking and must not affect scheduling.
type GroupObserver interface {
	OnGroupLeaseAcquired(ctx context.Context)
	OnGroupLeaseExpired(ctx context.Context)
	// OnGroupLeaseRenew reports one renewal attempt: its classification and
	// how long the backend round trip took. One call per attempt, so the
	// count and the duration histogram can never disagree.
	OnGroupLeaseRenew(ctx context.Context, result string, d time.Duration)
	// OnGroupCommit reports one group unit reaching a terminal result, and how
	// long it took from lease issue to commit.
	OnGroupCommit(ctx context.Context, outcome string, d time.Duration)
	// OnGroupAdmission reports one GROUP entry-unit admission attempt through
	// Engine.SeedExecutionFromEntry, and how long the backend admission round
	// trip took. One call per attempt, so the count and the duration histogram
	// can never disagree (the same reason OnGroupLeaseRenew and OnGroupCommit
	// are single calls rather than a pair of observer methods).
	//
	// Only entry units whose Graph.UnitKindAt resolves to graph.UnitGroup are
	// reported here — a non-group entry unit, a nil Graph, or an
	// EntryUnitIdx out of [0, Graph.UnitCount()) is not counted at all (not
	// counted as a failure, simply not observed), so this series is a lower
	// bound whenever the caller could not resolve group membership.
	OnGroupAdmission(ctx context.Context, outcome string, d time.Duration)
}

// NodeSkipObserver receives the units a scheduling transition resolved as skip
// rather than execute, partitioned by the flow that decided it.
//
// This is the only observation of the skip branch, and it is its own interface
// rather than a method on GroupObserver for the reason ItemFailureObserver is:
// a skip is not a group event. All three flows that produce one — entry
// admission, a node advance, and a group commit — reach it, and an engine that
// has no group units at all still skips. Folding it into GroupObserver would
// mean an engine could not observe skips without also being handed an observer
// whose other methods are about a subsystem it does not use.
//
// A skip is not a silent drop — it leaves a durable outbox intent
// (engine.TaskTypeNodeSkip) and it does advance the scheduling position, which
// is exactly why it needs a series: the downstream never sees that work, no
// error is raised, and nothing else in the system distinguishes it from a
// branch that legitimately had no data. A host that treats "no data loss" as a
// hard requirement has to be able to alert on this counter being nonzero.
//
// Implementations must be non-blocking and must not affect scheduling.
type NodeSkipObserver interface {
	// OnNodeSkip reports the downstream units one transition resolved as skip.
	//
	// flow is a closed enum naming the decision point, not the workflow:
	// "entry" (the SeedExecutionFromEntry fan-out), "advance" (AdvanceNode) and
	// "group" (CommitGroup).
	//
	// node is the downstream unit's representative node name, and count is how
	// many units of that name the one transition skipped — a single advance can
	// skip several arrivals at once, which is why this carries a count rather
	// than being a bare counter increment.
	//
	// It is called once per skipped node rather than once per transition, so an
	// implementation sees the node attribution the aggregate cannot carry.
	OnNodeSkip(ctx context.Context, flow string, node string, count int)
}

// WithNodeSkipObserver installs an observer for skipped downstream units.
func WithNodeSkipObserver(o NodeSkipObserver) Option {
	return func(e *Engine) {
		if o != nil {
			e.nodeSkipObserver = o
		}
	}
}

// WithGroupObserver installs an observer for group unit lease/commit lifecycle
// events. A nil observer leaves group observation disabled.
func WithGroupObserver(o GroupObserver) Option {
	return func(e *Engine) {
		if o != nil {
			e.groupObserver = o
		}
	}
}

func (e *Engine) notifyGroupLeaseAcquired(ctx context.Context) {
	if e.groupObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.groupObserver.OnGroupLeaseAcquired(observerCtx)
	})
}

func (e *Engine) notifyGroupLeaseExpired(ctx context.Context) {
	if e.groupObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.groupObserver.OnGroupLeaseExpired(observerCtx)
	})
}

func (e *Engine) notifyGroupLeaseRenew(ctx context.Context, result string, d time.Duration) {
	if e.groupObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.groupObserver.OnGroupLeaseRenew(observerCtx, result, d)
	})
}

func (e *Engine) notifyGroupCommit(ctx context.Context, outcome string, d time.Duration) {
	if e.groupObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.groupObserver.OnGroupCommit(observerCtx, outcome, d)
	})
}

func (e *Engine) notifyGroupAdmission(ctx context.Context, outcome string, d time.Duration) {
	if e.groupObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.groupObserver.OnGroupAdmission(observerCtx, outcome, d)
	})
}

// flowEntry is the OnNodeSkip flow label for the entry-admission fan-out, the
// one skip site reached from Engine.SeedExecutionFromEntry.
const flowEntry = "entry"

// flowAdvance is the OnNodeSkip flow label for the ordinary downstream
// advance (AtomicStateStore.AdvanceNode), which carries both the first
// propagation of a completed node and the skip cascade that follows it.
const flowAdvance = "advance"

// flowGroup is the OnNodeSkip flow label for a group unit's commit
// (GroupStateStore.CommitGroup), whose downstream fan-in is counted inside the
// commit transition rather than by a subsequent advance.
const flowGroup = "group"

// notifySkip reports the downstream units one transition resolved as skip.
//
// An empty list is not reported: an execute-only transition is the common case,
// and a call per advance would pay a hook invocation (panic recovery included)
// on the engine's hottest scheduling path to publish nothing. The list is
// therefore the gate, and a caller passes its result field straight through.
//
// No log. A skip is per entry unit per admission, and every one of them
// already leaves a durable outbox record carrying the execution, the node and
// the unit index — a log line here would duplicate, at proportional volume in
// the hot path, data an operator can already read off the outbox and this
// counter. The counter's flow/node labels are what make the aggregate
// actionable, which is the thing the outbox records cannot do.
//
// One call per skipped node, not one per transition. A transition can skip
// several downstream units at once, and collapsing them would mean either
// dropping the node attribution or inventing a sentinel for "several", which
// is not an operator's answer to "which node is being skipped".
func (e *Engine) notifySkip(ctx context.Context, flow string, skipped []SkippedUnit) {
	if len(skipped) == 0 || e.nodeSkipObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		for _, unit := range skipped {
			if unit.Count <= 0 {
				continue
			}
			e.nodeSkipObserver.OnNodeSkip(observerCtx, flow, unit.NodeName, unit.Count)
		}
	})
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

// notifyOutboxDrain reports one dispatcher drain to an observer that opted into
// the drain observation by implementing OutboxDispatchObserver. An observer
// that does not is simply not told, so the type assert is the whole wiring.
func (e *Engine) notifyOutboxDrain(ctx context.Context, discovered int, duration time.Duration) {
	observer, ok := e.outboxObserver.(OutboxDispatchObserver)
	if !ok {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		observer.OnOutboxDrain(observerCtx, discovered, duration)
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
	// Gate on the observer before the reader: notifyOutboxPending drops the
	// result when no observer is installed, so without this check the expensive
	// keyspace-wide OutboxMetrics scan runs only to be discarded.
	if e.outboxObserver == nil {
		return
	}
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
	e.notifyOutboxBacklog(ctx, snapshot)
}

// notifyOutboxBacklog hands the full snapshot to an observer that opted into
// the dispatcher's own observations. It is separate from notifyOutboxPending,
// which every OutboxObserver still receives: that callback predates the Ready
// field and has no argument for it, and widening it would break every existing
// implementation.
func (e *Engine) notifyOutboxBacklog(ctx context.Context, snapshot OutboxMetricsSnapshot) {
	observer, ok := e.outboxObserver.(OutboxDispatchObserver)
	if !ok {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		observer.OnOutboxBacklog(observerCtx, snapshot)
	})
}
