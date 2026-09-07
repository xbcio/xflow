package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Engine metric names.
const (
	metricNodeStarted               = "xflow_node_started_total"
	metricNodeCompleted             = "xflow_node_completed_total"
	metricNodeDuration              = "xflow_node_duration_seconds"
	metricNodeSuspended             = "xflow_node_suspended_total"
	metricNodeTimedOut              = "xflow_node_timed_out_total"
	metricNodeRetried               = "xflow_node_retried_total"
	metricExecutionCompleted        = "xflow_execution_completed_total"
	metricCommitOutcomes            = "xflow_commit_outcomes_total"
	metricOutboxRetries             = "xflow_outbox_retries_total"
	metricOutboxDeadLettersTotal    = "xflow_outbox_dead_letters_total"
	metricOutboxDeadLetters         = "xflow_outbox_dead_letters"
	metricOutboxDeadLettersReplayed = "xflow_outbox_dead_letters_replayed_total"
	metricOutboxPending             = "xflow_outbox_pending"
	metricOutboxOldestPendingAge    = "xflow_outbox_oldest_pending_age_seconds"
	metricOutboxErrors              = "xflow_outbox_errors_total"
	// Node execution timeout / duration metrics. Distinct from
	// metricNodeTimedOut (xflow_node_timed_out_total): that one counts
	// SUSPENDED nodes whose park timer fired and which wake normally; the
	// three below count handler invocations that exceeded their execution
	// deadline and terminated. Reusing the suspend metric would conflate a
	// normal wake with a terminal failure.
	metricNodeTimeouts         = "xflow_node_timeouts_total"
	metricNodeTimeoutAbandoned = "xflow_node_timeout_abandoned"
	metricNodeExecDuration     = "xflow_node_execution_duration_seconds"
	// metricNodeStartsSwept counts start times dropped by the age sweep rather
	// than by a completion or an execution end. Reaching it means a node started
	// and nothing ever reported what happened to it, so it is a defect signal,
	// not routine housekeeping.
	metricNodeStartsSwept = "xflow_node_starts_swept_total"
)

// Sub-execution metric names. A map body item and a group member run on an
// inner engine, and their nodes were invisible until these existed: the
// executor in execution/subgraph built that engine with no Hooks at all.
//
// They are a SEPARATE family rather than the same one with an extra label,
// because the two populations are not comparable. One top-level execution
// contains a map over N items, and each item is its own inner execution; a
// map over ten thousand items would add ten thousand to
// xflow_execution_completed_total, which an operator reads as "workflow runs
// that finished". Sharing the series would not make that number more detailed,
// it would make it wrong. Adding a label instead would change the identity of
// every existing series, breaking any dashboard that matches on an exact label
// set, and still leave the two mixed in every query that does not know to
// exclude one.
const (
	metricSubNodeStarted        = "xflow_subgraph_node_started_total"
	metricSubNodeCompleted      = "xflow_subgraph_node_completed_total"
	metricSubNodeDuration       = "xflow_subgraph_node_duration_seconds"
	metricSubNodeSuspended      = "xflow_subgraph_node_suspended_total"
	metricSubNodeTimedOut       = "xflow_subgraph_node_timed_out_total"
	metricSubNodeRetried        = "xflow_subgraph_node_retried_total"
	metricSubExecutionCompleted = "xflow_subgraph_execution_completed_total"
	metricSubNodeStartsSwept    = "xflow_subgraph_node_starts_swept_total"
)

// engineMetricNames selects which family one MetricsHooks instance writes to.
//
// The bookkeeping around node start times -- the tombstone ring, the age sweep,
// the re-lease overwrite -- is subtle enough that a second copy of it for
// sub-executions would be a second place for the leak in be1f7ba to come back.
// The two families differ only in what they are called.
type engineMetricNames struct {
	nodeStarted        string
	nodeCompleted      string
	nodeDuration       string
	nodeSuspended      string
	nodeTimedOut       string
	nodeRetried        string
	executionCompleted string
	nodeStartsSwept    string
}

var topLevelMetricNames = engineMetricNames{
	nodeStarted:        metricNodeStarted,
	nodeCompleted:      metricNodeCompleted,
	nodeDuration:       metricNodeDuration,
	nodeSuspended:      metricNodeSuspended,
	nodeTimedOut:       metricNodeTimedOut,
	nodeRetried:        metricNodeRetried,
	executionCompleted: metricExecutionCompleted,
	nodeStartsSwept:    metricNodeStartsSwept,
}

var subgraphMetricNames = engineMetricNames{
	nodeStarted:        metricSubNodeStarted,
	nodeCompleted:      metricSubNodeCompleted,
	nodeDuration:       metricSubNodeDuration,
	nodeSuspended:      metricSubNodeSuspended,
	nodeTimedOut:       metricSubNodeTimedOut,
	nodeRetried:        metricSubNodeRetried,
	executionCompleted: metricSubExecutionCompleted,
	nodeStartsSwept:    metricSubNodeStartsSwept,
}

// nodeStartMaxAge bounds how long an unclaimed start time is kept. It is a
// backstop, not the primary release path: OnExecutionComplete releases entries
// as soon as the execution ends, and this only catches executions that ended
// without any notification at all (engine.go's loadActiveGraph evicts a graph
// whose state snapshot came back nil, and no hook fires for it).
//
// It is set well above any plausible node duration. Sweeping a node that is
// merely slow — or legitimately suspended for hours — would silently drop the
// duration observation it was about to make, which is worse than holding a few
// hundred bytes longer than necessary.
const nodeStartMaxAge = 6 * time.Hour

// nodeStartSweepInterval bounds how often the sweep runs. The sweep is O(live
// executions), and it is triggered from OnNodeStart, which sits on the
// per-message path — running it on every start would make node dispatch scale
// with the number of executions in flight.
const nodeStartSweepInterval = time.Minute

// endedTombstones bounds how many recently-ended executions are remembered so a
// racing node start can be rejected.
//
// A tombstone is needed because OnNodeStart and OnExecutionComplete are not
// ordered: the start fires from BuildTaskLease when a runner acquires the task
// (engine/lease.go:129), the end from the commit path, and nothing serialises the
// two for one execution. Deleting the entry outright therefore lets a start that
// began before the end land afterwards and create a fresh entry that nothing will
// ever release — the original leak, one narrow window smaller.
//
// Bounded by COUNT, not by age. At trigger rates every message is an execution,
// so a time-based tombstone would hold hundreds of thousands of entries; a fixed
// ring holds a fixed few hundred KiB, and 4096 ends is far more slack than a
// hook-ordering race can consume.
const endedTombstones = 4096

// MetricsHooks turns engine lifecycle hooks into xflow_ Prometheus counters.
type MetricsHooks struct {
	Metrics *Metrics

	// names is the metric family this instance writes. Both constructors set
	// it; it is not optional, and a zero value would emit empty metric names
	// rather than fall back to anything.
	names engineMetricNames

	// started holds node start times until OnNodeComplete consumes them, keyed
	// by execution and then by node.
	//
	// Two levels rather than one flat "exec\x00node" map because an execution
	// that ends has to release every node it started, and finding those in a
	// flat map means ranging the whole thing — O(live nodes) per execution,
	// which is quadratic at trigger rates where every message is its own
	// execution.
	started sync.Map // types.ExecutionID -> *execNodeStarts

	// endedMu guards the tombstone ring below.
	endedMu sync.Mutex
	// ended is a ring of the most recently released execution IDs. Evicting one
	// from the ring is what finally removes its tombstone from started.
	ended []types.ExecutionID
	// endedNext is the ring's write position once it is full.
	endedNext int

	// lastSweepUnixNano is the time of the last age sweep, as unix nanos so it
	// can be CAS'd from the hot path without a lock.
	lastSweepUnixNano atomic.Int64
}

// execNodeStarts holds one execution's in-flight node start times.
//
// ended marks the entry as a tombstone: OnExecutionComplete released it, and a
// node start that arrives afterwards must drop its start time rather than write
// into a map nobody will ever read again.
//
// at is a slice, not a map. An execution holds a handful of concurrently running
// nodes at most, and a Go map costs a ~300-byte allocation the moment it is
// created — measured as a 25% regression on the node hook path when every
// message is its own execution. A linear scan over single digits is cheaper than
// the hash, and the slice does not allocate at all until the first node starts.
type execNodeStarts struct {
	mu    sync.Mutex
	ended bool
	at    []nodeStart
}

type nodeStart struct {
	name string
	at   time.Time
}

func NewMetricsHooks(metrics *Metrics) *MetricsHooks {
	return &MetricsHooks{Metrics: metrics, names: topLevelMetricNames}
}

// NewSubgraphMetricsHooks builds hooks for an inner engine — the one a map body
// item or a group member runs on. It is a distinct instance rather than a shared
// one because the start-time bookkeeping is keyed by execution ID, and an inner
// execution's ID is its own; mixing the two populations in one map would make
// residency depend on how many items a map fans out to.
func NewSubgraphMetricsHooks(metrics *Metrics) *MetricsHooks {
	return &MetricsHooks{Metrics: metrics, names: subgraphMetricNames}
}

func (h *MetricsHooks) OnNodeStart(ctx context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc(h.names.nodeStarted, h.nodeLabels(ctx, id, name, ""))
	h.storeNodeStartAt(id, name, time.Now())
	h.maybeSweepNodeStarts(time.Now())
}

func (h *MetricsHooks) OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	labels := h.nodeLabels(ctx, id, name, string(status))
	h.Metrics.Inc(h.names.nodeCompleted, labels)
	if started, ok := h.takeNodeStart(id, name); ok {
		h.Metrics.Observe(h.names.nodeDuration, labels, time.Since(started))
	}
}

// storeNodeStartAt records when a node started, unless the execution has already
// been released. Dropping the start time in that case costs one duration
// observation for a node that was never going to report one anyway; keeping it
// would cost an entry that nothing removes.
func (h *MetricsHooks) storeNodeStartAt(id types.ExecutionID, name string, at time.Time) {
	e := h.entryFor(id)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return
	}
	// A re-lease fires OnNodeStart again for a node already running — after a
	// runner died, say. Overwrite rather than append, so the entry does not grow
	// once per redelivery and the duration measures the attempt that finishes.
	for i := range e.at {
		if e.at[i].name == name {
			e.at[i].at = at
			return
		}
	}
	e.at = append(e.at, nodeStart{name: name, at: at})
}

// entryFor returns this execution's entry, creating one if it has none.
//
// Load before LoadOrStore because LoadOrStore evaluates its argument
// unconditionally: passing a freshly built entry every time allocates one per
// node start and throws it away for every node after the first.
func (h *MetricsHooks) entryFor(id types.ExecutionID) *execNodeStarts {
	if v, ok := h.started.Load(id); ok {
		return v.(*execNodeStarts)
	}
	v, _ := h.started.LoadOrStore(id, &execNodeStarts{})
	return v.(*execNodeStarts)
}

// takeNodeStart removes and returns one node's start time.
func (h *MetricsHooks) takeNodeStart(id types.ExecutionID, name string) (time.Time, bool) {
	v, ok := h.started.Load(id)
	if !ok {
		return time.Time{}, false
	}
	e := v.(*execNodeStarts)
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.at {
		if e.at[i].name != name {
			continue
		}
		at := e.at[i].at
		last := len(e.at) - 1
		e.at[i] = e.at[last]
		// Clear the vacated tail slot: the slice keeps its capacity, and a stale
		// node name held there would pin that string for as long as the entry
		// lives.
		e.at[last] = nodeStart{}
		e.at = e.at[:last]
		return at, true
	}
	return time.Time{}, false
}

// releaseExecutionNodeStarts drops every start time still held for an execution
// and leaves a tombstone in its place. Anything released here belongs to a node
// that started and will never complete: a commit that arrived after the
// execution went terminal, or a suspended node moved to Canceled through
// UpsertNode. Neither fires OnNodeComplete.
func (h *MetricsHooks) releaseExecutionNodeStarts(id types.ExecutionID) {
	// entryFor, not Load: an execution with no in-flight node still needs the
	// tombstone, because a lease being built concurrently is exactly the start
	// that would otherwise create an entry after the release.
	e := h.entryFor(id)
	e.mu.Lock()
	e.ended = true
	e.at = nil
	e.mu.Unlock()
	h.tombstone(id)
}

// tombstone records id as recently ended and removes whichever tombstone falls
// out of the ring. This is the only path that deletes a released entry, which is
// what makes residency bounded by endedTombstones rather than by time.
func (h *MetricsHooks) tombstone(id types.ExecutionID) {
	h.endedMu.Lock()
	var evicted types.ExecutionID
	if len(h.ended) < endedTombstones {
		h.ended = append(h.ended, id)
	} else {
		evicted = h.ended[h.endedNext]
		h.ended[h.endedNext] = id
		h.endedNext = (h.endedNext + 1) % endedTombstones
	}
	h.endedMu.Unlock()

	if evicted == "" {
		return
	}
	// Only delete a tombstone. Execution IDs are unique, so the entry under this
	// key cannot be a different live execution — but a start that raced the
	// release could have been rejected and the entry is still the tombstone we
	// put there, so the check costs nothing and states the invariant.
	if v, ok := h.started.Load(evicted); ok {
		e := v.(*execNodeStarts)
		e.mu.Lock()
		isTombstone := e.ended
		e.mu.Unlock()
		if isTombstone {
			h.started.Delete(evicted)
		}
	}
}

// maybeSweepNodeStarts runs the age sweep at most once per
// nodeStartSweepInterval. The CAS is what keeps this cheap enough to call from
// the per-node path: every caller but one loses it and returns immediately.
func (h *MetricsHooks) maybeSweepNodeStarts(now time.Time) {
	last := h.lastSweepUnixNano.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < nodeStartSweepInterval {
		return
	}
	if !h.lastSweepUnixNano.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	if last == 0 {
		// First call only primes the clock. Sweeping here would range a map
		// that cannot hold anything stale yet.
		return
	}
	h.sweepStaleNodeStarts(now)
}

// sweepStaleNodeStarts drops start times older than nodeStartMaxAge and counts
// them. The count is the point: an entry reaching this age means its execution
// ended without notifying, and a silent sweep would hide that behind a duration
// series that is quietly missing observations.
//
// The count carries an empty namespace rather than the namespace of whichever
// execution happened to trigger the sweep. Those are unrelated — the sweep runs
// from an arbitrary caller's OnNodeStart and drops entries belonging to any
// namespace — and attributing the count to the trigger would name the wrong
// tenant. Storing a namespace alongside every start time to make this
// attributable is not worth the memory for a series expected to stay at zero.
func (h *MetricsHooks) sweepStaleNodeStarts(now time.Time) {
	swept := 0
	h.started.Range(func(key, value any) bool {
		e := value.(*execNodeStarts)
		e.mu.Lock()
		if e.ended {
			// A tombstone. The ring owns its lifetime; deleting it here would
			// re-open the race window it exists to close.
			e.mu.Unlock()
			return true
		}
		for i := 0; i < len(e.at); {
			if now.Sub(e.at[i].at) <= nodeStartMaxAge {
				i++
				continue
			}
			last := len(e.at) - 1
			e.at[i] = e.at[last]
			e.at[last] = nodeStart{}
			e.at = e.at[:last]
			swept++
			// No i++: the slot now holds the element moved from the tail, which
			// has not been examined yet.
		}
		empty := len(e.at) == 0
		e.mu.Unlock()
		if empty {
			// Nothing in flight and no end was ever reported. A live execution
			// whose entry is removed here simply gets a fresh one on its next
			// node start.
			h.started.Delete(key)
		}
		return true
	})
	if swept > 0 {
		h.Metrics.Add(h.names.nodeStartsSwept, map[string]string{"namespace": ""}, float64(swept))
	}
}

// liveNodeStarts counts start times currently held. Test-only: it is the
// residency the eviction paths exist to bound.
func (h *MetricsHooks) liveNodeStarts() int {
	n := 0
	h.started.Range(func(_, value any) bool {
		e := value.(*execNodeStarts)
		e.mu.Lock()
		n += len(e.at)
		e.mu.Unlock()
		return true
	})
	return n
}

// trackedExecutions counts map entries including tombstones. Test-only: it is
// the residency that bounds memory, whereas liveNodeStarts is the one with a
// correct value.
func (h *MetricsHooks) trackedExecutions() int {
	n := 0
	h.started.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func (h *MetricsHooks) OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc(h.names.nodeSuspended, h.nodeLabels(ctx, id, name, ""))
}

// OnExecutionComplete counts the finished execution and releases any node start
// times it still holds. The release is not bookkeeping: an execution can end
// while a node is still Running (a fatal sibling, or a Cancel), and on those
// paths OnNodeComplete never fires. Without this, every such node's start time
// stayed resident for the life of the process — one per Kafka message.
func (h *MetricsHooks) OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	h.Metrics.Inc(h.names.executionCompleted, withNamespace(ctx, map[string]string{"status": string(status)}))
	h.releaseExecutionNodeStarts(id)
}

func (h *MetricsHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *MetricsHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}

func (h *MetricsHooks) OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string) {
	h.Metrics.Inc(h.names.nodeTimedOut, h.nodeLabels(ctx, id, nodeName, ""))
}

func (h *MetricsHooks) OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, _ int, _ time.Duration) {
	h.Metrics.Inc(h.names.nodeRetried, h.nodeLabels(ctx, id, name, ""))
}

func (h *MetricsHooks) nodeLabels(ctx context.Context, _ types.ExecutionID, name string, status string) map[string]string {
	labels := map[string]string{"node": name}
	if status != "" {
		labels["status"] = status
	}
	return withNamespace(ctx, labels)
}

var _ engine.Hooks = (*MetricsHooks)(nil)

// CommitMetrics observes structured runner result-commit outcomes.
type CommitMetrics struct {
	Metrics *Metrics
}

// NewCommitMetrics creates a commit outcome observer backed by Metrics.
func NewCommitMetrics(metrics *Metrics) CommitMetrics {
	return CommitMetrics{Metrics: metrics}
}

// OnCommitOutcome records one stable low-cardinality commit classification.
func (c CommitMetrics) OnCommitOutcome(ctx context.Context, outcome engine.CommitOutcome) {
	c.Metrics.Inc(metricCommitOutcomes, withNamespace(ctx, map[string]string{"outcome": string(outcome)}))
}

var _ engine.CommitObserver = CommitMetrics{}

// OutboxMetrics observes durable outbox delivery failures and periodic backlog
// snapshots. It intentionally does not expose entry, execution, or error IDs.
type OutboxMetrics struct {
	Metrics *Metrics
}

// NewOutboxMetrics creates a durable outbox observer backed by Metrics.
func NewOutboxMetrics(metrics *Metrics) OutboxMetrics {
	return OutboxMetrics{Metrics: metrics}
}

// OnOutboxRetry records a retryable task-queue handoff failure.
func (o OutboxMetrics) OnOutboxRetry(ctx context.Context, _ int) {
	o.Metrics.Inc(metricOutboxRetries, withNamespace(ctx, nil))
}

// OnOutboxDeadLetter records an entry moved to durable dead-letter storage.
func (o OutboxMetrics) OnOutboxDeadLetter(ctx context.Context) {
	o.Metrics.Inc(metricOutboxDeadLettersTotal, withNamespace(ctx, nil))
}

// OnOutboxReplayed records a dead-letter replay attempt, partitioned by
// outcome (replayed/not_found/rejected_terminal/rejected_inactive).
func (o OutboxMetrics) OnOutboxReplayed(ctx context.Context, outcome engine.DeadLetterReplayOutcome) {
	o.Metrics.Inc(metricOutboxDeadLettersReplayed, withNamespace(ctx, map[string]string{"outcome": string(outcome)}))
}

// OnOutboxPending records the current pending and dead-letter backlog gauges
// and observes the age of the oldest pending entry when one exists.
func (o OutboxMetrics) OnOutboxPending(ctx context.Context, pending int, deadLettered int, oldestAge time.Duration) {
	o.Metrics.Set(metricOutboxPending, withNamespace(ctx, nil), float64(pending))
	o.Metrics.Set(metricOutboxDeadLetters, withNamespace(ctx, nil), float64(deadLettered))
	if pending > 0 {
		o.Metrics.Observe(metricOutboxOldestPendingAge, withNamespace(ctx, nil), oldestAge)
	}
}

// OnOutboxError records an outbox operation failure without using error text
// as a metric label.
func (o OutboxMetrics) OnOutboxError(ctx context.Context, operation string, _ error) {
	o.Metrics.Inc(metricOutboxErrors, withNamespace(ctx, map[string]string{"op": operation}))
}

var _ engine.OutboxObserver = OutboxMetrics{}
