package engine

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Executions stores workflow execution lifecycle state.
type Executions interface {
	CreateExecution(ctx context.Context, e *ExecutionSnapshot) error
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error)
}

// Graphs stores and loads compiled graph IR for executions.
type Graphs interface {
	LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error)
}

// Nodes stores per-node runtime state.
type Nodes interface {
	UpsertNode(ctx context.Context, n *NodeSnapshot) error
	GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error)
	// AcquireTaskLease atomically transitions a node into Running for the
	// supplied lease. On success it returns the previous snapshot (or nil when
	// no snapshot existed) and acquired=true. On failure it returns the current
	// conflicting snapshot and acquired=false without mutating state.
	//
	// Implementations MUST persist lease.Task's replayable fields with the
	// lease: Type, Payload, and Generation. ReclaimLease rebuilds the redelivery
	// task from what ListExpiredLeases returns, so a field dropped here comes
	// back zeroed and silently misroutes the redelivery — a lost Generation
	// reads as "first generation" and resurrects an abandoned generation's
	// child.
	AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error)
	ClaimTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error)
	// ResetNodeForRetry was an unfenced retry-reset method with no production
	// callers — engine retry paths use the fenced ResetNodeForRetryWithOutbox
	// (AtomicStateStore) instead. Removed from the interface to prevent future
	// callers from reaching the unfenced transition.
	// ListExpiredLeases returns every Running, Committing, or expansion-Waiting
	// node whose lease deadline has passed (LeaseIssuedAt+LeaseTTL <= before).
	// Committing and Waiting claims retain the same token/deadline so a crash
	// before finalization is swept by the normal recovery path. Each returned
	// ExpiredLease must carry the claiming task's Type, Payload, and Generation
	// as stored by AcquireTaskLease, because ReclaimLease rebuilds the
	// redelivery from this record alone. Implementations may return at most a
	// reasonable per-call batch to avoid OOM on large backlogs; the sweeper
	// will re-poll until the list drains.
	ListExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error)
	// RevokeLease atomically clears an active lease and rolls the node back to
	// Pending so the task can be re-enqueued. It is used both by the expired
	// lease sweeper and by an execution boundary that can prove dispatch failed
	// before a handler started. Implementations MUST verify the supplied
	// LeaseToken still matches before mutating state — a non-matching token
	// means the runner already committed or a newer owner was issued. Returns
	// (revoked=true) only when the caller is responsible for re-enqueuing the
	// task.
	RevokeLease(ctx context.Context, id types.ExecutionID, name string, token LeaseToken) (bool, error)
}

// LeaseAcquireOptions carries the route decision the caller already resolved
// from the compiled graph, so an acquire can re-decide node routing itself
// instead of trusting reads made before it.
type LeaseAcquireOptions struct {
	// AllowCycles selects the cyclic graph's activation rules: a cyclic task
	// must carry a positive activation id, and a terminal node may be re-entered
	// by a strictly higher activation. An acyclic graph rejects a terminal node
	// outright, whatever activation the task carries.
	AllowCycles bool
}

// LeaseAcquireResult is the full verdict of an acquire that checked the
// execution and the node's route in the same transition as the lease write.
type LeaseAcquireResult struct {
	// Acquired reports whether the node was transitioned to Running.
	Acquired bool
	// Previous is the node snapshot observed before the transition (nil when no
	// snapshot existed), with the same fields AcquireTaskLease returns.
	Previous *NodeSnapshot
	// ExecutionStatus is the execution's lifecycle status observed by the
	// acquire. It is meaningful only when ExecutionFound is true.
	ExecutionStatus types.ExecutionStatus
	// ExecutionFound reports whether the execution status record existed. A
	// missing record is not a lease refusal by itself: the backend refuses the
	// acquire too, and the caller evicts the execution and reports it inactive.
	ExecutionFound bool
}

// RouteCheckedLeaseAcquirer is the optional StateStore capability that folds
// the two reads the lease path used to make before its one write into the
// write's own atomic transition:
//
//	GetExecutionStatus (is the execution still non-terminal?)
//	GetNode            (is the node's route still active?)
//	AcquireTaskLease
//
// Both reads answered questions the lease write then had to trust, and the
// answer could go stale between a read and the write. The decisive case is the
// execution one: an execution that terminalizes in that window would still
// receive a lease, and its runner would execute and commit a node for work the
// control plane already accounted for as over. Folding the checks into the
// acquire closes that race and removes two round trips from the hot dispatch
// path (three become one on the Redis backend).
//
// The implementation MUST refuse, without mutating state:
//   - an acquire whose execution record is absent or terminal
//     (types.IsTerminalExecutionStatus; canceling and paused are live statuses
//     and stay acquirable);
//   - an acyclic acquire for a terminal node, whatever activation it carries;
//   - a cyclic acquire whose task carries no activation id (<= 0);
//
// and MUST keep every guard AcquireTaskLease applies. A backend without this
// interface keeps the engine's read-then-write path unchanged.
type RouteCheckedLeaseAcquirer interface {
	AcquireTaskLeaseChecked(ctx context.Context, lease *TaskLease, opts LeaseAcquireOptions) (LeaseAcquireResult, error)
}

// Scheduling stores DAG scheduling counters and completion state.
type Scheduling interface {
	DecrementInDegree(ctx context.Context, id types.ExecutionID, unitIdx int, portActive bool) (remainingInDeg, arrivedActiveIn int, err error)
	CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (allDone bool, hasFailed bool, err error)
}

// Signals stores suspend/signal coordination state.
type Signals interface {
	SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error)
	DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (resumeNode string, payload *types.SignalPayload, err error)
	ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error)
	RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error)
	AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error)
	ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error)
}

// SubExecutions stores loop/split child execution state.
type SubExecutions interface {
	CreateSubExecution(ctx context.Context, sub *SubExecution) error
	CompleteSubExecution(ctx context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (allDone bool, err error)
	GetSubExecutionResults(ctx context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error)
}

// Outputs stores node output payloads.
type Outputs interface {
	PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
	GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error)
}

// BatchOutputReader is the optional batch form of Outputs.GetOutput: it reads
// many node outputs of one execution in a single round trip.
//
// buildInput is its only caller, and it reads outputs in exactly the places
// where a node's shape makes the count large: a fan-in reads one output per
// inbound edge, and a $nodes prefetch reads one per referenced node. Asked one
// name at a time, a node with F inbound edges and R references paid F+R
// sequential round trips before its handler could even start — the read side of
// the same fan-in cost the batch commit's write side already amortizes.
//
// The result maps output name to payload. A name with no stored output is
// ABSENT from the map, which is the same absence GetOutput reports as
// (nil, nil); callers must therefore treat a missing key as a nil payload. The
// returned map's iteration order carries no meaning — callers address it by
// name — and duplicate names in names are one read, not two.
//
// A backend that does not implement this interface is read one name at a time
// by the caller, so the two forms MUST agree per name for the same reason
// ExecutionStatusBatchReader and ExecutionStatusReader must.
type BatchOutputReader interface {
	GetOutputs(ctx context.Context, id types.ExecutionID, names []string) (map[string]map[string]any, error)
}

// ExecutionInputContext is the narrow slice of an execution's state that
// buildInput needs to assemble one node's input.
//
// It exists because buildInput runs on every task — BuildTaskLease and
// RecoverTaskLease both call it — and reads exactly these fields, while the
// snapshot it used to ask for is assembled from fourteen keys: the parent and
// rerun links, the terminal error, the test-run flag, the graph pointer and the
// registration namespace were decoded on every task and then dropped on the
// floor.
//
// Runtime is the SAME typed value GetExecution returns (types.Runtime marshals
// to {"vars": ...}); decoding it any other way would make the narrow read
// disagree with the full one for $vars, which every node reads.
//
// TraceID and SpanID are part of the slice even though they are not business
// input: execution/subgraph re-submits a child execution with the entry input's
// trace identity, so a narrow read that dropped them would detach a whole
// sub-execution from the caller's trace. A caller that skipped them would have
// to fall back to the full snapshot and lose the saving entirely.
type ExecutionInputContext struct {
	Status  types.ExecutionStatus
	Params  map[string]any
	Runtime *types.Runtime
	Scope   map[string]any
	TraceID string
	SpanID  string
}

// ExecutionInputContextReader is the optional narrow form of
// Executions.GetExecution used by buildInput.
//
// found=false means no such execution, which is the same signal GetExecution
// encodes as a nil snapshot; it is deliberately distinct from a zero-valued
// status so callers never mistake "gone" for "created but empty". A backend
// that does not implement this interface is read through GetExecution, so the
// two MUST agree field by field — a backend reporting different params, scope,
// runtime or trace identity here than GetExecution would make a node's input
// depend on which method the engine happened to call. The shared contract suite
// pins that agreement.
type ExecutionInputContextReader interface {
	GetExecutionInputContext(ctx context.Context, id types.ExecutionID) (*ExecutionInputContext, bool, error)
}

// ExecutionTraceCarrierReader is the optional narrow read of the W3C
// traceparent/tracestate carrier captured at submission.
//
// The control plane asks for this carrier on every dispatch to parent the
// asynchronous xflow.task.dispatch span to the original submit/invoke trace.
// Answering through GetExecution made that one question pay for the whole
// snapshot; this reads the single carrier key instead.
//
// The carrier is written once, when the execution is created, and never
// updated, which is what makes it safe for a caller to keep the answer. found=false
// means no carrier — an execution submitted with tracing disabled, or one that
// does not exist — and the engine treats both as nil, exactly as it does today
// for a nil snapshot or an empty TraceCarrier. A backend that does not
// implement this interface is read through GetExecution, so the two MUST agree
// for an execution that has one.
type ExecutionTraceCarrierReader interface {
	GetExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, bool, error)
}

// Events publishes and watches execution lifecycle events.
type Events interface {
	PublishExecutionEvent(ctx context.Context, event ExecutionEvent) error
	WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan ExecutionEvent, error)
}

// StateStore is the complete persistence abstraction used by the Engine.
type StateStore interface {
	Executions
	Graphs
	Nodes
	Scheduling
	Signals
	SubExecutions
	Outputs
	Events
}

// TaskQueue enqueues tasks for execution (immediate or delayed).
type TaskQueue interface {
	Enqueue(ctx context.Context, t *Task) error
	EnqueueDelayed(ctx context.Context, t *Task, delay time.Duration) error
}

// NonBlockingTaskQueue is an optional TaskQueue capability: offer a task and
// report ErrQueueFull rather than waiting for room.
//
// Only FlushOutbox uses it, and only because it must. FlushOutbox runs on a
// queue worker goroutine, so a bounded queue whose Enqueue blocks turns a
// fan-out wider than the queue's capacity into a permanent deadlock: every
// worker parks inside the send, and the only goroutines that could drain the
// queue are those same workers. Nothing is logged and no error is returned —
// the execution simply stops.
//
// Other callers keep blocking Enqueue on purpose. Submit's initial tasks and
// the legacy lease-revoke redelivery have no durable intent behind them, so
// failing them on a transient full queue would strand work that no sweeper can
// recover. FlushOutbox is safe precisely because the intent stays in the outbox.
//
// A queue whose Enqueue never blocks — one writing to an external broker, say
// — has no reason to implement this.
type NonBlockingTaskQueue interface {
	TryEnqueue(ctx context.Context, t *Task) error
}

// ErrQueueFull reports that a bounded queue has no room right now. It is
// backpressure, not a delivery failure: the intent stays in the outbox
// unacknowledged and its delivery-attempt counter is left alone, so repeated
// backpressure can never push an entry into the dead-letter store.
var ErrQueueFull = errors.New("task queue full")

// LegacyNodeCommitter is the fenced terminal-transition capability used by
// cyclic and experimental loop/split paths. It deliberately does not apply
// static-DAG completion counters or schedule downstream work: those paths
// retain their own scheduling protocol.
type LegacyNodeCommitter interface {
	CommitLeasedNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error)
}

// SuspendedNodeCanceler atomically transitions a node from Suspended to
// Canceled. It reports canceled=false (no error) when the node is no longer
// Suspended — e.g. a concurrent signal/timer resume already moved it to
// Running and issued a fresh lease — so the caller leaves that live lease
// untouched. This closes the Cancel TOCTOU window that a read-then-write
// UpsertNode cannot: between GetNode and UpsertNode a resume can slip in, and a
// blind UpsertNode(Canceled) would clobber the running lease. Optional: Cancel
// falls back to the best-effort read-then-write path when a backend does not
// implement it.
type SuspendedNodeCanceler interface {
	CancelSuspendedNode(ctx context.Context, id types.ExecutionID, nodeName string) (canceled bool, err error)
}

// ExecutionPauser is the optional StateStore capability that suspends and
// resumes an execution's dispatch.
//
// Pause has to be a store transition rather than an engine-side flag, for the
// same reason ConcurrencyLimitedStateStore is: the enforcement point is the
// store's own dispatch transaction. The engine delivers work by claiming
// outbox intents (AtomicStateStore.LeaseOutbox), and a check made anywhere
// else — in the engine before claiming, or in the flusher after a claim — is a
// check racing the very transition it is deciding: a claim that read "running"
// can still be granted after the pause landed, because between the read and
// the claim the pause may have become durable. The store that grants the claim
// is the only place that can refuse it atomically with the pause, so every
// backend that can hold an execution's work must refuse it there.
//
// What a pause does NOT stop is work already leased to a runner. That boundary
// is the point of the status: a leased task is executing outside the store's
// control, so its commit is accepted as usual — it may even finalize the
// execution — while the downstream work that commit produces stays in the
// outbox, undeliverable, until a resume releases it.
//
// A paused execution keeps its workflow concurrency slot, unlike a queued one.
// It holds intermediate state — in-flight leases, counter positions, buffered
// signals — that only a resume can put back to work, and releasing the slot
// would admit a replacement competing with the work the resume restarts.
//
// Pause is deliberately refused for an execution that is not running. A queued
// execution holds no intermediate state at all: nothing of it has ever been
// dispatched, so there is nothing to suspend. The caller gets
// ErrExecutionNotPausable, and cancels it instead if that is the intent.
type ExecutionPauser interface {
	// PauseExecution suspends dispatch of a running execution: its status
	// becomes types.ExecutionStatusPaused and nothing more of its ready set is
	// claimable, while the intents themselves are retained for a resume.
	//
	// An already-paused execution is a no-op success: the caller asked for a
	// state the execution is already in, and reporting a conflict would make a
	// retried pause — the ordinary shape after a response was lost — look like
	// a failure. Every other status returns ErrExecutionNotPausable, and an
	// absent execution returns it too: nothing was there to change.
	PauseExecution(ctx context.Context, id types.ExecutionID) error
	// ResumeExecution returns a paused execution to dispatch: its status
	// becomes running again and the work the pause retained becomes claimable.
	//
	// An execution that is not paused returns ErrExecutionNotPaused —
	// including one that reached a terminal state while it was paused, because
	// its last leased task committed while it was suspended. Resume is the
	// inverse of pause, not a way to start anything else.
	ResumeExecution(ctx context.Context, id types.ExecutionID) error
}

// ErrExecutionNotPausable reports a pause of an execution that is not running:
// queued, canceling, terminal, or absent. Nothing was changed.
var ErrExecutionNotPausable = errors.New("execution is not pausable")

// ErrExecutionNotPaused reports a resume of an execution that is not paused.
// Nothing was changed.
var ErrExecutionNotPaused = errors.New("execution is not paused")

// ErrPauseUnsupported reports a StateStore that has not implemented
// ExecutionPauser. It is not a verdict about the execution: the store cannot
// suspend anything, so no execution on it can be paused.
var ErrPauseUnsupported = errors.New("state store does not support execution pause")

// ExecutionStatusReader reads back only an execution's lifecycle status.
//
// This exists because the hot paths do not want a snapshot. loadActiveGraph
// asks one question — is this execution still non-terminal — and it asks it on
// every commit, lease, advance, signal, group and subgraph transition. Answering
// it through GetExecution makes the backend assemble params, runtime, scope,
// trace_id, span_id, trace_carrier and error as well, all of which the caller
// then drops on the floor. On a key-value backend that is seven extra reads and
// four JSON decodes per question.
//
// found=false means no such execution, which callers must distinguish from a
// zero-valued status; it is the same signal GetExecution encodes as a nil
// snapshot. Optional: loadActiveGraph falls back to GetExecution when a backend
// does not implement it, so the two MUST agree — a backend that reports a
// different status here than GetExecution().Status would make activeness depend
// on which method the engine happened to call. The shared contract suite pins
// that agreement.
type ExecutionStatusReader interface {
	GetExecutionStatus(ctx context.Context, id types.ExecutionID) (status types.ExecutionStatus, found bool, err error)
}

// ExecutionTerminalReader reads the terminal marker a backend may keep after an
// execution's status record is gone.
//
// It exists because the classification of an inactive execution cannot assume
// the status key's lifetime equals the retention the caller cares about. In
// transient mode the completion TTL deliberately shortens the status key to a
// fraction of the active TTL, and in a short-TTL durable deployment the status
// key simply expires. A task can then be consumed long after its execution
// finished (queue backlog), and without a marker the control plane would read
// "no status" as "work was never executed" — a false data-loss alarm on every
// late duplicate, exactly during the backlog incident the classification is for.
//
// Implementations write the marker whenever an execution reaches a terminal
// state, with a retention at least the execution's ACTIVE retention, and never
// shorten it with the rest of the execution's keys. found=false is the honest
// "no marker" answer; backends without expiring state (the in-memory store) may
// omit this interface entirely, because a status record there is never removed
// by time — the classifier falls back to Gone when the interface is absent,
// which is the correct answer for a backend whose absence really is absence.
type ExecutionTerminalReader interface {
	GetExecutionTerminalStatus(ctx context.Context, id types.ExecutionID) (status types.ExecutionStatus, found bool, err error)
}

// ExecutionRetentionReader reports the backend's evidence window for an
// execution: the active retention (execTTL, or a transient execution's active
// TTL) that bounds how long any record of the execution — the status key or a
// terminal marker — can survive after its last write. It is what lets the
// classifier bound its own verdict.
//
// The window exists because a terminal marker's lifetime is finite while a
// queue backlog is not: past the window, an execution that finished long ago
// is indistinguishable from one that expired under its queued work, and a
// "no evidence" verdict would report the former as lost work. The classifier
// therefore only calls a stamped delivery Gone when its wait is inside the
// window; beyond it (or with no stamp to measure the wait) the verdict is
// Unattributed, which claims neither loss nor health.
//
// Return 0 when no expiry applies — an in-memory store, or a deployment with
// time-unbounded state. Absence of evidence is then conclusive and the
// classifier keeps its original Gone verdict. Backends with expiring state
// should implement this and must return the TTL that actually bounds their
// status/marker writes, so the two cannot drift apart.
//
// Return ExecutionRetentionUnknown when the backend cannot determine the
// retention that actually bounded the execution's writes — for example, the
// record of a per-execution override has itself expired, so the best
// available fallback would report a longer window than the writes can
// support. The classifier then refuses the comparison and lands a no-evidence
// delivery in Unattributed: reporting a window the backend cannot stand
// behind is the one failure direction this contract exists to prevent, and
// under-reporting the loss claim is the deliberate cost.
type ExecutionRetentionReader interface {
	GetExecutionRetention(ctx context.Context, id types.ExecutionID) (time.Duration, error)
}

// ExecutionRetentionUnknown is the value an ExecutionRetentionReader returns
// when it cannot confirm the retention that bounded an execution's writes.
// It is negative — no real retention is — and distinct from 0, which keeps
// its meaning of "no expiry applies, so absence is conclusive". The
// classifier maps it to Unattributed rather than ever comparing a delivery's
// age against it; see ExecutionRetentionReader and
// Engine.inactiveExecutionError.
const ExecutionRetentionUnknown time.Duration = -1

// ExecutionStatusBatchReader is the optional batch form of ExecutionStatusReader:
// it answers the same activeness question for many executions in one round trip.
//
// It exists because both of its callers ask that question about a whole PAGE of
// assignments and need only a yes/no per execution — the runner's claim path and
// the dead-queued-assignment reaper — and because a page of candidates is expected
// to be mostly dead. Asked one id at a time, a 64-entry claim page cost 64
// sequential round trips and a 512-candidate reaper pass cost 512, so nearly the
// entire cost of both operations was spent learning "this one is gone".
//
// The result is positionally aligned with ids and an empty status means "no such
// execution" — the same absence GetExecutionStatus reports as found=false. A reader
// that does not implement this interface is probed one id at a time by the caller,
// so the two MUST agree per id, for the same reason GetExecutionStatus and
// GetExecution must.
type ExecutionStatusBatchReader interface {
	GetExecutionStatuses(ctx context.Context, ids []types.ExecutionID) ([]types.ExecutionStatus, error)
}

// CancelingExecution is one execution a backend has parked in
// types.ExecutionStatusCanceling, as reported by CancelingExecutionLister.
type CancelingExecution struct {
	ExecutionID types.ExecutionID
	// Namespace is the namespace the execution's keys live under, so a
	// maintenance sweep can re-drive the cancel against the namespace-scoped
	// store. It is empty when the backend has no namespace dimension (the
	// in-memory backend).
	Namespace namespace.Namespace
	// Since is the instant the backend first recorded the execution entering
	// canceling. It is a lower bound on the transition: a backend registers the
	// entry before it writes the status, so the value can precede the write by
	// one round trip — which can only make the execution look older, never
	// younger, so a stuck-threshold comparison stays conservative.
	Since time.Time
}

// CancelingExecutionLister is the optional StateStore capability a
// leader-driven maintenance sweep uses to find executions left in canceling by
// an interrupted Cancel.
//
// Cancel is idempotent and resumable (engine.Cancel): every step is fenced and
// can be re-run, and a second call on a canceling execution carries on from
// where the first one stopped. What it cannot do is run after the request that
// started it is gone. A crash, a canceled context, or one failed store step
// between the canceling write and the canceled write leaves the execution —
// and its workflow concurrency slot — in canceling with no actor left to finish
// it. This capability is what makes such an execution discoverable again.
//
// The invariant a backend must hold is REGISTER-BEFORE-WRITE: the entry for an
// execution is recorded before (or atomically with) a transition to canceling,
// and a failure to record it must fail that transition. A registration whose
// transition never landed is harmless — List verifies the live status and
// repairs the entry — but a canceling execution with no entry is exactly the
// invisible stuck state this exists to prevent. The timestamp is taken from the
// registration, and a repeated registration must not move it: the age is what
// the caller compares against its stuck threshold, so re-registering on every
// retried Cancel would reset the clock forever.
//
// ListCancelingExecutions returns at most limit entries whose Since is at or
// before olderThan — the caller's stuck threshold. It must:
//   - report the namespace alongside each execution;
//   - return only executions whose live status is still canceling, dropping
//     (or tolerating until dropped) entries whose execution left canceling or
//     no longer exists;
//   - be safe to run concurrently with ordinary cancel writes; and
//   - not require a namespace in ctx: the scan spans the namespaces the
//     backend serves.
type CancelingExecutionLister interface {
	ListCancelingExecutions(ctx context.Context, olderThan time.Time, limit int) ([]CancelingExecution, error)
}

// LeaseSuspender atomically converts a previously claimed lease into a
// suspended node. It validates the original lease token, persists optional
// resume-base output, consumes or registers signals, and clears lease expiry
// discovery in one state transition. committed=false means recovery or a new
// lease won the fence before the suspend was applied.
type LeaseSuspender interface {
	SuspendTaskLease(ctx context.Context, lease *TaskLease, output map[string]any, storeOutput bool, privateOutput bool, spec *types.SuspendSpec, oldSignalName string) (payload *types.SignalPayload, committed bool, err error)
}

// DurableLeaseSuspender atomically converts a claimed lease to Suspended and
// records any immediate resume, timer, or timeout delivery intents in the same
// backend transition. A successful result is therefore recoverable even if the
// caller crashes before it can reach TaskQueue.
type DurableLeaseSuspender interface {
	SuspendTaskLeaseWithOutbox(ctx context.Context, lease *TaskLease, output map[string]any, storeOutput bool, privateOutput bool, spec *types.SuspendSpec, oldSignalName string) (committed bool, err error)
}

// LeaseExpander coordinates the experimental Loop/Split parent state with its
// child batches. Every operation validates the original parent lease so a
// batch from a reclaimed expansion cannot update or finalize a newer attempt.
type LeaseExpander interface {
	BeginTaskExpansion(ctx context.Context, lease *TaskLease) (started bool, err error)
	CreateExpandedSubExecution(ctx context.Context, lease *TaskLease, sub *SubExecution) (accepted bool, err error)
	CompleteExpandedSubExecution(ctx context.Context, lease *TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (allDone bool, accepted bool, results []map[string]any, err error)
}

// DurableLeaseExpander atomically changes a claimed parent to Waiting, stores
// its generation-scoped child records, and records every batch delivery intent.
// This prevents a crash or queue outage from stranding a recoverable parent
// after the children have been created.
type DurableLeaseExpander interface {
	BeginTaskExpansionWithOutbox(ctx context.Context, lease *TaskLease, children []SubExecution, entries []OutboxEntry) (started bool, err error)
}

// SubWorkflowLink is the parent-side record of one xflow.subworkflow node's
// child execution: which execution the node is waiting on, and the lease it
// parked under.
//
// It is written once per node generation, in the same transition that moves the
// node to Waiting (BeginSubWorkflowWait), and it outlives that wait for two
// callers that cannot reconstruct it otherwise:
//
//   - the completion path, which commits the parent node from the child's
//     terminal verdict. The commit is fenced by the ORIGINAL lease identity, not
//     a freshly acquired one, so a late notification cannot commit a node that
//     has since been reclaimed or retried; the link is the only place that
//     identity is still recorded once the node itself has moved on.
//   - the redelivery path after a reclaim, which must resume the SAME child
//     rather than mint a second one. Re-deriving the child id from the lease
//     would not do: a reclaimed redelivery acquires a new lease, so the
//     derivation inputs are not stable across generations. The link is.
//
// Generation is what separates the two cases that both present as "a task for a
// node that already has a link": a reclaim redelivers the generation's own task
// (task generation == link generation: resume the same child), while a retry
// schedules a new one (task generation > link generation: mint the next child
// and overwrite the link). It is a monotonic counter local to (execution, node,
// activation), not a clock and not the lease attempt.
type SubWorkflowLink struct {
	// ChildExecutionID is the execution this node started and is waiting on.
	ChildExecutionID types.ExecutionID
	// ParentNode is the node that owns the link. It is redundant with the read
	// key on purpose: a link read back during recovery is validated against the
	// node it claims to belong to.
	ParentNode string
	// Generation is the child generation this link belongs to, starting at 1.
	Generation int
	// Depth is the child execution's sub-workflow nesting depth, so a
	// redelivery after a reclaim can re-check the runtime bound without
	// re-deriving it from a snapshot that may be gone.
	Depth int
	// WaitDeadline is the absolute instant this wait ends. A node that declares
	// a timeout parks with that bound; one that declares none parks with the
	// lease's own deadline, the reclaim clock that doubles as the notification
	// budget. A re-park after a reclaim keeps the value it was parked with, so
	// the wait's end does not move with the lease — which is what lets the
	// dispatch path fail a wait that has outlived its node timeout.
	WaitDeadline time.Time
	// LeaseID, LeaseToken, Attempt and ActivationID identify the lease the node
	// parked under. The completion path commits with exactly these, which is
	// what makes a stale notification a stale-token refusal rather than a
	// second commit.
	LeaseID      LeaseID
	LeaseToken   LeaseToken
	Attempt      int
	ActivationID int
}

// SubWorkflowLinkStore is the optional StateStore capability an
// xflow.subworkflow node needs: a single-slot transition that parks a claimed
// node on a child execution and records the link, and a read of that link.
//
// It is deliberately not "begin the wait and create the child" in one
// transaction. The child lives in its own execution slot — a different key, and
// on the Redis backend a different hash tag — so the two writes can never be
// atomic (the concurrency design hit the same wall for slot keys). What
// replaces atomicity is idempotence: the child id is derived from the parent,
// node and generation, so recreating it is a no-op after a crash, and a
// redelivery reads the link to resume exactly the child it parked on.
//
// A store that does not implement this cannot run a graph containing
// sub-workflow nodes, and the engine refuses such a submission up front with
// ErrSubWorkflowUnsupported rather than degrading to "create the child and move
// on without a link", which would strand the node Pending with a live child.
type SubWorkflowLinkStore interface {
	// BeginSubWorkflowWait atomically moves a claimed node to Waiting, preserving
	// the lease identity it parked under, and stores link. Implementations MUST
	// fence on the lease (LeaseID/LeaseToken/Attempt/ActivationID) and on the
	// node being in a claimable state — Running or Committing — exactly as
	// DurableLeaseExpander fences its parent. entries are delivery intents
	// written in the same transition when the backend supports an outbox.
	//
	// accepted=false means the fence refused: the task was reclaimed, retried,
	// or committed under a newer lease, and the caller must discard its work
	// rather than create a child nothing is waiting for.
	BeginSubWorkflowWait(ctx context.Context, lease *TaskLease, link SubWorkflowLink, entries []OutboxEntry) (accepted bool, err error)
	// GetSubWorkflowLink returns the link recorded for a node. ok=false means no
	// link exists — the node never parked, or the execution is gone. An
	// implementation must not error on absence.
	GetSubWorkflowLink(ctx context.Context, id types.ExecutionID, nodeName string) (SubWorkflowLink, bool, error)
}

// DurableSignalDeliverer atomically consumes an external signal that wakes a
// suspended waiter and records the resume delivery intent in the same state
// transition. This closes the window where the legacy two-step
// (state.DeliverSignal → queue.Enqueue) could lose a consumed signal if the
// caller crashed before enqueue succeeded — leaving the node stranded.
type DurableSignalDeliverer interface {
	// PeekResumeTarget returns the node name currently suspended and waiting
	// for signalName, or "" when no waiter exists (the signal will be stored).
	// It does not consume the signal; the subsequent DeliverSignalWithOutbox
	// re-validates atomically.
	PeekResumeTarget(ctx context.Context, id types.ExecutionID, signalName string) (string, error)
	// DeliverSignalWithOutbox atomically consumes the signal and writes a
	// resume outbox entry. resumeNode is non-empty and committed=true when a
	// waiter was woken (the engine then flushes the outbox); committed=false
	// (resumeNode="") when the signal was stored because no waiter exists or
	// multi-signal quorum was not yet reached.
	//
	// An empty intent.NodeName means the caller's PeekResumeTarget found no
	// waiter. A waiter may have appeared since; an implementation that finds one
	// must NOT consume it, because it cannot build a resume task from an intent
	// with no node — the indices would be zero and address the wrong unit. Store
	// the signal, leave the waiter and its timeout intact, and let the next
	// delivery (whose peek succeeds) drive the resume.
	//
	// payload is not part of the contract. The distributed backend always
	// returns nil and carries the payload only inside the outbox entry; the
	// memory backend returns it. The engine discards it either way.
	DeliverSignalWithOutbox(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any, intent ResumeIntent) (resumeNode string, payload *types.SignalPayload, committed bool, err error)
}

// ResumeIntent carries the graph metadata the backend needs to construct the
// resume task when atomically consuming a signal. The engine resolves it after
// peeking the resume target.
type ResumeIntent struct {
	NodeName string
	NodeIdx  int
	UnitIdx  int
	// ActivationID is retained for legacy callers but is IGNORED by the durable
	// DeliverSignalWithOutbox path: that path reads the authoritative live
	// activation_id from node meta inside the backend's atomic Lua transaction,
	// closing the TOCTOU window where a concurrent re-suspend could make a
	// Go-side snapshot stale.
	ActivationID int
	AutoDepth    int
}

// HandlerRegistry resolves an ActionHandler for a given execution + node.
type HandlerRegistry interface {
	Get(executionID types.ExecutionID, nodeName string, nodeType string, version int) (types.ActionHandler, error)
}

// HandlerRegistrar is the write-side of a handler registry: it lets the SDK
// register process-local and execution-scoped action handlers without depending
// on a concrete registry implementation. Implementations MUST be safe for
// concurrent use. Read-side resolution stays on HandlerRegistry; the two are
// kept separate so a read-only or remote registry can implement only Get.
type HandlerRegistrar interface {
	RegisterGlobal(nodeType string, h types.ActionHandler)
	RegisterNodeHandler(nodeName string, h types.ActionHandler)
	RegisterExecutionHandler(id types.ExecutionID, nodeName string, h types.ActionHandler)
}

// Hooks receives lifecycle events from the engine. All methods must be non-blocking.
type Hooks interface {
	OnNodeStart(ctx context.Context, id types.ExecutionID, name string)
	OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus)
	OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string)
	OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus)
	// Signal events
	OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any)
	OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string)
	OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string)
	// OnNodeRetry fires when the engine schedules a retry for a transient
	// handler failure (RetrySettings.MaxAttempts not yet exhausted). delay is
	// the backoff before the requeued task will run.
	OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, attempt int, delay time.Duration)
}

// Logger is the logging surface accepted by engine internals.
type Logger interface {
	Debug(msg string, args ...any)
	Debugf(format string, args ...any)
	Info(msg string, args ...any)
	Infof(format string, args ...any)
	Warn(msg string, args ...any)
	Warnf(format string, args ...any)
	Error(msg string, args ...any)
	Errorf(format string, args ...any)
	Panic(msg string, args ...any)
	Panicf(format string, args ...any)
}
