package engine

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine/graph"
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
	AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error)
	ClaimTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error)
	// ResetNodeForRetry was an unfenced retry-reset method with no production
	// callers — engine retry paths use the fenced ResetNodeForRetryWithOutbox
	// (AtomicStateStore) instead. Removed from the interface to prevent future
	// callers from reaching the unfenced transition.
	// ListExpiredLeases returns every Running, Committing, or expansion-Waiting
	// node whose lease deadline has passed (LeaseIssuedAt+LeaseTTL <= before).
	// Committing and Waiting claims retain the same token/deadline so a crash
	// before finalization is swept by the normal recovery path. Implementations may return at most a reasonable
	// per-call batch to avoid OOM on large backlogs; the sweeper will re-poll
	// until the list drains.
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
