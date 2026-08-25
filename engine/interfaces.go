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
	DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (remainingInDeg, arrivedActiveIn int, err error)
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

// LeaseSuspender atomically converts a previously claimed lease into a
// suspended node. It validates the original lease token, persists optional
// resume-base output, consumes or registers signals, and clears lease expiry
// discovery in one state transition. committed=false means recovery or a new
// lease won the fence before the suspend was applied.
type LeaseSuspender interface {
	SuspendTaskLease(ctx context.Context, lease *TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (payload *types.SignalPayload, committed bool, err error)
}

// DurableLeaseSuspender atomically converts a claimed lease to Suspended and
// records any immediate resume, timer, or timeout delivery intents in the same
// backend transition. A successful result is therefore recoverable even if the
// caller crashes before it can reach TaskQueue.
type DurableLeaseSuspender interface {
	SuspendTaskLeaseWithOutbox(ctx context.Context, lease *TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (committed bool, err error)
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
