package engine

import (
	"context"
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
	ClaimTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error)
	// ResetNodeForRetry rolls a Running node back to Pending so it can be
	// re-leased after a backoff delay. Implementations must:
	//   - validate that the current snapshot is in Running with a matching
	//     activation (silently no-op otherwise to keep the call idempotent),
	//   - clear LeaseID / LeaseToken,
	//   - preserve Attempt (incremented when the next lease is acquired) and
	//     ActivationID / AutoDepth.
	// Errors should be returned only for backend failures, not for
	// "nothing-to-reset" conditions.
	ResetNodeForRetry(ctx context.Context, id types.ExecutionID, name string) error
	// ListExpiredLeases returns every Running node whose lease deadline has
	// passed (LeaseIssuedAt+LeaseTTL <= before). Used by the sweeper to detect
	// runners that died mid-execute. Implementations may return at most a
	// reasonable per-call batch to avoid OOM on large backlogs; the sweeper
	// will re-poll until the list drains.
	ListExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error)
	// RevokeLease atomically clears the lease on a node whose deadline has
	// expired and rolls it back to Pending so the task can be re-enqueued.
	// Implementations MUST verify the supplied LeaseToken still matches before
	// mutating state — a non-matching token means the runner already committed
	// (or another sweeper beat us to it). Returns (revoked=true) only when the
	// caller is responsible for re-enqueuing the task.
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

// HandlerRegistry resolves an ActionHandler for a given execution + node.
type HandlerRegistry interface {
	Get(executionID types.ExecutionID, nodeName string, nodeType string, version int) (types.ActionHandler, error)
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
