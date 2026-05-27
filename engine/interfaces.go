package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// StateBackend is the persistence abstraction used by the Engine.
// Local mode implements it with in-memory maps; cluster mode uses Redis + MySQL.
type StateBackend interface {
	// Execution lifecycle
	CreateExecution(ctx context.Context, e *ExecutionSnapshot) error
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error)

	// LoadGraph retrieves the persisted Graph for an execution.
	// Used to recover the graph after a worker restart (cluster mode).
	LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error)

	// Node state
	UpsertNode(ctx context.Context, n *NodeSnapshot) error
	GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error)

	// Scheduling counters — atomically decrement in-degree and return updated counts.
	// portActive is true when the arriving edge's source port matches the active output port.
	// Returns (remainingInDeg, arrivedActiveIn, error).
	DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (remainingInDeg, arrivedActiveIn int, err error)

	// Completion check — returns (allDone, hasFailed, error).
	CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (allDone bool, hasFailed bool, err error)

	// Suspend / signal — atomic check-and-set to avoid races between suspend and signal delivery.
	// Returns a pre-delivered payload if one exists (immediate resume), or nil (node is now parked).
	SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *node.SuspendSpec) (*node.SignalPayload, error)
	// DeliverSignal stores the signal or wakes a parked node.
	// Returns the node name to resume, or "" if the signal was stored for later.
	DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (resumeNode string, err error)
	// ResuspendAtomic atomically transitions a node from one suspended signal to another.
	// It releases the resume lock, removes the old waiter, and either consumes a pre-delivered
	// signal for the new name (returning the payload) or parks the node on the new signal.
	ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
	// RevokeSignal atomically removes a previously delivered signal that has not yet been consumed.
	// Returns (true, nil) if the signal was successfully revoked, (false, nil) if it was already
	// consumed or does not exist.
	RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error)
	// AcquireResumeLock prevents duplicate resume tasks when multiple signals race.
	AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error)

	// Cancel support
	ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error)

	// Output store
	PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
	GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error)
}

// TaskQueue enqueues tasks for execution (immediate or delayed).
type TaskQueue interface {
	Enqueue(ctx context.Context, t *Task) error
	EnqueueDelayed(ctx context.Context, t *Task, delay time.Duration) error
}

// HandlerRegistry resolves a TaskHandler for a given execution + node.
type HandlerRegistry interface {
	Get(executionID types.ExecutionID, nodeName string, nodeType string) (node.TaskHandler, error)
}

// Hooks receives lifecycle events from the engine. All methods must be non-blocking.
type Hooks interface {
	OnNodeStart(ctx context.Context, id types.ExecutionID, name string)
	OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status string)
	OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string)
	OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.Status)
	// Signal events
	OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any)
	OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string)
	OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string)
}

// Logger is a minimal structured logger interface.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}
