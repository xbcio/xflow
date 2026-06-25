package engine

import (
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TaskType distinguishes initial execution from a resume after suspension.
type TaskType int

const (
	TaskTypeNodeExec   TaskType = iota // normal first-time execution
	TaskTypeNodeResume                 // resume after signal/timer
)

// Task is the unit of work dispatched to the queue.
type Task struct {
	ExecutionID types.ExecutionID    `json:"execution_id"`
	NodeName    string               `json:"node_name"`
	NodeIdx     int                  `json:"node_idx"`
	Type        TaskType             `json:"type"`
	Payload     *types.SignalPayload `json:"payload,omitempty"` // non-nil only for TaskTypeNodeResume
}

// LeaseID uniquely identifies one assignment of a queued task to a runner.
type LeaseID string

// LeaseToken is the fencing token required when committing a leased task result.
type LeaseToken string

// NodeResult is the runner-facing result submitted after a node task executes.
type NodeResult struct {
	Output  *types.Output      `json:"output,omitempty"`
	Suspend *types.SuspendSpec `json:"suspend,omitempty"`
	Error   error              `json:"-"`
}

// TaskResult is the protocol-facing task execution result.
type TaskResult = NodeResult

// TaskLease is the server-side assignment sent to a runner through Runner Protocol.
type TaskLease struct {
	LeaseID     LeaseID      `json:"lease_id,omitempty"`
	LeaseToken  LeaseToken   `json:"lease_token,omitempty"`
	Attempt     int          `json:"attempt,omitempty"`
	Task        Task         `json:"task"`
	Input       *types.Input `json:"input,omitempty"`
	NodeType    string       `json:"node_type"`
	NodeVersion int          `json:"node_version,omitempty"`
}

// RunnerCapability describes a node type/version a runner can execute.
type RunnerCapability struct {
	NodeType string `json:"node_type"`
	Version  int    `json:"version,omitempty"`
}

// RunnerHeartbeat reports runner capacity and supported capabilities.
type RunnerHeartbeat struct {
	RunnerID     string             `json:"runner_id"`
	Capacity     int                `json:"capacity"`
	InFlight     int                `json:"in_flight"`
	Capabilities []RunnerCapability `json:"capabilities,omitempty"`
}

// ExecutionSnapshot is the engine's view of a running execution stored in the backend.
type ExecutionSnapshot struct {
	ID       types.ExecutionID
	Graph    *graph.Graph
	Status   types.ExecutionStatus
	Params   map[string]any
	ParentID types.ExecutionID // non-empty for sub-executions
}

// NodeSnapshot is the engine's view of a single node's state stored in the backend.
type NodeSnapshot struct {
	ExecutionID types.ExecutionID
	Name        string
	NodeIdx     int
	Status      types.NodeStatus
	LeaseID     LeaseID
	LeaseToken  LeaseToken
	Attempt     int
	Output      map[string]any
	Port        string
	Error       string
}

// SubExecution tracks a child execution spawned by a loop/split node.
type SubExecution struct {
	ParentExecID types.ExecutionID
	ParentNode   string
	ChildExecID  types.ExecutionID
	BatchIndex   int
	Status       types.ExecutionStatus
	Result       map[string]any
}

// ExecutionEvent is emitted when an execution lifecycle state changes.
type ExecutionEvent struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status,omitempty"`
	Error       string                `json:"error,omitempty"`
	Data        map[string]any        `json:"data,omitempty"`
}
