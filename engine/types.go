package engine

import (
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
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
	ExecutionID types.ExecutionID
	NodeName    string
	NodeIdx     int
	Type        TaskType
	Payload     *node.SignalPayload // non-nil only for TaskTypeNodeResume
}

// ExecutionSnapshot is the engine's view of a running execution stored in the backend.
type ExecutionSnapshot struct {
	ID       types.ExecutionID
	Graph    *graph.Graph
	Status   types.Status
	Params   map[string]any
	ParentID types.ExecutionID // non-empty for sub-executions
}

// NodeSnapshot is the engine's view of a single node's state stored in the backend.
type NodeSnapshot struct {
	ExecutionID types.ExecutionID
	Name        string
	NodeIdx     int
	Status      string
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
	Status       types.Status
	Result       map[string]any
}
