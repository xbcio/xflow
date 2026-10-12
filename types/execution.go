package types

// ExecutionID uniquely identifies a single workflow execution instance.
type ExecutionID string

// ExecutionStatus represents the lifecycle state of a workflow execution.
type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusSuccess   ExecutionStatus = "success"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCanceling ExecutionStatus = "canceling"
	ExecutionStatusCanceled  ExecutionStatus = "canceled"
	ExecutionStatusTimeout   ExecutionStatus = "timeout"
)

// NodeStatus represents the lifecycle state of a workflow node.
type NodeStatus string

const (
	NodeStatusPending    NodeStatus = "pending"
	NodeStatusRunning    NodeStatus = "running"
	NodeStatusCommitting NodeStatus = "committing"
	NodeStatusSuccess    NodeStatus = "success"
	NodeStatusFailed     NodeStatus = "failed"
	NodeStatusSkipped    NodeStatus = "skipped"
	NodeStatusSuspended  NodeStatus = "suspended"
	NodeStatusContinued  NodeStatus = "continued"
	NodeStatusCanceled   NodeStatus = "canceled"
	NodeStatusWaiting    NodeStatus = "waiting"
	// NodeStatusPinned is a node the engine resolved from the workflow's
	// pin_data instead of running its handler: its output is the pinned mock
	// value and downstream treats it exactly like success.
	NodeStatusPinned NodeStatus = "pinned"
	// NodeStatusExpired is a read-side-only verdict for a member of a transient
	// group (groups[].mode: transient) whose records are gone rather than never
	// written: the execution is terminal and the group's artifacts — boundary
	// exits, unit status, lease fence — have passed the completion TTL that the
	// mode applies. The member ran, or may have; nothing about it is readable
	// any more, and reporting "pending" would assert the one thing that is
	// false. Nothing stores this status: it is derived at inspect time from the
	// group unit's own record (engine.GroupUnitPhaseReader).
	NodeStatusExpired NodeStatus = "expired"
)

// IsTerminalExecutionStatus reports whether s is an execution terminal state.
func IsTerminalExecutionStatus(s ExecutionStatus) bool {
	switch s {
	case ExecutionStatusSuccess, ExecutionStatusFailed, ExecutionStatusCanceled, ExecutionStatusTimeout:
		return true
	}
	return false
}

// IsTerminalNodeStatus reports whether s is a node terminal state.
func IsTerminalNodeStatus(s NodeStatus) bool {
	switch s {
	case NodeStatusSuccess, NodeStatusFailed, NodeStatusSkipped, NodeStatusCanceled, NodeStatusContinued, NodeStatusPinned:
		return true
	}
	return false
}

// Result holds the outcome of a completed (or failed) workflow execution.
type Result struct {
	ExecutionID ExecutionID     `json:"execution_id,omitempty"`
	Status      ExecutionStatus `json:"status,omitempty"`
	Output      map[string]any  `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
}
