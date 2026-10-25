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
	// ExecutionStatusQueued is a queue-mode submission waiting for a
	// concurrency slot. It is persisted in full — snapshot, outbox, node
	// rows — but nothing of it is deliverable until a slot is released and the
	// execution is promoted.
	//
	// It is deliberately not terminal. Wait and Cancel treat a live execution
	// as one whose status is non-terminal, which is what lets a caller wait on
	// a queued execution or give up on it, and what makes a queued execution
	// still hold its place in line.
	ExecutionStatusQueued ExecutionStatus = "queued"
	// ExecutionStatusPaused is an operator-suspended execution. Work already
	// leased to a runner runs to completion and commits its result; nothing in
	// the ready set is delivered until the execution is resumed, and suspended
	// nodes stay suspended — an arriving signal is still stored but does not
	// advance anything.
	//
	// It is deliberately not terminal, and it deliberately keeps its workflow
	// concurrency slot. The execution holds intermediate state that only a
	// resume can put back to work, so releasing the slot would admit a
	// replacement that competes with the work Resume restarts. A queued
	// execution holds no such state and must not be paused at all — it is
	// refused, because a submission that never started has nothing to suspend.
	ExecutionStatusPaused ExecutionStatus = "paused"
)

// Execution trigger kinds record HOW an execution was started. They are stored
// on the execution row and are always server-derived: a caller never supplies
// one, so a value on the wire describes the path that actually created the
// execution.
//
// "manual" is deliberately absent. A hand-submitted run and an API-submitted
// run are indistinguishable server-side (both arrive through the same execute
// face), so reporting them as different kinds would be a guess.
const (
	// TriggerKindAPI is a submission through the execute face (and the
	// embedded engine's Submit/Invoke, which is its in-process equivalent).
	TriggerKindAPI = "api"
	// TriggerKindRerun is a new execution derived from a terminal one.
	TriggerKindRerun = "rerun"
	// TriggerKindSubWorkflow is a child execution started by a subworkflow
	// node of another execution.
	TriggerKindSubWorkflow = "subworkflow"
	// TriggerKindTimer and the four after it are entry-unit triggers, named by
	// the node type that delivered the entry activation.
	TriggerKindTimer   = "timer"
	TriggerKindCron    = "cron"
	TriggerKindWebhook = "webhook"
	TriggerKindKafka   = "kafka"
	TriggerKindRedis   = "redis"
)

// TriggerKindFromNodeType maps an entry unit's builtin trigger node type to the
// trigger kind recorded on the execution it starts. It returns "" for any type
// that is not a builtin trigger — a value the caller must treat as UNKNOWN
// rather than as a kind, because guessing would label the execution with a
// source it did not come from.
func TriggerKindFromNodeType(nodeType string) string {
	switch nodeType {
	case "xflow.trigger.timer":
		return TriggerKindTimer
	case "xflow.trigger.cron":
		return TriggerKindCron
	case "xflow.trigger.webhook":
		return TriggerKindWebhook
	case "xflow.trigger.kafka":
		return TriggerKindKafka
	case "xflow.trigger.redis":
		return TriggerKindRedis
	}
	return ""
}

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
