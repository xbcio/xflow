package store

import (
	"time"

	"github.com/xbcio/xflow/types"
)

// ExecutionRecord holds the persistent state of a workflow execution.
type ExecutionRecord struct {
	ID           uint64
	ExecutionID  types.ExecutionID
	WorkflowName string
	WorkflowDef  []byte // JSON-encoded WorkflowDef
	Params       []byte // JSON-encoded input parameters
	Status       types.Status
	Error        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// NodeRecord holds the persistent state of a single node within an execution.
type NodeRecord struct {
	ID           uint64
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeType     string
	Status       string
	Output       []byte     // JSON-encoded output data
	Port         string     // active output port ("main", "error", "timeout")
	SignalName   string     // for suspended wait nodes (single-signal mode)
	SignalConfig []byte     // JSON: multi-signal config {signals:[], quorum:N}
	Timeout      *time.Time // suspension deadline
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SignalRecord holds a signal payload delivered to a workflow execution.
type SignalRecord struct {
	ID          uint64
	ExecutionID types.ExecutionID
	SignalName  string
	Payload     []byte // JSON-encoded signal data
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
