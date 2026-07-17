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
	WorkflowDef  []byte
	Params       []byte
	Runtime      []byte
	TraceID      string
	SpanID       string
	Status       types.ExecutionStatus
	Error        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (ExecutionRecord) TableName() string { return "xflow_executions" }

// NodeRecord holds the persistent state of a single node within an execution.
type NodeRecord struct {
	ID           uint64
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeType     string
	Status       types.NodeStatus
	LeaseID      string
	LeaseToken   string
	Attempt      int
	Output       []byte
	Port         string
	SignalName   string
	SignalConfig []byte
	Timeout      *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (NodeRecord) TableName() string { return "xflow_nodes" }

// SignalRecord holds a signal payload delivered to a workflow execution.
type SignalRecord struct {
	ID          uint64
	ExecutionID types.ExecutionID
	SignalName  string
	Payload     []byte
	Status      types.SignalStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (SignalRecord) TableName() string { return "xflow_signals" }
