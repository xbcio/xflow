package store

import (
	"time"

	"github.com/xbcio/xflow/types"
)

// ExecutionRecord holds the persistent state of a workflow execution. It is a
// domain record: ORM schema concerns (table name, GORM tags) live in
// store/sqlstore's internal dbExecution type, not here.
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

// NodeRecord holds the persistent state of a single node within an execution.
// Domain record; ORM schema lives in store/sqlstore.dbNode.
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

// SignalRecord holds a signal payload delivered to a workflow execution.
// Domain record; ORM schema lives in store/sqlstore.dbSignal.
type SignalRecord struct {
	ID          uint64
	ExecutionID types.ExecutionID
	SignalName  string
	Payload     []byte
	Status      types.SignalStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
