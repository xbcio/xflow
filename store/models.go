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

// AuditRecord holds one append-only authorization / mutation audit event.
// It is the durable projection of the in-process authz decision: identity
// (subject/tenant), operation, resource ids, decision, reason, outcome, and
// trace correlation. It must NEVER carry secrets (tokens, payloads,
// credentials). The authoritative operation receipts (Redis) are reconciled
// against the audit log; the audit log is not itself the source of truth
// for execution state.
//
// Domain record; ORM schema lives in store/sqlstore.dbAuditEvent.
type AuditRecord struct {
	ID          uint64
	RequestID   string
	Principal   string
	TenantID    string
	Operation   string
	Resource    string
	WorkflowID  string
	ExecutionID string
	Decision    string
	Reason      string
	Outcome     string // admitted / denied / reconciled
	TraceID     string
	Timestamp   time.Time
}
