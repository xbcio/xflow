package sqlstore

import (
	"time"
)

// dbAuditEvent is the internal GORM persistence type for an authorization /
// mutation audit event. It is the durable projection of
// service/apiserver.AuditEvent: append-only, one row per admission or
// outcome. The authoritative operation receipts (Redis) are reconciled
// against this table; this table is not itself the source of truth for
// execution state.
//
// Security: per the organization secure-coding policy this table must NEVER
// store token, payload, or other sensitive credentials. AuditEvent only
// carries identity (subject/tenant), operation, resource ids, decision,
// reason, and trace correlation ids — none of which are secrets.
type dbAuditEvent struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	RequestID    string    `gorm:"column:request_id;type:varchar(128)"`
	Principal    string    `gorm:"column:principal;type:varchar(255)"`
	TenantID     string    `gorm:"column:tenant_id;type:varchar(128)"`
	Operation    string    `gorm:"column:operation;type:varchar(64)"`
	Resource     string    `gorm:"column:resource;type:varchar(255)"`
	WorkflowID   string    `gorm:"column:workflow_id;type:varchar(255)"`
	ExecutionID  string    `gorm:"column:execution_id;type:varchar(64)"`
	Decision     string    `gorm:"column:decision;type:varchar(16)"`
	Reason       string    `gorm:"column:reason;type:varchar(128)"`
	Outcome      string    `gorm:"column:outcome;type:varchar(32)"`
	TraceID      string    `gorm:"column:trace_id;type:varchar(64)"`
	Timestamp    time.Time `gorm:"column:ts"`
	CreatedAt    time.Time `gorm:"column:created_at;autoCreateTime:milli"`
}

func (dbAuditEvent) TableName() string { return "xflow_audit_events" }
