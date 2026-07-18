package sqlstore

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// auditRepo implements store.AuditAppender against the SQL backend. Audit
// rows are append-only: AppendAudit inserts one row and never updates or
// deletes. The insert is the durability boundary — callers (the apiserver
// admission-audit check) fail-closed when this returns an error.
type auditRepo struct {
	db *gorm.DB
}

var _ store.AuditAppender = (*auditRepo)(nil)

func (r *auditRepo) AppendAudit(ctx context.Context, rec *store.AuditRecord) error {
	if rec == nil {
		return fmt.Errorf("append audit: nil record")
	}
	d := toDBAudit(rec)
	if err := wrapDBErr("append audit", r.db.WithContext(ctx).Create(d).Error); err != nil {
		return err
	}
	rec.ID = d.ID
	return nil
}

// AuditByID reads one audit row by primary key. Test/verify helper: it lets a
// reconciliation check confirm an append-only event persisted durably. There
// is deliberately no update or delete path.
func (r *auditRepo) AuditByID(ctx context.Context, id uint64) (*store.AuditRecord, error) {
	if id == 0 {
		return nil, fmt.Errorf("audit by id: %w", store.ErrNotFound)
	}
	var d dbAuditEvent
	err := r.db.WithContext(ctx).First(&d, id).Error
	if err := wrapDBErr("audit by id", err); err != nil {
		return nil, err
	}
	return fromDBAudit(&d), nil
}

func toDBAudit(r *store.AuditRecord) *dbAuditEvent {
	return &dbAuditEvent{
		RequestID:   r.RequestID,
		Principal:   r.Principal,
		TenantID:    r.TenantID,
		Operation:   r.Operation,
		Resource:    r.Resource,
		WorkflowID:  r.WorkflowID,
		ExecutionID: r.ExecutionID,
		Decision:    r.Decision,
		Reason:      r.Reason,
		Outcome:     r.Outcome,
		TraceID:     r.TraceID,
		Timestamp:   r.Timestamp,
	}
}

func fromDBAudit(d *dbAuditEvent) *store.AuditRecord {
	return &store.AuditRecord{
		ID:          d.ID,
		RequestID:   d.RequestID,
		Principal:   d.Principal,
		TenantID:    d.TenantID,
		Operation:   d.Operation,
		Resource:    d.Resource,
		WorkflowID:  d.WorkflowID,
		ExecutionID: d.ExecutionID,
		Decision:    d.Decision,
		Reason:      d.Reason,
		Outcome:     d.Outcome,
		TraceID:     d.TraceID,
		Timestamp:   d.Timestamp,
	}
}
