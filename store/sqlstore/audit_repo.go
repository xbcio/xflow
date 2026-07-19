package sqlstore

import (
	"context"
	"errors"
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

// AuditByReceiptAuditID reports whether a receipt projection row already
// exists for the given Redis receipt audit_id. The receipt projector uses it
// as a check-then-append idempotency guard: the Redis receipt is
// authoritative, so a duplicate SQL projection (e.g. after a retry or a
// process restart mid-reconcile) is skipped rather than appended again.
//
// An empty receiptAuditID never matches: admission/outcome audit rows leave
// ReceiptAuditID empty, so they are never falsely reported as duplicates.
func (r *auditRepo) AuditByReceiptAuditID(ctx context.Context, receiptAuditID string) (*store.AuditRecord, error) {
	if receiptAuditID == "" {
		return nil, fmt.Errorf("audit by receipt id: %w", store.ErrNotFound)
	}
	var d dbAuditEvent
	err := r.db.WithContext(ctx).Where("receipt_audit_id = ?", receiptAuditID).First(&d).Error
	if err := wrapDBErr("audit by receipt id", err); err != nil {
		return nil, err
	}
	return fromDBAudit(&d), nil
}

// AppendAuditIfAbsent appends rec only when no row already carries
// rec.ReceiptAuditID. It is the idempotent projection path used by the
// dead-letter receipt projector (T4) and, later, T9's outcome-phase worker.
// Returns appended=true when a new row was inserted and appended=false when a
// row with the same ReceiptAuditID already existed (a duplicate projection,
// skipped). A record with an empty ReceiptAuditID is always appended — it is
// not a receipt projection and has no idempotency key.
//
// The check-then-append is not atomic across concurrent writers; the
// projector runs under a single leader-gated instance (T9) or as a one-shot
// reconcile command (T4), so concurrency is not a practical concern. A unique
// index on receipt_audit_id is intentionally NOT added because
// admission/outcome rows legitimately share an empty value; the
// check-then-append guard plus single-writer is the idempotency contract.
func (r *auditRepo) AppendAuditIfAbsent(ctx context.Context, rec *store.AuditRecord) (appended bool, err error) {
	if rec == nil {
		return false, fmt.Errorf("append audit if absent: nil record")
	}
	if rec.ReceiptAuditID != "" {
		existing, err := r.AuditByReceiptAuditID(ctx, rec.ReceiptAuditID)
		if err == nil && existing != nil {
			return false, nil
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return false, err
		}
	}
	d := toDBAudit(rec)
	if err := wrapDBErr("append audit", r.db.WithContext(ctx).Create(d).Error); err != nil {
		return false, err
	}
	rec.ID = d.ID
	return true, nil
}

func toDBAudit(r *store.AuditRecord) *dbAuditEvent {
	return &dbAuditEvent{
		RequestID:      r.RequestID,
		Principal:      r.Principal,
		TenantID:       r.TenantID,
		Operation:      r.Operation,
		Resource:       r.Resource,
		WorkflowID:     r.WorkflowID,
		ExecutionID:    r.ExecutionID,
		Decision:       r.Decision,
		Reason:         r.Reason,
		Outcome:        r.Outcome,
		TraceID:        r.TraceID,
		Timestamp:      r.Timestamp,
		NodeID:         r.NodeID,
		ActivationID:   r.ActivationID,
		EntryID:        r.EntryID,
		ReceiptAuditID: r.ReceiptAuditID,
	}
}

func fromDBAudit(d *dbAuditEvent) *store.AuditRecord {
	return &store.AuditRecord{
		ID:             d.ID,
		RequestID:      d.RequestID,
		Principal:      d.Principal,
		TenantID:       d.TenantID,
		Operation:      d.Operation,
		Resource:       d.Resource,
		WorkflowID:     d.WorkflowID,
		ExecutionID:    d.ExecutionID,
		Decision:       d.Decision,
		Reason:         d.Reason,
		Outcome:        d.Outcome,
		TraceID:        d.TraceID,
		Timestamp:      d.Timestamp,
		NodeID:         d.NodeID,
		ActivationID:   d.ActivationID,
		EntryID:        d.EntryID,
		ReceiptAuditID: d.ReceiptAuditID,
	}
}
