package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"time"

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
var _ store.AuditReconciler = (*auditRepo)(nil)

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
		Phase:          r.Phase,
		NodeID:         r.NodeID,
		ActivationID:   r.ActivationID,
		EntryID:        r.EntryID,
		ReceiptAuditID: r.ReceiptAuditID,
	}
}

func fromDBAudit(d *dbAuditEvent) *store.AuditRecord {
	return &store.AuditRecord{
		ID:             d.ID,
		SeqID:          d.ID, // SeqID maps to the AUTO_INCREMENT primary key
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
		Phase:          d.Phase,
		NodeID:         d.NodeID,
		ActivationID:   d.ActivationID,
		EntryID:        d.EntryID,
		ReceiptAuditID: d.ReceiptAuditID,
	}
}

// ListUnreconciledAdmissions returns admitted mutation audit rows (phase=
// "admission", outcome="admitted") with created_at older than `before` for
// which no outcome-phase row (phase="outcome") exists for the same
// (tenant_id, request_id). These are the admissions the T9 reconcile worker
// must settle: the mutation was admitted (fail-closed admission audit
// persisted) but no post-handler outcome was ever appended (e.g. a crash
// between the mutation and the outcome audit, or a handler panic).
//
// When afterSeqID > 0 only rows with id > afterSeqID are returned (cursor
// pagination to skip past permanently-Indeterminate rows crowding the head).
//
// The NOT EXISTS subquery is covered by idx_tenant_request_phase, so the
// pending scan is index-only and bounded by `limit`. Rows are returned
// oldest-first (by id) so the cursor advances monotonically.
func (r *auditRepo) ListUnreconciledAdmissions(ctx context.Context, before time.Time, afterSeqID uint64, limit int) ([]*store.AuditRecord, error) {
	if limit <= 0 {
		limit = 256
	}
	var rows []dbAuditEvent
	err := r.db.WithContext(ctx).Raw(`
SELECT a.* FROM xflow_audit_events a
WHERE a.phase = ? AND a.outcome = ? AND a.created_at < ? AND a.id > ?
  AND NOT EXISTS (
      SELECT 1 FROM xflow_audit_events b
      WHERE b.tenant_id = a.tenant_id
        AND b.request_id = a.request_id
        AND b.phase = ?
  )
ORDER BY a.id ASC
LIMIT ?`, store.AuditPhaseAdmission, store.AuditOutcomeAdmitted, before, afterSeqID, store.AuditPhaseOutcome, limit).Scan(&rows).Error
	if err := wrapDBErr("list unreconciled admissions", err); err != nil {
		return nil, err
	}
	out := make([]*store.AuditRecord, len(rows))
	for i, d := range rows {
		out[i] = fromDBAudit(&d)
	}
	return out, nil
}

// AppendOutcomeIfAbsent idempotently appends an outcome-phase audit row. It
// is the T9 reconcile worker's settle path: before appending, it checks that
// no outcome row already exists for the same (tenant_id, request_id,
// phase="outcome"). A duplicate (e.g. a concurrent worker or a leader
// switch racing two sweeps) is caught by the check-then-append here AND by
// the unique uk_phase_key index on the generated phase_key column, so the
// duplicate insert surfaces as gorm.ErrDuplicatedKey and is reported as
// appended=false rather than an error.
//
// The row's Phase is forced to "outcome" so the idempotency key is stable
// regardless of caller. A record with an empty RequestID has no idempotency
// key and is always appended (it cannot be a reconcile outcome).
func (r *auditRepo) AppendOutcomeIfAbsent(ctx context.Context, rec *store.AuditRecord) (bool, error) {
	if rec == nil {
		return false, fmt.Errorf("append outcome if absent: nil record")
	}
	rec.Phase = store.AuditPhaseOutcome
	if rec.RequestID != "" && rec.TenantID != "" {
		var existing dbAuditEvent
		err := r.db.WithContext(ctx).
			Where("tenant_id = ? AND request_id = ? AND phase = ?", rec.TenantID, rec.RequestID, store.AuditPhaseOutcome).
			First(&existing).Error
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, wrapDBErr("append outcome if absent: lookup", err)
		}
	}
	d := toDBAudit(rec)
	if err := r.db.WithContext(ctx).Create(d).Error; err != nil {
		// A duplicate-key violation (concurrent insert between our check and
		// create) means another worker appended the outcome first; treat it
		// as a benign idempotent skip, not an error.
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return false, nil
		}
		return false, wrapDBErr("append outcome if absent", err)
	}
	rec.ID = d.ID
	return true, nil
}

// CountUnreconciledAdmissions returns the total count of pending admissions
// older than `before` and the timestamp of the oldest one (full-table backlog
// metrics). When no pending rows exist, pending=0 and oldest is the zero time.
//
// Cost model: the query is a full-table COUNT with a correlated NOT EXISTS
// subquery, executed once per reconcile sweep. This is bounded and acceptable
// at G1 (restricted single-tenant deployment with a bounded audit table).
//
// TODO(G2): at multi-tenant scale this should become an incrementally-maintained
// counter (e.g. a summary row updated on each admission/outcome append) or a
// periodic count (not every-sweep) to avoid per-sweep full-table cost on an
// unbounded Indeterminate backlog.
func (r *auditRepo) CountUnreconciledAdmissions(ctx context.Context, before time.Time) (int, time.Time, error) {
	type result struct {
		Cnt    int       `gorm:"column:cnt"`
		Oldest time.Time `gorm:"column:oldest"`
	}
	var res result
	err := r.db.WithContext(ctx).Raw(`
SELECT COUNT(*) AS cnt, MIN(a.created_at) AS oldest FROM xflow_audit_events a
WHERE a.phase = ? AND a.outcome = ? AND a.created_at < ?
  AND NOT EXISTS (
      SELECT 1 FROM xflow_audit_events b
      WHERE b.tenant_id = a.tenant_id
        AND b.request_id = a.request_id
        AND b.phase = ?
  )`, store.AuditPhaseAdmission, store.AuditOutcomeAdmitted, before, store.AuditPhaseOutcome).Scan(&res).Error
	if err := wrapDBErr("count unreconciled admissions", err); err != nil {
		return 0, time.Time{}, err
	}
	return res.Cnt, res.Oldest, nil
}
