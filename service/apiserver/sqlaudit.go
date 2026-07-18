package apiserver

import (
	"context"
	"time"

	"github.com/xbcio/xflow/store"
)

// SQLAuditSink is the durable AuditSink backed by store.AuditAppender (the SQL
// store in production). It is the authoritative reconcile target: admission
// audit events are persisted before a mutation executes, so a failing Append
// fails the mutation closed (the apiserver admission-audit check returns 503
// on error). Audit rows are append-only — never updated or deleted.
//
// Security: the AuditEvent → AuditRecord mapping carries identity, operation,
// resource ids, decision, reason, outcome, and trace correlation only. It
// must never include tokens, payloads, or credentials.
type SQLAuditSink struct {
	appender store.AuditAppender
}

// Compile-time interface check.
var _ AuditSink = (*SQLAuditSink)(nil)

// NewSQLAuditSink wraps a store.AuditAppender as a durable AuditSink. The
// appender is typically a *sqlstore.Provider (root) or a store.Set bound to a
// transaction (for atomic admission audit + execution write).
func NewSQLAuditSink(appender store.AuditAppender) *SQLAuditSink {
	return &SQLAuditSink{appender: appender}
}

// Append persists one audit event durably. On error the caller must fail
// closed for mutations — never execute an unaudited mutation.
func (s *SQLAuditSink) Append(ctx context.Context, ev AuditEvent) error {
	if s.appender == nil {
		return ErrAuditUnavailable
	}
	rec := &store.AuditRecord{
		RequestID:   ev.RequestID,
		Principal:   ev.Principal,
		TenantID:    ev.TenantID,
		Operation:   ev.Operation,
		Resource:    ev.Resource,
		WorkflowID:  ev.WorkflowID,
		ExecutionID: ev.ExecutionID,
		Decision:    string(ev.Decision),
		Reason:      ev.Reason,
		Outcome:     ev.Outcome,
		TraceID:     ev.TraceID,
		Timestamp:   ev.Timestamp,
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}
	return s.appender.AppendAudit(ctx, rec)
}
