package apiserver

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
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

// ReceiptAppender returns the underlying appender as a ReceiptAuditAppender
// when the backing store supports idempotent receipt projection (the SQL
// provider does; an in-memory sink does not). The dead-letter receipt
// projector uses it for idempotent append-by-audit_id; when nil, callers
// fall back to the non-idempotent Append path (dev only).
func (s *SQLAuditSink) ReceiptAppender() store.ReceiptAuditAppender {
	if ra, ok := s.appender.(store.ReceiptAuditAppender); ok {
		return ra
	}
	return nil
}

// Append persists one audit event durably. On error the caller must fail
// closed for mutations — never execute an unaudited mutation.
//
// Namespace boundary (Task 7.4): the authoritative namespace is read from ctx
// (namespace.FromContext). The event's Namespace is used only as a fallback when
// ctx carries no explicit namespace, preserving compatibility with callers that
// already set it from the authenticated principal.
func (s *SQLAuditSink) Append(ctx context.Context, ev AuditEvent) error {
	if s.appender == nil {
		return ErrAuditUnavailable
	}
	// Prefer the namespace in context (the consistent cross-cutting source); fall
	// back to the principal-bound namespace carried by the event. DefaultNamespace is
	// treated as "no explicit namespace in context" so single-namespace callers that
	// set ev.Namespace still persist it.
	namespaceID := string(namespace.FromContext(ctx))
	if namespaceID == "" || namespaceID == string(namespace.Default) {
		namespaceID = ev.Namespace
	}
	rec := &store.AuditRecord{
		RequestID:      ev.RequestID,
		Principal:      ev.Principal,
		Namespace:      namespaceID,
		Operation:      ev.Operation,
		Resource:       ev.Resource,
		WorkflowID:     ev.WorkflowID,
		ExecutionID:    ev.ExecutionID,
		Decision:       string(ev.Decision),
		Reason:         ev.Reason,
		Outcome:        ev.Outcome,
		Phase:          ev.Phase,
		TraceID:        ev.TraceID,
		Timestamp:      ev.Timestamp,
		NodeID:         ev.NodeID,
		ActivationID:   ev.ActivationID,
		EntryID:        ev.EntryID,
		ReceiptAuditID: ev.ReceiptAuditID,
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}
	return s.appender.AppendAudit(ctx, rec)
}
