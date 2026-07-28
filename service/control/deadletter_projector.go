package control

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// deadLetterProjectorOperation is the audit operation recorded for a replay
// receipt projection. It is intentionally distinct from the B3 admission
// operation (deadletter.replay) so a receipt projection row is
// distinguishable from admission and reconcile-outcome rows in the same
// table. T9's general outcome-phase worker can reuse the same discriminator
// scheme.
const deadLetterProjectorOperation = "deadletter.replay.receipt"

// ReceiptProjector durably projects one authoritative Redis replay receipt
// into the append-only SQL audit table, idempotently. The Redis receipt
// (written atomically by engine.DeadLetterStore) remains authoritative; this
// projection is the durable secondary that the reconcile command diff-scans
// against. A failed projection does not touch Redis — the receipt survives
// and a later reconcile re-projects it.
//
// Idempotency: the projector uses the receipt's AuditID (the Redis
// receipt_audit_id, stable for the lifetime of one RequestID) as the
// idempotency key. AppendAuditIfAbsent skips the insert when a row with the
// same ReceiptAuditID already exists, so a retry after a lost SQL write, a
// process restart mid-reconcile, or a duplicate projection from the manager
// path never appends a second row.
//
// Security: the projected row carries only operational metadata — identity,
// operation, resource ids, outcome, node/activation/entry correlation, and
// the receipt audit_id. It never carries the operator's free-text Reason
// (bounded but untrusted), the task body, or any credential. The Redis
// receipt retains the reason; the SQL projection is metadata-only.
type ReceiptProjector struct {
	appender store.ReceiptAuditAppender
}

// NewReceiptProjector wraps a store.ReceiptAuditAppender as a receipt
// projector. The appender is typically a *sqlstore.Provider (which
// implements both AuditAppender and ReceiptAuditAppender).
func NewReceiptProjector(appender store.ReceiptAuditAppender) *ReceiptProjector {
	return &ReceiptProjector{appender: appender}
}

// Project idempotently appends one durable projection row for the receipt.
// Returns appended=true when a new row was inserted and appended=false when a
// row with the same ReceiptAuditID already existed (a duplicate projection,
// skipped). Returns an error when the appender is unavailable or the insert
// failed; the caller retries (bounded backlog) and the reconcile command
// re-projects later.
func (p *ReceiptProjector) Project(ctx context.Context, r engine.ReplayReceipt) (bool, error) {
	if p == nil || p.appender == nil {
		return false, engine.ErrDeadLetterUnsupported
	}
	return p.appender.AppendAuditIfAbsent(ctx, receiptToAuditRecord(r))
}

// receiptToAuditRecord maps a Redis replay receipt to an append-only audit
// row. The Reason field is intentionally NOT populated with the operator's
// free-text rationale: the audit reason column holds reason codes
// (deny/admitted/audit_unavailable), not untrusted text. The Redis receipt
// retains the reason; the SQL projection is metadata-only.
func receiptToAuditRecord(r engine.ReplayReceipt) *store.AuditRecord {
	ts := time.Now().UTC()
	if r.TimestampMs > 0 {
		ts = time.UnixMilli(r.TimestampMs).UTC()
	}
	return &store.AuditRecord{
		RequestID:      r.RequestID,
		Principal:      r.Operator,
		Namespace:      r.Namespace,
		Operation:      deadLetterProjectorOperation,
		Resource:       "dead-letters/" + r.ExecutionID,
		ExecutionID:    r.ExecutionID,
		Decision:       "allow",
		Reason:         "replay_receipt",
		Outcome:        string(r.Outcome),
		Phase:          store.AuditPhaseReceipt,
		Timestamp:      ts,
		NodeID:         r.NodeID,
		ActivationID:   r.ActivationID,
		EntryID:        r.EntryID,
		ReceiptAuditID: r.AuditID,
	}
}

// receiptFromReplay builds the receipt the manager path projects. The
// manager does not read the Redis receipt hash back; it reconstructs the
// correlation fields from the ReplayDeadLetterResult (carrying the
// authoritative AuditID returned by the store) and the original request.
// The AuditID is the same idempotency key the reconcile command reads from
// the Redis receipt hash, so a projection from either path collapses to one
// row.
func receiptFromReplay(res engine.ReplayDeadLetterResult, req engine.ReplayDeadLetterRequest, namespaceID string) engine.ReplayReceipt {
	return engine.ReplayReceipt{
		Namespace:    namespaceID,
		ExecutionID:  string(res.ExecutionID),
		RequestID:    req.RequestID,
		AuditID:      res.AuditID,
		NodeID:       res.NodeID,
		ActivationID: res.ActivationID,
		Outcome:      res.Outcome,
		Operator:     req.Operator,
		Reason:       req.Reason,
		EntryID:      req.EntryID,
		TimestampMs:  time.Now().UnixMilli(),
	}
}

// projectorAuditSink adapts a ReceiptProjector to the engine.DeadLetterAuditSink
// interface the DeadLetterManager calls. It records a bounded in-memory retry
// on transient SQL failures and emits an alarm metric (via the OutboxObserver)
// when the projection ultimately fails — the Redis receipt remains
// authoritative and the reconcile command later re-projects it.
//
// This is the "retry/backlog + alarm" path required by the design: the
// in-request bounded retry handles transient SQL errors, and the durable
// backlog is the reconcile command's diff-scan over Redis receipts. A
// background goroutine queue is intentionally NOT used here (that is T9's
// leader-gated crash-safe audit worker); T4 ships the idempotent projector +
// failure retry + diff-scan command, leaving the general leader-gated
// admission/outcome/reconciled-phase framework to T9.
type projectorAuditSink struct {
	projector *ReceiptProjector
	observer  engine.OutboxObserver // may be nil
	maxRetry  int
	sleep     func(time.Duration)
}

// NewProjectorAuditSink returns a DeadLetterAuditSink that durably projects
// replay receipts via projector, with a bounded retry on transient failures.
// observer (when non-nil) records projection failures via OnOutboxError so a
// persistent sink outage is observable as an alarm.
func NewProjectorAuditSink(projector *ReceiptProjector, observer engine.OutboxObserver) engine.DeadLetterAuditSink {
	return &projectorAuditSink{
		projector: projector,
		observer:  observer,
		maxRetry:  3,
		sleep:     time.Sleep,
	}
}

// RecordReplay projects the receipt for one replay. It never returns an error
// that blocks the manager: the manager ignores the returned error anyway
// (Redis is authoritative), and a failed projection is recorded as an alarm
// metric for the reconcile command to catch up.
func (s *projectorAuditSink) RecordReplay(ctx context.Context, res engine.ReplayDeadLetterResult, req engine.ReplayDeadLetterRequest) error {
	namespaceID := namespaceIDFromContext(ctx)
	r := receiptFromReplay(res, req, namespaceID)
	var lastErr error
	backoff := 10 * time.Millisecond
	for attempt := 0; attempt <= s.maxRetry; attempt++ {
		_, err := s.projector.Project(ctx, r)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < s.maxRetry {
			s.sleep(backoff)
			backoff *= 2
		}
	}
	if s.observer != nil {
		s.observer.OnOutboxError(ctx, "replay_project", lastErr)
	}
	return lastErr
}

// namespaceIDFromContext returns the server-injected namespace from the request
// context. The authz wrapper injects the verified principal's namespace via
// namespace.WithNamespace; the manager path always carries one. A background
// context (reconcile command) yields the empty string and the projector
// relies on the receipt's namespace (read from the Redis receipt key).
func namespaceIDFromContext(ctx context.Context) string {
	return string(namespace.FromContext(ctx))
}
