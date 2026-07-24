package store

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// Executions persists workflow execution lifecycle state.
type Executions interface {
	CreateExecution(ctx context.Context, rec *ExecutionRecord) error
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionRecord, error)
}

// Nodes persists per-node execution state.
type Nodes interface {
	UpsertNode(ctx context.Context, rec *NodeRecord) error
	GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeRecord, error)
	ListNodes(ctx context.Context, id types.ExecutionID, opts ListOptions) ([]*NodeRecord, error)
	ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*NodeRecord, error)
	ListExpiredSuspensions(ctx context.Context, now time.Time, opts ListOptions) ([]*NodeRecord, error)
}

// Signals persists signal payloads delivered to executions.
type Signals interface {
	SaveSignal(ctx context.Context, rec *SignalRecord) error
	// ConsumeSignal atomically marks an active signal as consumed. Returns ErrNotFound if none is active.
	ConsumeSignal(ctx context.Context, id types.ExecutionID, name string) (*SignalRecord, error)
	// RevokeSignal atomically marks an active signal as revoked.
	// Returns (true, nil) on success, (false, nil) if the signal is missing or already consumed/revoked.
	RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error)
	CountSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) (int, error)
	ListSignalsByNames(ctx context.Context, id types.ExecutionID, names []string, opts ListOptions) ([]*SignalRecord, error)
}

// AuditAppender appends one durable audit record. Audit records are
// append-only: there is no update or delete path. A failing Append must
// surface an error so callers can fail-closed (mutations are admitted only
// once the admission audit is durably persisted).
//
// Audit records must never carry secrets (tokens, payloads, credentials);
// see docs/design/RELEASE-GATES.md §4. The fields here are identity,
// operation, resource ids, decision, reason, and trace correlation only.
type AuditAppender interface {
	AppendAudit(ctx context.Context, rec *AuditRecord) error
}

// ReceiptAuditAppender is an optional AuditAppender capability that appends
// a receipt projection idempotently by the receipt's AuditID
// (rec.ReceiptAuditID). The dead-letter receipt projector (T4) and T9's
// outcome-phase worker use it so a retry after a lost SQL write does not
// duplicate the durable projection. The Redis receipt remains authoritative;
// this is the durable secondary projection reconciled against it.
//
// AppendAuditIfAbsent returns appended=true when a new row was inserted and
// appended=false when a row with the same ReceiptAuditID already existed (a
// duplicate projection, skipped). A record with an empty ReceiptAuditID is
// always appended — it is not a receipt projection and has no idempotency
// key.
type ReceiptAuditAppender interface {
	AppendAuditIfAbsent(ctx context.Context, rec *AuditRecord) (bool, error)
	// AuditByReceiptAuditID reads one receipt projection row by its
	// ReceiptAuditID. Returns ErrNotFound when no row exists.
	AuditByReceiptAuditID(ctx context.Context, receiptAuditID string) (*AuditRecord, error)
}

// AuditReconciler is the optional AuditAppender capability the T9 crash-safe
// reconcile worker depends on. It scans the durable audit log for admitted
// mutations that never received a post-handler outcome (e.g. a crash between
// the mutation and the outcome append) and appends the missing outcome
// idempotently. The worker NEVER re-executes the mutation: it only observes
// authoritative state (engine StateStore) and appends an audit outcome.
//
// ListUnreconciledAdmissions returns admitted (phase="admission",
// outcome="admitted") rows older than `before` for which no outcome-phase
// row exists for the same (tenant_id, request_id). AppendOutcomeIfAbsent
// appends an outcome-phase row keyed idempotently on (tenant_id, request_id,
// phase="outcome"): a duplicate append (concurrent worker / leader switch)
// returns appended=false.
type AuditReconciler interface {
	ListUnreconciledAdmissions(ctx context.Context, before time.Time, afterSeqID uint64, limit int) ([]*AuditRecord, error)
	AppendOutcomeIfAbsent(ctx context.Context, rec *AuditRecord) (bool, error)
	// CountUnreconciledAdmissions returns the total count of pending
	// admissions older than `before` and the timestamp of the oldest one.
	// Used by the worker for full-table backlog metrics (independent of the
	// cursor position). When no pending rows exist, pending=0 and oldest is
	// the zero time.
	CountUnreconciledAdmissions(ctx context.Context, before time.Time) (pending int, oldest time.Time, err error)
}

// Audit phase constants (T9). Each audit row belongs to exactly one
// immutable phase; the (TenantID, RequestID, Phase) triple is the reconcile
// worker's idempotency key.
const (
	// AuditPhaseAdmission is the pre-handler fail-closed admission audit row
	// (outcome="admitted" for an allowed mutation, "denied" for a deny).
	AuditPhaseAdmission = "admission"
	// AuditPhaseOutcome is the post-handler outcome row (outcome="reconciled"
	// on a 2xx, "failed" otherwise) — written inline by the authz wrapper
	// or, after a crash, by the T9 reconcile worker.
	AuditPhaseOutcome = "outcome"
	// AuditPhaseReceipt is the T4 dead-letter replay receipt projection row.
	AuditPhaseReceipt = "receipt"
)

// Audit outcome constants (T9). The outcome column records the result of
// the phase's decision; phase + outcome together fully describe a row.
const (
	// AuditOutcomeAdmitted marks an admission row that allowed a mutation.
	AuditOutcomeAdmitted = "admitted"
	// AuditOutcomeDenied marks an admission row that denied a request.
	AuditOutcomeDenied = "denied"
	// AuditOutcomeReconciled marks an outcome row whose mutation landed.
	AuditOutcomeReconciled = "reconciled"
	// AuditOutcomeFailed marks an outcome row whose mutation did not land
	// (handler returned non-2xx, or the reconcile worker determined the
	// mutation had no effect after a crash).
	AuditOutcomeFailed = "failed"
)

// Store is the full persistence surface, composing the per-domain interfaces.
// Consumers that only need one domain should depend on the narrower interface
// (Executions, Nodes, Signals, or Audit) instead.
type Store interface {
	Executions
	Nodes
	Signals
	AuditAppender
}

// Set bundles the per-domain stores bound to a single backend or transaction.
// All stores in a Set returned by Transactor.Transaction share the same tx.
type Set struct {
	Execution Executions
	Node      Nodes
	Signal    Signals
	Audit     AuditAppender
}

// Transactor runs fn within a single transaction. Every store in the supplied
// Set is bound to that transaction, so cross-domain writes commit or roll back
// together. Returning a non-nil error rolls the transaction back.
type Transactor interface {
	Transaction(ctx context.Context, fn func(s Set) error) error
}
