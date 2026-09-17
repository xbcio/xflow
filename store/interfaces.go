package store

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Executions persists workflow execution lifecycle state.
//
// Adding ListExecutions and CountExecutions here makes them required of every
// store.Store implementation, including test doubles. The repository's
// failingStore and latencyStore deliberately provide stubs because neither test
// double lists executions. The alternative shape — keep Executions as-is and
// put the listing methods on a separate optional interface the way
// AuditReconciler is separate from AuditAppender — would have been non-breaking,
// but it would also let a backend claim store.Store while silently lacking
// tenant-scoped enumeration, and the listing methods are what GET
// /v1/executions needs from every backend.
type Executions interface {
	CreateExecution(ctx context.Context, rec *ExecutionRecord) error
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionRecord, error)

	// ListExecutions returns one page of the executions belonging to ns, newest
	// first (see ExecutionOrder), narrowed by filter.
	//
	// ns is a REQUIRED SCOPE, not an optional filter: it is matched by exact
	// equality, and an empty or malformed one is refused with
	// ErrInvalidNamespace rather than widened to every namespace. That refusal
	// is the whole security property of this method — an unscoped version is a
	// cross-tenant enumeration endpoint.
	//
	// A namespace that is valid but holds nothing returns an empty slice and a
	// nil error; it is not an error to ask about a namespace you do not own the
	// naming of, and the answer to it leaks nothing.
	//
	// Rows whose namespace is empty (unattributed: written before the namespace
	// column existed, or written by a caller that did not set it) match NO
	// namespace query. They cannot be reached by any argument to this method,
	// including one that happens to look like the empty scope, because that
	// argument is refused. Recovering them is an offline operator decision; the
	// store will not guess a namespace for a row, because a guess that puts one
	// tenant's execution in another tenant's list is not recoverable by a later
	// fix.
	//
	// Filtering is limited to what the row actually stores. Supported:
	// ExecutionFilter.Status (the status column) and ExecutionFilter.CreatedAfter/
	// CreatedBefore (created_at). NOT supported, with the column-level reason:
	//
	//   - by workflow: xflow_executions stores workflow_name only. There is no
	//     workflow_id / workflow_key / definition-hash column, and that is not an
	//     omission — types.WorkflowID belongs to the workflow registry, and a
	//     name is not an identity: two namespaces can hold the same name, and the
	//     name is not immutable across a re-registration under the same id. A
	//     name filter would therefore be a filter on a display string that does
	//     not resolve to a workflow.
	//   - by runner: runner_id is not a column of xflow_executions at all. It
	//     lives in the enrollment/identity tables (xflow_enroll_audit,
	//     xflow_issued_identities) and in the node lease columns, neither of
	//     which is a per-execution projection of "who ran this". Adding a runner
	//     filter requires a reliable projection first.
	//
	// Pagination is offset-based, per docs/design/API-SPECIFICATION.md §3.3:
	// opts.Offset/opts.Limit are passed through (Normalized first), and the
	// page-size ceiling that bounds an enumeration endpoint belongs to the HTTP
	// layer, which owns page/page_size parsing.
	ListExecutions(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter, opts ListOptions) ([]*ExecutionRecord, error)

	// CountExecutions returns how many executions in ns match filter — the
	// total, not the page size, so a page list can report a total without
	// scanning it. It applies exactly the same scope and filter rules as
	// ListExecutions, including the ErrInvalidNamespace refusal, so the two can
	// never disagree about which rows are in scope. opts does not participate:
	// the count is of the whole filtered set.
	CountExecutions(ctx context.Context, ns namespace.Namespace, filter ExecutionFilter) (int64, error)
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
// row exists for the same (namespace, request_id). AppendOutcomeIfAbsent
// appends an outcome-phase row keyed idempotently on (namespace, request_id,
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
// immutable phase; the (Namespace, RequestID, Phase) triple is the reconcile
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
	Supplies
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
