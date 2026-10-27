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
	// ExecutionFilter.Status (the status column), ExecutionFilter.WorkflowID
	// (the workflow_id column, exact match), ExecutionFilter.Query (a literal
	// prefix on EITHER the execution_id or the workflow_id column, ORed) and
	// ExecutionFilter.CreatedAfter/CreatedBefore (created_at). A row whose
	// workflow_id is empty (unattributed) matches no non-empty WorkflowID
	// filter and can match a Query only through its execution id, the same
	// fail-closed rule the namespace scope applies. NOT supported, with the
	// column-level reason:
	//
	//   - by workflow NAME: xflow_executions stores workflow_name, but a name is
	//     not an identity: two namespaces can hold the same name, and the name is
	//     not immutable across a re-registration under the same id. A name
	//     filter would therefore be a filter on a display string that does not
	//     resolve to a workflow; use the workflow_id column instead.
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

// ExecutionDriftCandidate is one durable execution row the drift scan
// considers possibly stale: the SQL projection still says "running" and its
// updated_at has not moved for the scan's staleness window. It carries only
// what the repair needs to identify the row, re-read its authority and resume
// the scan after it — the execution's id, its namespace (which the authoritative
// read needs to address the right key), the timestamp that made it a candidate,
// and the row's own primary key for the cursor tiebreak.
type ExecutionDriftCandidate struct {
	ID        types.ExecutionID
	Namespace namespace.Namespace
	UpdatedAt time.Time
	// RowID is the durable row's auto-increment primary key. It is the
	// tiebreak of the scan's documented total order and therefore half of the
	// cursor: updated_at alone is not unique (the column is DATETIME(3), and
	// rows written in the same millisecond share it).
	RowID uint64
}

// ExecutionDriftCursor addresses the last durable row a scan page stopped at:
// exactly its updated_at and primary-key values, in the scan's own order
// (updated_at ASC, id ASC). The zero value is the start of the scan.
//
// A cursor the caller did not obtain from a previous page is not meaningful;
// passing one is not an error but the resulting page starts wherever those
// values point, which is how a resumed sweep continues without re-reading the
// rows it already classified.
type ExecutionDriftCursor struct {
	UpdatedAt time.Time
	RowID     uint64
}

// ExecutionDriftScanner is the optional durable-projection capability the
// execution drift repair worker depends on.
//
// The SQL executions table is a best-effort mirror of Redis-as-authority: a
// status projection that fails (or a process that exits before writing it)
// leaves a row reading "running" for an execution Redis has already finished.
// No per-write path can repair that — the failed write is gone — so a
// background scan asks Redis for each stale row's authoritative status and
// rewrites the projection only when Redis explicitly reports a terminal one.
//
// This is deliberately NOT part of Store or Executions: it is a repair-only
// read that a backend without a durable secondary (memstore) has nothing to
// implement. The worker is built only when the store advertises it.
type ExecutionDriftScanner interface {
	// ListStaleRunningExecutions returns at most limit executions whose
	// durable row says "running" and whose updated_at is strictly before
	// updatedBefore, ordered by (updated_at ASC, id ASC) and strictly after
	// the after cursor in that same total order.
	//
	// The cursor is what keeps a sweep that starts at unrepairable rows (their
	// Redis status has expired) from starving every row behind them: the
	// worker pages past them within one sweep instead of re-reading the same
	// oldest batch forever. The zero cursor starts at the beginning.
	//
	// owned nil or empty is the legacy cross-namespace scan: every namespace
	// (including rows stored with an empty namespace) is eligible. A non-empty
	// set restricts the scan to those namespaces, matched by exact equality —
	// same as the namespace column's own rule, an empty namespace is
	// UNATTRIBUTED rather than the default namespace, so it is never widened
	// into an owned scan. A malformed owned entry is rejected with
	// ErrInvalidNamespace rather than silently narrowed or widened.
	//
	// limit must be positive; a non-positive limit is refused rather than read
	// as "unbounded", because this scan feeds writes back into the projection
	// and an accidental whole-table sweep is not recoverable by retrying.
	ListStaleRunningExecutions(ctx context.Context, owned []namespace.Namespace, updatedBefore time.Time, after ExecutionDriftCursor, limit int) ([]ExecutionDriftCandidate, error)
}

// ExecutionDriftRepairer is the optional durable-projection write capability
// the execution drift repair worker depends on. It is deliberately narrower
// than Executions.UpdateExecutionStatus: the repair is a conditional write, and
// making that conditional part of the global update path would change the
// semantics every ordinary projection write depends on.
//
// The scan and the repair are not atomic. Between them the ordinary projection
// path may have advanced the row to the very terminal status the repair was
// about to copy (with its error_msg), and an unconditional rewrite at that
// point would erase the message the real transition recorded. The conditional
// write makes that race benign: the row is no longer "running", so there is
// nothing to repair.
//
// This is optional like the scanner: a backend without a durable secondary has
// nothing to implement. A worker whose store lacks it classifies candidates but
// writes nothing, and reports the skips.
type ExecutionDriftRepairer interface {
	// RepairRunningExecutionStatus rewrites the durable row's status only while
	// it still reads "running", clearing error_msg only as part of that same
	// conditional write. It returns repaired=false (with a nil error) when no
	// row matched — the row was advanced by the ordinary projection path or no
	// longer exists — because that is a benign race, not a failure. A
	// non-nil error means the write itself failed.
	RepairRunningExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) (repaired bool, err error)
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

// ScopedAuditReconciler is an optional AuditReconciler capability: the same
// pending scan and backlog count, restricted to the audit rows of an owned
// namespace set. A deployment that shares its audit table with another one
// (a local dev process on the shared test database) must not settle the other
// deployment's admissions — the unscoped scan is cross-namespace by design,
// and settling means probing authoritative state that belongs to someone else.
//
// The scope is a hard filter on the row's own namespace; the NOT EXISTS
// outcome check keeps its (namespace, request_id) correlation, so a request id
// reused across namespaces never lets one scope's outcome settle another's
// admission. An empty or nil owned set matches nothing (fail closed): callers
// that want every namespace use the unscoped AuditReconciler methods
// explicitly rather than by passing an empty set. Rows with an empty namespace
// are the default namespace's (see AuditScopeValues).
type ScopedAuditReconciler interface {
	ListUnreconciledAdmissionsIn(ctx context.Context, owned []namespace.Namespace, before time.Time, afterSeqID uint64, limit int) ([]*AuditRecord, error)
	CountUnreconciledAdmissionsIn(ctx context.Context, owned []namespace.Namespace, before time.Time) (pending int, oldest time.Time, err error)
}

// AuditScopeValues expands an owned namespace set into the values of the audit
// row's namespace column it covers. It is the single definition of that rule
// for every ScopedAuditReconciler: audit rows written without a namespace
// carry the empty string and are the default namespace's, so they are covered
// only when namespace.Default is owned — a dedicated deployment must not
// inherit them. An empty or nil set expands to nothing.
func AuditScopeValues(owned []namespace.Namespace) []string {
	if len(owned) == 0 {
		return nil
	}
	values := make([]string, 0, len(owned)+1)
	seen := make(map[string]struct{}, len(owned)+1)
	add := func(v string) {
		if _, dup := seen[v]; dup {
			return
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}
	for _, ns := range owned {
		add(string(ns))
		if ns == namespace.Default {
			add("")
		}
	}
	return values
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
