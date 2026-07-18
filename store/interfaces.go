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
