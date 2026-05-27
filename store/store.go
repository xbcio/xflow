package store

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// ClusterStore is the full persistence interface required by the cluster adapter.
// All method names are unique to avoid ambiguity when the interface is embedded.
type ClusterStore interface {
	// Execution lifecycle
	CreateExecution(ctx context.Context, rec *ExecutionRecord) error
	UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error
	GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionRecord, error)

	// Node state
	UpsertNode(ctx context.Context, rec *NodeRecord) error
	GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeRecord, error)
	ListNodes(ctx context.Context, id types.ExecutionID) ([]*NodeRecord, error)
	ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*NodeRecord, error)
	ListExpiredSuspensions(ctx context.Context, now time.Time) ([]*NodeRecord, error)

	// Signal store
	SaveSignal(ctx context.Context, rec *SignalRecord) error
	// ConsumeSignal atomically retrieves and deletes a signal. Returns nil, nil if not found.
	ConsumeSignal(ctx context.Context, id types.ExecutionID, name string) (*SignalRecord, error)
	// RevokeSignal atomically marks a signal as revoked if it is still active (not yet consumed).
	// Returns (true, nil) on success, (false, nil) if the signal does not exist or was already consumed.
	RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error)
	CountSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) (int, error)
	ListSignalsByNames(ctx context.Context, id types.ExecutionID, names []string) ([]*SignalRecord, error)
}
