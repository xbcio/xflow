package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TriggerActivation represents a desired or active trigger activation for a
// workflow node-group. The activation controller reconciles these records to
// ensure each desired trigger has exactly one runner session driving it.
type TriggerActivation struct {
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	GroupID         string
	PackageHash     string
	Desired         bool
	RunnerID        string
	SessionID       string
	Generation      uint64
	LeaseDeadline   time.Time
}

// ActivationKey uniquely identifies a trigger activation record.
type ActivationKey struct {
	Namespace  namespace.Namespace
	WorkflowID types.WorkflowID
	GroupID    string
}

// TriggerActivationStore manages the lifecycle of trigger activations.
type TriggerActivationStore interface {
	SetDesired(ctx context.Context, act TriggerActivation) error
	ClearDesired(ctx context.Context, key ActivationKey) error
	ListActive(ctx context.Context) ([]TriggerActivation, error)
	ListByRunner(ctx context.Context, runnerID string) ([]TriggerActivation, error)
	AssignRunner(ctx context.Context, key ActivationKey, runnerID, sessionID string, leaseDeadline time.Time) (generation uint64, err error)
	RenewActivationLease(ctx context.Context, key ActivationKey, generation uint64, deadline time.Time) (renewed bool, err error)
	RevokeAssignment(ctx context.Context, key ActivationKey, generation uint64) (revoked bool, err error)
	GetActivation(ctx context.Context, key ActivationKey) (*TriggerActivation, error)
}
