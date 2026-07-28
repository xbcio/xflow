package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// GroupAuditEvent represents an auditable group lifecycle event.
type GroupAuditEvent struct {
	Timestamp    time.Time
	Operation    GroupAuditOperation
	ExecutionID  types.ExecutionID
	WorkflowID   types.WorkflowID
	GroupID      string
	UnitIdx      int
	RunnerID     string
	Generation   uint64
	Outcome      string // operation-specific outcome
	AdmissionKey string // for admission events
	Error        string // non-empty on failure
}

// GroupAuditOperation enumerates auditable group operations.
type GroupAuditOperation string

const (
	GroupAuditLeaseAcquired     GroupAuditOperation = "lease_acquired"
	GroupAuditLeaseExpired      GroupAuditOperation = "lease_expired"
	GroupAuditCommitted         GroupAuditOperation = "committed"
	GroupAuditAdmissionAccepted GroupAuditOperation = "admission_accepted"
	GroupAuditAdmissionConflict GroupAuditOperation = "admission_conflict"
	GroupAuditActivationChanged GroupAuditOperation = "activation_changed"
	GroupAuditSuspended         GroupAuditOperation = "suspended"
	GroupAuditResumed           GroupAuditOperation = "resumed"
	GroupAuditCanceled          GroupAuditOperation = "canceled"
	GroupAuditTimeout           GroupAuditOperation = "timeout"
)

// GroupAuditObserver receives group lifecycle audit events.
// Implementations must be safe for concurrent use and should not block.
type GroupAuditObserver interface {
	OnGroupAuditEvent(ctx context.Context, event GroupAuditEvent)
}

// NoopGroupAuditObserver is a no-op implementation for testing and when
// audit is disabled.
type NoopGroupAuditObserver struct{}

func (NoopGroupAuditObserver) OnGroupAuditEvent(context.Context, GroupAuditEvent) {}
