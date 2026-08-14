package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// GroupSignal is one entry in the durable signal journal. Each signal delivered
// to a suspended group's waiter is recorded here. On resume, the full journal
// is replayed to ensure deterministic execution.
type GroupSignal struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data,omitempty"`
}

// GroupSuspendSpec describes what the group's current waiter is waiting for.
// It is persisted durably when a group suspends.
type GroupSuspendSpec struct {
	NodeName    string        `json:"node_name"`          // member node that issued the suspend
	WaitSignals []string      `json:"wait_signals"`       // signal names the waiter accepts
	Quorum      int           `json:"quorum,omitempty"`   // multi-signal: how many required (0 or 1 = single)
	Timeout     time.Duration `json:"timeout,omitempty"`  // waiter timeout (0 = no timeout)
}

// GroupSuspendRequest carries a group suspend transition from the runner.
type GroupSuspendRequest struct {
	ExecutionID    types.ExecutionID
	GroupUnitIdx   int
	GroupID        string
	LeaseID        LeaseID
	LeaseToken     LeaseToken
	Attempt        int
	SuspendSpec    GroupSuspendSpec
	SignalJournal  []GroupSignal  // accumulated journal from prior resumes
	EntryInput     map[string]any // checkpoint for replay
	IdempotencyKey string
}

// GroupSuspendResult reports the outcome of a suspend transition.
type GroupSuspendResult struct {
	Committed       bool
	ImmediateResume bool // signal already available -> skip suspend, resume immediately
}

// GroupResumeRequest carries a resume transition triggered by signal delivery.
type GroupResumeRequest struct {
	ExecutionID  types.ExecutionID
	GroupUnitIdx int
	SignalName   string
	SignalData   map[string]any
}

// GroupResumeResult reports what happened after attempting to resume.
type GroupResumeResult struct {
	Resumed bool // true = quorum satisfied, resume dispatched
	Pending int  // for multi-signal: how many more signals needed
}

// GroupSuspendState is the durable snapshot of a suspended group unit.
type GroupSuspendState struct {
	Spec             GroupSuspendSpec `json:"spec"`
	SignalJournal    []GroupSignal    `json:"signal_journal"`
	EntryInput       map[string]any   `json:"entry_input"`
	IdempotencyKey   string           `json:"idempotency_key"`
	DeliveredSignals []GroupSignal    `json:"delivered_signals"` // partial signals toward quorum
}

// --- Interfaces ---
//
// NOTE: none of the interfaces below has a production caller. Both backends
// implement them and the shared contract test covers them, but nothing in
// cmd/ or service/ calls SuspendGroup, ResumeGroup, CancelSuspendedGroup,
// TimeoutSuspendedGroup or RevokeGroupSignal, and TaskTypeGroupResume has no
// consumer (engine/types.go: "里程碑 A 预留，暂不消费").
//
// Production disables group suspend on purpose: the only production
// GroupRuntime is built with runnersvc.WithSuspendDisabled(), because a
// suspended member would park a sub-execution the outer lease cannot resume.
// A member's wait therefore fails the task in commit.go before reaching here.
//
// Wiring this up needs a piece that does not exist on either backend: there is
// no "list suspended groups" primitive, so Engine.Cancel walks only
// ListSuspendedNodes and could not reach a suspended group even if it wanted
// to. See docs/design/NODE-GROUP-COLOCATION.md §6.

// GroupSuspender atomically transitions a running group unit to suspended state.
// It persists the suspend spec, signal journal, and entry input; clears the lease;
// and optionally writes a timeout outbox entry.
type GroupSuspender interface {
	SuspendGroup(ctx context.Context, req GroupSuspendRequest) (GroupSuspendResult, error)
}

// GroupResumer delivers a signal to a suspended group unit and, if quorum is
// satisfied, produces a TaskTypeGroupResume outbox entry for re-dispatch.
type GroupResumer interface {
	ResumeGroup(ctx context.Context, req GroupResumeRequest) (GroupResumeResult, error)
}

// GroupSuspendReader reads the current suspend state of a group unit.
type GroupSuspendReader interface {
	GetGroupSuspendState(ctx context.Context, execID types.ExecutionID, unitIdx int) (*GroupSuspendState, error)
}

// GroupCanceler cancels a suspended group unit (transitions to done/canceled).
type GroupCanceler interface {
	CancelSuspendedGroup(ctx context.Context, execID types.ExecutionID, unitIdx int) error
}

// CanceledSuspendedGroupError is the execution-level failure reason recorded
// when canceling a suspended group unit finalizes the execution as failed.
//
// This path is the one place a terminal failure has no node-level carrier at
// all: cancellation terminalizes the group unit itself, so no member node ever
// commits a failure and neither the caller nor the store has a reason string to
// pass along. Both backends record this same constant so the readback does not
// depend on which one is in use.
const CanceledSuspendedGroupError = "suspended group canceled"

// GroupSignalRevoker revokes a previously-delivered signal from a suspended group's waiter.
type GroupSignalRevoker interface {
	RevokeGroupSignal(ctx context.Context, execID types.ExecutionID, unitIdx int, signalName string) error
}

// GroupTimeoutHandler transitions a suspended group to done/timeout when the
// waiter's timeout fires.
type GroupTimeoutHandler interface {
	TimeoutSuspendedGroup(ctx context.Context, execID types.ExecutionID, unitIdx int) error
}
