package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// ErrRunnerSessionStale reports that the caller's runner session has been
// replaced by a newer registration for the same runner ID.
var ErrRunnerSessionStale = errors.New("runner session stale")

// AssignmentID uniquely identifies one queued assignment tracked by the
// control-plane directory.
type AssignmentID string

// ClaimID uniquely identifies one in-flight claim handed to a polling runner.
type ClaimID string

// Assignment is the route-first unit queued in the runner directory before a
// concrete task lease exists.
type Assignment struct {
	AssignmentID AssignmentID
	Task         engine.Task
	Routing      engine.TaskRouting
}

// BuildAssignmentID derives the stable control-plane identity for a queued
// assignment from immutable task fields.
func BuildAssignmentID(task *engine.Task) AssignmentID {
	payload := ""
	if task.Payload != nil {
		payload = fmt.Sprintf("%s:%d", task.Payload.Name, task.Payload.Triggered)
	}
	return AssignmentID(fmt.Sprintf("%s/%s/%d/%d/%d/%s", task.ExecutionID, task.NodeName, task.NodeIdx, task.ActivationID, task.AutoDepth, payload))
}

// RegisterRunnerRequest captures the data needed to register or replace a
// runner session in the directory.
type RegisterRunnerRequest struct {
	RunnerID     string
	Capacity     int
	Capabilities []protocol.Capability
	Policy       RunnerPolicy
	Now          time.Time
}

// RunnerSession identifies the current live session for a runner ID.
type RunnerSession struct {
	RunnerID  string
	SessionID string
}

// HeartbeatRequest updates observed runner capacity and liveness for an
// existing session.
type HeartbeatRequest struct {
	RunnerID  string
	SessionID string
	Capacity  int
	InFlight  int
	Now       time.Time
}

// ClaimRequest asks the directory for the next compatible assignment for a
// specific runner session.
type ClaimRequest struct {
	RunnerID     string
	SessionID    string
	Capacity     int
	Capabilities []protocol.Capability
	Now          time.Time
}

// Claim is the directory's temporary reservation for an assignment.
type Claim struct {
	ClaimID    ClaimID
	Assignment Assignment
}

// ReleaseClaimReason controls how an abandoned claim affects queue and seen
// bookkeeping.
type ReleaseClaimReason string

const (
	// ReleaseClaimRequeue returns the assignment to the front of the queue and
	// keeps it marked as seen.
	ReleaseClaimRequeue ReleaseClaimReason = "requeue"
	// ReleaseClaimDrop drops the assignment and clears its seen marker.
	ReleaseClaimDrop ReleaseClaimReason = "drop"
	// ReleaseClaimKeepSeen clears only claim accounting and keeps the seen mark.
	ReleaseClaimKeepSeen ReleaseClaimReason = "keep_seen"
)

// ReleaseLeasedRequest releases finalized leased capacity for a runner
// session. RemoveSeen controls whether the assignment can be enqueued again.
type ReleaseLeasedRequest struct {
	RunnerID     string
	SessionID    string
	AssignmentID AssignmentID
	LeaseID      engine.LeaseID
	LeaseToken   engine.LeaseToken
	RemoveSeen   bool
}

// RunnerDirectory owns runner registration, assignment placement, and session
// fencing independently from any specific transport or persistence backend.
type RunnerDirectory interface {
	Register(ctx context.Context, req RegisterRunnerRequest) (RunnerSession, error)
	ValidateSession(ctx context.Context, runnerID, sessionID string) error
	Heartbeat(ctx context.Context, req HeartbeatRequest) error
	EnqueueAssignment(ctx context.Context, assignment Assignment) (bool, error)
	ClaimForRunner(ctx context.Context, req ClaimRequest) (Claim, bool, error)
	FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error
	ReleaseClaim(ctx context.Context, claimID ClaimID, reason ReleaseClaimReason) error
	ReleaseLeased(ctx context.Context, req ReleaseLeasedRequest) error
	ClearAssignment(ctx context.Context, assignmentID AssignmentID) error
	Runner(ctx context.Context, runnerID string) (RunnerSnapshot, bool)
}
