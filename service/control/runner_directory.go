package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
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
	Namespace    namespace.Namespace
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
	Labels       map[string]string
	Capabilities []protocol.Capability
	Policy       RunnerPolicy
	Namespaces   []namespace.Namespace
	Now          time.Time
}

// RunnerSession identifies the current live session for a runner ID.
type RunnerSession struct {
	RunnerID  string
	SessionID string
}

// RunnerSnapshot is the read-only registration and liveness view returned by
// a RunnerDirectory. JSON tags are snake_case so the GET /v1/management/runners/{id}
// user-face response (spec §9.4) matches the rest of the wire contract.
type RunnerSnapshot struct {
	RunnerID      string                `json:"runner_id"`
	Capacity      int                   `json:"capacity"`
	InFlight      int                   `json:"in_flight"`
	Labels        map[string]string     `json:"labels,omitempty"`
	Capabilities  []protocol.Capability `json:"capabilities,omitempty"`
	Namespaces    []namespace.Namespace `json:"namespaces,omitempty"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
}

func cloneCapabilities(capabilities []protocol.Capability) []protocol.Capability {
	if len(capabilities) == 0 {
		return nil
	}
	clone := make([]protocol.Capability, len(capabilities))
	copy(clone, capabilities)
	return clone
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
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
	Labels       map[string]string
	Capabilities []protocol.Capability
	Now          time.Time
	// ActiveLeaseIDs are the leases this runner is executing right now. Lease
	// replay matches on (runner, session, state==leased), which is equally true
	// of a lease the runner is mid-handler on — so without this a runner with
	// Concurrency > 1 had every idle worker handed the same live lease, and each
	// node executed once per unit of concurrency. Only the runner knows what it
	// is actually running, so it says, and the directory replays everything
	// else. Empty means "nothing in flight", which is what a freshly restarted
	// process reports and is exactly when replay must fire.
	ActiveLeaseIDs []string
}

// Claim is either a temporary reservation for an assignment or a durable
// replay of a previously finalized lease. A non-nil Lease is already fenced
// and must be returned to the runner without issuing another engine lease.
type Claim struct {
	ClaimID    ClaimID
	Assignment Assignment
	Lease      *engine.TaskLease
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
// ExpiredLeaseReleaser is embedded so the LeaseSweeper can rely on a compile-time
// guarantee that any RunnerDirectory can clean up finalized lease/capacity/seen
// state before engine reclaim. No runtime type assertion is required.
type RunnerDirectory interface {
	ExpiredLeaseReleaser
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

// ClaimReclaimer is an optional durable-directory capability used by the
// control-plane lifecycle to recover expired claims even when no runner sends
// another request. Its operation must be idempotent and safe across replicas.
type ClaimReclaimer interface {
	ReclaimExpiredClaims(ctx context.Context) error
}

// LeaseLookupKey identifies one finalized lease by its immutable identity. The
// caller fills whichever fields it has; the directory resolves token > id >
// assignmentID, mirroring ReleaseLeased.
type LeaseLookupKey struct {
	AssignmentID AssignmentID
	LeaseID      engine.LeaseID
	LeaseToken   engine.LeaseToken
}

// LeaseLookup is an optional directory capability that returns the
// server-authoritative finalized lease for one (runner, session, lease-identity)
// triple. It is the authority source for namespace on the report path: the lease
// JSON a runner echoes back is unsigned and client-mutable, so reportResult
// must not trust req.Lease.Namespace. LookupLease resolves the lease from server
// state instead.
//
// ok=false (err=nil) means no finalized lease matches: the lease was never
// finalized, was already released, belongs to a different runner/session, or
// the token/leaseID did not match. A non-nil err signals an internal failure.
// Implementations must NOT distinguish "wrong namespace" from "not found" in the
// return value (both are ok=false) to avoid leaking cross-namespace state.
type LeaseLookup interface {
	LookupLease(ctx context.Context, runnerID, sessionID string, key LeaseLookupKey) (*engine.TaskLease, bool, error)
}

// ExpiredDirectoryLeaseRequest identifies an expired lease to release from the
// runner directory. All three fields must match the directory's finalized
// lease record; any mismatch fails closed.
type ExpiredDirectoryLeaseRequest struct {
	AssignmentID AssignmentID
	LeaseID      engine.LeaseID
	LeaseToken   engine.LeaseToken
}

// ExpiredDirectoryLeaseOutcome is the result of a token-fenced release.
type ExpiredDirectoryLeaseOutcome string

const (
	// ExpiredDirectoryLeaseReleased means the finalized lease record was
	// removed and capacity/seen bookkeeping was cleaned up.
	ExpiredDirectoryLeaseReleased ExpiredDirectoryLeaseOutcome = "released"
	// ExpiredDirectoryLeaseAlreadyReleased means the assignment has no
	// finalized lease record; repeated calls remain idempotent.
	ExpiredDirectoryLeaseAlreadyReleased ExpiredDirectoryLeaseOutcome = "already_released"
	// ExpiredDirectoryLeaseTokenMismatch means the lease identity did not
	// match the directory's record. The caller must not retry: another lease
	// generation now owns the assignment.
	ExpiredDirectoryLeaseTokenMismatch ExpiredDirectoryLeaseOutcome = "token_mismatch"
)

// ExpiredLeaseReleaser is an optional durable-directory capability used by the
// LeaseSweeper to clean up a directory's finalized lease/capacity/seen state
// before engine reclaim. It must fail closed on token mismatch.
type ExpiredLeaseReleaser interface {
	ReleaseExpiredLease(ctx context.Context, req ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error)
}
