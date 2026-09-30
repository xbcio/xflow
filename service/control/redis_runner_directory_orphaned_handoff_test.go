package control

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// seedOrphanedHandoff puts one claim into the exact shape the reaper exists for:
// fenced as lease_may_exist before an engine call that may have created a lease,
// then marked recoverable — the state ReclaimExpiredClaims leaves a claim in
// once its expiry has passed, without requeueing it. The owning runner is the
// argument, so a test decides whether that owner is still polling.
func seedOrphanedHandoff(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, assignmentID AssignmentID) ClaimID {
	t.Helper()

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment(assignmentID))
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}
	return claim.ClaimID
}

// killOrphanedHandoffOwner makes an owner look gone the way a process death
// does: its last heartbeat is far outside DefaultOrphanedHandoffOwnerStale.
func killOrphanedHandoffOwner(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession) {
	t.Helper()

	if err := directory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Capacity:  1,
		Now:       time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatalf("stale heartbeat: %v", err)
	}
}

// keepOrphanedHandoffOwnerAlive makes the owner look live: a heartbeat just now,
// inside the staleness window.
func keepOrphanedHandoffOwnerAlive(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession) {
	t.Helper()

	if err := directory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Capacity:  1,
		Now:       time.Now(),
	}); err != nil {
		t.Fatalf("fresh heartbeat: %v", err)
	}
}

// TestRedisRunnerDirectoryListsOrphanedHandoff is the primary path: a fenced,
// recoverable handoff whose owner is gone is listed, and listing takes the
// resolver token so a second control plane cannot settle the same debt.
func TestRedisRunnerDirectoryListsOrphanedHandoff(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-orphan"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignmentID := AssignmentID("exec-orphan/node/activation-1")
	claimID := seedOrphanedHandoff(t, ctx, directory, session, assignmentID)
	killOrphanedHandoffOwner(t, ctx, directory, session)

	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claims = %d, want 1", len(claims))
	}
	claim := claims[0]
	if claim.ClaimID != claimID {
		t.Fatalf("claim ID = %q, want %q", claim.ClaimID, claimID)
	}
	if claim.Handoff == nil || claim.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("handoff = %#v, want lease_may_exist debt", claim.Handoff)
	}
	if claim.Assignment.AssignmentID != assignmentID {
		t.Fatalf("assignment = %q, want %q", claim.Assignment.AssignmentID, assignmentID)
	}
	if got := server.HGet(directory.keys.handoffRecoveryReady, string(claimID)); got != "" {
		t.Fatalf("recovery-ready = %q, want the token taken", got)
	}
	if got := server.HGet(directory.keys.handoffRecoveryDeadline, string(claimID)); got == "" {
		t.Fatal("recovery deadline unset; the taken token is not fenced against a second resolver")
	}
}

// TestRedisRunnerDirectoryLeavesLiveHandoffOwnerAlone is the safety property the
// whole reaper rests on: recovery-ready means a resolver MAY act, not that no
// resolver is coming. A live owner's poll is that resolver, and taking its token
// would resolve debt out from under the runner that owns it. Delete the
// liveness gate and this turns red.
func TestRedisRunnerDirectoryLeavesLiveHandoffOwnerAlone(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-alive"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	seedOrphanedHandoff(t, ctx, directory, session, AssignmentID("exec-alive/node/activation-1"))
	keepOrphanedHandoffOwnerAlive(t, ctx, directory, session)

	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %d, want 0: the owner is still polling", len(claims))
	}
}

// TestRedisRunnerDirectoryLeavesUnrecoverableHandoffAlone covers the other half
// of eligibility. A handoff whose claim has not yet expired is deliberately
// unresolved: the owner may still be mid-dispatch, and the directory has not
// said a resolver may take it. Its token must stay untouched.
func TestRedisRunnerDirectoryLeavesUnrecoverableHandoffAlone(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-unexpired"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-unexpired/node/activation-1"))
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	killOrphanedHandoffOwner(t, ctx, directory, session)

	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %d, want 0: the claim has not expired, so no resolver may take it", len(claims))
	}
	// The judge is the directory's own permission flag, not this pass's opinion:
	// an unexpired handoff still has a live resolver deadline and no ready flag.
	if got := server.HGet(directory.keys.handoffRecoveryDeadline, string(claim.ClaimID)); got == "" {
		t.Fatal("recovery deadline cleared for an unexpired handoff")
	}
}

// TestRedisRunnerDirectoryIgnoresSettledHandoffStates keeps the pass off the two
// ledger states that are not its shape: 'reserved' has no engine call to be
// uncertain about yet, and 'finalized' is the lease sweeper's debt, not a
// resolver's.
func TestRedisRunnerDirectoryIgnoresSettledHandoffStates(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-states"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 2)

	// A reserved handoff: fenced by ClaimForRunner, before any engine call.
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-reserved/node/activation-1"))
	reserved := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	// A finalized handoff: the assignment is leased and the record is settled debt.
	assignment := redisDirectoryTestAssignment("exec-finalized/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-finalized", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	killOrphanedHandoffOwner(t, ctx, directory, session)

	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %d, want 0: neither 'reserved' nor 'finalized' is this pass's shape", len(claims))
	}
	if reserved.ClaimID == "" {
		t.Fatal("seed did not produce the reserved ledger state")
	}
}

// TestRedisRunnerDirectoryReturnsTokenWhenClaimCannotBeBuilt pins the failure
// path that would otherwise strand debt: the token is taken before the record is
// read, so a record that cannot be assembled has to give the token back or the
// debt waits out a deadline nobody can shorten.
func TestRedisRunnerDirectoryReturnsTokenWhenClaimCannotBeBuilt(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-unbuildable"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignmentID := AssignmentID("exec-unbuildable/node/activation-1")
	claimID := seedOrphanedHandoff(t, ctx, directory, session, assignmentID)
	killOrphanedHandoffOwner(t, ctx, directory, session)
	if err := rdb.HDel(ctx, directory.keys.assignmentData, string(assignmentID)).Err(); err != nil {
		t.Fatalf("drop assignment payload: %v", err)
	}

	claims, err := directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %d, want 0: the record cannot be assembled", len(claims))
	}
	if got := server.HGet(directory.keys.handoffRecoveryReady, string(claimID)); got != "1" {
		t.Fatalf("recovery-ready = %q, want the token returned", got)
	}
}

// TestRedisRunnerDirectoryOrphanedHandoffListIsBoundedAndIdempotent keeps the
// pass proportional to its limit and safe to repeat: a second call over an
// already-drained backlog lists nothing.
func TestRedisRunnerDirectoryOrphanedHandoffListIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-bounded"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	// One orphan per owner, so the bound is exercised across runners rather than
	// through repeated claims on one.
	const orphanCount = 3
	for i := 0; i < orphanCount; i++ {
		owner := "runner-bounded-" + strconv.Itoa(i)
		ownerSession := registerRedisDirectoryRunner(t, ctx, directory, owner, 1)
		seedOrphanedHandoff(t, ctx, directory, ownerSession, AssignmentID("exec-ob"+strconv.Itoa(i)+"/node/a"))
		killOrphanedHandoffOwner(t, ctx, directory, ownerSession)
	}
	// A fourth orphan owned by the first runner, which is now gone: the bound has
	// to hold across candidates from the same owner too.
	seedOrphanedHandoff(t, ctx, directory, session, AssignmentID("exec-bounded-extra/node/a"))
	killOrphanedHandoffOwner(t, ctx, directory, session)

	claims, err := directory.ListOrphanedHandoffs(ctx, 2)
	if err != nil {
		t.Fatalf("ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("claims = %d, want exactly the limit of 2", len(claims))
	}

	claims, err = directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("second ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("second claims = %d, want the remaining 2", len(claims))
	}

	claims, err = directory.ListOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("third ListOrphanedHandoffs() error = %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("third claims = %d, want 0: every token is already taken", len(claims))
	}
}

// TestCoreReapOrphanedHandoffsSettlesAgainstEngineAuthority is the end-to-end
// half: the directory lists the debt, and Core settles it on the engine's
// answer. Without this the reaper would release nothing and the assignment would
// stay wedged, which is the defect it exists to fix.
func TestCoreReapOrphanedHandoffsSettlesAgainstEngineAuthority(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-core"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignmentID := AssignmentID("exec-core/node/activation-1")
	claimID := seedOrphanedHandoff(t, ctx, directory, session, assignmentID)
	killOrphanedHandoffOwner(t, ctx, directory, session)

	core := &Core{
		engine:  &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable},
		runners: directory,
	}
	result, err := core.ReapOrphanedHandoffs(ctx, 16)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v", err)
	}
	if result.Inspected != 1 || result.Released != 1 {
		t.Fatalf("result = %+v, want 1 inspected and 1 settled", result)
	}
	if got := server.HGet(directory.keys.handoffState, string(claimID)); got != "" {
		t.Fatalf("handoff state = %q, want the record settled", got)
	}
	if got := server.HGet(directory.keys.assignmentState, string(assignmentID)); got != redisAssignmentQueued {
		t.Fatalf("assignment state = %q, want %q after a requeue", got, redisAssignmentQueued)
	}
}
