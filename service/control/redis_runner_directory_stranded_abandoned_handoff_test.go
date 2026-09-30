package control

import (
	"context"
	"testing"
	"time"
)

// The two reclaim paths both release the assignment's directory record first
// and settle its finalized handoff record second, because the release does not
// prove the lease token is dead and the settle must not run before it does.
// Nothing makes that pair atomic: the sweeper reclaims through the engine
// between the two, and the reaper settles after releasing. Either can die in
// between, and the state it leaves is one neither path can ever enumerate
// again — the release deletes the assignment's own record, so the lease walks
// no longer yield it, and the ledger is the only place it still appears.
//
// TestRedisRunnerDirectoryStrandedReapSettlesReleasedLeaseHandoff is that shape
// built the way a crash builds it: run the release, stop before the settle, and
// require the next pass to finish the job.
func TestRedisRunnerDirectoryStrandedReapSettlesReleasedLeaseHandoff(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-abandoned"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-abandoned/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-abandoned", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)
	claimID := server.HGet(directory.keys.handoffAssignment, assignmentID)
	if claimID == "" {
		t.Fatal("finalize did not record a handoff claim for the assignment")
	}

	outcome, err := directory.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: assignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err != nil {
		t.Fatalf("ReleaseExpiredLease() error = %v", err)
	}
	if outcome != ExpiredDirectoryLeaseReleased {
		t.Fatalf("release outcome = %v, want %v", outcome, ExpiredDirectoryLeaseReleased)
	}
	// The premise of the test: the release takes the assignment's record and
	// deliberately leaves the finalized one.
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state after release = %q, want gone", got)
	}
	if got := server.HGet(directory.keys.handoffState, claimID); got != string(HandoffDebtFinalized) {
		t.Fatalf("handoff state after release = %q, want %q", got, HandoffDebtFinalized)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1: the ledger still names the assignment", reap.Released)
	}
	if got := server.HGet(directory.keys.handoffState, claimID); got != "" {
		t.Fatalf("handoff state = %q, want settled", got)
	}
	// Settling clears the whole claim record, not just its state: the index the
	// owner polls and the assignment's reverse index are what keep the entry
	// alive in the ledger.
	for _, key := range []string{
		directory.keys.handoffGeneration,
		directory.keys.handoffLeaseMeta,
		directory.keys.handoffClaim,
		directory.keys.handoffRunner,
		directory.keys.handoffSession,
		directory.keys.handoffLeaseID,
		directory.keys.handoffLeaseToken,
		directory.keys.handoffRecoveryReady,
		directory.keys.handoffRecoveryDeadline,
	} {
		if got := server.HGet(key, claimID); got != "" {
			t.Fatalf("handoff field %s = %q, want cleared", key, got)
		}
	}
	if got := server.HGet(directory.keys.handoffAssignment, assignmentID); got != "" {
		t.Fatalf("reverse handoff index = %q, want cleared", got)
	}

	// Idempotent: the record is gone, so a second pass finds nothing of it.
	reap, err = directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("second ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("second released = %d, want 0", reap.Released)
	}
}

// TestRedisRunnerDirectoryStrandedReapLeavesAbandonedHandoffOfLiveLeaseAlone is
// the fence on that settle, and it is the property that keeps it safe against a
// lease that is merely not this reaper's business: the finalized record is
// present but its assignment still has a directory record, so nothing has
// released it and no engine outcome has made its token terminal. A record in
// that shape belongs to the lease it names; settling it here would delete the
// debt the sweeper still needs to close.
func TestRedisRunnerDirectoryStrandedReapLeavesAbandonedHandoffOfLiveLeaseAlone(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	const runnerID = "runner-kept"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-kept/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-kept", time.Hour)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)
	claimID := server.HGet(directory.keys.handoffAssignment, assignmentID)
	if claimID == "" {
		t.Fatal("finalize did not record a handoff claim for the assignment")
	}
	// The assignment record survives, in a state the release would not accept,
	// so the only thing standing between the record and the settle is the fence.
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != redisAssignmentLeased {
		t.Fatalf("assignment state = %q, want %q", got, redisAssignmentLeased)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 0 {
		t.Fatalf("released = %d, want 0", reap.Released)
	}
	if got := server.HGet(directory.keys.handoffState, claimID); got != string(HandoffDebtFinalized) {
		t.Fatalf("handoff state = %q, want the record kept", got)
	}
}
