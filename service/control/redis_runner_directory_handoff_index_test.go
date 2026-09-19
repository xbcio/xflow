package control

import (
	"context"
	"testing"
)

// TestClaimForRunnerReadsOnlyItsOwnHandoffIndex pins the poll path's handoff
// cost model. The recovery scan used to read the fleet-wide handoff ledger and
// filter it down to one runner, so a single runner's poll cost O(handoff debt
// anywhere in the fleet). The per-runner index makes that read proportional to
// this runner's own debt instead.
func TestClaimForRunnerReadsOnlyItsOwnHandoffIndex(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	ledgerHook := newLedgerHGetAllHook(directory.keys.handoffRunner)
	rdb.AddHook(ledgerHook)
	indexHook := newKeyCommandHook("smembers", directory.keys.handoffClaimIndexKey("runner-index-poll"))
	rdb.AddHook(indexHook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-index-poll", 1)

	// Put debt on a different runner. Its entries sit in the same fleet ledger the
	// old scan read whole, so this is the state that used to make the poll below
	// pay for debt it does not own.
	other := registerRedisDirectoryRunner(t, ctx, directory, "runner-index-other", 1)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-index/other/activation-1"))
	otherClaim := claimRedisDirectoryAssignment(t, ctx, directory, other, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, otherClaim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, otherClaim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-index/poll/activation-1"))
	ledgerHook.reset()
	indexHook.reset()
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if claim.Handoff != nil {
		t.Fatalf("first claim = %#v, want an ordinary queue claim", claim)
	}
	if n := ledgerHook.count(); n != 0 {
		t.Fatalf("handoff ledger HGetAll reads for a poll = %d, want 0", n)
	}
	if n := indexHook.count(); n == 0 {
		t.Fatal("per-runner handoff index reads for a poll = 0; the poll skipped its own index")
	}

	// The index must not be an empty-set fast path that hides real debt: make this
	// runner's own claim resolver-owned and the very next poll has to return it.
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}

	ledgerHook.reset()
	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("recovery = %#v, ok=%v, err=%v; want the unresolved handoff", recovery, ok, err)
	}
	if n := ledgerHook.count(); n != 0 {
		t.Fatalf("handoff ledger HGetAll reads for a poll with real debt = %d, want 0", n)
	}
}

// TestRecoverableHandoffPrunesStaleIndexEntry covers the index's only cleanup
// path. A deletion site that removes a handoff record does not also reach into
// the owner's index, so an entry can outlive the record it names; the next poll
// has to drop it rather than trust it.
func TestRecoverableHandoffPrunesStaleIndexEntry(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-index-prune", 1)
	indexKey := directory.keys.handoffClaimIndexKey("runner-index-prune")
	if err := rdb.SAdd(ctx, indexKey, "claim-that-no-longer-exists").Err(); err != nil {
		t.Fatalf("seed stale index entry: %v", err)
	}

	claim, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatalf("claim = %#v, ok=true; want no recovery from a stale index entry", claim)
	}
	stale, err := rdb.SIsMember(ctx, indexKey, "claim-that-no-longer-exists").Result()
	if err != nil {
		t.Fatalf("read index after poll: %v", err)
	}
	if stale {
		t.Fatal("stale index entry survived a poll; the index is not self-pruning")
	}
}
