package control

import (
	"context"
	"testing"
)

// TestClaimForRunnerSkipsHandoffScanWithoutDebt pins the poll path's handoff
// cost model. The handoff ledger is keyed by claim across the whole fleet, so
// the recovery scan reads every entry to find the ones a single runner owns.
// With no debt anywhere there is nothing to find, and the previous shape paid
// that fleet-wide read on every poll to learn it.
//
// The second half is the important half: the gate must not skip real debt. One
// unresolved handoff is produced through the public API and the very next poll
// has to return it, which is what makes the empty-ledger fast path safe.
func TestClaimForRunnerSkipsHandoffScanWithoutDebt(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := newLedgerHGetAllHook(directory.keys.handoffRunner)
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-handoff-gate", 1)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, redisDirectoryTestAssignment("exec-gate/redis/activation-1"))

	hook.reset()
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if claim.Handoff != nil {
		t.Fatalf("first claim = %#v, want an ordinary queue claim", claim)
	}
	if n := hook.count(); n != 0 {
		t.Fatalf("handoff ledger HGetAll reads for a debt-free poll = %d, want 0", n)
	}

	// Make the claim resolver-owned without settling it: that is exactly the
	// debt the gate must never skip over.
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}

	hook.reset()
	recovery, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || recovery.Handoff == nil || recovery.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("recovery = %#v, ok=%v, err=%v; want the unresolved handoff", recovery, ok, err)
	}
	if n := hook.count(); n == 0 {
		t.Fatal("handoff ledger HGetAll reads for a poll with real debt = 0; the gate skipped recoverable debt")
	}
}
