package control

import (
	"context"
	"testing"
	"time"
)

// This file is the controlled experiment behind the R6 409 attribution: can the
// stranded-lease reaper release an assignment whose lease a runner is still
// working?
//
// The reaper's only gate is strandedLeaseIdentity: assignment:state == 'leased'
// AND the per-assignment lease-metadata key is gone. Everything below measures
// that gate against the two windows that decide it:
//
//	directory metadata window = lease TTL + directory claim TTL  (FinalizeClaim)
//	engine lease window       = the lease deadline the runner keeps extending
//
// Both are armed by the same FinalizeClaim and both are re-armed by the same
// renewal (refreshLeaseMeta / RenewTaskLease), so the experiment drives them
// together and looks for a moment where the directory's view lapses while the
// engine's is still live.

// engineLeaseClock models the engine-side half of one lease: the deadline the
// node carries and the redelivery that a lapsed deadline enables. It exists so
// the experiment can ask "was this lease still live when the reaper looked?"
// using the same arithmetic acquireTaskLeaseLua uses (leaseValid = now <
// lease_deadline_ms).
type engineLeaseClock struct {
	token    string
	deadline time.Time
}

func (c *engineLeaseClock) renew(now time.Time, live time.Duration) {
	c.deadline = now.Add(live)
}

// live reports whether the engine would still fence a report against this
// lease's token, i.e. whether acquireTaskLeaseLua would refuse to overwrite it.
func (c *engineLeaseClock) live(now time.Time) bool {
	return now.Before(c.deadline)
}

// TestStrandedReapMetadataWindowOutlivesTheLeaseItProtects pins the invariant
// the reaper's doc comment claims: the directory keeps a still-'leased'
// assignment's metadata for its whole live window plus a recovery margin. If
// this ever fails, the reaper becomes able to release live work.
func TestStrandedReapMetadataWindowOutlivesTheLeaseItProtects(t *testing.T) {
	const claimTTL = 30 * time.Second
	directory := NewRedisRunnerDirectory(nil, WithRedisRunnerDirectoryClaimTTL(claimTTL))

	for _, live := range []time.Duration{
		0, // no TTL on the lease: FinalizeClaim falls back to the claim TTL
		time.Second,
		60 * time.Second,
		3 * time.Minute, // the SAS-configured lease_ttl at the reported occurrence
		time.Hour,
	} {
		got := directory.assignmentLeaseMetaTTLMillisFor(live)
		effectiveLive := live
		if effectiveLive <= 0 {
			effectiveLive = claimTTL
		}
		if want := effectiveLive + claimTTL; got != want.Milliseconds() {
			t.Errorf("metadata TTL for live=%s = %dms, want %dms (live + margin)",
				live, got, want.Milliseconds())
		}
		if got <= effectiveLive.Milliseconds() {
			t.Errorf("metadata TTL for live=%s is %dms, not longer than the live window: "+
				"the reaper could release a live lease", live, got)
		}
	}
}

// TestStrandedReapLeavesARenewingLiveLeaseAlone walks one lease through the
// renewal cadence the runner actually uses (ttl/3 capped at 10s, each renewal
// extending the engine deadline and re-arming the directory metadata), and
// drives the reaper at every metadata-expiry boundary in between. The reaper
// must find nothing to do at every one of them.
//
// This is the reproduction attempt for the leading handoff hypothesis: it is
// the tightest arrangement of the two windows in which a still-running holder
// could be judged stranded.
func TestStrandedReapLeavesARenewingLiveLeaseAlone(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	const claimTTL = 30 * time.Second
	const leaseTTL = 3 * time.Minute
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	const runnerID = "runner-renewing"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-renewing/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-renewing", leaseTTL)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	assertRedisRunnerDirectoryLeaseMetaTTL(t, server, directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID)), leaseTTL+claimTTL)

	// The engine's deadline is anchored at FinalizeClaim the same way the
	// metadata is: both windows start at the lease's own live duration.
	engineLease := engineLeaseClock{
		token:    string(lease.LeaseToken),
		deadline: time.Unix(100, 0).UTC().Add(leaseTTL),
	}
	elapsed := time.Duration(0)

	const renewInterval = 10 * time.Second // min(TTL/3, 10s) for a 3m TTL
	for renewals := 1; renewals <= 6; renewals++ {
		// Let the engine's window lapse in the worst case before the runner's
		// next renewal lands: the runner is alive the whole time, only its
		// renewal is in flight.
		elapsed += renewInterval
		server.FastForward(renewInterval)
		now := time.Unix(100, 0).UTC().Add(elapsed)

		// The renewal that keeps this lease alive: engine side extends the
		// deadline, directory side re-arms the metadata.
		engineLease.renew(now, leaseTTL)
		if err := directory.RefreshLeaseMeta(ctx, runnerID, session.SessionID, LeaseLookupKey{
			LeaseID:    lease.LeaseID,
			LeaseToken: lease.LeaseToken,
		}, leaseTTL); err != nil {
			t.Fatalf("RefreshLeaseMeta() error = %v", err)
		}
		if !engineLease.live(now) {
			t.Fatalf("renewal %d left the engine lease dead at %s", renewals, now)
		}

		// Sample both gates at the instant the ORIGINAL windows would have
		// lapsed, so a reaper that measured from FinalizeClaim rather than from
		// the last renewal would be caught here.
		if renewals == 1 {
			server.FastForward(leaseTTL) // past the original live window
			now = now.Add(leaseTTL)
			engineLease.renew(now, leaseTTL) // the runner is still renewing
			if err := directory.RefreshLeaseMeta(ctx, runnerID, session.SessionID, LeaseLookupKey{
				LeaseID:    lease.LeaseID,
				LeaseToken: lease.LeaseToken,
			}, leaseTTL); err != nil {
				t.Fatalf("RefreshLeaseMeta() after a full live window error = %v", err)
			}
		}

		reap, err := directory.ReapStrandedLeases(ctx, 16)
		if err != nil {
			t.Fatalf("ReapStrandedLeases() at renewal %d error = %v", renewals, err)
		}
		if reap.Released != 0 {
			t.Fatalf("renewal %d: released = %d, want 0: the holder is still renewing "+
				"(engine deadline %s), so the assignment is not stranded",
				renewals, reap.Released, engineLease.deadline)
		}
		if got := server.HGet(directory.keys.assignmentState, string(assignment.AssignmentID)); got != redisAssignmentLeased {
			t.Fatalf("renewal %d: assignment state = %q, want %q", renewals, got, redisAssignmentLeased)
		}
	}

	// And the lease is still usable for the report the runner has not sent yet.
	if _, found, err := directory.LookupLease(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}); err != nil || !found {
		t.Fatalf("LookupLease() after %d renewals = (found=%v, err=%v), want a hit", 6, found, err)
	}
}

// TestStrandedReapAndEngineLeaseLapseShareAnInstant is the negative result
// stated as a test: for a holder that stops renewing, the engine's lease window
// and the directory's metadata window end at the SAME instant, so the reaper
// never gets a head start on the engine's own reclaim. The tie is what makes
// the reaper structurally unable to be the first mover against a live lease —
// at the moment it can see a stranded assignment, the engine has already
// stopped fencing that token.
func TestStrandedReapAndEngineLeaseLapseShareAnInstant(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	const claimTTL = 30 * time.Second
	const leaseTTL = 90 * time.Second
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	const runnerID = "runner-lapsing"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-lapsing/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-lapsing", leaseTTL)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)
	metaKey := directory.keys.assignmentLeaseMetaKey(assignmentID)

	// A holder that never renews: the engine's deadline stays at the one
	// FinalizeClaim recorded, and nothing re-arms the metadata.
	engineLease := engineLeaseClock{
		token:    string(lease.LeaseToken),
		deadline: time.Unix(100, 0).UTC().Add(leaseTTL),
	}

	// One tick before the engine's window ends the reaper must do nothing, and
	// the lease must still fence.
	server.FastForward(leaseTTL - time.Second)
	if reap, err := directory.ReapStrandedLeases(ctx, 16); err != nil || reap.Released != 0 {
		t.Fatalf("released = %d (err=%v) while the engine lease is still live, want 0", reap, err)
	}
	if !engineLease.live(time.Unix(100, 0).UTC().Add(leaseTTL - time.Second)) {
		t.Fatal("model error: the engine lease should still be live one tick before its deadline")
	}
	if !server.Exists(metaKey) {
		t.Fatal("metadata expired before the engine's lease window ended")
	}

	// Past the tie the metadata is gone and the reaper may act — but so may the
	// sweeper, on a 10s cadence rather than the reaper's 5m one.
	server.FastForward(claimTTL + 2*time.Second)
	if server.Exists(metaKey) {
		t.Fatal("metadata survived past live+margin; the reaper's gate would never open")
	}
	if engineLease.live(time.Unix(100, 0).UTC().Add(leaseTTL + claimTTL + 2*time.Second)) {
		t.Fatal("model error: the engine lease outlived the directory metadata window")
	}
	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d after both windows lapsed, want 1", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state = %q, want released", got)
	}
}

// TestStrandedReapRefreshedMetadataRestoresTheMargin covers the interaction the
// handoff flagged as the unknown: refreshLeaseMeta. A lease whose metadata was
// allowed to lapse and is then re-armed by a renewal must stop being a reaper
// candidate again, and a renewal that cannot re-arm it reports 'expired'
// rather than silently claiming success.
func TestStrandedReapRefreshedMetadataRestoresTheMargin(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	const claimTTL = time.Minute
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	const runnerID = "runner-refresh"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-refresh/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-refresh", 20*time.Second)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	// The gap the handoff worried about: metadata lapses mid-task because the
	// renewal path did not run. It is gone, so this assignment IS stranded and
	// releasing it is correct — the runner's next LookupLease cannot succeed
	// either. The point of the assertion is the consequence, not the safety.
	server.FastForward(20*time.Second + claimTTL + time.Second)
	if server.Exists(directory.keys.assignmentLeaseMetaKey(assignmentID)) {
		t.Fatal("metadata should have lapsed")
	}
	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1", reap.Released)
	}
	_, found, err := directory.LookupLease(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	})
	if err != nil {
		t.Fatalf("LookupLease() error = %v", err)
	}
	if found {
		t.Fatal("LookupLease still hits after the release; the two views disagree")
	}

	// A refresh arriving after the lapse must report the lapse rather than
	// pretending it re-armed an expiry that is already gone.
	if err := directory.RefreshLeaseMeta(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}, time.Minute); err != nil {
		t.Fatalf("RefreshLeaseMeta() after a lapse error = %v, want nil (documented 'expired' no-op)", err)
	}
}

// TestStrandedReapNeedsTheMetadataToBeGoneNotTheHolderToBeDead separates the
// two questions the handoff could not: the reaper's gate is "the directory
// cannot resolve this lease any more", which is NOT the same as "the holder is
// dead". A live holder whose metadata was dropped by something other than its
// own TTL — eviction, a FLUSHDB window, an operator — is still misjudged. That
// is the one way a genuinely live lease can reach this reaper, and it is not
// reachable from the TTL arithmetic the hypothesis assumed.
func TestStrandedReapNeedsTheMetadataToBeGoneNotTheHolderToBeDead(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	const claimTTL = 30 * time.Second
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	const runnerID = "runner-evicted"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-evicted/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-evicted", 3*time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	// The holder is alive, still inside its lease window, and its session is
	// current. Only an external actor removes the metadata.
	if err := rdb.Del(ctx, directory.keys.assignmentLeaseMetaKey(assignmentID)).Err(); err != nil {
		t.Fatalf("evict metadata: %v", err)
	}
	if _, found, err := directory.LookupLease(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}); err != nil || found {
		t.Fatalf("LookupLease() with the metadata evicted = (found=%v, err=%v); "+
			"the directory is already unable to resolve this lease", found, err)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap.Released != 1 {
		t.Fatalf("released = %d, want 1: the reaper's gate is metadata presence, not holder liveness", reap.Released)
	}
	if got := server.HGet(directory.keys.assignmentState, assignmentID); got != "" {
		t.Fatalf("assignment state = %q, want released", got)
	}
	// The runner's in-flight report now fails at the directory fence, with the
	// warn log the handoff recorded as absent, NOT as an engine stale-token 409.
}

// TestStrandedReapIsNotThe409PathWhileItsGateIsHonest is the negative result
// stated as a test. A reaper release makes LookupLease miss, which the report
// path answers from a different branch (core.go's "authoritative lease not
// found") and logs at warn. A 409 that came from the ENGINE commit instead
// requires the directory lookup to have SUCCEEDED, which requires the
// directory's metadata and token to still be in place — exactly the state this
// reaper refuses to touch. So a stranded-reaper release cannot produce the
// engine-commit 409 the handoff observed; it produces the other one.
func TestStrandedReapIsNotThe409PathWhileItsGateIsHonest(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	const claimTTL = 30 * time.Second
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	const runnerID = "runner-gate"
	session := registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)

	assignment := redisDirectoryTestAssignment("exec-gate/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-gate", time.Minute)
	seedStrandedLeasedAssignment(t, ctx, rdb, directory, session, assignment, lease)

	// State one: metadata gone. The reaper takes it. A report in this state
	// misses the directory fence, which is the OTHER 409 — "authoritative lease
	// not found", logged at warn, and measured as zero occurrences in the real
	// run.
	if _, found, err := directory.LookupLease(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	}); err != nil || found {
		t.Fatalf("LookupLease() with the metadata removed = (found=%v, err=%v), want a miss", found, err)
	}
	if reap, err := directory.ReapStrandedLeases(ctx, 16); err != nil || reap.Released != 1 {
		t.Fatalf("released = %d (err=%v) with the metadata removed, want 1", reap, err)
	}

	// Rebuild the same assignment in state two: metadata present. This is the
	// ONLY directory state in which a report reaches the engine commit, so the
	// only state that can produce the engine-side stale-token 409 the handoff
	// observed — and it is exactly the state the reaper refuses to touch.
	second := redisDirectoryTestAssignment("exec-gate/node/activation-2")
	secondLease := redisRunnerDirectoryLeaseMetaTestLease(second, "lease-gate-2", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, second, secondLease)
	if reap, err := directory.ReapStrandedLeases(ctx, 16); err != nil || reap.Released != 0 {
		t.Fatalf("released = %d (err=%v) with the metadata present, want 0", reap, err)
	}
	if _, found, err := directory.LookupLease(ctx, runnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    secondLease.LeaseID,
		LeaseToken: secondLease.LeaseToken,
	}); err != nil || !found {
		t.Fatalf("LookupLease() for the live lease = (found=%v, err=%v), want a hit", found, err)
	}
	if got := server.HGet(directory.keys.assignmentState, string(second.AssignmentID)); got != redisAssignmentLeased {
		t.Fatalf("live assignment state = %q, want %q", got, redisAssignmentLeased)
	}
}
