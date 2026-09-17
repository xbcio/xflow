package control

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
)

func TestRedisRunnerDirectoryFinalizeClaimStoresAssignmentLeaseMetaWithTTL(t *testing.T) {
	const claimTTL = 2 * time.Second

	for _, tc := range []struct {
		name      string
		leaseTTL  time.Duration
		exactTTL  time.Duration
		wantBound bool
	}{
		{
			name:     "lease ttl plus claim ttl",
			leaseTTL: 5 * time.Second,
			exactTTL: 7 * time.Second,
		},
		{
			name:      "zero lease ttl has finite fallback",
			leaseTTL:  0,
			wantBound: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 1)
			assignment := redisDirectoryTestAssignment(AssignmentID("exec-lease-meta/" + tc.name + "/activation-1"))
			lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-"+tc.name, tc.leaseTTL)

			finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

			key := directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
			if got := server.Type(key); got != "string" {
				t.Fatalf("metadata key type = %q, want string", got)
			}
			gotTTL := server.TTL(key)
			if tc.wantBound {
				if gotTTL <= 0 {
					t.Fatalf("zero-TTL lease metadata TTL = %s, want finite positive fallback", gotTTL)
				}
				return
			}
			if gotTTL != tc.exactTTL {
				t.Fatalf("metadata TTL = %s, want lease TTL + claim TTL = %s", gotTTL, tc.exactTTL)
			}
		})
	}
}

func TestRedisRunnerDirectoryAssignmentLeaseMetaKeysAndTTLsAreIndependent(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 2)
	first := redisDirectoryTestAssignment("exec-lease-meta/first/activation-1")
	second := redisDirectoryTestAssignment("exec-lease-meta/second/activation-1")
	firstLease := redisRunnerDirectoryLeaseMetaTestLease(first, "lease-first", 3*time.Second)
	secondLease := redisRunnerDirectoryLeaseMetaTestLease(second, "lease-second", 9*time.Second)

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, first)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, second)
	firstClaim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	secondClaim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
	if firstClaim.Assignment.AssignmentID != first.AssignmentID || secondClaim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("claims = %q, %q; want %q, %q", firstClaim.Assignment.AssignmentID, secondClaim.Assignment.AssignmentID, first.AssignmentID, second.AssignmentID)
	}
	if err := directory.FinalizeClaim(ctx, firstClaim.ClaimID, firstLease); err != nil {
		t.Fatalf("FinalizeClaim(first): %v", err)
	}
	if err := directory.FinalizeClaim(ctx, secondClaim.ClaimID, secondLease); err != nil {
		t.Fatalf("FinalizeClaim(second): %v", err)
	}

	firstKey := directory.keys.assignmentLeaseMetaKey(string(first.AssignmentID))
	secondKey := directory.keys.assignmentLeaseMetaKey(string(second.AssignmentID))
	if firstKey == secondKey {
		t.Fatalf("assignment metadata keys collide: %q", firstKey)
	}
	assertRedisRunnerDirectoryLeaseMetaTTL(t, server, firstKey, firstLease.TTL+claimTTL)
	assertRedisRunnerDirectoryLeaseMetaTTL(t, server, secondKey, secondLease.TTL+claimTTL)

	server.FastForward(firstLease.TTL + claimTTL)
	if server.Exists(firstKey) {
		t.Fatalf("first assignment metadata key %q survived its own TTL", firstKey)
	}
	if !server.Exists(secondKey) {
		t.Fatalf("second assignment metadata key %q expired with first assignment", secondKey)
	}
	if got, want := server.TTL(secondKey), secondLease.TTL-firstLease.TTL; got != want {
		t.Fatalf("second metadata TTL after first expiry = %s, want %s", got, want)
	}
}

func TestRedisRunnerDirectoryLeaseMetaExpirySkipsReplayAndLookup(t *testing.T) {
	const claimTTL = 2 * time.Second
	ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, claimTTL, 1)
	assignment := redisDirectoryTestAssignment("exec-lease-meta/expiry/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-expiry", 3*time.Second)

	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	key := directory.keys.assignmentLeaseMetaKey(string(assignment.AssignmentID))
	server.FastForward(lease.TTL + claimTTL)
	if server.Exists(key) {
		t.Fatalf("metadata key %q survived its TTL", key)
	}

	replay, replayed, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() after metadata expiry: %v", err)
	}
	if replayed {
		t.Fatalf("ClaimForRunner() after metadata expiry = %+v, ok=true; want no replay", replay)
	}

	got, ok, err := directory.LookupLease(ctx, session.RunnerID, session.SessionID, LeaseLookupKey{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
	})
	if err != nil {
		t.Fatalf("LookupLease() after metadata expiry: %v", err)
	}
	if ok || got != nil {
		t.Fatalf("LookupLease() after metadata expiry = %+v, ok=%v; want nil, false", got, ok)
	}
}

func TestRedisRunnerDirectoryLeaseMetaCleanup(t *testing.T) {
	t.Run("ReleaseLeased", func(t *testing.T) {
		ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
		assignment := redisDirectoryTestAssignment("exec-lease-meta/release-leased/activation-1")
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-release-leased", time.Second)
		finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

		if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID: session.RunnerID, SessionID: session.SessionID,
			AssignmentID: assignment.AssignmentID, LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken,
			RemoveSeen: true,
		}); err != nil {
			t.Fatalf("ReleaseLeased(): %v", err)
		}
		assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
	})

	t.Run("ReleaseExpiredLease", func(t *testing.T) {
		ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
		assignment := redisDirectoryTestAssignment("exec-lease-meta/release-expired/activation-1")
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-release-expired", time.Second)
		finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

		outcome, err := directory.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
			AssignmentID: assignment.AssignmentID,
			LeaseID:      lease.LeaseID,
			LeaseToken:   lease.LeaseToken,
		})
		if err != nil {
			t.Fatalf("ReleaseExpiredLease(): %v", err)
		}
		if outcome != ExpiredDirectoryLeaseReleased {
			t.Fatalf("ReleaseExpiredLease() outcome = %q, want %q", outcome, ExpiredDirectoryLeaseReleased)
		}
		assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
	})

	t.Run("ClearAssignment", func(t *testing.T) {
		ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
		assignment := redisDirectoryTestAssignment("exec-lease-meta/clear/activation-1")
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-clear", time.Second)
		finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

		if err := directory.ClearAssignment(ctx, assignment.AssignmentID); err != nil {
			t.Fatalf("ClearAssignment(): %v", err)
		}
		assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
	})

	t.Run("EnqueueReuse", func(t *testing.T) {
		ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
		assignment := redisDirectoryTestAssignment("exec-lease-meta/enqueue-reuse/activation-1")
		lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-enqueue-reuse", time.Second)
		finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
		if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID: session.RunnerID, SessionID: session.SessionID,
			AssignmentID: assignment.AssignmentID, LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken,
			RemoveSeen: false,
		}); err != nil {
			t.Fatalf("ReleaseLeased(): %v", err)
		}
		injectRedisRunnerDirectoryLeaseMeta(t, server, directory, assignment.AssignmentID)

		enqueued, err := directory.EnqueueAssignment(ctx, assignment)
		if err != nil {
			t.Fatalf("EnqueueAssignment() reuse: %v", err)
		}
		if !enqueued {
			t.Fatal("EnqueueAssignment() reuse enqueued=false, want stale metadata cleanup and reuse")
		}
		assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
	})

	for _, reason := range []ReleaseClaimReason{ReleaseClaimRequeue, ReleaseClaimDrop} {
		reason := reason
		t.Run("ReleaseClaim/"+string(reason), func(t *testing.T) {
			ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
			assignment := redisDirectoryTestAssignment(AssignmentID("exec-lease-meta/release-claim/" + string(reason) + "/activation-1"))
			mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
			claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
			injectRedisRunnerDirectoryLeaseMeta(t, server, directory, assignment.AssignmentID)

			if err := directory.ReleaseClaim(ctx, claim.ClaimID, reason); err != nil {
				t.Fatalf("ReleaseClaim(%q): %v", reason, err)
			}
			assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
		})
	}

	for _, disposition := range []HandoffDisposition{HandoffDispositionRequeue, HandoffDispositionDrop} {
		disposition := disposition
		t.Run("SettleClaimHandoff/"+string(disposition), func(t *testing.T) {
			ctx, server, directory, session := newRedisRunnerDirectoryLeaseMetaTestDirectory(t, time.Second, 1)
			assignment := redisDirectoryTestAssignment(AssignmentID("exec-lease-meta/settle-handoff/" + string(disposition) + "/activation-1"))
			mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
			claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
			if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
				t.Fatalf("MarkClaimLeaseMayExist(): %v", err)
			}
			if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
				t.Fatalf("MakeClaimHandoffRecoverable(): %v", err)
			}
			injectRedisRunnerDirectoryLeaseMeta(t, server, directory, assignment.AssignmentID)

			if err := directory.SettleClaimHandoff(ctx, claim.ClaimID, disposition); err != nil {
				t.Fatalf("SettleClaimHandoff(%q): %v", disposition, err)
			}
			assertRedisRunnerDirectoryLeaseMetaMissing(t, server, directory, assignment.AssignmentID)
		})
	}
}

func newRedisRunnerDirectoryLeaseMetaTestDirectory(t *testing.T, claimTTL time.Duration, capacity int) (context.Context, *miniredis.Miniredis, *RedisRunnerDirectory, RunnerSession) {
	t.Helper()

	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(claimTTL))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-lease-meta", capacity)
	return ctx, server, directory, session
}

func redisRunnerDirectoryLeaseMetaTestLease(assignment Assignment, leaseID string, ttl time.Duration) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID(leaseID),
		LeaseToken: engine.LeaseToken("token-" + leaseID),
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
		IssuedAt:   time.Unix(100, 0).UTC(),
		TTL:        ttl,
	}
}

func finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession, assignment Assignment, lease *engine.TaskLease) {
	t.Helper()

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim(): %v", err)
	}
}

func assertRedisRunnerDirectoryLeaseMetaTTL(t *testing.T, server *miniredis.Miniredis, key string, want time.Duration) {
	t.Helper()

	if got := server.Type(key); got != "string" {
		t.Fatalf("metadata key type = %q, want string", got)
	}
	if got := server.TTL(key); got != want {
		t.Fatalf("metadata key TTL = %s, want %s", got, want)
	}
}

func injectRedisRunnerDirectoryLeaseMeta(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, assignmentID AssignmentID) {
	t.Helper()

	key := directory.keys.assignmentLeaseMetaKey(string(assignmentID))
	if err := server.Set(key, "stale lease metadata"); err != nil {
		t.Fatalf("inject stale lease metadata: %v", err)
	}
	if !server.Exists(key) {
		t.Fatalf("injected stale lease metadata key %q does not exist", key)
	}
}

func assertRedisRunnerDirectoryLeaseMetaMissing(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, assignmentID AssignmentID) {
	t.Helper()

	key := directory.keys.assignmentLeaseMetaKey(string(assignmentID))
	if server.Exists(key) {
		t.Fatalf("stale lease metadata key %q still exists", key)
	}
}
