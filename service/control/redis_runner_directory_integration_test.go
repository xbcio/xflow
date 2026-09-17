package control

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestRedisRunnerDirectoryRealRedisDurableHandoff exercises the Lua scripts
// against a real Redis server. It is opt-in because CI and local unit test
// runs do not require a Redis daemon:
//
//	XFLOW_TEST_REDIS_ADDR=127.0.0.1:6380 go test ./service/control -run TestRedisRunnerDirectoryRealRedisDurableHandoff
//
// The variable used to be XFLOW_REDIS_ADDR, which is the production runtime's
// (cmd/server, cmd/runner) and is set by no harness in this repo. This test —
// the only coverage the runner-directory Lua has against a real Redis — has
// therefore never executed, on any machine or CI run, while reporting green.
func TestRedisRunnerDirectoryRealRedisDurableHandoff(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-directory test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	directory := newRealRedisRunnerDirectory(t, rdb)
	firstSession := registerRedisDirectoryRunner(t, ctx, directory, "runner-real", 1)

	firstAssignment := redisDirectoryTestAssignment("exec-real/node-replay/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, firstAssignment)
	firstClaim := claimRedisDirectoryAssignment(t, ctx, directory, firstSession, 1)
	lease := &engine.TaskLease{
		LeaseID:     "lease-real",
		LeaseToken:  "token-real",
		Attempt:     2,
		Task:        firstAssignment.Task,
		Input:       &types.Input{Data: map[string]any{"request_id": "real-redis"}},
		NodeType:    firstAssignment.Routing.NodeType,
		NodeVersion: firstAssignment.Routing.NodeVersion,
		IssuedAt:    time.Now().UTC(),
		TTL:         time.Minute,
	}
	if err := directory.FinalizeClaim(ctx, firstClaim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	recreated := &RedisRunnerDirectory{
		rdb:      rdb,
		claimTTL: directory.claimTTL,
		keys:     directory.keys,
	}
	replay := mustReplayRedisDirectoryLease(t, ctx, recreated, firstSession)
	if replay.Lease.LeaseToken != lease.LeaseToken || replay.Lease.Input.Data["request_id"] != "real-redis" {
		t.Fatalf("first-session replay = %+v, want complete durable lease", replay.Lease)
	}

	secondSession := registerRedisDirectoryRunner(t, ctx, recreated, firstSession.RunnerID, 1)
	if secondSession.SessionID == firstSession.SessionID {
		t.Fatal("re-registration reused the prior session ID")
	}
	replay = mustReplayRedisDirectoryLease(t, ctx, recreated, secondSession)
	if replay.Lease.LeaseID != lease.LeaseID || replay.Lease.LeaseToken != lease.LeaseToken {
		t.Fatalf("re-registered replay lease = %+v, want %q/%q", replay.Lease, lease.LeaseID, lease.LeaseToken)
	}

	if err := recreated.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     secondSession.RunnerID,
		SessionID:    secondSession.SessionID,
		AssignmentID: firstAssignment.AssignmentID,
		LeaseID:      "stale-lease",
		LeaseToken:   "stale-token",
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("stale ReleaseLeased() error = %v", err)
	}
	mustReplayRedisDirectoryLease(t, ctx, recreated, secondSession)

	if err := recreated.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     secondSession.RunnerID,
		SessionID:    secondSession.SessionID,
		AssignmentID: firstAssignment.AssignmentID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("current ReleaseLeased() error = %v", err)
	}

	expiringAssignment := redisDirectoryTestAssignment("exec-real/node-expiry/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, recreated, expiringAssignment)
	expiringClaim := claimRedisDirectoryAssignment(t, ctx, recreated, secondSession, 1)
	if err := rdb.ZAdd(ctx, recreated.keys.claimsExpiry, redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(expiringClaim.ClaimID),
	}).Err(); err != nil {
		t.Fatalf("expire claim score: %v", err)
	}
	if err := recreated.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims() error = %v", err)
	}
	reclaimed := claimRedisDirectoryAssignment(t, ctx, recreated, secondSession, 1)
	if reclaimed.Assignment.AssignmentID != expiringAssignment.AssignmentID {
		t.Fatalf("reclaimed assignment = %q, want %q", reclaimed.Assignment.AssignmentID, expiringAssignment.AssignmentID)
	}
	if reclaimed.ClaimID == expiringClaim.ClaimID {
		t.Fatalf("reclaimed claim ID = %q, want a fresh claim", reclaimed.ClaimID)
	}
}

func mustReplayRedisDirectoryLease(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession) Claim {
	t.Helper()

	replay, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil {
		t.Fatalf("ClaimForRunner() replay error = %v", err)
	}
	if !ok || replay.Lease == nil || replay.ClaimID != "" {
		t.Fatalf("ClaimForRunner() replay = %+v, ok=%v; want finalized durable lease", replay, ok)
	}
	return replay
}

func newRealRedisRunnerDirectory(t *testing.T, rdb *redis.Client) *RedisRunnerDirectory {
	t.Helper()

	prefix := fmt.Sprintf("xflow:runner-directory:{control-real-%s}", uuid.NewString())
	directory := &RedisRunnerDirectory{
		rdb:      rdb,
		claimTTL: defaultRedisRunnerDirectoryClaimTTL,
		keys:     newRedisRunnerDirectoryKeys(prefix),
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if err := rdb.Del(ctx, redisRunnerDirectoryAllKeys(directory.keys)...).Err(); err != nil {
			t.Errorf("cleanup fixed Redis runner-directory keys: %v", err)
		}
		if err := cleanupRedisRunnerDirectoryAssignmentLeaseMeta(ctx, rdb, directory.keys); err != nil {
			t.Errorf("cleanup dynamic Redis runner-directory lease metadata: %v", err)
		}
		_ = rdb.Close()
	})
	return directory
}

// redisRunnerDirectoryAllKeys lists the directory's fixed keys for real-Redis
// test teardown. Assignment-scoped lease metadata is stored as separate
// TTL-backed keys and is removed by cleanupRedisRunnerDirectoryAssignmentLeaseMeta.
// The fixed keys are not uniformly TTL-bound, so a key missing from this list
// can still remain in Redis after the test that made it has gone.
// TestRedisRunnerDirectoryCleanupDeletesEveryKeyItCreates holds this list to
// the key struct, which is how runnerLabels and runnerNamespaces were found
// missing.
func redisRunnerDirectoryAllKeys(keys redisRunnerDirectoryKeys) []string {
	return []string{
		keys.queue,
		keys.seen,
		keys.assignmentData,
		keys.assignmentState,
		keys.assignmentClaim,
		keys.assignmentRunner,
		keys.assignmentSession,
		keys.assignmentLeaseID,
		keys.assignmentLeaseToken,
		keys.claimsAssignment,
		keys.claimsRunner,
		keys.claimsSession,
		keys.claimsExpiry,
		keys.runnerSession,
		keys.runnerCapacity,
		keys.runnerInflight,
		keys.runnerCapabilities,
		keys.runnerLabels,
		keys.runnerPolicy,
		keys.runnerNamespaces,
		keys.runnerHeartbeat,
		keys.runnerClaimCount,
		keys.runnerLeaseCount,
		keys.leaseByID,
		keys.leaseByToken,
		keys.runnerControlDesired,
		keys.runnerControlGeneration,
		keys.runnerControlRequestedAt,
		keys.runnerControlActor,
		keys.runnerControlReason,
		keys.runnerControlDrainDeadline,
		keys.runnerControlReceiptHash,
		keys.runnerControlReceiptDesired,
		keys.runnerControlReceiptGeneration,
		keys.runnerControlReceiptRequestedAt,
		keys.runnerControlReceiptReason,
		keys.runnerControlReceiptClaims,
		keys.runnerControlReceiptLeases,
		keys.runnerControlReceiptUnsettledDebt,
		keys.runnerControlReceiptHandoffDebt,
		keys.runnerControlReceiptLeaseMayExistDebt,
		keys.runnerControlReceiptReplayableDebt,
		keys.runnerControlReceiptPendingActivationCleanup,
		keys.runnerControlReceiptDrainDeadline,
		keys.runnerControlReceiptStoredAt,
		keys.runnerControlReceiptStatus,
		keys.runnerControlReceiptExpiry,
		keys.runnerControlAudit,
		keys.deactivationObligationState,
		keys.deactivationObligationRunner,
		keys.deactivationObligationSession,
		keys.deactivationObligationNamespace,
		keys.deactivationObligationWorkflowID,
		keys.deactivationObligationWorkflowVersion,
		keys.deactivationObligationEntryUnitID,
		keys.deactivationObligationReplicaIndex,
		keys.deactivationObligationGeneration,
		keys.deactivationObligationDrainGeneration,
		keys.runnerActivationInventory,
		keys.runnerDrainObservation,
		keys.handoffState,
		keys.handoffGeneration,
		keys.handoffLeaseMeta,
		keys.handoffClaim,
		keys.handoffAssignment,
		keys.handoffRunner,
		keys.handoffSession,
		keys.handoffLeaseID,
		keys.handoffLeaseToken,
		keys.handoffRecoveryReady,
		keys.handoffRecoveryDeadline,
	}
}

func TestCleanupRedisRunnerDirectoryAssignmentLeaseMeta(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	// The literal asterisk makes the SCAN pattern broader than the namespace;
	// the helper must keep only keys that have the exact string prefix.
	keys := newRedisRunnerDirectoryKeys("xflow:runner-directory:{lease-meta-cleanup}*")
	dynamic := []string{
		keys.assignmentLeaseMetaKey("assignment-a"),
		keys.assignmentLeaseMetaKey("assignment-b"),
	}
	for _, key := range dynamic {
		if err := rdb.Set(ctx, key, "lease", 0).Err(); err != nil {
			t.Fatalf("seed dynamic lease metadata %q: %v", key, err)
		}
	}
	outsidePrefix := "xflow:runner-directory:{lease-meta-cleanup}other:assignment:lease-meta:keep"
	if err := rdb.Set(ctx, outsidePrefix, "keep", 0).Err(); err != nil {
		t.Fatalf("seed broad-pattern non-member %q: %v", outsidePrefix, err)
	}

	if err := cleanupRedisRunnerDirectoryAssignmentLeaseMeta(ctx, rdb, keys); err != nil {
		t.Fatalf("cleanupRedisRunnerDirectoryAssignmentLeaseMeta() error = %v", err)
	}
	for _, key := range dynamic {
		exists, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("exists dynamic lease metadata %q: %v", key, err)
		}
		if exists != 0 {
			t.Errorf("dynamic lease metadata %q still exists after cleanup", key)
		}
	}
	exists, err := rdb.Exists(ctx, outsidePrefix).Result()
	if err != nil {
		t.Fatalf("exists broad-pattern non-member %q: %v", outsidePrefix, err)
	}
	if exists != 1 {
		t.Errorf("cleanup deleted non-member %q", outsidePrefix)
	}
}

// cleanupRedisRunnerDirectoryAssignmentLeaseMeta removes the assignment-scoped
// lease metadata keys that cannot appear in redisRunnerDirectoryAllKeys. SCAN
// keeps cleanup incremental, and the literal-prefix check prevents a glob-like
// prefix from broadening the deletion set.
func cleanupRedisRunnerDirectoryAssignmentLeaseMeta(ctx context.Context, rdb *redis.Client, keys redisRunnerDirectoryKeys) error {
	prefix := keys.prefix + ":assignment:lease-meta:"
	pattern := prefix + "*"
	var cursor uint64
	for {
		found, next, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("scan assignment lease metadata keys: %w", err)
		}
		deleteKeys := make([]string, 0, len(found))
		for _, key := range found {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				deleteKeys = append(deleteKeys, key)
			}
		}
		if len(deleteKeys) > 0 {
			if err := rdb.Del(ctx, deleteKeys...).Err(); err != nil {
				return fmt.Errorf("delete assignment lease metadata keys: %w", err)
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
