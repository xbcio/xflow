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
//	XFLOW_REDIS_ADDR=127.0.0.1:6379 go test ./service/control -run TestRedisRunnerDirectoryRealRedisDurableHandoff
func TestRedisRunnerDirectoryRealRedisDurableHandoff(t *testing.T) {
	addr := os.Getenv("XFLOW_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_REDIS_ADDR is required for the real Redis runner-directory test")
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
		_ = rdb.Del(context.Background(), redisRunnerDirectoryAllKeys(directory.keys)...).Err()
		_ = rdb.Close()
	})
	return directory
}

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
		keys.assignmentLeaseMeta,
		keys.claimsAssignment,
		keys.claimsRunner,
		keys.claimsSession,
		keys.claimsExpiry,
		keys.runnerSession,
		keys.runnerCapacity,
		keys.runnerInflight,
		keys.runnerCapabilities,
		keys.runnerPolicy,
		keys.runnerHeartbeat,
		keys.runnerClaimCount,
		keys.runnerLeaseCount,
		keys.leaseByID,
		keys.leaseByToken,
	}
}
