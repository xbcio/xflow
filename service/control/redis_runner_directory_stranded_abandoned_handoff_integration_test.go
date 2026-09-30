package control

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisRunnerDirectoryRealRedisStrandedAbandonedHandoff drives the released-
// but-unsettled handoff record against a real Redis.
//
// Its miniredis twin builds the shape and settles it, which proves the fence and
// the intent; what it cannot prove is that the settle script behaves as written
// on the server that produced the shape. That script re-derives the claim from
// the assignment rather than trusting the caller's claimID, and the whole fix
// rests on it still resolving through handoffClaim after the release has deleted
// every other key the assignment owned — a resolution miniredis's Lua subset is
// not evidence for.
//
//	XFLOW_TEST_REDIS_ADDR=127.0.0.1:6380 go test ./service/control -run TestRedisRunnerDirectoryRealRedisStrandedAbandonedHandoff
func TestRedisRunnerDirectoryRealRedisStrandedAbandonedHandoff(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-directory test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	directory := newRealRedisRunnerDirectory(t, rdb)

	runner := registerRedisDirectoryRunnerAt(t, ctx, directory, "sas-host-abandoned", time.Now().UTC())
	assignment := redisDirectoryTestAssignment("exec-abandoned-real/node-sink/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-abandoned-real", time.Hour)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, runner, assignment, lease)
	assignmentID := string(assignment.AssignmentID)

	// The release both reclaim paths perform first, and the process dying before
	// the settle they both perform second.
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
	if _, err := rdb.HGet(ctx, directory.keys.handoffAssignment, assignmentID).Result(); err != nil {
		t.Fatalf("read reverse handoff index after the release: %v; want the record kept", err)
	}

	reap, err := directory.ReapStrandedLeases(ctx, 16)
	if err != nil {
		t.Fatalf("ReapStrandedLeases() error = %v", err)
	}
	if reap != (ReapResult{Inspected: 1, Released: 1}) {
		t.Fatalf("reap = %+v, want one inspected and one settled", reap)
	}
	if _, err := rdb.HGet(ctx, directory.keys.handoffAssignment, assignmentID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("reverse handoff index after the reap err=%v; want the record settled", err)
	}
}
