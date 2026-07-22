package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestMemoryReleaseExpiredLeaseTokenMismatchFailsClosed(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	lease, asgID := finalizeOneLeaseForTest(t, dir, ctx)

	out, err := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   engine.LeaseToken("wrong-token"),
	})
	if err != nil {
		t.Fatalf("wrong token returned error: %v", err)
	}
	if out == ExpiredDirectoryLeaseReleased {
		t.Fatalf("wrong token must not release; got out=%v", out)
	}

	out2, err2 := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err2 != nil || out2 != ExpiredDirectoryLeaseReleased {
		t.Fatalf("correct token must release; got out=%v err=%v", out2, err2)
	}

	out3, err3 := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err3 != nil || out3 != ExpiredDirectoryLeaseAlreadyReleased {
		t.Fatalf("repeat must be already_released; got out=%v err=%v", out3, err3)
	}
}

func TestRedisReleaseExpiredLeaseSameContract(t *testing.T) {
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	dir := NewRedisRunnerDirectory(rdb)
	ctx := context.Background()
	lease, asgID := finalizeOneLeaseForTestRedis(t, dir, ctx)

	out, err := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   engine.LeaseToken("wrong-token"),
	})
	if err != nil {
		t.Fatalf("wrong token returned error: %v", err)
	}
	if out == ExpiredDirectoryLeaseReleased {
		t.Fatalf("wrong token released")
	}

	out2, err2 := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err2 != nil || out2 != ExpiredDirectoryLeaseReleased {
		t.Fatalf("correct token: out=%v err=%v", out2, err2)
	}

	out3, err3 := dir.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: asgID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
	})
	if err3 != nil || out3 != ExpiredDirectoryLeaseAlreadyReleased {
		t.Fatalf("repeat: out=%v err=%v", out3, err3)
	}
}

func finalizeOneLeaseForTest(t *testing.T, dir *MemoryRunnerDirectory, ctx context.Context) (engine.TaskLease, AssignmentID) {
	t.Helper()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-reclaim",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	assignment := testAssignment("exec-reclaim/node-a/activation-1")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := engine.TaskLease{LeaseID: "lease-reclaim", LeaseToken: "token-reclaim"}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return lease, assignment.AssignmentID
}

func finalizeOneLeaseForTestRedis(t *testing.T, dir *RedisRunnerDirectory, ctx context.Context) (engine.TaskLease, AssignmentID) {
	t.Helper()

	session := registerRedisDirectoryRunner(t, ctx, dir, "runner-reclaim", 2)
	assignment := redisDirectoryTestAssignment("exec-reclaim/node-a/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, dir, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, dir, session, 2)
	lease := engine.TaskLease{LeaseID: "lease-reclaim", LeaseToken: "token-reclaim"}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return lease, assignment.AssignmentID
}
