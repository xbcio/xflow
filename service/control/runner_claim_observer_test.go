package control

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

type runnerClaimObserverRecorder struct {
	reclaimed int
	replayed  int
}

func (r *runnerClaimObserverRecorder) OnRunnerClaimReclaimed(ctx context.Context, count int) {
	r.reclaimed += count
}

func (r *runnerClaimObserverRecorder) OnRunnerLeaseReplayed(ctx context.Context) {
	r.replayed++
}

func TestRedisRunnerDirectoryObserverTracksReclaimAndReplay(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	recorder := &runnerClaimObserverRecorder{}
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryObserver(recorder))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-observer", 1)
	assignment := redisDirectoryTestAssignment("exec-observer/node/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	first := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)

	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: string(first.ClaimID),
	}).Err(); err != nil {
		t.Fatalf("expire claim index entry: %v", err)
	}
	if err := directory.ReclaimExpiredClaims(ctx); err != nil {
		t.Fatalf("ReclaimExpiredClaims() error = %v", err)
	}
	if recorder.reclaimed != 1 {
		t.Fatalf("reclaimed observation = %d, want 1", recorder.reclaimed)
	}

	second := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	lease := &engine.TaskLease{LeaseID: "lease-observer", LeaseToken: "token-observer", Task: assignment.Task}
	if err := directory.FinalizeClaim(ctx, second.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	replay, ok, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 1))
	if err != nil || !ok || replay.Lease == nil {
		t.Fatalf("ClaimForRunner() replay=%+v ok=%v err=%v, want durable replay", replay, ok, err)
	}
	if recorder.replayed != 1 {
		t.Fatalf("lease replay observation = %d, want 1", recorder.replayed)
	}
}
