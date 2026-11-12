package rstate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// forEachLeaseIndexBackend runs fn against miniredis and, when
// XFLOW_TEST_REDIS_ADDR is set, against that real Redis. The prune path pairs a
// ZRANGEBYSCORE LIMIT with pipelined single-key EXISTS, so it is worth pinning
// against a real server while keeping the package runnable with no dependency.
func forEachLeaseIndexBackend(t *testing.T, fn func(t *testing.T, s *Store, rdb *redis.Client)) {
	t.Helper()
	t.Run("miniredis", func(t *testing.T) {
		state, _, rdb := newTestRedisState(t)
		fn(t, state, rdb)
	})
	t.Run("redis", func(t *testing.T) {
		addr := realRedisAddr(t)
		rdb := freshRealRedis(t, addr)
		fn(t, New(rdb, nil, time.Minute), rdb)
	})
}

// seedLeaseIndexOrphans writes count bare index members with the given score.
// No per-execution lease ZSET is created, so every one of them is an orphan.
func seedLeaseIndexOrphans(t *testing.T, rdb *redis.Client, count int, score float64) []types.ExecutionID {
	t.Helper()
	ctx := context.Background()
	ids := make([]types.ExecutionID, count)
	pipe := rdb.Pipeline()
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("orphan-%05d", i))
		pipe.ZAdd(ctx, leaseExpiryExecIndexKey(namespace.Default), redis.Z{Score: score, Member: string(ids[i])})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed %d orphan index members: %v", count, err)
	}
	return ids
}

func requireLeaseIndexCard(t *testing.T, rdb *redis.Client, want int64) {
	t.Helper()
	got, err := rdb.ZCard(context.Background(), leaseExpiryExecIndexKey(namespace.Default)).Result()
	if err != nil {
		t.Fatalf("ZCard lease index: %v", err)
	}
	if got != want {
		t.Fatalf("lease index cardinality = %d, want %d", got, want)
	}
}

// TestListExpiredLeasesPrunesOrphanLeaseIndex pins the bulk orphan prune: a
// single sweep must clear thousands of orphan members that the bounded reclaim
// read could never reach (it only ever examines leaseIndexBatchLimit members),
// while still returning and keeping the real expired leases.
func TestListExpiredLeasesPrunesOrphanLeaseIndex(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		const live = 3
		// Real expired leases score older than the orphans, so they sit at the
		// head of the index and are reachable even by the unfixed sweep.
		base := time.Now().Add(-time.Hour).UTC()
		liveIDs := make(map[types.ExecutionID]struct{}, live)
		for i := 0; i < live; i++ {
			id := types.ExecutionID(fmt.Sprintf("live-%d", i))
			liveIDs[id] = struct{}{}
			mustUpsertRunning(t, state, id, "n", fmt.Sprintf("tok-live-%d", i),
				base.Add(time.Duration(i)*time.Second), time.Second)
		}
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		if len(got) != live {
			t.Fatalf("returned %d expired leases, want the %d real ones", len(got), live)
		}
		for _, lease := range got {
			if _, ok := liveIDs[lease.ExecutionID]; !ok {
				t.Fatalf("returned orphan lease %q", lease.ExecutionID)
			}
		}
		requireLeaseIndexCard(t, rdb, live)
		for id := range liveIDs {
			if _, ok := leaseIndexScore(t, rdb, id); !ok {
				t.Fatalf("live member %q was pruned", id)
			}
		}
	})
}

// TestLeaseIndexPruneReachesOrphansBehindSaturatedHead pins the Offset=kept
// rule: a head full of real expired leases (more than one chunk, more than the
// reclaim budget) must not stop the prune from reaching the orphans after them.
func TestLeaseIndexPruneReachesOrphansBehindSaturatedHead(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		const live = 300
		base := time.Now().Add(-time.Hour).UTC()
		liveIDs := make(map[types.ExecutionID]struct{}, live)
		for i := 0; i < live; i++ {
			id := types.ExecutionID(fmt.Sprintf("live-%03d", i))
			liveIDs[id] = struct{}{}
			mustUpsertRunning(t, state, id, "n", fmt.Sprintf("tok-live-%03d", i),
				base.Add(time.Duration(i)*time.Second), time.Second)
		}
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		if len(got) != leaseIndexBatchLimit {
			t.Fatalf("returned %d expired leases, want the %d-member reclaim budget",
				len(got), leaseIndexBatchLimit)
		}
		for _, lease := range got {
			if _, ok := liveIDs[lease.ExecutionID]; !ok {
				t.Fatalf("returned orphan lease %q", lease.ExecutionID)
			}
		}
		requireLeaseIndexCard(t, rdb, live)
	})
}

// TestLeaseIndexPruneLeavesFutureRegistrations pins that the prune only looks
// at members the reclaim read would consider: a live execution whose index
// score is still in the future is neither removed nor rescored.
func TestLeaseIndexPruneLeavesFutureRegistrations(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		mustUpsertRunning(t, state, "future-1", "n", "tok-future", time.Now().UTC(), time.Hour)
		// Registration scores min(now, deadline), i.e. now; re-arm to the exact
		// minimum so the member carries the future deadline as intended.
		state.rearmLeaseExpiryIndex(ctx, namespace.Default, "future-1")
		want, err := rdb.ZScore(ctx, leaseExpiryZSetKey(namespace.Default, "future-1"),
			leaseExpiryMember("future-1", "n")).Result()
		if err != nil {
			t.Fatal(err)
		}
		if want <= float64(time.Now().UnixMilli()) {
			t.Fatalf("precondition: lease score = %v, want a future deadline", want)
		}

		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil || len(got) != 0 {
			t.Fatalf("ListExpiredLeases() = %+v err=%v, want none", got, err)
		}
		if score, ok := leaseIndexScore(t, rdb, "future-1"); !ok || score != want {
			t.Fatalf("future registration = %v ok=%v, want untouched at %v", score, ok, want)
		}
	})
}

// TestLeaseIndexPruneIsIdempotent pins that a second sweep after a fully
// pruned index neither errors nor changes the result.
func TestLeaseIndexPruneIsIdempotent(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		base := time.Now().Add(-time.Hour).UTC()
		mustUpsertRunning(t, state, "live", "n", "tok-live", base, time.Second)
		seedLeaseIndexOrphans(t, rdb, 5000, float64(base.Add(30*time.Minute).UnixMilli()))

		first, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil || len(first) != 1 {
			t.Fatalf("first ListExpiredLeases() = %+v err=%v, want the live lease", first, err)
		}
		requireLeaseIndexCard(t, rdb, 1)
		second, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("second ListExpiredLeases() error = %v", err)
		}
		if len(second) != len(first) || second[0].ExecutionID != first[0].ExecutionID {
			t.Fatalf("second = %+v, want the first result %+v", second, first)
		}
		requireLeaseIndexCard(t, rdb, 1)
	})
}

// addOnLeaseIndexExistsPipeline fires fn once, after the first pipeline of
// single-key EXISTS of the prune has returned. For the prune that instant is
// exactly "read the execution as an orphan, ZREM not sent yet" — where a
// producer would slip its per-execution ZSET write in.
type addOnLeaseIndexExistsPipeline struct {
	once sync.Once
	key  string
	fn   func()
}

func (h *addOnLeaseIndexExistsPipeline) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *addOnLeaseIndexExistsPipeline) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *addOnLeaseIndexExistsPipeline) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if len(cmds) > 0 && cmds[0].Name() == "exists" &&
			len(cmds[0].Args()) > 1 && cmds[0].Args()[1] == h.key {
			h.once.Do(h.fn)
		}
		return err
	}
}

// TestLeaseIndexPruneVerifyRestoresConcurrentRegistration pins the prune's
// verify step: a lease ZSET that appears after the orphan read but before the
// ZREM must be re-registered in the index, not lost with the orphan.
func TestLeaseIndexPruneVerifyRestoresConcurrentRegistration(t *testing.T) {
	forEachLeaseIndexBackend(t, func(t *testing.T, state *Store, rdb *redis.Client) {
		ctx := context.Background()
		zkey := leaseExpiryZSetKey(namespace.Default, "e1")
		if err := rdb.ZAdd(ctx, leaseExpiryExecIndexKey(namespace.Default),
			redis.Z{Score: 1, Member: "e1"}).Err(); err != nil {
			t.Fatal(err)
		}
		rdb.AddHook(&addOnLeaseIndexExistsPipeline{key: zkey, fn: func() {
			_ = rdb.ZAdd(context.Background(), zkey, redis.Z{Score: 4242, Member: "e1|n"}).Err()
		}})

		state.pruneOrphanLeaseIndex(ctx, namespace.Default, fmt.Sprintf("%d", time.Now().UnixMilli()))

		if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != 4242 {
			t.Fatalf("index member = %v ok=%v, want restored at the exact minimum 4242", score, ok)
		}
	})
}
