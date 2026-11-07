package rstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func leaseIndexScore(t *testing.T, rdb *redis.Client, id types.ExecutionID) (float64, bool) {
	t.Helper()
	score, err := rdb.ZScore(context.Background(), leaseExpiryExecIndexKey(namespace.Default), string(id)).Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZScore lease index: %v", err)
	}
	return score, true
}

func dropLeaseIndex(t *testing.T, rdb *redis.Client) {
	t.Helper()
	if err := rdb.Del(context.Background(), leaseExpiryExecIndexKey(namespace.Default)).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExpiryIndexRegistrationOnEveryAddPath(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(t *testing.T, s *Store, rdb *redis.Client, id types.ExecutionID){
		"AcquireTaskLease": func(t *testing.T, s *Store, _ *redis.Client, id types.ExecutionID) {
			lease := &engine.TaskLease{
				LeaseID: "L", LeaseToken: "tok", IssuedAt: time.Now().UTC(), TTL: time.Minute,
				Task: engine.Task{ExecutionID: id, NodeName: "n", ActivationID: 1},
			}
			if _, ok, err := s.AcquireTaskLease(ctx, lease); err != nil || !ok {
				t.Fatalf("AcquireTaskLease ok=%v err=%v", ok, err)
			}
		},
		"AcquireGroupLease": func(t *testing.T, s *Store, _ *redis.Client, id types.ExecutionID) {
			lease := &engine.GroupLease{
				LeaseID: "L", LeaseToken: "tok", Attempt: 1, ExecutionID: id, GroupUnitIdx: 3,
				GroupID: "g", IssuedAt: time.Now().UTC(), TTL: time.Minute,
			}
			if ok, err := s.AcquireGroupLease(ctx, lease); err != nil || !ok {
				t.Fatalf("AcquireGroupLease ok=%v err=%v", ok, err)
			}
		},
		"UpsertNode": func(t *testing.T, s *Store, _ *redis.Client, id types.ExecutionID) {
			mustUpsertRunning(t, s, id, "n", "tok", time.Now(), time.Minute)
		},
		"RepairLeaseIndex": func(t *testing.T, s *Store, rdb *redis.Client, id types.ExecutionID) {
			mustUpsertRunning(t, s, id, "n", "tok", time.Now(), time.Minute)
			if err := rdb.Del(ctx, leaseExpiryZSetKey(namespace.Default, id), leaseExpiryExecIndexKey(namespace.Default)).Err(); err != nil {
				t.Fatal(err)
			}
			if n, err := s.RepairLeaseIndex(ctx, 100); err != nil || n != 1 {
				t.Fatalf("RepairLeaseIndex n=%d err=%v", n, err)
			}
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			state, _, rdb := newTestRedisState(t)
			run(t, state, rdb, "e-reg")
			if _, ok := leaseIndexScore(t, rdb, "e-reg"); !ok {
				t.Fatalf("%s did not register the execution in the lease index", name)
			}
		})
	}
}

func TestLeaseExpiryIndexRegistrationNeverRaisesAnEarlierScore(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	past := time.Now().Add(-time.Minute)
	mustUpsertRunning(t, state, "e1", "old", "tok-old", past, time.Second)
	mustUpsertRunning(t, state, "e1", "new", "tok-new", time.Now(), time.Hour)
	score, ok := leaseIndexScore(t, rdb, "e1")
	if !ok || score > float64(time.Now().UnixMilli()) {
		t.Fatalf("score = %v ok=%v, want <= now", score, ok)
	}
}

func TestListExpiredLeasesReadsRegisteredExecutionAndRearmsExactMin(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	mustUpsertRunning(t, state, "e1", "past", "tok-past", time.Now().Add(-time.Minute), time.Second)
	future := time.Now().Add(time.Hour)
	mustUpsertRunning(t, state, "e1", "future", "tok-fut", future, time.Second)

	got, err := state.ListExpiredLeases(ctx, time.Now())
	if err != nil || len(got) != 1 || got[0].NodeName != "past" {
		t.Fatalf("expired = %+v err=%v, want [past]", got, err)
	}
	// "past" is still in the ZSET until the sweeper revokes it, so the exact
	// minimum is its own deadline.
	want, err := rdb.ZScore(ctx, leaseExpiryZSetKey(namespace.Default, "e1"), leaseExpiryMember("e1", "past")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != want {
		t.Fatalf("index score = %v ok=%v, want exact min %v", score, ok, want)
	}

	// Once reclaimed, only the future lease remains and the index points at it.
	if ok, err := state.RevokeLease(ctx, "e1", "past", "tok-past"); err != nil || !ok {
		t.Fatalf("RevokeLease ok=%v err=%v", ok, err)
	}
	if got, err := state.ListExpiredLeases(ctx, time.Now()); err != nil || len(got) != 0 {
		t.Fatalf("expired = %+v err=%v, want none", got, err)
	}
	want = float64(future.UTC().Truncate(time.Millisecond).Add(time.Second).UnixMilli())
	if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != want {
		t.Fatalf("index score = %v ok=%v, want %v", score, ok, want)
	}
}

func TestListExpiredLeasesPrunesEmptyExecutionFromIndex(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	mustUpsertRunning(t, state, "e1", "n", "tok", time.Now().Add(-time.Minute), time.Second)
	if ok, err := state.RevokeLease(ctx, "e1", "n", "tok"); err != nil || !ok {
		t.Fatalf("RevokeLease ok=%v err=%v", ok, err)
	}
	if _, ok := leaseIndexScore(t, rdb, "e1"); !ok {
		t.Fatal("precondition: execution should still be indexed")
	}
	if _, err := state.ListExpiredLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok := leaseIndexScore(t, rdb, "e1"); ok {
		t.Fatal("empty execution was not pruned from the lease index")
	}
}

// addOnZRemHook runs fn once, right after the first ZREM of the lease index,
// which is the instant a concurrent producer would slip in.
type addOnZRemHook struct {
	once sync.Once
	fn   func()
}

func (h *addOnZRemHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *addOnZRemHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "zrem" && len(cmd.Args()) > 1 && cmd.Args()[1] == leaseExpiryExecIndexKey(namespace.Default) {
			h.once.Do(h.fn)
		}
		return err
	}
}
func (h *addOnZRemHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRearmLeaseExpiryIndexVerifyRepairsConcurrentAdd(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	zkey := leaseExpiryZSetKey(namespace.Default, "e1")
	// The per-execution ZSET is empty when the prune reads it; a producer adds a
	// lease between the prune's ZREM and its verify read.
	rdb.AddHook(&addOnZRemHook{fn: func() {
		_ = rdb.ZAdd(context.Background(), zkey, redis.Z{Score: 4242, Member: "e1|n"}).Err()
	}})
	if err := rdb.ZAdd(ctx, leaseExpiryExecIndexKey(namespace.Default), redis.Z{Score: 1, Member: "e1"}).Err(); err != nil {
		t.Fatal(err)
	}
	state.rearmLeaseExpiryIndex(ctx, namespace.Default, "e1")
	if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != 4242 {
		t.Fatalf("index score = %v ok=%v, want repaired to 4242", score, ok)
	}
}

// addOnReadHook adds a lower-scored lease right after the first per-execution
// ZRANGE, i.e. after the re-arm read the old minimum but before its ZADD.
type addOnReadHook struct {
	once sync.Once
	fn   func()
}

func (h *addOnReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *addOnReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "zrange" && len(cmd.Args()) > 1 && cmd.Args()[1] == leaseExpiryZSetKey(namespace.Default, "e1") {
			h.once.Do(h.fn)
		}
		return err
	}
}
func (h *addOnReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRearmLeaseExpiryIndexVerifyClosesClobberRace(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	zkey := leaseExpiryZSetKey(namespace.Default, "e1")
	if err := rdb.ZAdd(ctx, zkey, redis.Z{Score: 9000, Member: "e1|a"}).Err(); err != nil {
		t.Fatal(err)
	}
	rdb.AddHook(&addOnReadHook{fn: func() {
		_ = rdb.ZAdd(context.Background(), zkey, redis.Z{Score: 100, Member: "e1|b"}).Err()
	}})
	state.rearmLeaseExpiryIndex(ctx, namespace.Default, "e1")
	if score, ok := leaseIndexScore(t, rdb, "e1"); !ok || score != 100 {
		t.Fatalf("index score = %v ok=%v, want 100 (the earlier concurrent lease)", score, ok)
	}
}

func TestLeaseExpiryWalkRecoversLostRegistration(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	mustUpsertRunning(t, state, "e-lost", "n", "tok", time.Now().Add(-time.Minute), time.Second)
	dropLeaseIndex(t, rdb) // process died between the write and the ZADD

	// The backfill runs on the first call after start and registers the lease.
	got, err := state.ListExpiredLeases(ctx, time.Now())
	if err != nil || len(got) != 1 || got[0].ExecutionID != "e-lost" {
		t.Fatalf("expired = %+v err=%v, want e-lost recovered", got, err)
	}

	// After backfill a lost registration waits for a throttled step.
	dropLeaseIndex(t, rdb)
	if got, _ := state.ListExpiredLeases(ctx, time.Now()); len(got) != 0 {
		t.Fatalf("throttled walk ran early: %+v", got)
	}
	state.leaseWalks[namespace.Default].lastStep = time.Now().Add(-2 * leaseWalkStepInterval)
	got, err = state.ListExpiredLeases(ctx, time.Now())
	if err != nil || len(got) != 1 {
		t.Fatalf("expired = %+v err=%v, want recovered by the throttled step", got, err)
	}
}

func TestListExpiredLeasesIssuesNoScanAfterBackfill(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	for i := 0; i < 3000; i++ {
		if err := rdb.Set(ctx, "unrelated:"+time.Duration(i).String(), "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	mustUpsertRunning(t, state, "e1", "n", "tok", time.Now().Add(-time.Minute), time.Second)
	hook := &scanCountHook{}
	rdb.AddHook(hook)

	if _, err := state.ListExpiredLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if hook.take() == 0 {
		t.Fatal("backfill call issued no SCAN; the hook is not counting")
	}
	for i := 0; i < 5; i++ {
		got, err := state.ListExpiredLeases(ctx, time.Now())
		if err != nil || len(got) != 1 {
			t.Fatalf("call %d: expired = %+v err=%v", i, got, err)
		}
	}
	if n := hook.take(); n != 0 {
		t.Fatalf("ListExpiredLeases issued %d SCAN commands after backfill, want 0", n)
	}
}
