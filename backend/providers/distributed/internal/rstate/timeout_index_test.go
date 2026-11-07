package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func timeoutIndexScore(t *testing.T, rdb *redis.Client, id types.ExecutionID) (float64, bool) {
	t.Helper()
	score, err := rdb.ZScore(context.Background(), TimeoutIndexKey(namespace.Default), string(id)).Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZSCORE timeout index: %v", err)
	}
	return score, true
}

func seedParkable(t *testing.T, state *Store, rdb *redis.Client, id types.ExecutionID, node string) {
	t.Helper()
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := rdb.Set(ctx, nodeStatusKey(namespace.Default, id, node), "running", time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}
}

func TestTimeoutIndexKeyIsInvisibleToExecScan(t *testing.T) {
	key := TimeoutIndexKey(namespace.Default)
	if _, ok := TimeoutExecutionID(namespace.Default, key); ok {
		t.Fatalf("index key %q parsed as an execution timeout key", key)
	}
	id, ok := TimeoutExecutionID(namespace.Default, timeoutZSetKey(namespace.Default, "e1"))
	if !ok || id != "e1" {
		t.Fatalf("TimeoutExecutionID = %q, %v; want e1, true", id, ok)
	}
}

// Every path that can add a member to an execution's timeout ZSET registers
// the execution in the namespace index.
func TestTimeoutIndexRegisteredOnEveryAddPath(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		run  func(t *testing.T, s *Store, id types.ExecutionID)
	}{
		{"SuspendOrConsume", func(t *testing.T, s *Store, id types.ExecutionID) {
			_, err := s.SuspendOrConsume(ctx, id, "w", &types.SuspendSpec{Signals: []string{"s1", "s2"}, Timeout: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"SuspendOrConsumeMulti", func(t *testing.T, s *Store, id types.ExecutionID) {
			_, err := s.SuspendOrConsume(ctx, id, "w", &types.SuspendSpec{
				Mode: types.ModeMultiSignal, Signals: []string{"s1", "s2"}, Timeout: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"ResuspendAtomic", func(t *testing.T, s *Store, id types.ExecutionID) {
			spec := &types.SuspendSpec{Signals: []string{"s1"}, Timeout: time.Minute}
			if _, err := s.ResuspendAtomic(ctx, id, "w", "s0", "s1", spec); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, _, rdb := newTestRedisState(t)
			id := types.ExecutionID("tidx-" + tc.name)
			seedParkable(t, state, rdb, id, "w")
			before := time.Now().UnixMilli()
			tc.run(t, state, id)
			if n, _ := rdb.ZCard(ctx, timeoutZSetKey(namespace.Default, id)).Result(); n != 1 {
				t.Fatalf("per-exec timeout ZSET has %d members, want 1", n)
			}
			score, ok := timeoutIndexScore(t, rdb, id)
			if !ok {
				t.Fatal("execution not registered in timeout index")
			}
			if int64(score) < before {
				t.Fatalf("index score %v is before the write (%d); want ms-scale now", score, before)
			}
		})
	}
}

func TestTimeoutIndexNoTimeoutNoRegistration(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	id := types.ExecutionID("tidx-none")
	seedParkable(t, state, rdb, id, "w")
	if _, err := state.SuspendOrConsume(context.Background(), id, "w", &types.SuspendSpec{Signals: []string{"s1"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := timeoutIndexScore(t, rdb, id); ok {
		t.Fatal("registered although the suspend has no timeout")
	}
}

func TestDueTimeoutExecutionsFindsRegistered(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	if err := MarkTimeoutIndex(ctx, rdb, namespace.Default, "due"); err != nil {
		t.Fatal(err)
	}
	rdb.ZAdd(ctx, TimeoutIndexKey(namespace.Default), redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: "later"})
	got, err := DueTimeoutExecutions(ctx, rdb, namespace.Default, time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "due" {
		t.Fatalf("due = %v, want [due]", got)
	}
}

func TestRefreshTimeoutIndexRearmsExactMinMs(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("rearm")
	key := timeoutZSetKey(namespace.Default, id)
	rdb.ZAdd(ctx, key, redis.Z{Score: 2000, Member: "b"}, redis.Z{Score: 1000, Member: "a"})
	if err := RefreshTimeoutIndex(ctx, rdb, namespace.Default, id); err != nil {
		t.Fatal(err)
	}
	if score, ok := timeoutIndexScore(t, rdb, id); !ok || score != 1000*1000 {
		t.Fatalf("index score = %v, %v; want 1000000 (min seconds * 1000)", score, ok)
	}
}

func TestRefreshTimeoutIndexPrunesEmpty(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	_ = MarkTimeoutIndex(ctx, rdb, namespace.Default, "gone")
	if err := RefreshTimeoutIndex(ctx, rdb, namespace.Default, "gone"); err != nil {
		t.Fatal(err)
	}
	if _, ok := timeoutIndexScore(t, rdb, "gone"); ok {
		t.Fatal("empty execution still in index")
	}
}

// hookFn runs a callback after a matching command, to inject a concurrent
// producer at an exact point of the refresh sequence.
type hookFn struct {
	after func(cmd redis.Cmder)
}

func (h hookFn) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h hookFn) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.after(cmd)
		return err
	}
}
func (h hookFn) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A producer that adds a timeout and registers right after the prune's ZREM is
// repaired by the verify read.
func TestRefreshTimeoutIndexPruneVerifyRepairsConcurrentAdd(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("race-prune")
	side := redis.NewClient(rdb.Options())
	t.Cleanup(func() { _ = side.Close() })
	rdb.AddHook(hookFn{after: func(cmd redis.Cmder) {
		if cmd.Name() == "zrem" {
			side.ZAdd(ctx, timeoutZSetKey(namespace.Default, id), redis.Z{Score: 5000, Member: "m"})
		}
	}})
	_ = MarkTimeoutIndex(ctx, rdb, namespace.Default, id)
	if err := RefreshTimeoutIndex(ctx, rdb, namespace.Default, id); err != nil {
		t.Fatal(err)
	}
	if score, ok := timeoutIndexScore(t, rdb, id); !ok || score != 5000*1000 {
		t.Fatalf("index = %v, %v; want repaired to 5000000", score, ok)
	}
}

// A producer that adds an EARLIER timeout between the re-arm read and the
// re-arm ZADD must not be clobbered by the later exact score.
func TestRefreshTimeoutIndexRearmClobberRace(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("race-rearm")
	key := timeoutZSetKey(namespace.Default, id)
	rdb.ZAdd(ctx, key, redis.Z{Score: 9000, Member: "late"})
	side := redis.NewClient(rdb.Options())
	t.Cleanup(func() { _ = side.Close() })
	fired := false
	rdb.AddHook(hookFn{after: func(cmd redis.Cmder) {
		if cmd.Name() == "zrange" && !fired {
			fired = true
			side.ZAdd(ctx, key, redis.Z{Score: 100, Member: "early"})
		}
	}})
	if err := RefreshTimeoutIndex(ctx, rdb, namespace.Default, id); err != nil {
		t.Fatal(err)
	}
	if score, ok := timeoutIndexScore(t, rdb, id); !ok || score != 100*1000 {
		t.Fatalf("index = %v, %v; want 100000 (earliest member)", score, ok)
	}
}
