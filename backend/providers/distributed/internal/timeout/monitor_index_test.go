package timeout

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// These tests run on miniredis: they pin the index protocol and the SCAN cost,
// not engine delivery. Due members are malformed so the monitor drops them
// without needing a live engine.

type cmdCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *cmdCounter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *cmdCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.add(cmd.Name())
		return next(ctx, cmd)
	}
}
func (c *cmdCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.add(cmd.Name())
		}
		return next(ctx, cmds)
	}
}
func (c *cmdCounter) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[strings.ToLower(name)]++
}
func (c *cmdCounter) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}

func indexTestMonitor(t *testing.T) (*Monitor, *redis.Client, *cmdCounter) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	counter := &cmdCounter{n: map[string]int{}}
	rdb.AddHook(counter)
	m := New(rdb, deadEngine(), nil, nil, time.Minute)
	m.SetNamespaces([]namespace.Namespace{namespace.Default})
	return m, rdb, counter
}

func tick(m *Monitor, at time.Time) {
	m.processTimeouts(context.Background(), at)
}

func seedDue(t *testing.T, rdb *redis.Client, id types.ExecutionID, at time.Time) string {
	t.Helper()
	key := rstate.TimeoutZSetKey(namespace.Default, id)
	if err := rdb.ZAdd(context.Background(), key, redis.Z{Score: float64(at.Add(-time.Minute).Unix()), Member: "malformed"}).Err(); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestMonitorProcessesRegisteredExecutionAndPrunes(t *testing.T) {
	m, rdb, _ := indexTestMonitor(t)
	ctx := context.Background()
	now := time.Now()
	tick(m, now) // finish backfill on an empty keyspace
	key := seedDue(t, rdb, "e1", now)
	if err := rstate.MarkTimeoutIndex(ctx, rdb, namespace.Default, "e1"); err != nil {
		t.Fatal(err)
	}
	tick(m, now.Add(time.Second))
	if n, _ := rdb.ZCard(ctx, key).Result(); n != 0 {
		t.Fatalf("due member not processed, ZCARD=%d", n)
	}
	if _, err := rdb.ZScore(ctx, rstate.TimeoutIndexKey(namespace.Default), "e1").Result(); err != redis.Nil {
		t.Fatalf("empty execution not pruned from index: %v", err)
	}
}

func TestMonitorRearmsToExactMin(t *testing.T) {
	m, rdb, _ := indexTestMonitor(t)
	ctx := context.Background()
	now := time.Now()
	tick(m, now)
	key := seedDue(t, rdb, "e2", now)
	future := now.Add(time.Hour).Unix()
	rdb.ZAdd(ctx, key, redis.Z{Score: float64(future), Member: "m-future"})
	_ = rstate.MarkTimeoutIndex(ctx, rdb, namespace.Default, "e2")
	tick(m, now.Add(time.Second))
	score, err := rdb.ZScore(ctx, rstate.TimeoutIndexKey(namespace.Default), "e2").Result()
	if err != nil || int64(score) != future*1000 {
		t.Fatalf("index score = %v, %v; want %d ms", score, err, future*1000)
	}
}

func TestMonitorWalkRecoversLostRegistration(t *testing.T) {
	m, rdb, counter := indexTestMonitor(t)
	ctx := context.Background()
	now := time.Now()
	tick(m, now)                        // backfill done
	key := seedDue(t, rdb, "lost", now) // written, registration lost
	tick(m, now.Add(time.Second))
	if n, _ := rdb.ZCard(ctx, key).Result(); n != 1 {
		t.Fatalf("throttled walk ran before its interval; ZCARD=%d", n)
	}
	scans := counter.count("scan")
	tick(m, now.Add(timeoutWalkEvery+time.Second))
	if counter.count("scan") == scans {
		t.Fatal("steady-state walk burst never ran")
	}
	if n, _ := rdb.ZCard(ctx, key).Result(); n != 0 {
		t.Fatalf("lost registration not recovered by walk, ZCARD=%d", n)
	}
}

func TestMonitorBackfillRegistersPreexistingKeys(t *testing.T) {
	m, rdb, _ := indexTestMonitor(t)
	ctx := context.Background()
	now := time.Now()
	key := seedDue(t, rdb, "old", now)
	tick(m, now)
	if n, _ := rdb.ZCard(ctx, key).Result(); n != 0 {
		t.Fatalf("pre-existing key not handled on first tick, ZCARD=%d", n)
	}
}

// After backfill, a tick must not SCAN at all however many unrelated keys
// exist: that is the whole point of the index.
func TestMonitorTickIssuesNoScanAfterBackfill(t *testing.T) {
	m, rdb, counter := indexTestMonitor(t)
	ctx := context.Background()
	pipe := rdb.Pipeline()
	for i := 0; i < 5000; i++ {
		pipe.Set(ctx, fmt.Sprintf("unrelated:%d", i), "x", 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tick(m, now)
	if counter.count("scan") == 0 {
		t.Fatal("backfill tick issued no SCAN; the cost test would be vacuous")
	}
	_ = rstate.MarkTimeoutIndex(ctx, rdb, namespace.Default, "idle")
	before := counter.count("scan")
	for i := 1; i <= 3; i++ {
		tick(m, now.Add(time.Duration(i)*time.Second))
	}
	if got := counter.count("scan") - before; got != 0 {
		t.Fatalf("post-backfill ticks issued %d SCAN commands, want 0", got)
	}
}
