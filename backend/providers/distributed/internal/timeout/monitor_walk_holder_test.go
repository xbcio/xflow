package timeout

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func twoMonitors(t *testing.T) (*miniredis.Miniredis, [2]*Monitor, [2]*cmdCounter, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	var ms [2]*Monitor
	var cs [2]*cmdCounter
	var first *redis.Client
	for i := range ms {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		cs[i] = &cmdCounter{n: map[string]int{}}
		rdb.AddHook(cs[i])
		ms[i] = New(rdb, deadEngine(), nil, nil, time.Minute)
		ms[i].SetNamespaces([]namespace.Namespace{namespace.Default})
		if first == nil {
			first = rdb
		}
	}
	return mr, ms, cs, first
}

func TestTimeoutWalkSingleHolder(t *testing.T) {
	mr, ms, cs, rdb := twoMonitors(t)
	now := time.Now()

	tick(ms[0], now) // first holder, no marker: backfill, marker written
	if cs[0].count("scan") == 0 {
		t.Fatal("first holder did not walk")
	}
	if ok, _ := redisx.WalkRoundRecent(t.Context(), rdb, redisx.WalkDoneKey(string(namespace.Default), timeoutWalkKind)); !ok {
		t.Fatal("round-done marker not written")
	}

	// Both monitors run the same ticks across several steps; only the holder scans.
	cs[0].n = map[string]int{}
	for i := 1; i <= 5; i++ {
		at := now.Add(time.Duration(i) * (timeoutWalkEvery + time.Second))
		tick(ms[1], at)
		tick(ms[0], at)
	}
	if n, d := cs[1].count("scan"), cs[1].count("dbsize"); n != 0 || d != 0 {
		t.Fatalf("non-holder issued %d SCAN and %d DBSIZE, want 0 and 0", n, d)
	}
	if cs[0].count("scan") == 0 {
		t.Fatal("holder stopped walking")
	}

	// Handover: holder goes quiet, lock expires, the other takes over without a
	// full-speed backfill (marker present): at most the steady page count.
	mr.FastForward(redisx.WalkLockTTL + time.Second)
	at := now.Add(time.Hour)
	tick(ms[1], at)
	if n := cs[1].count("scan"); n < 1 || n > timeoutWalkMaxBurstPages {
		t.Fatalf("takeover step issued %d SCAN, want one steady step", n)
	}
	if !ms[1].walks[namespace.Default].backfilled {
		t.Fatal("taker-over did not adopt the marker (would backfill at full speed)")
	}
}

func TestTimeoutWalkFirstHolderWithoutMarkerBackfillsFully(t *testing.T) {
	_, ms, cs, rdb := twoMonitors(t)
	for i := 0; i < 3; i++ {
		seedDue(t, rdb, types.ExecutionID(fmt.Sprintf("old-%d", i)), time.Now())
	}
	tick(ms[0], time.Now())
	if ms[0].walks[namespace.Default].backfilled != true || cs[0].count("scan") == 0 {
		t.Fatal("holder without marker did not complete its backfill round")
	}
	if n := rdb.Exists(t.Context(), redisx.WalkDoneKey(string(namespace.Default), timeoutWalkKind)).Val(); n != 1 {
		t.Fatalf("marker exists = %d, want 1", n)
	}
}

func TestTimeoutWalkLostRegistrationRecoveredAfterTakeover(t *testing.T) {
	mr, ms, _, rdb := twoMonitors(t)
	now := time.Now()
	tick(ms[0], now)
	key := seedDue(t, rdb, "lost", now)
	mr.FastForward(redisx.WalkLockTTL + time.Second)
	tick(ms[1], now.Add(timeoutWalkEvery+time.Second))
	if n, _ := rdb.ZCard(t.Context(), key).Result(); n != 0 {
		t.Fatalf("lost registration not recovered by the new holder, ZCARD=%d", n)
	}
}

func TestTimeoutWalkStepPagesAtMeasuredKeyspace(t *testing.T) {
	pages := redisx.WalkPagesPerStep(2_770_000, timeoutScanCount, timeoutWalkRound, timeoutWalkEvery)
	if pages < 23 || pages > 25 {
		t.Fatalf("pages per step = %d, want about 24", pages)
	}
}
