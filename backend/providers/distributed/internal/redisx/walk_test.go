package redisx

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func walkTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestWalkLock(t *testing.T) {
	ctx := context.Background()
	mr, rdb := walkTestRedis(t)
	key := WalkLockKey("ns", "timeouts")

	t.Run("acquire when free, second owner refused", func(t *testing.T) {
		if ok, err := AcquireWalkLock(ctx, rdb, key, "a", WalkLockTTL); err != nil || !ok {
			t.Fatalf("first acquire = %v, %v", ok, err)
		}
		if ok, err := AcquireWalkLock(ctx, rdb, key, "b", WalkLockTTL); err != nil || ok {
			t.Fatalf("second owner acquire = %v, %v; want refused", ok, err)
		}
		if ok, err := AcquireWalkLock(ctx, rdb, key, "a", WalkLockTTL); err != nil || !ok {
			t.Fatalf("owner re-acquire = %v, %v", ok, err)
		}
	})
	t.Run("renew only by owner", func(t *testing.T) {
		if ok, _ := RenewWalkLock(ctx, rdb, key, "b", WalkLockTTL); ok {
			t.Fatal("non-owner renewed the lock")
		}
		mr.FastForward(WalkLockTTL - time.Second)
		if ok, err := RenewWalkLock(ctx, rdb, key, "a", WalkLockTTL); err != nil || !ok {
			t.Fatalf("owner renew = %v, %v", ok, err)
		}
		mr.FastForward(WalkLockTTL - time.Second)
		if ok, _ := RenewWalkLock(ctx, rdb, key, "a", WalkLockTTL); !ok {
			t.Fatal("renew did not extend the TTL")
		}
	})
	t.Run("expiry hands over and old owner cannot renew", func(t *testing.T) {
		mr.FastForward(WalkLockTTL + time.Second)
		if ok, _ := RenewWalkLock(ctx, rdb, key, "a", WalkLockTTL); ok {
			t.Fatal("renew succeeded on an expired lock")
		}
		if ok, _ := AcquireWalkLock(ctx, rdb, key, "b", WalkLockTTL); !ok {
			t.Fatal("takeover after expiry refused")
		}
		if ok, _ := RenewWalkLock(ctx, rdb, key, "a", WalkLockTTL); ok {
			t.Fatal("deposed owner renewed")
		}
	})
}

func TestWalkMarkerAndBeginWalk(t *testing.T) {
	ctx := context.Background()
	mr, rdb := walkTestRedis(t)

	holding, recent, err := BeginWalk(ctx, rdb, "ns", "leases", "a", false)
	if err != nil || !holding || recent {
		t.Fatalf("first BeginWalk = %v %v %v; want holder without marker", holding, recent, err)
	}
	if holding, _, _ := BeginWalk(ctx, rdb, "ns", "leases", "b", false); holding {
		t.Fatal("second owner became holder")
	}
	if err := MarkWalkRoundDone(ctx, rdb, WalkDoneKey("ns", "leases"), time.Now(), WalkRound); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(WalkLockTTL + time.Second)
	holding, recent, err = BeginWalk(ctx, rdb, "ns", "leases", "b", false)
	if err != nil || !holding || !recent {
		t.Fatalf("takeover BeginWalk = %v %v %v; want holder with marker", holding, recent, err)
	}
	mr.FastForward(2*WalkRound + time.Second)
	if ok, _ := WalkRoundRecent(ctx, rdb, WalkDoneKey("ns", "leases")); ok {
		t.Fatal("marker outlived 2 rounds")
	}
}

func TestWalkKeysAreClusterAndScanSafe(t *testing.T) {
	for _, k := range []string{WalkLockKey("ns", "outbox"), WalkDoneKey("ns", "outbox")} {
		if strings.ContainsAny(k, "{}") || strings.Contains(k, ":exec:") {
			t.Fatalf("walk key %q has a hash tag or matches an exec glob", k)
		}
	}
}

func TestWalkPagesPerStepAtMeasuredKeyspace(t *testing.T) {
	got := WalkPagesPerStep(2_770_000, WalkScanCount, WalkRound, 30*time.Second)
	if got < 23 || got > 25 {
		t.Fatalf("pages per step = %d, want about 24", got)
	}
	if perSecond := float64(got) / 30; perSecond >= 1 {
		t.Fatalf("one holder issues %.2f SCAN/s, want < 1", perSecond)
	}
	if WalkPagesPerStep(0, WalkScanCount, WalkRound, 30*time.Second) != 1 {
		t.Fatal("empty keyspace must still make one page of progress")
	}
}
