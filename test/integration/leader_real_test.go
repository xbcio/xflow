//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/redis/go-redis/v9"
)

func TestRedisLeaderElectionRealRedis(t *testing.T) {
	addr := requireRedis(t)
	key := "xflow:test:leader:" + t.Name()
	ttl := 2 * time.Second

	t.Run("single instance becomes leader", func(t *testing.T) {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		el := asynq.NewRedisLeaderElector(rdb, key+":single", ttl)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := el.Campaign(ctx); err != nil {
			t.Fatalf("campaign: %v", err)
		}
		if !el.IsLeader() {
			t.Fatalf("IsLeader() after campaign = %v, want true", el.IsLeader())
		}
		if err := el.Resign(context.Background()); err != nil {
			t.Fatalf("resign: %v", err)
		}
		if el.IsLeader() {
			t.Fatalf("IsLeader() after resign = %v, want false", el.IsLeader())
		}
	})

	t.Run("only one of two wins", func(t *testing.T) {
		k := key + ":pair"
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		// clear stale key
		_ = rdb.Del(context.Background(), k).Err()

		a := asynq.NewRedisLeaderElector(rdb, k, ttl)
		b := asynq.NewRedisLeaderElector(rdb, k, ttl)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.Campaign(ctx); err != nil {
			t.Fatalf("a campaign: %v", err)
		}
		// b should NOT win while a holds the lease
		bctx, bcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer bcancel()
		if err := b.Campaign(bctx); err == nil {
			t.Fatal("b unexpectedly became leader while a holds lease")
		}
		if !a.IsLeader() {
			t.Fatalf("a.IsLeader() = %v, want true", a.IsLeader())
		}
		if b.IsLeader() {
			t.Fatalf("b.IsLeader() = %v, want false", b.IsLeader())
		}
	})

	t.Run("resign allows immediate takeover", func(t *testing.T) {
		k := key + ":resign"
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		_ = rdb.Del(context.Background(), k).Err()

		a := asynq.NewRedisLeaderElector(rdb, k, ttl)
		b := asynq.NewRedisLeaderElector(rdb, k, ttl)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.Campaign(ctx); err != nil {
			t.Fatalf("a campaign: %v", err)
		}
		if err := a.Resign(context.Background()); err != nil {
			t.Fatalf("a resign: %v", err)
		}
		bctx, bcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer bcancel()
		if err := b.Campaign(bctx); err != nil {
			t.Fatalf("b campaign after resign: %v", err)
		}
		if !b.IsLeader() {
			t.Fatalf("b.IsLeader() = %v, want true", b.IsLeader())
		}
	})

	t.Run("leader change within TTL after kill", func(t *testing.T) {
		k := key + ":kill"
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		_ = rdb.Del(context.Background(), k).Err()

		// Use a separate client for a so we can close it to simulate process death.
		aRdb := redis.NewClient(&redis.Options{Addr: addr})
		a := asynq.NewRedisLeaderElector(aRdb, k, ttl)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.Campaign(ctx); err != nil {
			_ = aRdb.Close()
			t.Fatalf("a campaign: %v", err)
		}
		// Simulate process death: close a's redis connection so its renewal goroutine
		// will fail and the lease will expire after TTL.
		_ = aRdb.Close()

		// b waits out the TTL then wins.
		b := asynq.NewRedisLeaderElector(rdb, k, ttl)
		notify := b.Notify()
		// poll campaign with a budget > ttl
		bctx, bcancel := context.WithTimeout(context.Background(), 3*ttl)
		defer bcancel()
		go func() { _ = b.Campaign(bctx) }()
		// Notify sends current state (false) immediately on subscribe, then true when b wins.
		// Drain until we receive true (leadership acquired).
		for {
			select {
			case v := <-notify:
				if v {
					if !b.IsLeader() {
						t.Fatal("notify fired true but b not leader")
					}
					bcancel()
					return
				}
				// received false (initial state) — keep waiting
			case <-bctx.Done():
				t.Fatalf("b did not become leader within 3*ttl=%v", 3*ttl)
			}
		}
	})
}
