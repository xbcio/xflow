package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisLeaderElector(t *testing.T, addr, key string, ttl time.Duration) *RedisLeaderElector {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisLeaderElector(rdb, key, ttl)
}

func TestRedisLeaderElectorSingleInstanceBecomesLeader(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:single", time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if !l.IsLeader() {
		t.Fatal("IsLeader() = false after Campaign, want true")
	}
}

func TestRedisLeaderElectorOnlyOneOfTwoBecomesLeader(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l1 := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:contend", time.Second)
	l2 := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:contend", time.Second)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel1()
	if err := l1.Campaign(ctx1); err != nil {
		t.Fatalf("l1.Campaign() error = %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	err = l2.Campaign(ctx2)
	if err == nil {
		t.Fatal("l2.Campaign() succeeded while l1 holds the lease, want it to block until ctx timeout")
	}
	if l2.IsLeader() {
		t.Fatal("l2.IsLeader() = true, want false while l1 holds the lease")
	}
}

func TestRedisLeaderElectorResignAllowsImmediateTakeover(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l1 := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:resign", 10*time.Second)
	ctx := context.Background()
	if err := l1.Campaign(ctx); err != nil {
		t.Fatalf("l1.Campaign() error = %v", err)
	}
	if err := l1.Resign(ctx); err != nil {
		t.Fatalf("l1.Resign() error = %v", err)
	}
	if l1.IsLeader() {
		t.Fatal("l1.IsLeader() = true after Resign, want false")
	}

	l2 := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:resign", 10*time.Second)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := l2.Campaign(ctx2); err != nil {
		t.Fatalf("l2.Campaign() error = %v after l1 resigned", err)
	}
}

// TestRedisLeaderElectorRecampaignsAfterRenewalFailureWithoutLeak forces real
// renewal failures (by deleting the underlying Redis key out from under the
// elector) and confirms that once the elector loses leadership, calling
// Campaign again successfully re-acquires it. This exercises the real
// startRenewal failure-cleanup branch (leader.go) and its renewGen guard
// through actual goroutine interleaving under real Redis/timing rather than
// a simulated one, run under -race to catch data races in that interleaving.
func TestRedisLeaderElectorRecampaignsAfterRenewalFailureWithoutLeak(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ttl := 60 * time.Millisecond
	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:recampaign", ttl)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	// Drain the initial acquire notification so the assertion below observes
	// the *next* transition (the forced renewal failure), not this one.
	select {
	case <-l.Notify():
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit after initial Campaign")
	}

	// Rip the lease out from under the elector so every renewal tick fails
	// the token check, driving the real failures>=3 cleanup path.
	mr.Del("test:leader:recampaign")

	select {
	case v, ok := <-l.Notify():
		if !ok {
			t.Fatal("Notify() channel closed")
		}
		if v {
			t.Fatal("Notify() emitted true, want false after renewal failures")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Notify() did not emit false within 2s after forced renewal failure")
	}
	if l.IsLeader() {
		t.Fatal("IsLeader() = true after forced renewal failure, want false")
	}

	// Re-Campaign must succeed (the key is free again) without the elector
	// being stuck due to a leaked/uncancelable stopRenew from the old
	// generation.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := l.Campaign(ctx2); err != nil {
		t.Fatalf("re-Campaign() error = %v", err)
	}
	if !l.IsLeader() {
		t.Fatal("IsLeader() = false after re-Campaign, want true")
	}

	if err := l.Resign(context.Background()); err != nil {
		t.Fatalf("Resign() error = %v", err)
	}
	l.mu.Lock()
	stop := l.stopRenew
	l.mu.Unlock()
	if stop != nil {
		t.Fatal("stopRenew still set after final Resign(), want nil")
	}
}

func TestRedisLeaderElectorNotifyEmitsOnAcquire(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:notify", time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	select {
	case v := <-l.Notify():
		if !v {
			t.Fatal("Notify() emitted false, want true after successful Campaign")
		}
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit within 1s")
	}
}
