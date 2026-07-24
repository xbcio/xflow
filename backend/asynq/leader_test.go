package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
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

func TestRedisLeaderElectorIsLeaderExpiresWhenRenewalStops(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ttl := 40 * time.Millisecond
	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:local-expiry", ttl)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if !l.IsLeader() {
		t.Fatal("IsLeader() = false after Campaign, want true")
	}

	l.mu.Lock()
	stop := l.stopRenew
	l.stopRenew = nil
	l.mu.Unlock()
	if stop == nil {
		t.Fatal("stopRenew is nil after Campaign")
	}
	stop()

	time.Sleep(2 * ttl)
	if l.IsLeader() {
		t.Fatal("IsLeader() = true after local lease deadline passed without renewal, want false")
	}
}

func TestRedisLeaderElectorInvalidTTLStillSetsExpiringLease(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	key := "test:leader:invalid-ttl"
	l := newTestRedisLeaderElector(t, mr.Addr(), key, 0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	ttl := mr.TTL(key)
	if ttl <= 0 {
		t.Fatalf("Redis key TTL = %v, want positive expiration for invalid configured TTL", ttl)
	}
}

func TestRedisLeaderElectorUsesFreshTokenPerCampaign(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:fresh-token", time.Second)
	ctx := context.Background()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("first Campaign() error = %v", err)
	}
	firstToken := l.token
	if firstToken == "" {
		t.Fatal("first campaign token is empty")
	}
	if err := l.Resign(ctx); err != nil {
		t.Fatalf("Resign() error = %v", err)
	}
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("second Campaign() error = %v", err)
	}
	secondToken := l.token
	if secondToken == "" {
		t.Fatal("second campaign token is empty")
	}
	if secondToken == firstToken {
		t.Fatalf("campaign token was reused: %q", secondToken)
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
	notify := l.Notify()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	// Drain the initial acquire notification so the assertion below observes
	// the *next* transition (the forced renewal failure), not this one.
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit after initial Campaign")
	}

	// Rip the lease out from under the elector so every renewal tick fails
	// the token check, driving the real failures>=3 cleanup path.
	mr.Del("test:leader:recampaign")

	select {
	case v, ok := <-notify:
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

func TestRedisLeaderElectorNotifyBroadcastsToSubscribers(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ttl := 120 * time.Millisecond
	key := "test:leader:broadcast"
	l := newTestRedisLeaderElector(t, mr.Addr(), key, ttl)
	notifyA := l.Notify()
	notifyB := l.Notify()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	expectLeaderNotify(t, notifyA, true)
	expectLeaderNotify(t, notifyB, true)

	if err := mr.Set(key, "different-owner"); err != nil {
		t.Fatal(err)
	}
	mr.SetTTL(key, ttl)

	expectLeaderNotify(t, notifyA, false)
	expectLeaderNotify(t, notifyB, false)
}

func TestRedisLeaderElectorDropsLeadershipImmediatelyOnTokenMismatch(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ttl := 120 * time.Millisecond
	key := "test:leader:token-mismatch"
	l := newTestRedisLeaderElector(t, mr.Addr(), key, ttl)
	notify := l.Notify()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit after initial Campaign")
	}

	if err := mr.Set(key, "different-owner"); err != nil {
		t.Fatal(err)
	}
	mr.SetTTL(key, ttl)

	expectLeaderNotify(t, notify, false)
	if l.IsLeader() {
		t.Fatal("IsLeader() = true after token mismatch, want false")
	}
}

func expectLeaderNotify(t *testing.T, ch <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("Notify() emitted %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("Notify() did not emit %v within 1s", want)
	}
}

func TestBackendImplementsLeaderElector(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	b, err := New(mr.Addr(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.rdb.Close() }()

	var _ backend.LeaderElector = (*RedisLeaderElector)(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.LeaderElector().Campaign(ctx); err != nil {
		t.Fatalf("LeaderElector().Campaign() error = %v", err)
	}
	if !b.LeaderElector().IsLeader() {
		t.Fatal("LeaderElector().IsLeader() = false after Campaign, want true")
	}
}
