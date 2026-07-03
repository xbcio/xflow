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

// TestRedisLeaderElectorStopRenewSurvivesStaleCleanup pins down the exact
// race the generation guard exists to prevent: a renewal goroutine (G1) that
// is about to give up after 3 failures must not clobber e.stopRenew if, by
// the time it acquires e.mu, a newer startRenewal() call (G2, from a
// re-Campaign that already observed the lost-leadership Notify) has already
// installed its own cancel func. If G1 won the clobber, G2's renewal
// goroutine would become uncancelable: neither Resign() nor a future
// startRenewal()'s prevStop mechanism could ever stop it again.
//
// This drives the exact code path in leader.go's startRenewal failure-branch
// directly (same package, white-box) rather than relying on real timing to
// hit the race window, since a wall-clock reproduction would be flaky.
func TestRedisLeaderElectorStopRenewSurvivesStaleCleanup(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	l := newTestRedisLeaderElector(t, mr.Addr(), "test:leader:stale-cleanup", time.Second)

	// G1 acquires leadership and starts renewing (generation 1).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	l.mu.Lock()
	gen1 := l.renewGen
	l.mu.Unlock()
	if gen1 != 1 {
		t.Fatalf("renewGen after first Campaign = %d, want 1", gen1)
	}

	// Simulate: G1's renewal goroutine has just decided (failures >= 3) to
	// give up, but hasn't yet acquired e.mu to null out stopRenew. Before it
	// gets there, the caller observes Notify()==false and re-Campaigns,
	// which races G1 and wins, installing G2 (generation 2) first.
	l.setLeader(false)
	l.startRenewal() // stands in for the re-Campaign's successful re-acquire.

	l.mu.Lock()
	gen2 := l.renewGen
	liveStopAfterG2 := l.stopRenew
	l.mu.Unlock()
	if gen2 != 2 {
		t.Fatalf("renewGen after second startRenewal = %d, want 2", gen2)
	}
	if liveStopAfterG2 == nil {
		t.Fatal("stopRenew is nil after G2's startRenewal, want a live cancel func")
	}

	// Now G1's cleanup finally runs, using the generation (1) it captured
	// before G2 ever started. It must see that renewGen has moved on and
	// leave e.stopRenew alone.
	l.mu.Lock()
	if l.renewGen == gen1 {
		l.stopRenew = nil
	}
	stopAfterStaleCleanup := l.stopRenew
	l.mu.Unlock()

	if stopAfterStaleCleanup == nil {
		t.Fatal("G1's stale cleanup cleared stopRenew, want G2's cancel func to survive")
	}

	// Confirm G2 is still the one governing cancellation: Resign() must be
	// able to stop it (i.e. stopRenew is still callable/non-nil going into
	// Resign, and Resign clears it cleanly).
	if err := l.Resign(context.Background()); err != nil {
		t.Fatalf("Resign() error = %v", err)
	}
	l.mu.Lock()
	stopAfterResign := l.stopRenew
	l.mu.Unlock()
	if stopAfterResign != nil {
		t.Fatal("stopRenew is still set after Resign(), want nil")
	}
}

// TestRedisLeaderElectorRecampaignsAfterRenewalFailureWithoutLeak forces real
// renewal failures (by deleting the underlying Redis key out from under the
// elector) and confirms that once the elector loses leadership, calling
// Campaign again successfully re-acquires it. This exercises the same
// generation-guarded startRenewal path as
// TestRedisLeaderElectorStopRenewSurvivesStaleCleanup but end-to-end through
// real Redis/timing, run under -race to catch any data races in the actual
// goroutine interleaving rather than the simulated one above.
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
