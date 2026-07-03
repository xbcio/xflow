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
