package trigger

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestTriggerDedupIsSharedAcrossPrimitiveInstances(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-dedup-" + uuid.NewString()
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerDedupKey(key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerDedupKey(key), err)
		}
	})

	first := New(rdb)
	second := New(rdb)

	ok, err := first.Dedup(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("Dedup(first) error = %v", err)
	}
	if !ok {
		t.Fatalf("Dedup(first) = %v, want true", ok)
	}

	ok, err = second.Dedup(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("Dedup(second) error = %v", err)
	}
	if ok {
		t.Fatalf("Dedup(second) = %v, want false", ok)
	}
}

func TestTriggerLockIsSharedAcrossPrimitiveInstances(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-" + uuid.NewString()
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerLockKey(key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerLockKey(key), err)
		}
	})

	first := New(rdb)
	second := New(rdb)

	lock, ok, err := first.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(first) error = %v", err)
	}
	if !ok {
		t.Fatalf("TryLock(first) ok = %v, want true", ok)
	}

	otherLock, ok, err := second.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(second) error = %v", err)
	}
	if ok {
		t.Fatalf("TryLock(second) ok = %v, want false", ok)
	}
	if otherLock != nil {
		t.Fatalf("TryLock(second) lock = %#v, want nil", otherLock)
	}

	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release(first) error = %v", err)
	}

	reacquired, ok, err := second.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(reacquire) error = %v", err)
	}
	if !ok {
		t.Fatalf("TryLock(reacquire) ok = %v, want true", ok)
	}
	if err := reacquired.Release(ctx); err != nil {
		t.Fatalf("Release(reacquire) error = %v", err)
	}
}

func TestTriggerLockRenewPreservesOwnership(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-renew-" + uuid.NewString()
	lockKey := triggerLockKey(key)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, lockKey).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", lockKey, err)
		}
	})

	first := New(rdb)
	second := New(rdb)

	lock, ok, err := first.TryLock(ctx, key, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock(first) error = %v", err)
	}
	if !ok {
		t.Fatal("first lock was not acquired")
	}

	renewable, ok := lock.(interface {
		Renew(context.Context, time.Duration) (bool, error)
	})
	if !ok {
		t.Fatal("lock does not support renewal")
	}

	renewed, err := renewable.Renew(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed {
		t.Fatal("Renew() = false, want true")
	}

	ttl, err := rdb.PTTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("PTTL(%q) error = %v", lockKey, err)
	}
	if ttl < 500*time.Millisecond {
		t.Fatalf("PTTL(%q) = %s, want renewed TTL", lockKey, ttl)
	}

	_, ok, err = second.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(second) error = %v", err)
	}
	if ok {
		t.Fatal("second lock acquired after renewal")
	}
}

func TestTriggerLockRenewPositiveSubMillisecondTTLDoesNotExpireImmediately(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-renew-sub-ms-" + uuid.NewString()
	lockKey := triggerLockKey(key)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, lockKey).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", lockKey, err)
		}
	})

	first := New(rdb)

	lock, ok, err := first.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(first) error = %v", err)
	}
	if !ok {
		t.Fatal("first lock was not acquired")
	}

	renewable, ok := lock.(interface {
		Renew(context.Context, time.Duration) (bool, error)
	})
	if !ok {
		t.Fatal("lock does not support renewal")
	}

	renewed, err := renewable.Renew(ctx, 500*time.Microsecond)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed {
		t.Fatal("Renew() = false, want true")
	}

	exists, err := rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v", lockKey, err)
	}
	if exists == 0 {
		t.Fatal("sub-millisecond renewal expired the lock immediately")
	}
}

func TestTriggerLockReleaseDoesNotDeleteNewOwner(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-stale-release-" + uuid.NewString()
	lockKey := triggerLockKey(key)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, lockKey).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", lockKey, err)
		}
	})

	first := New(rdb)
	second := New(rdb)

	lock, ok, err := first.TryLock(ctx, key, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock(first) error = %v", err)
	}
	if !ok {
		t.Fatal("first lock was not acquired")
	}

	waitForRedisCondition(t, time.Second, func() bool {
		exists, err := rdb.Exists(ctx, lockKey).Result()
		return err == nil && exists == 0
	})

	reacquired, ok, err := second.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(second) error = %v", err)
	}
	if !ok {
		t.Fatal("second lock was not acquired")
	}

	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release(first stale token) error = %v", err)
	}

	exists, err := rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v", lockKey, err)
	}
	if exists == 0 {
		t.Fatal("stale release deleted the current owner lock")
	}

	_, ok, err = first.TryLock(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(third) error = %v", err)
	}
	if ok {
		t.Fatal("third lock acquired after stale release")
	}

	if err := reacquired.Release(ctx); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
}

func TestTriggerStateIsSharedAcrossPrimitiveInstances(t *testing.T) {
	ctx := context.Background()
	rdb := newTriggerRuntimeTestRedisClient(t)

	scope := "test-trigger-state-" + uuid.NewString()
	key := "payload"
	value := []byte(`{"ok":true}`)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerStateKey(scope, key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerStateKey(scope, key), err)
		}
	})

	first := New(rdb)
	second := New(rdb)

	if err := first.State(ctx, scope).Set(ctx, key, value, time.Minute); err != nil {
		t.Fatalf("State(first).Set() error = %v", err)
	}

	got, err := second.State(ctx, scope).Get(ctx, key)
	if err != nil {
		t.Fatalf("State(second).Get() error = %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("State(second).Get() = %q, want %q", got, value)
	}

	if err := second.State(ctx, scope).Delete(ctx, key); err != nil {
		t.Fatalf("State(second).Delete() error = %v", err)
	}

	got, err = first.State(ctx, scope).Get(ctx, key)
	if err != nil {
		t.Fatalf("State(first).Get() after delete error = %v", err)
	}
	if got != nil {
		t.Fatalf("State(first).Get() after delete = %q, want nil", got)
	}
}

func newTriggerRuntimeTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("XFLOW_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_REDIS_ADDR is required")
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return rdb
}

func waitForRedisCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before timeout")
		}
	}
}
