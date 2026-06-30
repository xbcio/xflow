package asynq

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

	first := newTriggerPrimitives(rdb)
	second := newTriggerPrimitives(rdb)

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

	first := newTriggerPrimitives(rdb)
	second := newTriggerPrimitives(rdb)

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

	first := newTriggerPrimitives(rdb)
	second := newTriggerPrimitives(rdb)

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
