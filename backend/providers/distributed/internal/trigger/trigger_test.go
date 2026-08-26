package trigger

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
)

func TestTriggerDedupIsSharedAcrossPrimitiveInstances(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-dedup-" + uuid.NewString()
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerDedupKey(namespace.FromContext(ctx), key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerDedupKey(namespace.FromContext(ctx), key), err)
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
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-" + uuid.NewString()
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerLockKey(namespace.FromContext(ctx), key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerLockKey(namespace.FromContext(ctx), key), err)
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
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-renew-" + uuid.NewString()
	lockKey := triggerLockKey(namespace.FromContext(ctx), key)
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
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-renew-sub-ms-" + uuid.NewString()
	lockKey := triggerLockKey(namespace.FromContext(ctx), key)
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
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-stale-release-" + uuid.NewString()
	lockKey := triggerLockKey(namespace.FromContext(ctx), key)
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

// TestTriggerLockRenewReturnsFalseForAStaleToken covers the branch Renew's
// existing tests never exercise: every other test in this file calls Renew
// on a lock that still owns the key, so `return renewed == 1, nil` (the last
// line of Renew) always observed 1 and the `renewed == 1` comparison could be
// replaced with an unconditional `true` and every prior test would stay
// green. This test acquires a lock, lets it expire, lets a second owner take
// the key, then calls Renew on the first (now-stale) lock and asserts it
// reports false without deleting or re-extending the second owner's lock —
// mirroring the same stale-token setup TestTriggerLockReleaseDoesNotDeleteNewOwner
// uses for Release, since renewTriggerLockScriptSrc has the identical
// GET-then-compare structure as releaseTriggerLockScriptSrc.
func TestTriggerLockRenewReturnsFalseForAStaleToken(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-lock-stale-renew-" + uuid.NewString()
	lockKey := triggerLockKey(namespace.FromContext(ctx), key)
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

	renewable, ok := lock.(interface {
		Renew(context.Context, time.Duration) (bool, error)
	})
	if !ok {
		t.Fatal("lock does not support renewal")
	}

	renewed, err := renewable.Renew(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Renew(first stale token) error = %v", err)
	}
	if renewed {
		t.Fatal("Renew(first stale token) = true, want false — the token no longer owns the key")
	}

	exists, err := rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v", lockKey, err)
	}
	if exists == 0 {
		t.Fatal("stale renew deleted the current owner's lock")
	}

	pttl, err := rdb.PTTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("PTTL(%q) error = %v", lockKey, err)
	}
	// The second owner asked for a 1-minute lock; a stale renew from the first
	// owner must not have re-extended it beyond that.
	if pttl <= 0 || pttl > time.Minute {
		t.Fatalf("PTTL(%q) = %s, want a positive TTL at or below the second owner's 1m; a stale renew must not extend the current owner's TTL", lockKey, pttl)
	}

	if err := reacquired.Release(ctx); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
}

func TestTriggerStateIsSharedAcrossPrimitiveInstances(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	scope := "test-trigger-state-" + uuid.NewString()
	key := "payload"
	value := []byte(`{"ok":true}`)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, triggerStateKey(namespace.FromContext(ctx), scope, key)).Err(); err != nil {
			t.Fatalf("Del(%q) error = %v", triggerStateKey(namespace.FromContext(ctx), scope, key), err)
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

// TestTriggerNamespaceIsolation asserts that dedup, lock, and state keys are
// isolated by namespace.
func TestTriggerNamespaceIsolation(t *testing.T) {
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
	ctxB := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-b"))
	rdb := newTriggerRuntimeTestRedisClient(t)

	key := "test-trigger-isolation-" + uuid.NewString()
	stateScope := "test-trigger-scope-" + uuid.NewString()
	t.Cleanup(func() {
		tnA := namespace.FromContext(ctxA)
		tnB := namespace.FromContext(ctxB)
		_ = rdb.Del(ctxA, triggerDedupKey(tnA, key), triggerLockKey(tnA, key), triggerStateKey(tnA, stateScope, key)).Err()
		_ = rdb.Del(ctxB, triggerDedupKey(tnB, key), triggerLockKey(tnB, key), triggerStateKey(tnB, stateScope, key)).Err()
	})

	first := New(rdb)

	// Dedup is independent per namespace.
	ok, err := first.Dedup(ctxA, key, time.Minute)
	if err != nil {
		t.Fatalf("Dedup(namespace-a) error = %v", err)
	}
	if !ok {
		t.Fatalf("Dedup(namespace-a) = %v, want true", ok)
	}
	ok, err = first.Dedup(ctxB, key, time.Minute)
	if err != nil {
		t.Fatalf("Dedup(namespace-b) error = %v", err)
	}
	if !ok {
		t.Fatalf("Dedup(namespace-b) = %v, want true", ok)
	}

	// Lock is independent per namespace.
	lockA, ok, err := first.TryLock(ctxA, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(namespace-a) error = %v", err)
	}
	if !ok {
		t.Fatalf("TryLock(namespace-a) = %v, want true", ok)
	}
	lockB, ok, err := first.TryLock(ctxB, key, time.Minute)
	if err != nil {
		t.Fatalf("TryLock(namespace-b) error = %v", err)
	}
	if !ok {
		t.Fatalf("TryLock(namespace-b) = %v, want true", ok)
	}
	if err := lockA.Release(ctxA); err != nil {
		t.Fatalf("Release(namespace-a) error = %v", err)
	}
	if err := lockB.Release(ctxB); err != nil {
		t.Fatalf("Release(namespace-b) error = %v", err)
	}

	// State is independent per namespace.
	valueA := []byte(`{"namespace":"a"}`)
	valueB := []byte(`{"namespace":"b"}`)
	if err := first.State(ctxA, stateScope).Set(ctxA, key, valueA, time.Minute); err != nil {
		t.Fatalf("State(namespace-a).Set() error = %v", err)
	}
	if err := first.State(ctxB, stateScope).Set(ctxB, key, valueB, time.Minute); err != nil {
		t.Fatalf("State(namespace-b).Set() error = %v", err)
	}
	gotA, err := first.State(ctxA, stateScope).Get(ctxA, key)
	if err != nil {
		t.Fatalf("State(namespace-a).Get() error = %v", err)
	}
	gotB, err := first.State(ctxB, stateScope).Get(ctxB, key)
	if err != nil {
		t.Fatalf("State(namespace-b).Get() error = %v", err)
	}
	if string(gotA) != string(valueA) {
		t.Fatalf("State(namespace-a).Get() = %q, want %q", gotA, valueA)
	}
	if string(gotB) != string(valueB) {
		t.Fatalf("State(namespace-b).Get() = %q, want %q", gotB, valueB)
	}
}

func newTriggerRuntimeTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	// XFLOW_TEST_REDIS_ADDR, not XFLOW_REDIS_ADDR. The latter is the production
	// runtime's variable (cmd/server, cmd/runner, cmd/xflow) and no test harness
	// in this repo has ever set it — not the Makefile, not ci.yml. Reading it
	// here meant these seven tests skipped on every machine and every CI run
	// since they were written: the trigger dedup, lock renewal and namespace
	// isolation they cover were never once exercised against a real Redis, and
	// the suite reported green anyway.
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping real-Redis trigger runtime test")
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

// TestTriggerLuaScriptsAreSingleKey is a static lock on the cluster-safety
// audit performed in G2 Phase 2 / Task 2.2. It asserts that every Lua script
// in this package operates on a single key only. If a future change introduces
// multi-key Lua (or dynamic key construction), this test must be updated and
// the cluster-safety note in trigger.go must be revisited.
func TestTriggerLuaScriptsAreSingleKey(t *testing.T) {
	scripts := map[string]string{
		"releaseTriggerLockScript": releaseTriggerLockScriptSrc,
		"renewTriggerLockScript":   renewTriggerLockScriptSrc,
	}

	keysPattern := regexp.MustCompile(`KEYS\[(\d+)\]`)

	for name, src := range scripts {
		// Multi-key Lua would reference KEYS[2] or higher.
		for _, match := range keysPattern.FindAllStringSubmatch(src, -1) {
			idx, err := strconv.Atoi(match[1])
			if err != nil {
				t.Fatalf("%s: cannot parse KEYS index %q: %v", name, match[1], err)
			}
			if idx >= 2 {
				t.Errorf("%s references multiple KEYS (KEYS[%d]); add a shared hash tag and update the cluster-safety audit", name, idx)
			}
		}

		// Dynamic key construction inside redis.call / redis.pcall is a CROSSSLOT
		// risk unless the constructed prefix carries the same hash tag as the
		// declared KEYS.
		for line := range strings.SplitSeq(src, "\n") {
			line = strings.TrimSpace(line)
			if (strings.Contains(line, "redis.call") || strings.Contains(line, "redis.pcall")) && strings.Contains(line, "..") {
				t.Errorf("%s constructs a key dynamically with '..'; verify the prefix shares the KEYS hash tag", name)
			}
		}
	}
}
