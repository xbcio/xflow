package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/namespace"
)

// TestTriggerDedupNonPositiveTTLDefaultsToTwentyFourHours covers the ttl <= 0
// fallback in Dedup. go-redis's SetNX treats a literal zero duration as "no
// expiration at all" (see redis/go-redis/v9 string_commands.go: "Zero
// expiration means the key has no expiration time."), so if this fallback
// were ever removed, a caller passing ttl<=0 would leave a dedup marker in
// Redis forever instead of getting the intended 24h default. That is a much
// worse outcome than "the default is merely wrong length": the key never
// gets cleaned up. This test asserts both that the key does expire and that
// it expires with the documented 24h TTL, not some other value.
func TestTriggerDedupNonPositiveTTLDefaultsToTwentyFourHours(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"zero", 0},
		{"negative", -5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
			rdb := newTriggerRuntimeTestRedisClient(t)

			key := "test-trigger-dedup-ttl-default-" + tc.name + "-" + uuid.NewString()
			dedupKey := triggerDedupKey(namespace.FromContext(ctx), key)
			t.Cleanup(func() {
				if err := rdb.Del(ctx, dedupKey).Err(); err != nil {
					t.Fatalf("Del(%q) error = %v", dedupKey, err)
				}
			})

			p := New(rdb)
			ok, err := p.Dedup(ctx, key, tc.ttl)
			if err != nil {
				t.Fatalf("Dedup(ttl=%v) error = %v", tc.ttl, err)
			}
			if !ok {
				t.Fatalf("Dedup(ttl=%v) = %v, want true", tc.ttl, ok)
			}

			pttl, err := rdb.PTTL(ctx, dedupKey).Result()
			if err != nil {
				t.Fatalf("PTTL(%q) error = %v", dedupKey, err)
			}
			// PTTL returns -1 for a key that exists with no expiration at all
			// (the "leaked forever" failure mode) and -2 for a missing key.
			if pttl <= 0 {
				t.Fatalf("PTTL(%q) = %s, want a positive TTL close to 24h; the dedup key must not be persistent", dedupKey, pttl)
			}
			if pttl > 24*time.Hour || pttl < 23*time.Hour {
				t.Fatalf("PTTL(%q) = %s, want within [23h,24h] of the documented 24h default", dedupKey, pttl)
			}
		})
	}
}

// TestTriggerTryLockNonPositiveTTLDefaultsToOneMinute covers the ttl <= 0
// fallback in TryLock. Like Dedup's fallback, dropping this guard would let
// a zero/negative caller-supplied ttl reach go-redis's SetNX verbatim, which
// leaves the lock key with no expiration at all instead of the documented
// 1-minute default -- a lock that can never be reclaimed if its holder
// crashes before calling Release.
func TestTriggerTryLockNonPositiveTTLDefaultsToOneMinute(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"zero", 0},
		{"negative", -3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
			rdb := newTriggerRuntimeTestRedisClient(t)

			key := "test-trigger-lock-ttl-default-" + tc.name + "-" + uuid.NewString()
			lockKey := triggerLockKey(namespace.FromContext(ctx), key)
			t.Cleanup(func() {
				if err := rdb.Del(ctx, lockKey).Err(); err != nil {
					t.Fatalf("Del(%q) error = %v", lockKey, err)
				}
			})

			p := New(rdb)
			lock, ok, err := p.TryLock(ctx, key, tc.ttl)
			if err != nil {
				t.Fatalf("TryLock(ttl=%v) error = %v", tc.ttl, err)
			}
			if !ok {
				t.Fatalf("TryLock(ttl=%v) ok = %v, want true", tc.ttl, ok)
			}
			if lock == nil {
				t.Fatal("TryLock() returned ok=true with a nil lock")
			}

			pttl, err := rdb.PTTL(ctx, lockKey).Result()
			if err != nil {
				t.Fatalf("PTTL(%q) error = %v", lockKey, err)
			}
			if pttl <= 0 {
				t.Fatalf("PTTL(%q) = %s, want a positive TTL close to 1m; the lock must not be persistent", lockKey, pttl)
			}
			if pttl > time.Minute || pttl < 50*time.Second {
				t.Fatalf("PTTL(%q) = %s, want within [50s,60s] of the documented 1m default", lockKey, pttl)
			}
		})
	}
}

// TestTriggerLockRenewNonPositiveTTLDefaultsToOneMinuteAndDoesNotDeleteLock
// covers Renew's own ttl <= 0 fallback, which is a separate guard from
// TryLock's (Renew does not call TryLock's default path). This one has a
// sharper failure mode than "leaks forever": the renew Lua script runs
// PEXPIRE KEYS[1] ARGV[2], and Redis treats an explicit PEXPIRE ... 0 (or a
// negative value) as "expire the key right now" -- verified against the test
// Redis instance with `PEXPIRE key 0` followed by `EXISTS key` returning 0.
// So if this guard were dropped, Renew(ctx, ttl<=0) would delete a live lock
// out from under its holder instead of extending it, which is the opposite
// of what a lock renewal is supposed to do.
func TestTriggerLockRenewNonPositiveTTLDefaultsToOneMinuteAndDoesNotDeleteLock(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"zero", 0},
		{"negative", -3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-a"))
			rdb := newTriggerRuntimeTestRedisClient(t)

			key := "test-trigger-lock-renew-ttl-default-" + tc.name + "-" + uuid.NewString()
			lockKey := triggerLockKey(namespace.FromContext(ctx), key)
			t.Cleanup(func() {
				if err := rdb.Del(ctx, lockKey).Err(); err != nil {
					t.Fatalf("Del(%q) error = %v", lockKey, err)
				}
			})

			p := New(rdb)
			lock, ok, err := p.TryLock(ctx, key, time.Minute)
			if err != nil {
				t.Fatalf("TryLock() error = %v", err)
			}
			if !ok {
				t.Fatal("TryLock() did not acquire the lock")
			}

			renewable, ok := lock.(interface {
				Renew(context.Context, time.Duration) (bool, error)
			})
			if !ok {
				t.Fatal("lock does not support renewal")
			}

			renewed, err := renewable.Renew(ctx, tc.ttl)
			if err != nil {
				t.Fatalf("Renew(ttl=%v) error = %v", tc.ttl, err)
			}
			if !renewed {
				t.Fatalf("Renew(ttl=%v) = false, want true", tc.ttl)
			}

			exists, err := rdb.Exists(ctx, lockKey).Result()
			if err != nil {
				t.Fatalf("Exists(%q) error = %v", lockKey, err)
			}
			if exists == 0 {
				t.Fatalf("Renew(ttl=%v) deleted the lock instead of extending it", tc.ttl)
			}

			pttl, err := rdb.PTTL(ctx, lockKey).Result()
			if err != nil {
				t.Fatalf("PTTL(%q) error = %v", lockKey, err)
			}
			if pttl <= 0 {
				t.Fatalf("PTTL(%q) = %s, want a positive TTL close to 1m; the lock must not be persistent", lockKey, pttl)
			}
			if pttl > time.Minute || pttl < 50*time.Second {
				t.Fatalf("PTTL(%q) = %s, want within [50s,60s] of the documented 1m default", lockKey, pttl)
			}
		})
	}
}
