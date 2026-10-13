package ratelimit

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newMiniredisBucket returns a bucket over a miniredis whose clock is pinned,
// so refill is driven by SetTime rather than by sleeping.
func newMiniredisBucket(t *testing.T) (*TokenBucket, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(time.Unix(1_700_000_000, 0))
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, ""), mr
}

func TestTakeSpendsBurstThenRefusesWithRetryAfter(t *testing.T) {
	b, _ := newMiniredisBucket(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		ok, _, err := b.Take(ctx, "ns-a", 2, 3)
		if err != nil || !ok {
			t.Fatalf("take %d = (%v, %v), want granted inside burst", i, ok, err)
		}
	}
	ok, wait, err := b.Take(ctx, "ns-a", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("fourth take granted past a burst of 3")
	}
	// Empty bucket at 2 tokens/s: one token is 500ms away.
	if wait != 500*time.Millisecond {
		t.Fatalf("retry after = %v, want 500ms", wait)
	}
}

func TestTakeRefillsFromRedisClock(t *testing.T) {
	b, mr := newMiniredisBucket(t)
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)

	for i := 0; i < 2; i++ {
		if ok, _, _ := b.Take(ctx, "ns-a", 1, 2); !ok {
			t.Fatalf("take %d refused inside burst", i)
		}
	}
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 2); ok {
		t.Fatal("granted on an empty bucket")
	}
	mr.SetTime(start.Add(1500 * time.Millisecond))
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 2); !ok {
		t.Fatal("refused after 1.5s refill at 1 token/s")
	}
	if ok, wait, _ := b.Take(ctx, "ns-a", 1, 2); ok || wait != 500*time.Millisecond {
		t.Fatalf("second take after refill = (%v, %v), want refused with 500ms", ok, wait)
	}
	// A long idle period refills to burst, never past it.
	mr.SetTime(start.Add(time.Hour))
	for i := 0; i < 2; i++ {
		if ok, _, _ := b.Take(ctx, "ns-a", 1, 2); !ok {
			t.Fatalf("take %d after idle refused", i)
		}
	}
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 2); ok {
		t.Fatal("idle refill exceeded burst")
	}
}

func TestTakeKeysAreIndependent(t *testing.T) {
	b, _ := newMiniredisBucket(t)
	ctx := context.Background()
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 1); !ok {
		t.Fatal("ns-a first take refused")
	}
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 1); ok {
		t.Fatal("ns-a second take granted")
	}
	if ok, _, _ := b.Take(ctx, "ns-b", 1, 1); !ok {
		t.Fatal("ns-b was charged for ns-a's traffic")
	}
}

// A clock that steps backwards (failover onto a host behind the old one) must
// refill nothing rather than mint or destroy tokens.
func TestTakeIgnoresBackwardClock(t *testing.T) {
	b, mr := newMiniredisBucket(t)
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 1); !ok {
		t.Fatal("first take refused")
	}
	mr.SetTime(start.Add(-time.Minute))
	if ok, _, _ := b.Take(ctx, "ns-a", 1, 1); ok {
		t.Fatal("backward clock refilled the bucket")
	}
}

func TestTakeSetsExpiry(t *testing.T) {
	b, mr := newMiniredisBucket(t)
	if _, _, err := b.Take(context.Background(), "ns-a", 10, 5); err != nil {
		t.Fatal(err)
	}
	// burst/rate = 500ms, plus 1s slack.
	if ttl := mr.TTL(DefaultKeyPrefix + "ns-a"); ttl != 1500*time.Millisecond {
		t.Fatalf("ttl = %v, want 1.5s", ttl)
	}
}

func TestTakeReportsUnreachableRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	mr.Close()
	if _, _, err := New(client, "").Take(context.Background(), "ns-a", 1, 1); err == nil {
		t.Fatal("Take against a closed Redis returned no error")
	}
}

func TestTakeRejectsInvalidBucket(t *testing.T) {
	b, _ := newMiniredisBucket(t)
	if _, _, err := b.Take(context.Background(), "k", 0, 1); err == nil {
		t.Fatal("zero rate accepted")
	}
	if _, _, err := b.Take(context.Background(), "k", 1, 0); err == nil {
		t.Fatal("zero burst accepted")
	}
}

// TestTakeRealRedisSharedAcrossClients runs the script on a real Redis: two
// clients stand in for two replicas, and together they must spend exactly one
// burst. Real Redis is what proves the script's TIME-then-write shape is
// accepted and replicated by an actual server, which miniredis cannot.
func TestTakeRealRedisSharedAcrossClients(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping real-Redis rate limit test")
	}
	ctx := context.Background()
	c1 := redis.NewClient(&redis.Options{Addr: addr})
	c2 := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	if err := c1.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	prefix := "xflow:test:ratelimit:" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	t.Cleanup(func() { _ = c1.Del(ctx, prefix+"ns-a").Err() })
	r1, r2 := New(c1, prefix), New(c2, prefix)

	// A rate low enough that refill during the test cannot grant an extra token.
	const burst = 10
	granted := 0
	for i := 0; i < 2*burst; i++ {
		b := r1
		if i%2 == 1 {
			b = r2
		}
		ok, wait, err := b.Take(ctx, "ns-a", 0.001, burst)
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		if ok {
			granted++
		} else if wait <= 0 {
			t.Fatalf("refused take %d carried no retry-after", i)
		}
	}
	if granted != burst {
		t.Fatalf("granted %d across two clients, want exactly the shared burst %d", granted, burst)
	}
	if ttl := c1.PTTL(ctx, prefix+"ns-a").Val(); ttl <= 0 {
		t.Fatalf("bucket key has no expiry (pttl=%v)", ttl)
	}
}
