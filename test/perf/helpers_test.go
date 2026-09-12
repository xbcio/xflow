//go:build perf

package perf

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// realRedisAddr returns the real-Redis address, skipping the benchmark when
// Redis is unreachable. Under XFLOW_REQUIRE_REDIS_INTEGRATION=1 (CI gating
// mode) it fails instead, so a missing dependency cannot be mistaken for a
// passing gate: `-bench` output prints no SKIP line, so a silently skipped
// benchmark is invisible in a run that still reports "ok".
// Shape mirrors requireRedisLoad in e2e_load_bench_test.go, which gates the
// *testing.T side of this same package.
func realRedisAddr(b *testing.B) string {
	b.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		// addr never embeds a credential, so it is safe to print here.
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			b.Fatalf("XFLOW_REQUIRE_REDIS_INTEGRATION=1: redis unavailable at %s: %v (set XFLOW_TEST_REDIS_ADDR)", addr, err)
		}
		b.Skipf("redis unavailable at %s: %v", addr, err)
	}
	return addr
}

// waitForCond polls cond() every 2 ms using a ticker (no time.Sleep).
// Returns true if cond() became true within budgetMs milliseconds.
func waitForCond(budgetMs int, cond func() bool) bool {
	if cond() {
		return true
	}
	deadline := time.NewTimer(time.Duration(budgetMs) * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if cond() {
				return true
			}
		case <-deadline.C:
			return cond()
		}
	}
}
