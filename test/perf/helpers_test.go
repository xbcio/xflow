//go:build perf

package perf

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func realRedisAddr(b *testing.B) string {
	b.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
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
