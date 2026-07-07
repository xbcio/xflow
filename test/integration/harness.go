//go:build integration

package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// --- env discovery ---

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func redisAddr(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("XFLOW_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	if p := os.Getenv("REDIS_PORT"); p != "" {
		return "localhost:" + p
	}
	return "localhost:6379"
}

func mysqlDSN(t *testing.T) string {
	t.Helper()
	return envOr("XFLOW_TEST_MYSQL_DSN", "root:xflow@tcp(localhost:3306)/xflow?parseTime=true&multiStatements=true")
}

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	raw := envOr("XFLOW_TEST_KAFKA_BROKERS", "localhost:9092")
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if b := strings.TrimSpace(p); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// --- reachability gates (skip, not fail) ---

func requireRedis(t *testing.T) string {
	t.Helper()
	addr := redisAddr(t)
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v (run `make env-up`)", addr, err)
	}
	return addr
}

func requireMySQL(t *testing.T) string {
	t.Helper()
	dsn := mysqlDSN(t)
	// lazy import to avoid pulling driver into non-integration builds
	if err := pingMySQL(dsn); err != nil {
		t.Skipf("mysql unavailable (%s): %v (run `make env-up && make env-migrate`)", dsn, err)
	}
	return dsn
}

func requireKafka(t *testing.T) []string {
	t.Helper()
	brokers := kafkaBrokers(t)
	if err := pingKafka(brokers); err != nil {
		t.Skipf("kafka unavailable (%v): %v (run `make env-up`)", brokers, err)
	}
	return brokers
}

// --- completion polling (no time.Sleep) ---

func isTerminal(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout:
		return true
	}
	return false
}

// waitForCompletion polls GetExecution until terminal or ctx deadline.
func waitForCompletion(ctx context.Context, t *testing.T, state engine.StateStore, id types.ExecutionID) types.Result {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		if err == nil && isTerminal(snap.Status) {
			out := map[string]any{}
			for _, n := range []string{"start"} {
				if v, e := state.GetOutput(ctx, id, n); e == nil && v != nil {
					out[n] = v
				}
			}
			return types.Result{ExecutionID: id, Status: snap.Status, Output: out}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for execution %s: %v", id, ctx.Err())
		case <-ticker.C:
		}
	}
}
