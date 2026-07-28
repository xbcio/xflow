//go:build integration

package integration

import (
	"context"
	"fmt"
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
	if v := os.Getenv("XFLOW_TEST_MYSQL_DSN"); v != "" {
		return v
	}
	port := envOr("MYSQL_PORT", "3306")
	pw := envOr("MYSQL_ROOT_PASSWORD", "xflow")
	db := envOr("MYSQL_DATABASE", "xflow")
	return fmt.Sprintf("root:%s@tcp(localhost:%s)/%s?parseTime=true&multiStatements=true", pw, port, db)
}

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	def := "localhost:" + envOr("KAFKA_PORT", "9092")
	raw := envOr("XFLOW_TEST_KAFKA_BROKERS", def)
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

// requireRedis returns the Redis address, skipping the test when Redis is
// unreachable. Under XFLOW_REQUIRE_REDIS_INTEGRATION=1 (CI gating mode) it
// fails the test instead, so a missing dependency cannot be mistaken for a
// passing gate (per 2026-07-18 remediation §6.3).
func requireRedis(t *testing.T) string {
	t.Helper()
	addr := redisAddr(t)
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_REDIS_INTEGRATION=1: redis unavailable at %s: %v (run `make env-up`)", addr, err)
		}
		t.Skipf("redis unavailable at %s: %v (run `make env-up`)", addr, err)
	}
	return addr
}

func requireMySQL(t *testing.T) string {
	t.Helper()
	dsn := mysqlDSN(t)
	// lazy import to avoid pulling driver into non-integration builds
	if err := pingMySQL(dsn); err != nil {
		// Do not echo the DSN: it embeds MYSQL_ROOT_PASSWORD. Print only the
		// host:port so a skipped test does not leak the credential.
		port := envOr("MYSQL_PORT", "3306")
		// Under XFLOW_REQUIRE_MYSQL_INTEGRATION=1 (CI gating mode) fail the test
		// instead of skipping, so a missing MySQL dependency cannot be mistaken
		// for a passing gate (R7, mirrors Redis required mode).
		if os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_MYSQL_INTEGRATION=1: mysql unavailable at localhost:%s: %v (run `make env-up && make env-migrate`)", port, err)
		}
		t.Skipf("mysql unavailable at localhost:%s: %v (run `make env-up && make env-migrate`)", port, err)
	}
	return dsn
}

func requireKafka(t *testing.T) []string {
	t.Helper()
	brokers := kafkaBrokers(t)
	if err := pingKafka(brokers); err != nil {
		// Under XFLOW_REQUIRE_KAFKA_INTEGRATION=1 (CI gating mode) fail the test
		// instead of skipping, so a missing Kafka dependency cannot be mistaken
		// for a passing gate (R7, mirrors Redis required mode).
		if os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: kafka unavailable (%v): %v (run `make env-up`)", brokers, err)
		}
		t.Skipf("kafka unavailable (%v): %v (run `make env-up`)", brokers, err)
	}
	return brokers
}

// flushAsynqKeys deletes only asynq's own key namespace ("asynq:*") rather
// than the whole Redis DB, so it does not disturb keys other integration
// tests (e.g. leader election) may be using concurrently in the same DB.
// flushAsynqKeys deletes all asynq:* keys so a stale task from a prior crashed
// run cannot race the consumer this harness starts.
func flushAsynqKeys(ctx context.Context, t *testing.T, rdb *redis.Client) {
	t.Helper()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "asynq:*", 500).Result()
		if err != nil {
			t.Fatalf("scan asynq keys: %v", err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("del asynq keys: %v", err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

// flushXflowKeys deletes all xflow:* keys so stale runner directory entries,
// assignment queues, and execution state from prior crashed runs cannot
// interfere with the current test. Used by the production harness which
// accumulates state across subtests and needs a clean slate.
func flushXflowKeys(ctx context.Context, t *testing.T, rdb *redis.Client) {
	t.Helper()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "xflow:*", 500).Result()
		if err != nil {
			t.Fatalf("scan xflow keys: %v", err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("del xflow keys: %v", err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

// --- completion polling (no time.Sleep) ---

// outputNodes lists the node names whose outputs should be collected into
// types.Result.Output; if none are supplied the Output map is empty.
func waitForCompletion(ctx context.Context, t *testing.T, state engine.StateStore, id types.ExecutionID, outputNodes ...string) types.Result {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		if err == nil && types.IsTerminalExecutionStatus(snap.Status) {
			out := map[string]any{}
			for _, n := range outputNodes {
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
