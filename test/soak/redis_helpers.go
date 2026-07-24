//go:build soak

package soak

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// envOr returns the env var key's value, or def when unset/empty. Mirrors
// test/integration/harness.go:envOr so the soak package stays self-contained
// (it cannot import the integration package's unexported helpers).
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// redisAddr resolves the target Redis address from XFLOW_TEST_REDIS_ADDR (set
// by the test/env podman harness: Redis on host port 6380) or REDIS_PORT,
// falling back to localhost:6379. Mirrors test/integration/harness.go.
func redisAddr() string {
	if v := os.Getenv("XFLOW_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	if p := os.Getenv("REDIS_PORT"); p != "" {
		return "localhost:" + p
	}
	return "localhost:6379"
}

// flushAsynqKeys deletes only asynq's own key namespace ("asynq:*") rather
// than the whole Redis DB, mirroring test/integration/harness.go so a soak
// run does not disturb keys other tests (e.g. leader election) may use in
// the same DB. Used by real-environment soak entry points to reset asynq
// queues between iterations; not invoked by the miniredis in-process smoke
// (miniredis is fresh per test).
//
//nolint:unused // retained for real-env soak iteration reset (Task 5.3 / future real-env fault runs)
func flushAsynqKeys(ctx context.Context, t interface {
	Helper()
	Fatalf(format string, args ...any)
}, rdb *redis.Client) {
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

// pingRedis returns nil if addr is reachable within the timeout. Used by
// RequireRedis (real-environment soak preflight).
func pingRedis(addr string, timeout time.Duration) error {
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Ping(ctx).Err()
}

// closeBackendRdb best-effort closes the Redis client underlying a
// distributed.Backend. The Backend has no exported Close: on the success path
// ControlPlane.Shutdown closes rdb via its unbind func; this helper is only
// for the failure-rollback path ( apiserver.Start failed before the unbind func
// was installed). The RedisClient() capability returns a redis.Cmdable; the
// concrete value (*redis.Client / *redis.ClusterClient) also satisfies
// io.Closer.
func closeBackendRdb(b interface{ RedisClient() redis.Cmdable }) {
	if b == nil {
		return
	}
	if c, ok := b.RedisClient().(io.Closer); ok {
		_ = c.Close()
	}
}
