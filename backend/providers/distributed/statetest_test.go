package distributed

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newRedisStateTestClient returns a redis.Client backed by an ephemeral
// miniredis instance for backend-level tests. The state-store's own copy of
// this helper lives in the internal/rstate package; this one serves the
// tests that exercise the distributed.Backend facade directly.
func newRedisStateTestClient(t *testing.T) *redis.Client {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
