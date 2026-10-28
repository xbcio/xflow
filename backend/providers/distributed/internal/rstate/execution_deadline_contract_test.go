package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMiniredisExecutionDeadlineContract runs the shared ExecutionDeadlineExpirer
// contract against the Redis/Lua backend so its register-before-write index,
// its withdrawal on a terminal write and its keep-until-ack timeout cannot drift
// from the in-memory reference.
func TestMiniredisExecutionDeadlineContract(t *testing.T) {
	statestoretest.RunExecutionDeadlineContract(t, rstateCapabilityManifest(), func(t *testing.T) engine.StateStore {
		t.Helper()
		srv, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run() error = %v", err)
		}
		t.Cleanup(srv.Close)
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// TestRedisExecutionDeadlineContract runs the same suite against a real Redis
// when XFLOW_TEST_REDIS_ADDR is set. Worth pinning against the server the
// deployment actually runs: the index orders members by score and then by
// member bytes, and the score is a millisecond epoch carried as a float64.
func TestRedisExecutionDeadlineContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunExecutionDeadlineContract(t, rstateCapabilityManifest(), func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
