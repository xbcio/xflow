package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMiniredisIdempotencyContract runs the shared engine.IdempotentStateStore
// contract against the Redis backend on miniredis. miniredis only expires keys
// when told to, so the harness fast-forwards it alongside the wall-clock sleep
// the claim lease is judged by.
func TestMiniredisIdempotencyContract(t *testing.T) {
	statestoretest.RunIdempotencyContract(t, func(t *testing.T) statestoretest.IdempotencyHarness {
		t.Helper()
		srv, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run() error = %v", err)
		}
		t.Cleanup(srv.Close)
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return statestoretest.IdempotencyHarness{
			State: New(rdb, nil, time.Minute),
			Elapse: func(d time.Duration) {
				time.Sleep(d)
				srv.FastForward(d)
			},
		}
	})
}

// TestRedisIdempotencyContract runs the same suite against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set: miniredis runs a different Lua engine, so this
// is what proves the claim script means the same thing on a server.
func TestRedisIdempotencyContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunIdempotencyContract(t, func(t *testing.T) statestoretest.IdempotencyHarness {
		t.Helper()
		return statestoretest.IdempotencyHarness{State: New(freshRealRedis(t, addr), nil, time.Minute)}
	})
}
