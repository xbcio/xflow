package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMiniredisExecutionSeedContract runs the shared ExecutionSeeder contract
// against the Redis backend.
//
// The whole seed — the resolutions, the "seed" scheduling markers and the
// fan-in they gate — happens inside one Lua script, so miniredis proves the
// script executes at all, and TestRedisExecutionSeedContract against a real
// server is what proves the two passes over its key blocks mean the same thing
// under a real script engine.
func TestMiniredisExecutionSeedContract(t *testing.T) {
	statestoretest.RunExecutionSeedContract(t, func(t *testing.T) engine.StateStore {
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

// TestRedisExecutionSeedContract runs the same suite against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set.
func TestRedisExecutionSeedContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunExecutionSeedContract(t, func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
