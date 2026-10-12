package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMiniredisExecutionPauseContract runs the shared ExecutionPauser contract
// against the Redis backend.
//
// The pause is enforced inside Lua — a status read added to the claim and read
// scripts — so miniredis proves the scripts execute at all, and
// TestRedisExecutionPauseContract against a real server is what proves the
// refusal means the same thing under a real script engine.
func TestMiniredisExecutionPauseContract(t *testing.T) {
	statestoretest.RunExecutionPauseContract(t, func(t *testing.T) engine.StateStore {
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

// TestRedisExecutionPauseContract runs the same suite against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set.
func TestRedisExecutionPauseContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunExecutionPauseContract(t, func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
