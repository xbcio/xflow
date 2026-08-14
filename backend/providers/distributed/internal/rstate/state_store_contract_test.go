package rstate

import (
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/redis/go-redis/v9"
)

// TestMiniredisStateStoreContract runs the shared StateStore contract suite
// against the Redis/Lua backend so the in-memory and Redis implementations stay
// semantically aligned (terminal protection, lease claim fencing, signal
// consume, resume lock, pub/sub). Previously only the memory backend ran the
// basic contract; the Redis backend only ran the concurrency suite.
func TestMiniredisStateStoreContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	state := New(rdb, nil, time.Minute)
	statestoretest.RunStateStoreContract(t, state)
}

// TestRedisStateStoreContract runs the same suite against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set, matching the group/entry-admission contracts
// which each have both a miniredis and a real-Redis runner.
//
// miniredis is not a substitute here: several contract assertions land entirely
// inside Lua (updateExecutionStatusLua's cancel-aware fencing and its
// non-empty-only error-key write), and miniredis runs a different Lua engine
// than Redis does.
func TestRedisStateStoreContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	statestoretest.RunStateStoreContract(t, New(freshRealRedis(t, addr), nil, time.Minute))
}
