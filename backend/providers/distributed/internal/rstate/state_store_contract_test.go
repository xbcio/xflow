package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
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
	addr := realRedisAddr(t)
	statestoretest.RunStateStoreContract(t, New(freshRealRedis(t, addr), nil, time.Minute))
}

// Node lease renewal is what keeps a handler that legitimately outruns the
// engine's default lease TTL from being reclaimed mid-flight. On this backend
// the expiry scan reads a separate ZSET, so renewal has to move the index and
// not just the metadata hash — a distinction the memory backend cannot expose.
func TestMiniredisNodeLeaseRenewContract(t *testing.T) {
	statestoretest.RunNodeLeaseRenewContract(t, func(t *testing.T) statestoretest.NodeLeaseRenewStore {
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

func TestRedisNodeLeaseRenewContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunNodeLeaseRenewContract(t, func(t *testing.T) statestoretest.NodeLeaseRenewStore {
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
