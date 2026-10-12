package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
)

// TestMiniredisConcurrencyAdmissionContract runs the shared
// ConcurrencyLimitedStateStore contract against the Redis backend.
//
// miniredis runs a different Lua engine than Redis does, so this proves the
// scripts at least execute; TestRedisConcurrencyAdmissionContract against a real
// server is what proves they mean the same thing there.
func TestMiniredisConcurrencyAdmissionContract(t *testing.T) {
	statestoretest.RunConcurrencyAdmissionContract(t, func(t *testing.T) engine.StateStore {
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

// TestMiniredisConcurrencyQueueContract runs the queue half of the shared
// contract against the Redis backend.
func TestMiniredisConcurrencyQueueContract(t *testing.T) {
	statestoretest.RunConcurrencyQueueContract(t, func(t *testing.T) engine.StateStore {
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

// TestMiniredisConcurrencySeedContract runs the entry-seed half of the shared
// contract against the Redis backend: a trigger-group seed contends for a slot,
// and queues, exactly as a submission does. The seed path reaches the slot set
// through two transitions instead of one, so this is where that split is
// exercised.
func TestMiniredisConcurrencySeedContract(t *testing.T) {
	statestoretest.RunConcurrencySeedContract(t, func(t *testing.T) engine.StateStore {
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

// TestRedisConcurrencySeedContract runs the same suite against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set.
func TestRedisConcurrencySeedContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunConcurrencySeedContract(t, func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}

// TestRedisConcurrencyQueueContract runs the queue half of the same suite
// against a real Redis when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisConcurrencyQueueContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunConcurrencyQueueContract(t, func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}

// TestRedisConcurrencyAdmissionContract runs the same suite against a real
// Redis when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisConcurrencyAdmissionContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunConcurrencyAdmissionContract(t, func(t *testing.T) engine.StateStore {
		t.Helper()
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
