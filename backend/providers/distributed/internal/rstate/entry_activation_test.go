package rstate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/redis/go-redis/v9"
)

// TestMiniredisEntryActivationContract runs the shared EntryActivationStore
// contract against the Redis/Lua backend using miniredis (no external deps).
func TestMiniredisEntryActivationContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunEntryActivationContract(t, func(t *testing.T) engine.EntryActivationStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return NewEntryActivationStore(rdb, time.Minute)
	})
}

// TestRedisEntryActivationContract runs the shared EntryActivationStore contract
// against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisEntryActivationContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	// Skip cleanly when the addr is set but the server is unreachable (the
	// podman env may be down) rather than failing every subtest.
	probe := redis.NewClient(&redis.Options{Addr: addr})
	pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := probe.Ping(pctx).Err(); err != nil {
		_ = probe.Close()
		t.Skipf("redis at %s unreachable: %v", addr, err)
	}
	_ = probe.Close()
	statestoretest.RunEntryActivationContract(t, func(t *testing.T) engine.EntryActivationStore {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() {
			_ = rdb.FlushDB(t.Context()).Err()
			_ = rdb.Close()
		})
		return NewEntryActivationStore(rdb, time.Minute)
	})
}
