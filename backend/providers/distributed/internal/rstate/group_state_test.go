package rstate

import (
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/redis/go-redis/v9"
)

// TestMiniredisGroupStateContract runs the shared GroupStateStore contract suite
// against the Redis/Lua backend using miniredis (no external dependencies).
func TestMiniredisGroupStateContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunGroupStateContract(t, func(t *testing.T) statestoretest.GroupStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// freshRealRedis returns a client to the real Redis at addr with the database
// flushed. The shared contracts require a fresh, empty store per subtest, and
// their keys are deterministic, so leftovers from an earlier aborted run would
// otherwise collide and fail the run spuriously. Flushing on entry (not only on
// cleanup) makes a rerun independent of how the previous run ended.
func freshRealRedis(t *testing.T, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}
	t.Cleanup(func() {
		_ = rdb.FlushDB(t.Context()).Err()
		_ = rdb.Close()
	})
	return rdb
}

// TestRedisGroupStateContract runs the shared GroupStateStore contract suite
// against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisGroupStateContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	statestoretest.RunGroupStateContract(t, func(t *testing.T) statestoretest.GroupStore {
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}

// TestMiniredisEntryAdmissionContract runs the EntryAdmissionStore contract
// suite against the Redis/Lua backend using miniredis.
func TestMiniredisEntryAdmissionContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunEntryAdmissionContract(t, func(t *testing.T) statestoretest.EntryAdmissionTestStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// TestRedisEntryAdmissionContract runs the EntryAdmissionStore contract
// suite against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisEntryAdmissionContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	statestoretest.RunEntryAdmissionContract(t, func(t *testing.T) statestoretest.EntryAdmissionTestStore {
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
