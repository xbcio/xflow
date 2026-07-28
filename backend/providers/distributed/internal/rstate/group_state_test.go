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

// TestRedisGroupStateContract runs the shared GroupStateStore contract suite
// against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisGroupStateContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	statestoretest.RunGroupStateContract(t, func(t *testing.T) statestoretest.GroupStore {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
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
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// TestMiniredisGroupSuspendContract runs the shared GroupSuspend contract suite
// against the Redis/Lua backend using miniredis (no external dependencies).
func TestMiniredisGroupSuspendContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunGroupSuspendContract(t, func(t *testing.T) statestoretest.GroupSuspendTestStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// TestRedisGroupSuspendContract runs the shared GroupSuspend contract suite
// against a real Redis instance when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisGroupSuspendContract(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XFLOW_TEST_REDIS_ADDR unset; set 127.0.0.1:6380 for the podman env")
	}
	statestoretest.RunGroupSuspendContract(t, func(t *testing.T) statestoretest.GroupSuspendTestStore {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}
