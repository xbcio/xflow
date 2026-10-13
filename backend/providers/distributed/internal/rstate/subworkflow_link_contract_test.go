package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMiniredisSubWorkflowLinkContract runs the shared SubWorkflowLinkStore
// contract suite against the Redis/Lua backend using miniredis.
func TestMiniredisSubWorkflowLinkContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)

	statestoretest.RunSubWorkflowLinkContract(t, func(t *testing.T) statestoretest.SubWorkflowLinkStore {
		rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return New(rdb, nil, time.Minute)
	})
}

// TestRedisSubWorkflowLinkContract runs the shared SubWorkflowLinkStore
// contract suite against a real Redis instance when XFLOW_TEST_REDIS_ADDR is
// set.
func TestRedisSubWorkflowLinkContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunSubWorkflowLinkContract(t, func(t *testing.T) statestoretest.SubWorkflowLinkStore {
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}
