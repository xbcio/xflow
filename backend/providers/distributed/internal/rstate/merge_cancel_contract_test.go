package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// TestMiniredisMergeCancelContract runs the shared merge-cancel contract
// against the Redis/Lua backend under miniredis: the fourth fenced System
// commit shape (canceled) decided inside commitNodeLua, and the MergeCancel
// intent that rides the advance intent's slot.
//
// The suite drives the store directly. The engine has no consumer for
// TaskTypeMergeCancel intents in this package's tests, so nothing here may let
// an engine flush the outbox.
func TestMiniredisMergeCancelContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunMergeCancelContract(t, rstateCapabilityManifest(), New(rdb, nil, time.Minute))
}

// TestRedisMergeCancelContract runs the same contract against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set. miniredis runs a different Lua engine than
// Redis does, and the fence this contract pins lives entirely inside Lua.
func TestRedisMergeCancelContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunMergeCancelContract(t, rstateCapabilityManifest(), New(freshRealRedis(t, addr), nil, time.Minute))
}
