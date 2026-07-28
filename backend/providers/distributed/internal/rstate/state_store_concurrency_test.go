//go:build concurrency

// Spec: .claude/specs/lua-concurrency-tests.md
package rstate

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/redis/go-redis/v9"
)

func TestRedisStateStore_Concurrency(t *testing.T) {
	statestoretest.RunStateStoreConcurrencySuite(t, func(t *testing.T) engine.StateStore {
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
