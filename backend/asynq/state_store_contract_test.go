package asynq

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/contract"
	"github.com/redis/go-redis/v9"
)

// TestRedisStateStoreContract runs the shared StateStore contract suite against
// the Redis/Lua backend so the in-memory and Redis implementations stay
// semantically aligned (terminal protection, lease claim fencing, signal
// consume, resume lock, pub/sub). Previously only the memory backend ran the
// basic contract; the Redis backend only ran the concurrency suite.
func TestRedisStateStoreContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	state := newRedisState(rdb, nil, time.Minute)
	contract.RunStateStoreContract(t, state)
}
