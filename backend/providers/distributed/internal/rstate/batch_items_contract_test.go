package rstate

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
)

// batchItemsContractInlineMax is small enough that the contract's large arrays
// are a few KiB rather than the production 64KiB, and large enough that a
// one-element array stays inline.
const batchItemsContractInlineMax = 256

// TestMiniredisBatchItemsContract runs the shared batch-items contract against
// the Redis backend with a blob store configured, which is the only shape in
// which the capability writes anything.
func TestMiniredisBatchItemsContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunBatchItemsContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.BatchItemsBackend {
		f := newBlobFixture(t, rdb, blobFixtureOptions{InlineMax: batchItemsContractInlineMax})
		return statestoretest.BatchItemsBackend{Store: f.state, InlineMax: batchItemsContractInlineMax, Namespaced: true}
	})
}

// TestRedisBatchItemsContract runs the same contract against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set: ExternalizeItems reads the persisted retention
// record before linking, and that read is a real command.
func TestRedisBatchItemsContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunBatchItemsContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.BatchItemsBackend {
		f := newBlobFixture(t, freshRealRedis(t, addr), blobFixtureOptions{InlineMax: batchItemsContractInlineMax})
		return statestoretest.BatchItemsBackend{Store: f.state, InlineMax: batchItemsContractInlineMax, Namespaced: true}
	})
}
