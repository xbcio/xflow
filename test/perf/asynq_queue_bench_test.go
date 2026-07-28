//go:build perf

// Package perf contains throughput benchmarks for the Asynq queue backend.
// Run with:
//
//	XFLOW_TEST_REDIS_ADDR=localhost:6380 go test -tags=perf -bench=BenchmarkAsynqQueue -benchtime=2s ./test/perf/ -timeout 5m
package perf

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// BenchmarkAsynqQueueEnqueueRealRedis measures raw Enqueue throughput against a
// real Redis instance (consumer disabled — no competing reader).
func BenchmarkAsynqQueueEnqueueRealRedis(b *testing.B) {
	addr := realRedisAddr(b)
	bk, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		b.Fatal(err)
	}
	q := bk.Queue()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := &engine.Task{
			ExecutionID: types.ExecutionID(fmt.Sprintf("q-%d", i)),
			NodeName:    "n",
		}
		if err := q.Enqueue(ctx, t); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAsynqQueueEnqueueConsumeRealRedis measures end-to-end
// enqueue-to-consume throughput: b.N tasks are enqueued and the benchmark
// waits (via ticker, no time.Sleep) until all have been processed.
func BenchmarkAsynqQueueEnqueueConsumeRealRedis(b *testing.B) {
	addr := realRedisAddr(b)
	bk, err := distributed.New(addr, nil, distributed.WithConcurrency(4), distributed.WithConsumer(true))
	if err != nil {
		b.Fatal(err)
	}

	eng := engine.New(bk.State(), bk.Queue())

	var processed int64
	stop := bk.BindHandler(eng, func(_ context.Context, _ *engine.Task) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})
	defer stop()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := &engine.Task{
			ExecutionID: types.ExecutionID(fmt.Sprintf("qc-%d", i)),
			NodeName:    "n",
		}
		if err := bk.Queue().Enqueue(ctx, t); err != nil {
			b.Fatal(err)
		}
	}
	// Wait for consumer to catch up (ticker-based poll, no time.Sleep).
	b.StopTimer()
	if !waitForCond(10_000, func() bool {
		return atomic.LoadInt64(&processed) >= int64(b.N)
	}) {
		b.Fatalf("timed out: processed %d < %d", atomic.LoadInt64(&processed), b.N)
	}
}
