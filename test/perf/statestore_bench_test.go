//go:build perf

// Package perf contains performance benchmarks for the xflow scheduler.
// Run with: go test -tags=perf -bench=BenchmarkStateStore -benchtime=2s ./test/perf/ -timeout 5m
package perf

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// benchGraph is a compiled single-node graph shared across iterations so that
// graph compilation cost is not included in the benchmark measurement.
var benchGraph *graph.Graph

func init() {
	var err error
	benchGraph, err = graph.Compile(&types.WorkflowDef{
		Name:  "bench-wf",
		Nodes: []types.NodeDef{{Name: "n0", Type: "bench.noop"}},
	})
	if err != nil {
		panic("benchGraph compile: " + err.Error())
	}
}

// benchStateStore runs CreateExecution + GetExecution b.N times against addr.
// Setup (distributed.New) happens before b.ResetTimer so it is excluded from the
// measured time.
func benchStateStore(b *testing.B, addr string, skipOnErr bool) {
	b.Helper()
	bk, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		if skipOnErr {
			b.Skipf("redis not reachable at %s: %v", addr, err)
			return
		}
		b.Fatal(err)
	}
	state := bk.State()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := types.ExecutionID(fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i))
		snap := &engine.ExecutionSnapshot{
			ID:     id,
			Graph:  benchGraph,
			Status: types.ExecutionStatusRunning,
		}
		if err := state.CreateExecution(ctx, snap); err != nil {
			b.Fatal(err)
		}
		if _, err := state.GetExecution(ctx, id); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStateStoreMiniredis measures CreateExecution+GetExecution against
// an in-process miniredis instance (no real network I/O).
func BenchmarkStateStoreMiniredis(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	benchStateStore(b, mr.Addr(), false)
}

// BenchmarkStateStoreRealRedis measures CreateExecution+GetExecution against a
// real Redis instance.  Set XFLOW_TEST_REDIS_ADDR (e.g. localhost:6380) before
// running; the benchmark is skipped when the address is unreachable.
func BenchmarkStateStoreRealRedis(b *testing.B) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	benchStateStore(b, addr, true)
}
