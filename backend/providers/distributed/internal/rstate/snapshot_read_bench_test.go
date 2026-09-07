package rstate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// realRedisBench returns a flushed client to the real Redis at
// XFLOW_TEST_REDIS_ADDR, or skips. These benchmarks exist to measure network
// round trips, so miniredis (in-process, no socket) would report the one number
// they are not asking about.
func realRedisBench(b *testing.B) *redis.Client {
	b.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		b.Skip("XFLOW_TEST_REDIS_ADDR not set: round-trip benchmarks need a real socket")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		b.Fatalf("FlushDB() error = %v", err)
	}
	b.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// BenchmarkGetExecution measures the cost of assembling one execution snapshot
// from the eight per-field keys it is stored across.
//
// It exists so the pipelining in GetExecution stays falsifiable. Reverting that
// function to eight sequential GETs is measurable here and nowhere else in the
// suite -- the returned snapshot is identical either way, so no correctness test
// can tell the two apart.
//
// Measured against Redis in a podman VM (~150us per round trip, which is the
// interesting regime -- pure loopback would understate it, and production runs
// over a real network). Three interleaved A/B rounds, each -count=3:
//
//	pipelined:  151/170/190  148/149/159  166/188/133  us/op
//	sequential: 1223/1269/1216  1243/1171/1261  1208/1281/1222 us/op
//
// About 7.7x, which is the 8-to-1 round-trip ratio almost exactly. The intervals
// do not overlap: the slowest pipelined run is still 6x faster than the fastest
// sequential one. Arms were alternated rather than run A-then-B, because an
// earlier attempt at this comparison silently measured the same code twice --
// the two arms differ by a commit, not by a flag, so `git stash` on an
// already-committed file produced no stash and no error.
func BenchmarkGetExecution(b *testing.B) {
	s := New(realRedisBench(b), nil, time.Minute)
	ctx := context.Background()
	id := types.ExecutionID("bench-get-execution")
	if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning,
		Params: map[string]any{"a": 1}, TraceID: "tr", SpanID: "sp",
	}); err != nil {
		b.Fatalf("CreateExecution() error = %v", err)
	}
	for b.Loop() {
		// Asserted, not discarded: a pipeline that dropped fields would
		// otherwise report a smaller number and look like an improvement.
		snap, err := s.GetExecution(ctx, id)
		if err != nil || snap == nil || snap.TraceID != "tr" || snap.SpanID != "sp" {
			b.Fatalf("GetExecution() = %+v, %v", snap, err)
		}
	}
}

// BenchmarkGetNode is the same measurement for the node snapshot: a status GET
// and a meta HGETALL, two round trips before pipelining and one after. Two
// interleaved rounds, each -count=3:
//
//	pipelined:  162/206/159  154/187/150 us/op
//	sequential: 293/293/367  382/306/303 us/op
//
// About 1.9x, again the round-trip ratio. Read next to BenchmarkGetExecution it
// says something the two numbers alone do not: one round trip costs ~150us here
// whether it carries two commands or eight, so on this path the payload is
// noise and the hop is the whole cost.
func BenchmarkGetNode(b *testing.B) {
	s := New(realRedisBench(b), nil, time.Minute)
	ctx := context.Background()
	g := testGraphOneNode()
	id := types.ExecutionID("bench-get-node")
	if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		b.Fatalf("CreateExecution() error = %v", err)
	}
	idx, _ := g.NodeIndex("start")
	if _, ok, err := s.AcquireTaskLease(ctx, &engine.TaskLease{
		LeaseID: "L", LeaseToken: "T", TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}); err != nil || !ok {
		b.Fatalf("AcquireTaskLease() ok=%v err=%v", ok, err)
	}
	for b.Loop() {
		ns, err := s.GetNode(ctx, id, "start")
		if err != nil || ns == nil || ns.LeaseToken != "T" {
			b.Fatalf("GetNode() = %+v, %v", ns, err)
		}
	}
}
