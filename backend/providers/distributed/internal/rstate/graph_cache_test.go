package rstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// storeGraph seeds the persisted graph an execution carries, the way
// CreateExecution writes it, and returns the size LoadGraph will charge it.
func storeGraph(t *testing.T, rdb *redis.Client, id types.ExecutionID) int64 {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "graph-cache-" + string(id),
		Nodes: []types.NodeDef{{Name: "A", Type: "test.a"}},
	})
	if err != nil {
		t.Fatalf("compile graph: %v", err)
	}
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	if err := rdb.Set(context.Background(), execKey(namespace.Default, id, "graph"), raw, 0).Err(); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	return graphWireToHeapFactor * int64(len(raw))
}

func graphKey(id types.ExecutionID) string {
	return execKey(namespace.Default, id, "graph")
}

// TestLoadGraphCachesDecodedGraph proves the load cache is populated: once
// loaded, the graph survives the Redis key disappearing, and the second call
// returns the same decoded pointer rather than decoding again.
func TestLoadGraphCachesDecodedGraph(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	storeGraph(t, rdb, "exec-a")

	first, err := state.LoadGraph(ctx, "exec-a")
	if err != nil {
		t.Fatalf("first LoadGraph: %v", err)
	}
	if first == nil {
		t.Fatal("first LoadGraph returned no graph")
	}

	// With the persisted copy gone, only the in-memory cache can answer.
	if err := rdb.Del(ctx, graphKey("exec-a")).Err(); err != nil {
		t.Fatalf("delete graph key: %v", err)
	}
	second, err := state.LoadGraph(ctx, "exec-a")
	if err != nil {
		t.Fatalf("second LoadGraph: %v", err)
	}
	if second != first {
		t.Error("✓ the second load must come from the cache, same pointer")
	}
}

// TestLoadGraphCacheHonoursCapacity proves New's WithGraphCache option reaches
// the cache: with a byte budget that fits one graph but not two, loading a
// second execution evicts the first, so its next load has to go back to Redis.
func TestLoadGraphCacheHonoursCapacity(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	sizeA := storeGraph(t, rdb, "exec-a")
	sizeB := storeGraph(t, rdb, "exec-b")
	state := New(rdb, nil, time.Minute, WithGraphCache(sizeA+sizeB-1, 0))

	for _, id := range []types.ExecutionID{"exec-a", "exec-b"} {
		if g, err := state.LoadGraph(ctx, id); err != nil || g == nil {
			t.Fatalf("LoadGraph(%q) = %v, %v", id, g, err)
		}
	}

	if err := rdb.Del(ctx, graphKey("exec-a")).Err(); err != nil {
		t.Fatalf("delete graph key: %v", err)
	}
	if g, err := state.LoadGraph(ctx, "exec-a"); err != nil || g != nil {
		t.Errorf("LoadGraph(exec-a) after eviction = %v, %v; want nil, nil (it must re-read Redis)", g, err)
	}
	// exec-b is still cached, so its persisted copy no longer matters.
	if err := rdb.Del(ctx, graphKey("exec-b")).Err(); err != nil {
		t.Fatalf("delete graph key: %v", err)
	}
	if g, err := state.LoadGraph(ctx, "exec-b"); err != nil || g == nil {
		t.Errorf("LoadGraph(exec-b) = %v, %v; want the cached graph", g, err)
	}
}

// TestEvictExecutionCachesDropsGraph pins the terminal-transition path: after
// evictExecutionCaches the graph must be re-read rather than served stale.
func TestEvictExecutionCachesDropsGraph(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	storeGraph(t, rdb, "exec-a")

	if g, err := state.LoadGraph(ctx, "exec-a"); err != nil || g == nil {
		t.Fatalf("LoadGraph = %v, %v", g, err)
	}
	state.evictExecutionCaches("exec-a")
	if err := rdb.Del(ctx, graphKey("exec-a")).Err(); err != nil {
		t.Fatalf("delete graph key: %v", err)
	}
	if g, err := state.LoadGraph(ctx, "exec-a"); err != nil || g != nil {
		t.Errorf("LoadGraph after evict = %v, %v; want nil, nil", g, err)
	}
}

// TestCheckCompletionColdCacheLeavesTheDecisionToTheWriter documents the
// deliberate behaviour change: a replica that never loaded the graph answers
// "nothing to finalize here" instead of decoding the graph solely to build the
// Lua key list. The fenced writer on the replica that does hold the graph
// remains the authority, so an already-complete execution is still finalized —
// just by whichever replica knows the graph.
func TestCheckCompletionColdCacheLeavesTheDecisionToTheWriter(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	storeGraph(t, rdb, "exec-a")

	// The execution is complete in Redis: running, with its only node successful.
	if err := rdb.Set(ctx, execKey(namespace.Default, "exec-a", "status"), string(types.ExecutionStatusRunning), 0).Err(); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	if err := rdb.Set(ctx, nodeStatusKey(namespace.Default, "exec-a", "A"), string(types.NodeStatusSuccess), 0).Err(); err != nil {
		t.Fatalf("seed node status: %v", err)
	}

	// Cold cache (a distinct replica): no finalization, no error, no decode.
	cold := New(rdb, nil, time.Minute)
	done, failed, err := cold.CheckCompletion(ctx, "exec-a", 1)
	if err != nil {
		t.Fatalf("cold CheckCompletion: %v", err)
	}
	if done || failed {
		t.Errorf("cold CheckCompletion = (%v, %v); want no decision", done, failed)
	}
	if got := mustGet(t, rdb, execKey(namespace.Default, "exec-a", "status")); got != string(types.ExecutionStatusRunning) {
		t.Errorf("cold replica changed the status to %q", got)
	}

	// The replica holding the graph finalizes it.
	if g, err := state.LoadGraph(ctx, "exec-a"); err != nil || g == nil {
		t.Fatalf("LoadGraph = %v, %v", g, err)
	}
	done, failed, err = state.CheckCompletion(ctx, "exec-a", 1)
	if err != nil {
		t.Fatalf("warm CheckCompletion: %v", err)
	}
	if !done || failed {
		t.Errorf("warm CheckCompletion = (%v, %v); want (true, false)", done, failed)
	}
	if got := mustGet(t, rdb, execKey(namespace.Default, "exec-a", "status")); got != string(types.ExecutionStatusSuccess) {
		t.Errorf("status after completion = %q, want success", got)
	}
}

func mustGet(t *testing.T, rdb *redis.Client, key string) string {
	t.Helper()
	val, err := rdb.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return val
}
