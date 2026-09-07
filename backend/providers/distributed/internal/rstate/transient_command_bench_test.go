package rstate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// countCmds runs fn against a fresh transient-mode Store on a miniredis
// whose pre-hook counts Redis commands, returning the total command count and
// the SMEMBERS count issued during fn.
func countCmds(t testing.TB, transient bool, fn func(state *Store, id types.ExecutionID)) (total, smembers int) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	var mu sync.Mutex
	var tot, sm int
	mr.Server().SetPreHook(server.Hook(func(_ *server.Peer, cmd string, _ ...string) bool {
		mu.Lock()
		defer mu.Unlock()
		tot++
		if strings.ToLower(cmd) == "smembers" {
			sm++
		}
		return false
	}))
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = transient
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-bench")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: testGraphOneNode(),
	}); err != nil {
		t.Fatal(err)
	}
	// Reset counts after setup so only fn's mutation traffic is measured.
	mu.Lock()
	tot, sm = 0, 0
	mu.Unlock()

	fn(state, id)

	mu.Lock()
	defer mu.Unlock()
	return tot, sm
}

// BenchmarkTransientNodeMutationRedisCommands measures the Redis command count
// for a single transient node status mutation (UpsertNode). It reports
// redis_cmds/mutation and smembers/mutation. After optimization 4 the
// per-mutation refresh is a no-op, so transient issues zero SMEMBERS and the
// same 3 commands as the default mode (upsertNodeLua + meta HSet + lease ZADD/
// ZREM) — the per-mutation transient bookkeeping overhead is fully eliminated.
func BenchmarkTransientNodeMutationRedisCommands(b *testing.B) {
	ctx := context.Background()
	total, smembers := countCmds(b, true, func(state *Store, id types.ExecutionID) {
		for range b.N {
			if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
				ExecutionID: id, Name: "start", NodeIdx: 0, Status: types.NodeStatusRunning,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(total)/float64(b.N), "redis_cmds/mutation")
	b.ReportMetric(float64(smembers)/float64(b.N), "smembers/mutation")
	if smembers != 0 {
		b.Fatalf("transient mutation issued %d SMEMBERS, want 0", smembers)
	}
}

// BenchmarkDefaultNodeMutationRedisCommands is the non-transient counterpart: it
// issues zero transient-TTL bookkeeping commands (refreshTransientTTL is a
// no-op when !s.transient), establishing the floor the transient path is
// measured against.
func BenchmarkDefaultNodeMutationRedisCommands(b *testing.B) {
	ctx := context.Background()
	total, smembers := countCmds(b, false, func(state *Store, id types.ExecutionID) {
		for range b.N {
			if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
				ExecutionID: id, Name: "start", NodeIdx: 0, Status: types.NodeStatusRunning,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(total)/float64(b.N), "redis_cmds/mutation")
	b.ReportMetric(float64(smembers)/float64(b.N), "smembers/mutation")
	if smembers != 0 {
		b.Fatalf("default mutation issued %d SMEMBERS, want 0", smembers)
	}
}

// twoNodeLinearGraph compiles start -> next, an acyclic 2-node DAG. Committing
// node 0 leaves one node remaining, so CommitNode writes an advance outbox
// entry — the exact write the round-2 outbox-coalescing (item 4) removes.
func twoNodeLinearGraph(tb testing.TB) *graph.Graph {
	tb.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "bench-two-node",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		tb.Fatalf("Compile() error = %v", err)
	}
	return g
}

// armedRecorder counts Redis commands only between arm() and disarm(), so a
// benchmark can exclude per-iteration setup (CreateExecution) and measure only
// the node hot path.
type armedRecorder struct {
	mu    sync.Mutex
	armed bool
	total int
	byCmd map[string]int
}

func (r *armedRecorder) hook(_ *server.Peer, cmd string, _ ...string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.armed {
		return false
	}
	r.total++
	r.byCmd[strings.ToLower(cmd)]++
	return false
}

func (r *armedRecorder) arm()    { r.mu.Lock(); r.armed = true; r.mu.Unlock() }
func (r *armedRecorder) disarm() { r.mu.Lock(); r.armed = false; r.mu.Unlock() }

// benchNodeCommitCycle measures the Redis command count of one node's core hot
// path — AcquireTaskLease + CommitNode(with advance) + ListOutbox + AckOutbox —
// against a fresh execution per iteration. Per-iteration CreateExecution runs
// disarmed so only the hot path is counted. This is the round-2 (item 4/5)
// write-amplification baseline: outbox-into-Lua coalescing and per-node key
// consolidation are measured against redis_cmds/cycle here.
func benchNodeCommitCycle(b *testing.B, transient bool) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	defer mr.Close()
	rec := &armedRecorder{byCmd: map[string]int{}}
	mr.Server().SetPreHook(server.Hook(rec.hook))

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = transient
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	g := twoNodeLinearGraph(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := types.ExecutionID(fmt.Sprintf("bench-cycle-%d", i))
		createEntry := engine.OutboxEntry{
			ID:   "exec/" + string(id) + "/start/0",
			Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		}
		if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID:     id,
			Graph:  g,
			Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{createEntry}); err != nil {
			b.Fatal(err)
		}

		rec.arm()
		// Drain the initial exec intent (the engine's FlushOutbox does this).
		drainOutbox(b, state, ctx, id)

		lease := &engine.TaskLease{
			LeaseID:    "lease",
			LeaseToken: "token",
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task:       engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		}
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			b.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
		}
		advance := &engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeAdvance}
		if _, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			LeaseID:     "lease",
			LeaseToken:  "token",
			Attempt:     1,
			Status:      types.NodeStatusSuccess,
			Output:      map[string]any{"ok": true},
			StoreOutput: true,
			Port:        "main",
			AdvanceTask: advance,
		}); err != nil {
			b.Fatalf("CommitNode() error = %v", err)
		}
		// Drain the advance intent produced by the commit.
		drainOutbox(b, state, ctx, id)
		rec.disarm()
	}
	b.StopTimer()

	if rec.total == 0 {
		b.Fatal("commit cycle issued 0 Redis commands, recorder likely not armed")
	}
	b.ReportMetric(float64(rec.total)/float64(b.N), "redis_cmds/cycle")
}

func drainOutbox(b *testing.B, state *Store, ctx context.Context, id types.ExecutionID) {
	b.Helper()
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Minute), 16)
	if err != nil {
		b.Fatalf("ListOutbox() error = %v", err)
	}
	for _, e := range entries {
		if err := state.AckOutbox(ctx, id, e.ID); err != nil {
			b.Fatalf("AckOutbox() error = %v", err)
		}
	}
}

// BenchmarkTransientNodeCommitCycleRedisCommands measures the per-node commit
// cycle command count in transient mode. After optimization 4 (no-op
// refreshTransientTTL) it is 43 redis_cmds/cycle — identical to the default-mode
// floor below; the ~10-command transient write amplification (SADD + structural
// EXPIRE + keyset EXPIRE in AcquireTaskLease's refresh) is gone.
func BenchmarkTransientNodeCommitCycleRedisCommands(b *testing.B) { benchNodeCommitCycle(b, true) }

// BenchmarkDefaultNodeCommitCycleRedisCommands is the non-transient counterpart,
// establishing the default-mode command floor for the same hot path.
func BenchmarkDefaultNodeCommitCycleRedisCommands(b *testing.B) { benchNodeCommitCycle(b, false) }
