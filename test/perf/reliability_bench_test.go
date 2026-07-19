//go:build perf

package perf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

var errPerfQueueUnavailable = errors.New("performance queue unavailable")

// perfSwitchableQueue retains durable intents while a fixture is being built,
// then delegates delivery to the real Asynq queue during the measured drain.
type perfSwitchableQueue struct {
	mu       sync.RWMutex
	delegate engine.TaskQueue
	err      error
}

func (q *perfSwitchableQueue) Enqueue(ctx context.Context, task *engine.Task) error {
	return q.enqueue(ctx, task, 0, false)
}

func (q *perfSwitchableQueue) EnqueueDelayed(ctx context.Context, task *engine.Task, delay time.Duration) error {
	return q.enqueue(ctx, task, delay, true)
}

func (q *perfSwitchableQueue) enqueue(ctx context.Context, task *engine.Task, delay time.Duration, delayed bool) error {
	q.mu.RLock()
	delegate, err := q.delegate, q.err
	q.mu.RUnlock()
	if err != nil {
		return err
	}
	if delegate == nil {
		return errPerfQueueUnavailable
	}
	if delayed {
		return delegate.EnqueueDelayed(ctx, task, delay)
	}
	return delegate.Enqueue(ctx, task)
}

func (q *perfSwitchableQueue) failDelivery() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.delegate = nil
	q.err = errPerfQueueUnavailable
}

func (q *perfSwitchableQueue) deliverTo(delegate engine.TaskQueue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.delegate = delegate
	q.err = nil
}

// perfDiscardQueue makes state-only fixtures deterministic without adding an
// Asynq delivery cost to the lease-sweep benchmark.
type perfDiscardQueue struct{}

func (perfDiscardQueue) Enqueue(context.Context, *engine.Task) error { return nil }
func (perfDiscardQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return nil
}

func BenchmarkRedisCommitNode(b *testing.B) {
	ctx := context.Background()
	backend, rdb := newPerfRedisBackend(b)
	queue := &perfSwitchableQueue{}
	queue.failDelivery()
	eng := engine.New(backend.State(), queue)
	b.Cleanup(backend.Bind(eng))

	state := requirePerfAtomicState(b, backend.State())
	workflow := perfIndependentGraph(b, "commit-node", 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		queue.failDelivery()
		id := submitPerfWorkflow(b, ctx, eng, workflow)
		entries := listPerfOutbox(b, ctx, state, id, 1)
		if len(entries) != 1 {
			b.Fatalf("initial outbox entries = %d, want 1", len(entries))
		}
		lease, err := eng.BuildTaskLease(ctx, &entries[0].Task)
		if err != nil {
			b.Fatalf("BuildTaskLease() error = %v", err)
		}

		b.StartTimer()
		result, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID:  lease.Task.ExecutionID,
			NodeName:     lease.Task.NodeName,
			NodeIdx:      lease.Task.NodeIdx,
			ActivationID: lease.Task.ActivationID,
			AutoDepth:    lease.Task.AutoDepth,
			LeaseID:      lease.LeaseID,
			LeaseToken:   lease.LeaseToken,
			Attempt:      lease.Attempt,
			Status:       types.NodeStatusSuccess,
			Output:       map[string]any{"ok": true},
			StoreOutput:  true,
			Port:         "main",
			AdvanceTask: &engine.Task{
				ExecutionID:  lease.Task.ExecutionID,
				NodeName:     lease.Task.NodeName,
				NodeIdx:      lease.Task.NodeIdx,
				Type:         engine.TaskTypeNodeAdvance,
				ActivationID: lease.Task.ActivationID,
				AutoDepth:    lease.Task.AutoDepth,
			},
		})
		b.StopTimer()
		if err != nil {
			b.Fatalf("CommitNode() error = %v", err)
		}
		if !result.Applied || result.Outcome != engine.CommitOutcomeAccepted || !result.ExecutionDone {
			b.Fatalf("CommitNode() result = %+v, want accepted terminal transition", result)
		}
		deletePerfExecution(b, ctx, rdb, id)
	}
}

func BenchmarkRedisOutboxDrain(b *testing.B) {
	const entriesPerDrain = 1000

	ctx := context.Background()
	backend, rdb := newPerfRedisBackend(b)
	queue := &perfSwitchableQueue{}
	queue.failDelivery()
	eng := engine.New(backend.State(), queue)
	b.Cleanup(backend.Bind(eng))
	b.Cleanup(func() { deletePerfAsynqKeys(b, ctx, rdb) })
	deletePerfAsynqKeys(b, ctx, rdb)

	state := requirePerfAtomicState(b, backend.State())
	workflow := perfIndependentGraph(b, "outbox-drain", entriesPerDrain)

	b.ReportAllocs()
	b.ReportMetric(entriesPerDrain, "outbox_entries/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		queue.failDelivery()
		id := submitPerfWorkflow(b, ctx, eng, workflow)
		if got := len(listPerfOutbox(b, ctx, state, id, entriesPerDrain)); got != entriesPerDrain {
			b.Fatalf("initial outbox entries = %d, want %d", got, entriesPerDrain)
		}
		queue.deliverTo(backend.Queue())

		b.StartTimer()
		err := eng.FlushOutbox(ctx, id)
		b.StopTimer()
		if err != nil {
			b.Fatalf("FlushOutbox() error = %v", err)
		}
		if got := len(listPerfOutbox(b, ctx, state, id, entriesPerDrain)); got != 0 {
			b.Fatalf("outbox entries after drain = %d, want 0", got)
		}
		deletePerfExecution(b, ctx, rdb, id)
		deletePerfAsynqKeys(b, ctx, rdb)
	}
}

func BenchmarkRedisLeaseSweep10K(b *testing.B) {
	const leaseCount = 10_000

	ctx := context.Background()
	backend, rdb := newPerfRedisBackend(b)
	eng := engine.New(backend.State(), perfDiscardQueue{})
	b.Cleanup(backend.Bind(eng))
	workflow := perfIndependentGraph(b, "lease-sweep", leaseCount)

	b.ReportAllocs()
	b.ReportMetric(leaseCount, "leases/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		id := submitPerfWorkflow(b, ctx, eng, workflow)
		createExpiredPerfLeases(b, ctx, backend.State(), id, leaseCount)

		sweeper := control.NewLeaseSweeper(backend.State(), eng, control.LeaseSweeperConfig{
			LeaseRepairPeriod: time.Hour,
			LeaseRepairBatch:  leaseCount,
		})
		// Repair is measured independently from the drain itself. Running it
		// before the timer also suppresses the periodic repair inside SweepOnce.
		sweeper.RepairOnce(ctx)

		b.StartTimer()
		reclaimed := 0
		for passes := 0; reclaimed < leaseCount; passes++ {
			if passes > leaseCount/256+1 {
				b.Fatalf("sweep stalled after %d passes with %d/%d leases reclaimed", passes, reclaimed, leaseCount)
			}
			count := sweeper.SweepOnce(ctx)
			if count == 0 {
				b.Fatalf("sweep stalled after %d passes with %d/%d leases reclaimed", passes, reclaimed, leaseCount)
			}
			reclaimed += count
		}
		b.StopTimer()
		if reclaimed != leaseCount {
			b.Fatalf("reclaimed leases = %d, want %d", reclaimed, leaseCount)
		}
		deletePerfExecution(b, ctx, rdb, id)
	}
}

func BenchmarkRedisRunnerReconnectStorm(b *testing.B) {
	const runnerIDs = 1000

	addr := realRedisAddr(b)
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		b.Fatalf("ping isolated Redis database: %v", err)
	}
	deletePerfRunnerDirectoryKeys(b, ctx, rdb)
	b.Cleanup(func() {
		deletePerfRunnerDirectoryKeys(b, ctx, rdb)
		_ = rdb.Close()
	})

	directory := control.NewRedisRunnerDirectory(rdb)
	capabilities := []protocol.Capability{{NodeType: "bench.noop"}}
	policy := control.RunnerPolicy{AllowedNodeTypes: []string{"bench.noop"}}

	var sequence uint64
	var errMu sync.Mutex
	var firstErr error
	b.ReportAllocs()
	b.ReportMetric(runnerIDs, "runner_ids")
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&sequence, 1)
			_, err := directory.Register(ctx, control.RegisterRunnerRequest{
				RunnerID:     fmt.Sprintf("perf-reconnect-%04d", id%runnerIDs),
				Capacity:     1,
				Capabilities: capabilities,
				Policy:       policy,
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	if firstErr != nil {
		b.Fatalf("Register() error during reconnect storm: %v", firstErr)
	}
}

func newPerfRedisBackend(b *testing.B) (*distributed.Backend, *redis.Client) {
	b.Helper()
	addr := realRedisAddr(b)
	backend, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		b.Fatalf("distributed.New() error = %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		b.Fatalf("ping Redis: %v", err)
	}
	b.Cleanup(func() { _ = rdb.Close() })
	return backend, rdb
}

func requirePerfAtomicState(b *testing.B, state engine.StateStore) engine.AtomicStateStore {
	b.Helper()
	atomicState, ok := state.(engine.AtomicStateStore)
	if !ok {
		b.Fatal("Redis StateStore does not implement AtomicStateStore")
	}
	return atomicState
}

func perfIndependentGraph(b *testing.B, name string, nodes int) *graph.Graph {
	b.Helper()
	defs := make([]types.NodeDef, nodes)
	for i := range defs {
		defs[i] = types.NodeDef{Name: perfNodeName(i), Type: "bench.noop"}
	}
	workflow, err := graph.Compile(&types.WorkflowDef{Name: name, Nodes: defs})
	if err != nil {
		b.Fatalf("Compile() error = %v", err)
	}
	return workflow
}

func perfNodeName(index int) string { return fmt.Sprintf("node-%d", index) }

func submitPerfWorkflow(b *testing.B, ctx context.Context, eng *engine.Engine, workflow *graph.Graph) types.ExecutionID {
	b.Helper()
	id, err := eng.Submit(ctx, workflow, nil)
	if err != nil {
		b.Fatalf("Submit() error = %v", err)
	}
	return id
}

func listPerfOutbox(b *testing.B, ctx context.Context, state engine.AtomicStateStore, id types.ExecutionID, limit int) []engine.OutboxEntry {
	b.Helper()
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Minute), limit)
	if err != nil {
		b.Fatalf("ListOutbox() error = %v", err)
	}
	return entries
}

func createExpiredPerfLeases(b *testing.B, ctx context.Context, state engine.StateStore, id types.ExecutionID, count int) {
	b.Helper()
	issuedAt := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < count; i++ {
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID(fmt.Sprintf("perf-lease-%d", i)),
			LeaseToken: engine.LeaseToken(fmt.Sprintf("perf-token-%d", i)),
			Task: engine.Task{
				ExecutionID: id,
				NodeName:    perfNodeName(i),
				NodeIdx:     i,
				Type:        engine.TaskTypeNodeExec,
			},
			IssuedAt: issuedAt,
			TTL:      time.Second,
		}
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil {
			b.Fatalf("AcquireTaskLease(%d) error = %v", i, err)
		} else if !acquired {
			b.Fatalf("AcquireTaskLease(%d) acquired=false", i)
		}
	}
}

func deletePerfExecution(b *testing.B, ctx context.Context, rdb *redis.Client, id types.ExecutionID) {
	b.Helper()
	if err := deletePerfKeys(ctx, rdb, fmt.Sprintf("xflow:tdefault:exec:{%s}:*", id)); err != nil {
		b.Fatalf("delete execution %q keys: %v", id, err)
	}
}

func deletePerfAsynqKeys(b *testing.B, ctx context.Context, rdb *redis.Client) {
	b.Helper()
	if err := deletePerfKeys(ctx, rdb, "asynq:*"); err != nil {
		b.Fatalf("delete Asynq keys: %v", err)
	}
}

func deletePerfKeys(ctx context.Context, rdb *redis.Client, pattern string) error {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return fmt.Errorf("scan %q: %w", pattern, err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("delete %q: %w", pattern, err)
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func deletePerfRunnerDirectoryKeys(b *testing.B, ctx context.Context, rdb *redis.Client) {
	b.Helper()
	const prefix = "xflow:runner-directory:{control}"
	keys := []string{
		prefix + ":queue",
		prefix + ":seen",
		prefix + ":assignment:data",
		prefix + ":assignment:state",
		prefix + ":assignment:claim",
		prefix + ":assignment:runner",
		prefix + ":assignment:session",
		prefix + ":assignment:lease-id",
		prefix + ":assignment:lease-token",
		prefix + ":assignment:lease-meta",
		prefix + ":claim:assignment",
		prefix + ":claim:runner",
		prefix + ":claim:session",
		prefix + ":claim:expiry",
		prefix + ":runner:session",
		prefix + ":runner:capacity",
		prefix + ":runner:inflight",
		prefix + ":runner:capabilities",
		prefix + ":runner:policy",
		prefix + ":runner:heartbeat",
		prefix + ":runner:claim-count",
		prefix + ":runner:lease-count",
		prefix + ":lease:by-id",
		prefix + ":lease:by-token",
	}
	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		b.Fatalf("delete isolated runner directory keys: %v", err)
	}
}
