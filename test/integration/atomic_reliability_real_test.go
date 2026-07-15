//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

var errAtomicReliabilityQueueUnavailable = errors.New("task queue unavailable")

type atomicReliabilityQueue struct {
	mu    sync.Mutex
	err   error
	tasks []*engine.Task
}

var _ engine.TaskQueue = (*atomicReliabilityQueue)(nil)

func (q *atomicReliabilityQueue) Enqueue(_ context.Context, task *engine.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.tasks = append(q.tasks, task)
	return nil
}

func (q *atomicReliabilityQueue) EnqueueDelayed(ctx context.Context, task *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, task)
}

func (q *atomicReliabilityQueue) setError(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.err = err
}

func (q *atomicReliabilityQueue) drain() []*engine.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := q.tasks
	q.tasks = nil
	return tasks
}

// TestAtomicReliabilityRealRedisLeaseCommitAndOutboxRecovery verifies the
// production Redis Lua paths rather than their miniredis-compatible unit-test
// equivalents. A queue handoff outage must not roll back either the acquired
// lease or the atomic terminal commit; the retained durable outbox entry is
// delivered exactly once when the queue becomes available again.
func TestAtomicReliabilityRealRedisLeaseCommitAndOutboxRecovery(t *testing.T) {
	addr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backend, err := asynq.New(addr, nil, asynq.WithConsumer(false))
	if err != nil {
		t.Fatalf("asynq.New() error = %v", err)
	}
	queue := &atomicReliabilityQueue{err: errAtomicReliabilityQueueUnavailable}
	eng := engine.New(backend.State(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(4),
	)
	t.Cleanup(backend.Bind(eng))

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	id := types.ExecutionID(fmt.Sprintf("atomic-reliability-%d", time.Now().UnixNano()))
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, rdb, id) })

	workflow, err := graph.Compile(&types.WorkflowDef{
		Name:  "atomic-reliability-real-redis",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.atomic.reliability"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	// Submit persists the root intent before attempting the unavailable queue.
	// The enqueue failure is intentionally not returned as a submission failure.
	submitted, err := eng.Submit(ctx, workflow, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v, want durable success during queue outage", err)
	}
	if submitted == "" {
		t.Fatal("Submit() returned an empty execution ID")
	}
	id = submitted

	state := backend.State()
	atomicState, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatal("Redis StateStore does not implement AtomicStateStore")
	}
	entries, err := atomicState.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil {
		t.Fatalf("ListOutbox() after submit error = %v", err)
	}
	if len(entries) != 1 || entries[0].Task.NodeName != "start" || entries[0].CreatedAt.IsZero() {
		t.Fatalf("durable root outbox = %+v, want one timestamped start task", entries)
	}

	lease, err := eng.BuildTaskLease(ctx, &entries[0].Task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if lease == nil || lease.LeaseToken == "" || lease.Attempt != 1 {
		t.Fatalf("lease = %+v, want acquired first lease", lease)
	}
	node, err := state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusRunning || node.LeaseToken != lease.LeaseToken {
		t.Fatalf("node after lease acquisition = %+v, err=%v, want running with current token", node, err)
	}

	// Atomic commit persists terminal state before its follow-up FlushOutbox
	// call. The failing queue therefore surfaces an error without undoing the
	// committed node/execution state or losing the original root intent.
	err = eng.CommitTaskResult(ctx, lease, engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}})
	if !errors.Is(err, errAtomicReliabilityQueueUnavailable) {
		t.Fatalf("CommitTaskResult() error = %v, want queue outage after durable commit", err)
	}
	node, err = state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("node after failed delivery = %+v, err=%v, want durable success", node, err)
	}
	execution, err := state.GetExecution(ctx, id)
	if err != nil || execution == nil || execution.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution after failed delivery = %+v, err=%v, want durable success", execution, err)
	}
	entries, err = atomicState.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil || len(entries) != 1 || entries[0].ID == "" {
		t.Fatalf("outbox after failed delivery entries=%+v err=%v, want retained root intent", entries, err)
	}

	queue.setError(nil)
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() after queue recovery error = %v", err)
	}
	delivered := queue.drain()
	if len(delivered) != 1 || delivered[0].ExecutionID != id || delivered[0].NodeName != "start" {
		t.Fatalf("recovered queue delivery = %+v, want one original start task", delivered)
	}
	entries, err = atomicState.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil || len(entries) != 0 {
		t.Fatalf("outbox after recovery entries=%+v err=%v, want empty", entries, err)
	}
}

func deleteAtomicReliabilityKeys(t *testing.T, rdb *redis.Client, id types.ExecutionID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, fmt.Sprintf("xflow:exec:{%s}:*", id), 128).Result()
		if err != nil {
			t.Errorf("scan test execution keys: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete test execution keys: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
