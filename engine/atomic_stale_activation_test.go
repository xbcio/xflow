package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingQueue is a minimal in-memory task queue used to isolate the engine
// test from real broker consumers.
type recordingQueue struct {
	mu    sync.Mutex
	tasks []*engine.Task
}

func (q *recordingQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *recordingQueue) EnqueueDelayed(_ context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(context.Background(), t)
}

func (q *recordingQueue) Drain() []*engine.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.tasks
	q.tasks = nil
	return out
}

// TestEngineHandleSystemTaskDropsStaleActivationAdvance is a regression test
// for the engine-level stale-activation guard on internal advance tasks. A
// source node is committed at activation 0, its stored activation is then
// bumped to 1 (simulating a cyclic re-entry or out-of-order durable replay),
// and a TaskTypeNodeAdvance carrying the old activation 0 is delivered. The
// engine must drop the task without mutating downstream scheduling counters or
// enqueueing duplicate work.
func TestEngineHandleSystemTaskDropsStaleActivationAdvance(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	be, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}

	queue := &recordingQueue{}
	eng := engine.New(be.State(), queue)

	wf := &types.WorkflowDef{
		Name: "stale-advance",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"default": []types.Connection{{Node: "end", Input: "default"}}},
		},
	}

	g, err := graph.Compile(wf)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Acquire a lease for the source node and commit it successfully. This
	// writes the terminal source node and schedules the downstream node.
	lease, err := eng.BuildTaskLease(ctx, &engine.Task{
		ExecutionID:  id,
		NodeName:     "start",
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: 0,
	})
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}

	if err := eng.CommitTaskResult(ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}, Port: "default"},
	}); err != nil {
		t.Fatalf("CommitTaskResult: %v", err)
	}

	// Drain the queue so any downstream task from the successful commit does
	// not interfere with the stale-advance assertions.
	_ = queue.Drain()

	// Simulate the source node moving to a newer activation while an old
	// advance task is still in flight.
	metaKey := fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:meta", tenant.DefaultTenant, id, "start")
	if err := be.RedisClient().HSet(ctx, metaKey, "activation_id", 1).Err(); err != nil {
		t.Fatalf("bump activation: %v", err)
	}

	// Snapshot downstream scheduling state before the stale advance.
	rdb := be.RedisClient()
	indegreeKey := fmt.Sprintf("xflow:t%s:exec:{%s}:indegree:1", tenant.DefaultTenant, id)
	activeKey := fmt.Sprintf("xflow:t%s:exec:{%s}:active_inputs:1", tenant.DefaultTenant, id)
	outboxKey := fmt.Sprintf("xflow:t%s:exec:{%s}:outbox:ready", tenant.DefaultTenant, id)

	beforeIndegree, err := rdb.Get(ctx, indegreeKey).Int()
	if err != nil {
		t.Fatalf("get indegree: %v", err)
	}
	beforeActive, err := rdb.Get(ctx, activeKey).Int()
	if err != nil {
		t.Fatalf("get active inputs: %v", err)
	}
	beforeOutbox, err := rdb.ZCard(ctx, outboxKey).Result()
	if err != nil {
		t.Fatalf("get outbox size: %v", err)
	}

	// Deliver the stale internal advance task.
	handled, err := eng.HandleSystemTask(ctx, &engine.Task{
		ExecutionID:  id,
		NodeName:     "start",
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeAdvance,
		ActivationID: 0,
	})
	if err != nil {
		t.Fatalf("HandleSystemTask error = %v", err)
	}
	if !handled {
		t.Fatal("HandleSystemTask did not handle the stale advance task")
	}

	afterIndegree, err := rdb.Get(ctx, indegreeKey).Int()
	if err != nil {
		t.Fatalf("get indegree after: %v", err)
	}
	afterActive, err := rdb.Get(ctx, activeKey).Int()
	if err != nil {
		t.Fatalf("get active inputs after: %v", err)
	}
	afterOutbox, err := rdb.ZCard(ctx, outboxKey).Result()
	if err != nil {
		t.Fatalf("get outbox size after: %v", err)
	}

	if afterIndegree != beforeIndegree {
		t.Errorf("indegree changed: before=%d after=%d", beforeIndegree, afterIndegree)
	}
	if afterActive != beforeActive {
		t.Errorf("active inputs changed: before=%d after=%d", beforeActive, afterActive)
	}
	if afterOutbox != beforeOutbox {
		t.Errorf("outbox ready size changed: before=%d after=%d", beforeOutbox, afterOutbox)
	}
	if got := queue.Drain(); len(got) != 0 {
		t.Errorf("stale advance enqueued %d unexpected task(s): %v", len(got), got)
	}
}
