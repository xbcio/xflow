package rstate

// Engine-level orphan tests: the store-level tests in concurrency_orphan_test.go
// submit through CreateExecutionWithConcurrency and never hand a graph to an
// engine cache. These drive a real engine.Engine over the Redis store and pin
// the shape an operator actually hit: Redis lost only the graph key while the
// status key (and the concurrency slot) survived, and the process that was
// already executing the workflow still has the decoded graph in memory.
//
// The warm-cache claim is proved behaviourally, not by reaching into the cache:
// after the graph key is deleted, a fresh engine over a fresh Store (the
// "restarted process" shape) refuses to lease the same task, while the warm
// engine leases and commits it to a terminal status. Only the in-process cache
// can explain the difference, because the persisted graph is demonstrably gone.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// engOrphanQueue is a TaskQueue that only remembers what it was handed. The
// tests read the queue themselves, so a task is delivered exactly when the test
// asks for it and no background consumer exists.
type engOrphanQueue struct {
	tasks []*engine.Task
}

func (q *engOrphanQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *engOrphanQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func (q *engOrphanQueue) takeFor(id types.ExecutionID) *engine.Task {
	for i, task := range q.tasks {
		if task.ExecutionID == id {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task
		}
	}
	return nil
}

func (q *engOrphanQueue) pending() int { return len(q.tasks) }

// engOrphanRig is one engine over one Redis store on its own miniredis, with
// the queue the tests inspect.
type engOrphanRig struct {
	t     *testing.T
	ctx   context.Context
	rdb   *redis.Client
	state *Store
	db    store.Store
	queue *engOrphanQueue
	eng   *engine.Engine
}

func newEngOrphanRig(t *testing.T, opts ...engine.Option) *engOrphanRig {
	t.Helper()
	state, rdb, db := newOrphanStore(t)
	queue := &engOrphanQueue{}
	return &engOrphanRig{
		t:     t,
		ctx:   namespace.WithNamespace(context.Background(), namespace.Default),
		rdb:   rdb,
		state: state,
		db:    db,
		queue: queue,
		eng:   engine.New(state, queue, opts...),
	}
}

func (r *engOrphanRig) submit(g *graph.Graph) types.ExecutionID {
	r.t.Helper()
	id, err := r.eng.Submit(r.ctx, g, map[string]any{"msg": "hello"})
	if err != nil {
		r.t.Fatalf("Submit(%s) error = %v", g.Name(), err)
	}
	return id
}

// dropGraph removes only the persisted graph key, leaving every status,
// concurrency and node key intact: the partial Redis loss this file models.
func (r *engOrphanRig) dropGraph(id types.ExecutionID) {
	r.t.Helper()
	orphanDeleteGraph(r.t, r.ctx, r.rdb, id)
	if n, err := r.rdb.Exists(r.ctx, execKey(namespace.Default, id, "graph")).Result(); err != nil || n != 0 {
		r.t.Fatalf("graph key after delete: exists=%d err=%v, want 0/nil", n, err)
	}
}

// dropRunningGraph is dropGraph plus the expectation the progress tests start
// from: the execution's own status key survived the loss.
func (r *engOrphanRig) dropRunningGraph(id types.ExecutionID) {
	r.t.Helper()
	r.dropGraph(id)
	if got := orphanStatus(r.t, r.ctx, r.state, id); got != types.ExecutionStatusRunning {
		r.t.Fatalf("status after graph loss = %q, want running", got)
	}
}

// engOrphanTwoNodeGraph compiles the smallest graph whose second node proves
// the engine can still SCHEDULE after the graph key is gone: committing the
// first node has to produce the next node's task out of the cached graph.
func engOrphanTwoNodeGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// TestEngineWarmGraphCacheKeepsGraphLostExecutionSchedulable pins the behaviour
// the reclaim guard must not break: while the executing process still has the
// graph cached, losing the Redis graph key does not stall the execution. It can
// still lease the pending node, commit its result, schedule the downstream node
// from the cached graph, and reach a terminal status.
func TestEngineWarmGraphCacheKeepsGraphLostExecutionSchedulable(t *testing.T) {
	t.Run("unleased running execution", func(t *testing.T) {
		rig := newEngOrphanRig(t)
		g := engOrphanTwoNodeGraph(t, "orphan-engine-unleased")
		id := rig.submit(g)
		task := rig.queue.takeFor(id)
		if task == nil || task.NodeName != "start" {
			t.Fatalf("queued root = %+v, want start", task)
		}

		rig.dropRunningGraph(id)

		// The restarted-process shape: a brand new Store has no in-memory graph
		// either, so Redis is the only source and it is gone.
		cold := engine.New(New(rig.rdb, nil, time.Minute), &engOrphanQueue{})
		if _, err := cold.BuildTaskLease(rig.ctx, task); !errors.Is(err, engine.ErrExecutionInactive) {
			t.Fatalf("cold-engine BuildTaskLease error = %v, want ErrExecutionInactive", err)
		}

		// The same process keeps routing from its cache.
		lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
		if err != nil {
			t.Fatalf("warm-engine BuildTaskLease(start) error = %v, want the cached graph to carry it", err)
		}
		if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"step": 1}},
		}); err != nil {
			t.Fatalf("CommitTaskResult(start) error = %v", err)
		}

		// The downstream task has to come out of the cached graph: Redis holds
		// no graph any more. This is the "continues to be scheduled" evidence.
		next := rig.queue.takeFor(id)
		if next == nil || next.NodeName != "next" {
			t.Fatalf("downstream task = %+v, want next (graph-cached scheduling is broken)", next)
		}
		nextLease, err := rig.eng.BuildTaskLease(rig.ctx, next)
		if err != nil {
			t.Fatalf("warm-engine BuildTaskLease(next) error = %v", err)
		}
		if err := rig.eng.CommitTaskResult(rig.ctx, nextLease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"step": 2}},
		}); err != nil {
			t.Fatalf("CommitTaskResult(next) error = %v", err)
		}

		if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusSuccess {
			t.Fatalf("final status = %q, want success", got)
		}
		if got := orphanSQLStatus(t, rig.ctx, rig.db, id); got != types.ExecutionStatusSuccess {
			t.Fatalf("final SQL status = %q, want success", got)
		}
	})

	t.Run("leased node", func(t *testing.T) {
		rig := newEngOrphanRig(t)
		g := engOrphanTwoNodeGraph(t, "orphan-engine-leased")
		id := rig.submit(g)
		task := rig.queue.takeFor(id)

		lease, err := rig.eng.BuildTaskLease(rig.ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease(start) error = %v", err)
		}
		// The lease is a real one: the lease-expiry index the reclaim judgement
		// reads is written by the same acquire transition.
		if n, err := rig.rdb.ZCard(rig.ctx, leaseExpiryZSetKey(namespace.Default, id)).Result(); err != nil || n != 1 {
			t.Fatalf("lease index size = %d err=%v, want 1", n, err)
		}

		rig.dropRunningGraph(id)

		// An in-flight node finishing after the graph key vanished: the commit
		// is fenced on the execution status (still running), not on the graph
		// key, and it advances using the cached graph.
		if err := rig.eng.CommitTaskResult(rig.ctx, lease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"step": 1}},
		}); err != nil {
			t.Fatalf("CommitTaskResult after graph loss error = %v, want nil", err)
		}
		next := rig.queue.takeFor(id)
		if next == nil || next.NodeName != "next" {
			t.Fatalf("downstream task after commit = %+v, want next", next)
		}
		nextLease, err := rig.eng.BuildTaskLease(rig.ctx, next)
		if err != nil {
			t.Fatalf("BuildTaskLease(next) error = %v", err)
		}
		if err := rig.eng.CommitTaskResult(rig.ctx, nextLease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"step": 2}},
		}); err != nil {
			t.Fatalf("CommitTaskResult(next) error = %v", err)
		}
		if got := orphanStatus(t, rig.ctx, rig.state, id); got != types.ExecutionStatusSuccess {
			t.Fatalf("final status = %q, want success", got)
		}
	})
}
