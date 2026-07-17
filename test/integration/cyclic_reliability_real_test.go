//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// errCyclicQueueUnavailable models a transient queue handoff outage between
// the fenced terminal commit and downstream task delivery.
var errCyclicQueueUnavailable = errors.New("cyclic task queue unavailable")

// cyclicFakeQueue is a controllable engine.TaskQueue that records delivered
// tasks and can be toggled to fail Enqueue, modelling a queue outage / process
// crash between the fenced terminal commit and downstream delivery. The real
// Redis state store remains authoritative: a failed Enqueue leaves the durable
// outbox entry intact for the background OutboxDispatcher to replay.
//
// It implements only engine.TaskQueue (the producer surface). The backend's
// own Asynq transport remains the consumer side, so WithConsumer(true)+Bind
// still starts the real background OutboxDispatcher — the component A0 must
// verify. The Asynq consumer is idle here because production-side enqueues go
// through this fake queue, not the Asynq transport.
type cyclicFakeQueue struct {
	mu    sync.Mutex
	err   error
	tasks []*engine.Task
}

var _ engine.TaskQueue = (*cyclicFakeQueue)(nil)

func (q *cyclicFakeQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.tasks = append(q.tasks, t)
	return nil
}

func (q *cyclicFakeQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func (q *cyclicFakeQueue) setError(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.err = err
}

func (q *cyclicFakeQueue) drain() []*engine.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := q.tasks
	q.tasks = nil
	return tasks
}

func (q *cyclicFakeQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// cyclicReliabilityGraph builds the start<->review cyclic graph used by the
// engine's own cyclic durability unit test: start.main -> review,
// review.reject -> start. AllowCycles is required so the legacy cyclic commit
// path persists downstream delivery intents in the same fenced transition.
func cyclicReliabilityGraph(t *testing.T, name string, maxDepth int) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    name,
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: maxDepth},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// cyclicReliabilityEnv bundles a backend+engine+fake queue backed by real Redis.
// The background OutboxDispatcher is started via Bind (WithConsumer true).
type cyclicReliabilityEnv struct {
	backend *distributed.Backend
	eng     *engine.Engine
	queue   *cyclicFakeQueue
	state   engine.AtomicStateStore
	rdb     *redis.Client
	addr    string
	stop    func()
}

func newCyclicReliabilityEnv(t *testing.T, addr string) *cyclicReliabilityEnv {
	t.Helper()
	backend, err := distributed.New(addr, nil, distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New() error = %v", err)
	}
	queue := &cyclicFakeQueue{}
	eng := engine.New(backend.State(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(3),
	)
	stop := backend.Bind(eng)

	rdb := redis.NewClient(&redis.Options{Addr: addr})

	state, ok := backend.State().(engine.AtomicStateStore)
	if !ok {
		t.Fatal("Redis StateStore does not implement AtomicStateStore")
	}

	env := &cyclicReliabilityEnv{
		backend: backend,
		eng:     eng,
		queue:   queue,
		state:   state,
		rdb:     rdb,
		addr:    addr,
		stop:    stop,
	}
	t.Cleanup(func() {
		_ = rdb.Close()
		stop()
	})
	return env
}

// submitCyclic submits the cyclic graph and drains the auto-delivered root
// start task so the caller can drive the first lease. The queue must be
// available during delivery; this helper toggles it on, flushes, and drains.
func (env *cyclicReliabilityEnv) submitCyclic(t *testing.T, ctx context.Context, g *graph.Graph, params map[string]any) (types.ExecutionID, *engine.Task) {
	t.Helper()
	env.queue.setError(nil)
	id, err := env.eng.Submit(ctx, g, params)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	// Submit's inline flushInitialOutbox delivers the root start task; drain it.
	tasks := env.queue.drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("after Submit delivered = %+v, want one start task", tasks)
	}
	return id, tasks[0]
}

// commitStart advances the start node to its main port, delivering the review
// downstream task, and returns that task so the caller can drive the review lease.
func (env *cyclicReliabilityEnv) commitStart(t *testing.T, ctx context.Context, startTask *engine.Task) *engine.Task {
	t.Helper()
	env.queue.setError(nil)
	startLease, err := env.eng.BuildTaskLease(ctx, startTask)
	if err != nil {
		t.Fatalf("BuildTaskLease(start) error = %v", err)
	}
	if err := env.eng.CommitTaskResult(ctx, startLease, engine.TaskResult{Output: &types.Output{Port: "main", Data: map[string]any{"round": 1}}}); err != nil {
		t.Fatalf("CommitTaskResult(start) error = %v", err)
	}
	tasks := env.queue.drain()
	if len(tasks) != 1 || tasks[0].NodeName != "review" {
		t.Fatalf("after start commit delivered = %+v, want one review task", tasks)
	}
	return tasks[0]
}

// TestCyclicReliabilityRealRedis verifies the A0 production-readiness gap
// (.claude/specs/2026-07-17-server-production-readiness-design.md §A0) against
// a real Redis instance: durable cyclic outbox retention across queue outages,
// background OutboxDispatcher auto-replay after process rebuild, fenced
// duplicate-commit safety, and atomic terminal-branch finalization.
func TestCyclicReliabilityRealRedis(t *testing.T) {
	addr := requireRedis(t)

	t.Run("RejectPersistsNextActivationOutbox", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		env := newCyclicReliabilityEnv(t, addr)
		g := cyclicReliabilityGraph(t, "cyclic-a0-reject", 10)
		id, startTask := env.submitCyclic(t, ctx, g, map[string]any{"round": 1})
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env.rdb, id) })

		reviewTask := env.commitStart(t, ctx, startTask)
		reviewLease, err := env.eng.BuildTaskLease(ctx, reviewTask)
		if err != nil {
			t.Fatalf("BuildTaskLease(review) error = %v", err)
		}

		// Inject a queue outage for the downstream of the reject branch. The
		// terminal commit and its cyclic outbox entry are persisted in one fenced
		// transition; the follow-up Enqueue fails without losing the intent.
		env.queue.setError(errCyclicQueueUnavailable)
		err = env.eng.CommitTaskResult(ctx, reviewLease, engine.TaskResult{Output: &types.Output{Port: "reject", Data: map[string]any{"round": 2}}})
		if !errors.Is(err, errCyclicQueueUnavailable) {
			t.Fatalf("CommitTaskResult(review.reject) error = %v, want queue outage after durable commit", err)
		}

		entries, err := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil {
			t.Fatalf("ListOutbox() after reject error = %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("outbox after reject = %d entries, want exactly 1 (start@next activation); entries=%+v", len(entries), entries)
		}
		got := entries[0].Task
		if got.NodeName != "start" || got.ActivationID != reviewTask.ActivationID+1 {
			t.Fatalf("durable cyclic intent = %s@%d, want start@%d", got.NodeName, got.ActivationID, reviewTask.ActivationID+1)
		}
		if entries[0].ID == "" || entries[0].CreatedAt.IsZero() {
			t.Fatalf("outbox entry missing id/created-at: %+v", entries[0])
		}
	})

	t.Run("QueueOutageTerminalCommitLandsOutboxRetained", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		env := newCyclicReliabilityEnv(t, addr)
		g := cyclicReliabilityGraph(t, "cyclic-a0-outage", 10)
		id, startTask := env.submitCyclic(t, ctx, g, map[string]any{"round": 1})
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env.rdb, id) })

		reviewTask := env.commitStart(t, ctx, startTask)
		reviewLease, err := env.eng.BuildTaskLease(ctx, reviewTask)
		if err != nil {
			t.Fatalf("BuildTaskLease(review) error = %v", err)
		}

		env.queue.setError(errCyclicQueueUnavailable)
		err = env.eng.CommitTaskResult(ctx, reviewLease, engine.TaskResult{Output: &types.Output{Port: "reject", Data: map[string]any{"round": 2}}})
		if !errors.Is(err, errCyclicQueueUnavailable) {
			t.Fatalf("CommitTaskResult(review.reject) error = %v, want transient queue outage after durable commit", err)
		}

		// The review terminal state must be durable (Success) despite the failed
		// downstream handoff, the execution must remain Running, and the durable
		// downstream intent must be retained.
		reviewNode, err := env.backend.State().GetNode(ctx, id, "review")
		if err != nil || reviewNode == nil || reviewNode.Status != types.NodeStatusSuccess {
			t.Fatalf("review node after outage = %+v err=%v, want durable Success", reviewNode, err)
		}
		execution, err := env.backend.State().GetExecution(ctx, id)
		if err != nil || execution == nil || execution.Status != types.ExecutionStatusRunning {
			t.Fatalf("execution after outage = %+v err=%v, want still Running", execution, err)
		}
		entries, err := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil || len(entries) != 1 {
			t.Fatalf("outbox after outage entries=%+v err=%v, want exactly 1 retained intent", entries, err)
		}
	})

	// ProcessRebuild_BackgroundDispatcherAutoReplays is the core A0 regression:
	// after destroying backend/engine/dispatcher (the Bind stop function) while a
	// durable cyclic outbox entry is stranded, a freshly bound backend must
	// redeliver it via the *background* OutboxDispatcher — with no manual
	// FlushOutbox call from the test.
	t.Run("ProcessRebuild_BackgroundDispatcherAutoReplays", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Phase 1: produce a stranded durable cyclic intent under a queue outage.
		env1 := newCyclicReliabilityEnv(t, addr)
		g := cyclicReliabilityGraph(t, "cyclic-a0-rebuild", 10)
		id, startTask := env1.submitCyclic(t, ctx, g, map[string]any{"round": 1})
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env1.rdb, id) })

		reviewTask := env1.commitStart(t, ctx, startTask)
		reviewLease, err := env1.eng.BuildTaskLease(ctx, reviewTask)
		if err != nil {
			t.Fatalf("BuildTaskLease(review) error = %v", err)
		}
		env1.queue.setError(errCyclicQueueUnavailable)
		if err := env1.eng.CommitTaskResult(ctx, reviewLease, engine.TaskResult{Output: &types.Output{Port: "reject", Data: map[string]any{"round": 2}}}); !errors.Is(err, errCyclicQueueUnavailable) {
			t.Fatalf("CommitTaskResult(review.reject) error = %v, want queue outage", err)
		}
		entries, err := env1.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil || len(entries) != 1 {
			t.Fatalf("pre-rebuild outbox entries=%+v err=%v, want exactly 1 stranded intent", entries, err)
		}
		expectedActivation := entries[0].Task.ActivationID

		// Phase 2: destroy the backend/engine/dispatcher (process rebuild). This
		// stops the OutboxDispatcher, the Asynq consumer, and the timeout monitor,
		// and closes the Redis client owned by the backend. env1.rdb (a separate
		// client kept for cleanup) and the Redis outbox state both persist.
		env1.stop()

		// Phase 3: rebuild a fresh backend+engine bound to the same Redis. Do NOT
		// call FlushOutbox; the background OutboxDispatcher started by Bind must
		// discover and replay the stranded intent on its own.
		env2 := newCyclicReliabilityEnv(t, addr)
		// Leave env2 in t.Cleanup; we only assert here, then let cleanup tear it down.

		// Poll (no time.Sleep) for the background dispatcher to flush the stranded
		// entry: outbox drained and the downstream task redelivered to the queue.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.Now().Add(15 * time.Second)
		for {
			entries, err := env2.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
			if err != nil {
				t.Fatalf("post-rebuild ListOutbox() error = %v", err)
			}
			delivered := env2.queue.drain()
			if len(entries) == 0 && len(delivered) >= 1 {
				got := delivered[len(delivered)-1]
				if got.NodeName != "start" || got.ActivationID != expectedActivation {
					t.Fatalf("redelivered task = %s@%d, want start@%d", got.NodeName, got.ActivationID, expectedActivation)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for background OutboxDispatcher replay: outbox entries=%d, delivered=%d", len(entries), env2.queue.count())
			}
			<-ticker.C
		}
	})

	t.Run("DuplicateCommitFencedDoesNotDoubleAdvance", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		env := newCyclicReliabilityEnv(t, addr)
		g := cyclicReliabilityGraph(t, "cyclic-a0-dup", 10)
		id, startTask := env.submitCyclic(t, ctx, g, map[string]any{"round": 1})
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env.rdb, id) })

		reviewTask := env.commitStart(t, ctx, startTask)
		reviewLease, err := env.eng.BuildTaskLease(ctx, reviewTask)
		if err != nil {
			t.Fatalf("BuildTaskLease(review) error = %v", err)
		}

		// Keep the queue available so the fenced outcome is observable rather than
		// masked by a transient flush error. This mirrors the engine's own
		// cyclic_outbox_durability unit test: the first commit delivers the
		// reject-branch downstream exactly once; a redelivered duplicate commit is
		// classified DuplicateTerminal and must not enqueue a second copy.
		env.queue.setError(nil)
		result := engine.TaskResult{Output: &types.Output{Port: "reject", Data: map[string]any{"round": 2}}}
		outcome1, err := env.eng.CommitTaskResultWithOutcome(ctx, reviewLease, result)
		if err != nil {
			t.Fatalf("first commit error = %v, want nil", err)
		}
		if outcome1 != engine.CommitOutcomeAccepted {
			t.Fatalf("first commit outcome = %q, want %q", outcome1, engine.CommitOutcomeAccepted)
		}
		first := env.queue.drain()
		if len(first) != 1 || first[0].NodeName != "start" {
			t.Fatalf("first commit delivered = %+v, want one downstream start task", first)
		}
		expectedActivation := first[0].ActivationID

		// Redelivered duplicate commit (same lease/token): must be fenced as a
		// duplicate-terminal and must not re-enqueue the downstream task.
		outcome2, err := env.eng.CommitTaskResultWithOutcome(ctx, reviewLease, result)
		if err != nil {
			t.Fatalf("duplicate commit error = %v, want nil (duplicate terminal is not an error)", err)
		}
		if outcome2 != engine.CommitOutcomeDuplicateTerminal {
			t.Fatalf("duplicate commit outcome = %q, want %q", outcome2, engine.CommitOutcomeDuplicateTerminal)
		}
		if dup := env.queue.drain(); len(dup) != 0 {
			t.Fatalf("duplicate commit must not re-enqueue downstream, got %d tasks (first activation=%d)", len(dup), expectedActivation)
		}

		// The durable outbox is empty: the single downstream intent was delivered
		// and acked on the first commit, and the duplicate commit re-added nothing
		// (HSETNX dedup on the deterministic cyclic outbox key).
		entries, err := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outbox after duplicate commits entries=%+v err=%v, want empty (exactly one intent, acked)", entries, err)
		}
	})

	t.Run("TerminalBranchFinalizesExecutionAtomically", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		env := newCyclicReliabilityEnv(t, addr)
		// A cyclic graph whose review node has an "approve" port with NO downstream
		// edge: completing review on that port terminates the execution in the same
		// fenced commit (no separate, crash-exposed status write).
		def := &types.WorkflowDef{
			Name:    "cyclic-a0-terminal",
			Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "review", Type: "test.review"},
			},
			Connections: types.Connections{
				"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
				"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
			},
		}
		g, err := graph.Compile(def)
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		id, startTask := env.submitCyclic(t, ctx, g, nil)
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env.rdb, id) })

		reviewTask := env.commitStart(t, ctx, startTask)
		reviewLease, err := env.eng.BuildTaskLease(ctx, reviewTask)
		if err != nil {
			t.Fatalf("BuildTaskLease(review) error = %v", err)
		}

		// "approve" port has no outgoing edge: the fenced commit must finalize the
		// execution to Success and produce no downstream task.
		env.queue.setError(nil)
		if err := env.eng.CommitTaskResult(ctx, reviewLease, engine.TaskResult{Output: &types.Output{Port: "approve", Data: map[string]any{"ok": true}}}); err != nil {
			t.Fatalf("CommitTaskResult(review.approve) error = %v", err)
		}
		execution, err := env.backend.State().GetExecution(ctx, id)
		if err != nil || execution == nil || execution.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution after terminal branch = %+v err=%v, want Success", execution, err)
		}
		if extra := env.queue.drain(); len(extra) != 0 {
			t.Fatalf("no downstream expected after terminal branch, got %d tasks", len(extra))
		}
		entries, err := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outbox after terminal branch entries=%+v err=%v, want empty", entries, err)
		}
	})
}
