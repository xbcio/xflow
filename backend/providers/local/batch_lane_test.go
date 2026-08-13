package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// A wide map's batches must not sit at the head of the line in front of an
// unrelated execution.
//
// Measured on the single-FIFO queue this replaces: a small execution submitted
// into a queue already saturated by a 200-batch map waited 854ms — exactly as
// long as the map took to drain. The map's own wall-clock is unchanged by the
// split (1.054s -> 1.079s), so this buys latency isolation for free.

type sleepingBodyHandler struct {
	d  time.Duration
	mu sync.Mutex
	n  int
}

func (h *sleepingBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_sleep"}
}

func (h *sleepingBodyHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	time.Sleep(h.d)
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"id": in.Data["$item"]}}, nil
}

func (h *sleepingBodyHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// stampingHandler records the wall-clock instant of its first invocation. That
// instant — not the execution's completion — is the latency that matters: it is
// when the small execution stopped waiting for a queue slot.
type stampingHandler struct {
	mu    sync.Mutex
	first time.Time
}

func (h *stampingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.stamp"}
}

func (h *stampingHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.mu.Lock()
	if h.first.IsZero() {
		h.first = time.Now()
	}
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *stampingHandler) firstAt() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.first
}

func TestASmallExecutionDoesNotWaitBehindAWideMap(t *testing.T) {
	const batches = 200
	const perItem = 20 * time.Millisecond
	// The map takes ~1s of wall-clock. Before the lane split the small
	// execution waited for all of it; the threshold has to be far enough below
	// that to be a real assertion and far enough above the measured 10-12ms to
	// survive a loaded CI box.
	const maxWait = 250 * time.Millisecond

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &wideFanoutHandler{n: batches})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &sleepingBodyHandler{d: perItem}
	reg.RegisterGlobal("test.body_sleep", body)
	stamp := &stampingHandler{}
	reg.RegisterGlobal("test.stamp", stamp)

	b := New(WithConcurrency(4), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	defer b.Bind(eng)()

	bigDef := &types.WorkflowDef{
		Name: "big-map",
		Nodes: []types.NodeDef{{Name: "m", Type: "xflow.map", Parameters: map[string]any{
			"items": "$input.items",
			"body": map[string]any{
				"type": "xflow.subgraph",
				"parameters": map[string]any{
					"nodes": []any{map[string]any{"name": "s", "type": "test.body_sleep"}},
				},
			},
		}}},
	}
	bigCompiled, err := graph.Compile(bigDef)
	if err != nil {
		t.Fatalf("compile big: %v", err)
	}
	smallCompiled, err := graph.Compile(&types.WorkflowDef{
		Name:  "small",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.stamp"}},
	})
	if err != nil {
		t.Fatalf("compile small: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bigID, err := eng.Submit(ctx, bigCompiled, nil)
	if err != nil {
		t.Fatalf("submit big: %v", err)
	}

	// Give the map time to expand and saturate the queue before the small
	// execution arrives — the scenario is "a latency-sensitive request lands
	// while a batch job is already running", not a race at t=0.
	time.Sleep(200 * time.Millisecond)
	submitted := time.Now()
	smallID, err := eng.Submit(ctx, smallCompiled, nil)
	if err != nil {
		t.Fatalf("submit small: %v", err)
	}
	if _, err := b.WaitDone(ctx, smallID); err != nil {
		t.Fatalf("WaitDone small: %v", err)
	}
	waited := stamp.firstAt().Sub(submitted)
	if _, err := b.WaitDone(ctx, bigID); err != nil {
		t.Fatalf("WaitDone big: %v", err)
	}

	if waited > maxWait {
		t.Errorf("small execution waited %s for its first node, want < %s — "+
			"the map's batches are back at the head of the shared line",
			waited.Round(time.Millisecond), maxWait)
	}
	// The isolation must not have come at the cost of dropping batch work.
	if got := body.count(); got < batches {
		t.Errorf("body ran %d times, want at least %d", got, batches)
	}
}

// The batch lane is served less often, never not at all. A steady stream of
// ordinary tasks must not park an in-flight map forever: strict priority is as
// fast as this on the head-of-line measurement but starves a map that shares
// its worker with a busy interactive lane, and nothing reports the stall.
func TestAnInteractiveStreamCannotStarveTheBatchLane(t *testing.T) {
	const interactive = 100
	const batchTasks = 5

	q := newMemoryQueue(1)
	var mu sync.Mutex
	var interactiveDone int
	// batchAt[i] is how many interactive tasks had completed when the i-th
	// batch task ran.
	var batchAt []int
	q.SetHandler(func(_ context.Context, task *engine.Task) error {
		mu.Lock()
		defer mu.Unlock()
		if task.Type == engine.TaskTypeNodeBatch {
			batchAt = append(batchAt, interactiveDone)
		} else {
			interactiveDone++
		}
		return nil
	})

	ctx := context.Background()
	// Both lanes are loaded before any worker starts, so the interleaving under
	// test is the worker's choice, not a race between producers.
	for i := 0; i < batchTasks; i++ {
		if err := q.Enqueue(ctx, &engine.Task{ExecutionID: "big", NodeName: "m", Type: engine.TaskTypeNodeBatch}); err != nil {
			t.Fatalf("enqueue batch: %v", err)
		}
	}
	for i := 0; i < interactive; i++ {
		if err := q.Enqueue(ctx, &engine.Task{ExecutionID: "small", NodeName: "n"}); err != nil {
			t.Fatalf("enqueue interactive: %v", err)
		}
	}

	q.Start()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := interactiveDone == interactive && len(batchAt) == batchTasks
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			mu.Lock()
			t.Fatalf("queue did not drain: %d/%d interactive, %d/%d batch",
				interactiveDone, interactive, len(batchAt), batchTasks)
		}
		time.Sleep(time.Millisecond)
	}
	q.Stop()

	// With interactiveBurst=8 the batch tasks land at roughly 8, 16, 24, 32, 40
	// interactive tasks in. Strict priority would put every one of them at 100.
	if got := batchAt[len(batchAt)-1]; got >= interactive {
		t.Errorf("the last batch task ran only after all %d interactive tasks "+
			"(positions %v) — the batch lane is starved", interactive, batchAt)
	}
}

// A batch task that is delayed or requeued must come back on the batch lane.
// Routing only at the two Enqueue entry points leaves both of these paths
// hardcoded to the interactive lane, which quietly re-opens the head-of-line
// block for exactly the batches that hit a retry.
func TestDelayedAndRequeuedBatchTasksStayOnTheBatchLane(t *testing.T) {
	t.Run("delayed", func(t *testing.T) {
		q := newMemoryQueue(1)
		batch := &engine.Task{ExecutionID: "e", NodeName: "m", Type: engine.TaskTypeNodeBatch}
		if err := q.EnqueueDelayed(context.Background(), batch, time.Millisecond); err != nil {
			t.Fatalf("EnqueueDelayed: %v", err)
		}
		select {
		case env := <-q.batchCh:
			if env.task.Type != engine.TaskTypeNodeBatch {
				t.Fatalf("batch lane delivered a %v task", env.task.Type)
			}
		case env := <-q.ch:
			t.Fatalf("delayed batch task landed on the interactive lane: %+v", env.task)
		case <-time.After(2 * time.Second):
			t.Fatal("delayed batch task never arrived")
		}
		q.Stop()
	})

	t.Run("requeued after a transient failure", func(t *testing.T) {
		q := newMemoryQueue(1)
		q.SetHandler(func(_ context.Context, _ *engine.Task) error {
			return errors.New("no runner yet")
		})
		batch := &engine.Task{ExecutionID: "e", NodeName: "m", Type: engine.TaskTypeNodeBatch}
		// dispatch directly: no workers are running, so whatever lane the
		// requeue picks is observable rather than immediately consumed.
		q.dispatch(queueEnvelope{task: batch})
		select {
		case env := <-q.batchCh:
			if env.transientTries != 1 {
				t.Fatalf("requeued envelope transientTries = %d, want 1", env.transientTries)
			}
		case env := <-q.ch:
			t.Fatalf("requeued batch task landed on the interactive lane: %+v", env.task)
		case <-time.After(2 * time.Second):
			t.Fatal("requeued batch task never arrived")
		}
		q.Stop()
	})
}
