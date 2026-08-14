package local

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// wideFanoutHandler emits n batches of one item each — the descriptor an
// xflow.map node produces for a body-form expansion.
type wideFanoutHandler struct{ n int }

func (h *wideFanoutHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *wideFanoutHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	items := make([]any, h.n)
	batches := make([][]any, h.n)
	for i := 0; i < h.n; i++ {
		items[i] = map[string]any{"id": i}
		batches[i] = []any{items[i]}
	}
	return &types.Output{Data: map[string]any{
		"items":       items,
		"batches":     batches,
		"batch_size":  1,
		"total":       h.n,
		"batch_count": h.n,
	}}, nil
}

type countingBodyHandler struct {
	mu sync.Mutex
	n  int
}

func (h *countingBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *countingBodyHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"id": in.Data["$item"]}}, nil
}

func (h *countingBodyHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// newFanOutEngine builds a backend whose map bodies run on an inner engine,
// the same wiring sdk/xflow and the runner's group/subgraph runtimes use.
func newFanOutEngine(t *testing.T, concurrency int, perMap int, opts ...engine.Option) (*Backend, *engine.Engine, *countingBodyHandler, func()) {
	t.Helper()
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &wideFanoutHandler{n: perMap})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	body := &countingBodyHandler{}
	reg.RegisterGlobal("test.body_item", body)

	b := New(WithConcurrency(concurrency), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(),
		append([]engine.Option{engine.WithBatchBodyExecutor(bodies)}, opts...)...)
	return b, eng, body, b.Bind(eng)
}

func mapBodyDef() map[string]any {
	return map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{map[string]any{"name": "echo", "type": "test.body_item"}},
		},
	}
}

// A single fan-out wider than the queue's buffer must complete.
//
// FlushOutbox runs ON a queue worker goroutine (engine/atomic.go dispatches
// TaskTypeNodeBatch through ExecuteBatch). Before the fix, memoryQueue.Enqueue
// blocked when the 1024-slot channel filled, so the only goroutine that could
// drain the channel was the one trying to fill it. With one worker a fan-out of
// 1500 wedged permanently: zero body invocations, no error, no log.
func TestFanOutWiderThanTheQueueBufferCompletesOnOneWorker(t *testing.T) {
	const n = 1500
	b, eng, body, stop := newFanOutEngine(t, 1, n)
	defer stop()

	def := &types.WorkflowDef{
		Name: "wide-fanout",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items", "body": mapBodyDef(),
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, compiled, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v (body ran %d/%d) — a fan-out wider than the queue "+
			"buffer wedged the only worker inside FlushOutbox", err, body.count(), n)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %v, want success", res.Status)
	}
	// Exactly n: one worker means no concurrent FlushOutbox, and the delivery
	// lease covers the concurrent case. See the multi-worker test.
	if got := body.count(); got != n {
		t.Errorf("body ran %d times, want exactly %d", got, n)
	}
}

// The general form: no single map exceeds the buffer, but their batches share
// one queue. Four parallel maps × 400 batches = 1600 > 1024 on the DEFAULT
// concurrency of 4 — every worker ends up inside FlushOutbox at once.
//
// This is the configuration that matters: the deadlock needed no unusual
// setup, just a wide enough total fan-out.
func TestParallelFanOutsCompleteOnTheDefaultWorkerPool(t *testing.T) {
	const perMap, maps = 400, 4
	b, eng, body, stop := newFanOutEngine(t, 4, perMap)
	defer stop()

	nodes := []types.NodeDef{{Name: "start", Type: "test.echo"}}
	conns := types.Connections{"start": {"main": {Targets: []types.Connection{}}}}
	for i := 0; i < maps; i++ {
		name := fmt.Sprintf("m%d", i)
		nodes = append(nodes, types.NodeDef{Name: name, Type: "xflow.map", Parameters: map[string]any{
			"items": "$input.items", "body": mapBodyDef(),
		}})
		tg := conns["start"]["main"]
		tg.Targets = append(tg.Targets, types.Connection{Node: name, Input: "main"})
		conns["start"]["main"] = tg
	}
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "parallel-maps", Nodes: nodes, Connections: conns,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, compiled, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := b.WaitDone(ctx, id); err != nil {
		t.Fatalf("WaitDone: %v (body ran %d/%d) — all four workers wedged inside "+
			"FlushOutbox", err, body.count(), perMap*maps)
	}
	// Exactly n even with four workers: ListOutbox leases every entry it hands
	// out, so a concurrent flush of the same execution no longer lists work
	// that is already in flight. Delivery is still at-least-once — a deliverer
	// that dies leaves its entries to be relisted once the lease lapses — but
	// ordinary concurrency is no longer a source of duplicates.
	// TestConcurrentFlushDoesNotRedeliverTheSameIntent pins that directly.
	if got := body.count(); got != perMap*maps {
		t.Errorf("body ran %d times, want exactly %d", got, perMap*maps)
	}
}

// Backpressure must not consume the delivery-attempt budget.
//
// FlushOutbox's ordinary enqueue-failure path calls RecordOutboxFailure, which
// increments Attempts and dead-letters the entry once it reaches the limit. A
// full queue is normal backpressure — routing it through that path would
// silently dead-letter healthy intents on any sufficiently wide fan-out, with
// no symptom until the work simply never ran.
func TestQueueFullDoesNotSpendTheDeliveryBudget(t *testing.T) {
	const n = 1500
	obs := &outboxDeliveryObserver{}
	// One worker and a fan-out of n fills the 1024-slot buffer repeatedly, so
	// every backpressure event this execution produces shows up here.
	b, eng, body, stop := newFanOutEngine(t, 1, n, engine.WithOutboxObserver(obs))
	defer stop()

	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "budget",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items", "body": mapBodyDef(),
			}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, compiled, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := b.WaitDone(ctx, id); err != nil {
		t.Fatalf("WaitDone: %v (body ran %d/%d)", err, body.count(), n)
	}

	if got := obs.deadLetters(); got != 0 {
		t.Errorf("dead-lettered %d intents; backpressure was charged as delivery "+
			"failure, so a wide enough fan-out silently drops work", got)
	}
	if got := obs.retries(); got != 0 {
		t.Errorf("recorded %d delivery retries; a full queue is backpressure, "+
			"not a failed delivery attempt", got)
	}
	if got := obs.deliveryErrors(); got != 0 {
		t.Errorf("reported %d delivery errors; a full queue is not a delivery "+
			"failure and must not be surfaced as one", got)
	}
	// Delivery still completed — backpressure only defers, and the entries left
	// behind are picked up by the next FlushOutbox re-entry.
	if got := body.count(); got != n {
		t.Errorf("body ran %d times, want %d — deferred intents were not redelivered", got, n)
	}
}

// TryEnqueue reports fullness rather than blocking, and Enqueue keeps blocking.
//
// The asymmetry is load-bearing. Submit's initial tasks and the legacy
// lease-revoke redelivery have no durable intent behind them — failing those on
// a transient full queue strands work no sweeper can recover (see the comment
// at engine/lease.go's Enqueue call). Only FlushOutbox, which does have the
// outbox to fall back on, may take the non-blocking path.
func TestTryEnqueueReportsFullnessWhileEnqueueBlocks(t *testing.T) {
	q := newMemoryQueue(1)
	q.ch = make(chan queueEnvelope, 1)
	task := &engine.Task{ExecutionID: "e", NodeName: "n"}

	if err := q.TryEnqueue(context.Background(), task); err != nil {
		t.Fatalf("TryEnqueue into an empty queue: %v", err)
	}
	err := q.TryEnqueue(context.Background(), task)
	if !errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue on a full queue = %v, want engine.ErrQueueFull", err)
	}

	// Enqueue must still wait for room rather than reporting fullness.
	blocked := make(chan error, 1)
	go func() { blocked <- q.Enqueue(context.Background(), task) }()
	select {
	case err := <-blocked:
		t.Fatalf("Enqueue returned %v on a full queue; callers without a durable "+
			"intent rely on it waiting", err)
	case <-time.After(50 * time.Millisecond):
	}
	<-q.ch // make room
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("Enqueue after room freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue did not proceed after room was freed")
	}
}

// outboxDeliveryObserver counts the three signals that would fire if
// backpressure were charged against the delivery budget.
type outboxDeliveryObserver struct {
	mu     sync.Mutex
	dead   int
	retry  int
	errors int
}

func (o *outboxDeliveryObserver) OnOutboxRetry(_ context.Context, _ int) {
	o.mu.Lock()
	o.retry++
	o.mu.Unlock()
}

func (o *outboxDeliveryObserver) OnOutboxDeadLetter(_ context.Context) {
	o.mu.Lock()
	o.dead++
	o.mu.Unlock()
}

func (o *outboxDeliveryObserver) OnOutboxReplayed(_ context.Context, _ engine.DeadLetterReplayOutcome) {
}

func (o *outboxDeliveryObserver) OnOutboxPending(_ context.Context, _ int, _ int, _ time.Duration) {
}

func (o *outboxDeliveryObserver) OnOutboxError(_ context.Context, operation string, _ error) {
	if operation != "delivery" {
		return
	}
	o.mu.Lock()
	o.errors++
	o.mu.Unlock()
}

func (o *outboxDeliveryObserver) deadLetters() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dead
}

func (o *outboxDeliveryObserver) retries() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retry
}

func (o *outboxDeliveryObserver) deliveryErrors() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.errors
}
