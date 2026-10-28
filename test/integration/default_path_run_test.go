//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"

	_ "github.com/xbcio/xflow/node" // registers the builtin nodes (start, end, map, wait)
)

const (
	dpItems     = 12 // items the map node fans out
	dpBatchSize = 4  // items per batch task, so dpItems/dpBatchSize batch tasks
)

// dpFlaky fails transiently on its first call and succeeds from then on, which
// makes the node retry exactly once.
type dpFlaky struct{ calls atomic.Int32 }

func (*dpFlaky) Descriptor() types.Descriptor { return types.Descriptor{Type: "dp.flaky"} }

func (h *dpFlaky) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	if h.calls.Add(1) == 1 {
		return nil, types.NewTransientError("dp_flaky", "first call fails")
	}
	return &types.Output{Data: map[string]any{"recovered": true}}, nil
}

// dpWorkflowDef is the default-path workflow: start -> fan (map, several
// batches) -> flaky (one retry) -> w (suspends until a signal) -> done.
func dpWorkflowDef() *types.WorkflowDef {
	link := func(to string) types.PortConnections {
		return types.PortConnections{Targets: []types.Connection{{Node: to, Input: "main"}}}
	}
	return &types.WorkflowDef{
		Name: "default-path-baseline",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "fan", Type: "xflow.map", Parameters: map[string]any{
				"items":      "$input.items",
				"batch_size": dpBatchSize,
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{map[string]any{
							"name": "sq", "type": "xflow.function",
							"parameters": map[string]any{"code": `{"id": $item.id, "sq": $item.id * $item.id}`},
						}},
						"connections": map[string]any{},
					},
				},
			}},
			{Name: "flaky", Type: "dp.flaky", Retry: &types.RetrySettings{
				MaxAttempts: 3, Strategy: types.RetryStrategyFixed, InitialInterval: 200,
			}},
			{Name: "w", Type: "xflow.wait", Parameters: map[string]any{"mode": "signal", "signal_name": "go"}},
			{Name: "done", Type: "xflow.end"},
		},
		Connections: types.Connections{
			"start": {"main": link("fan")},
			"fan":   {"main": link("flaky")},
			"flaky": {"main": link("w")},
			"w":     {"main": link("done")},
		},
	}
}

// dpWire is the queue payload: the public task plus the scheduler metadata the
// transport carries under underscore-prefixed keys. It is the decoding half of
// the transport's codec, kept here because that codec is internal.
type dpWire struct {
	engine.Task
	Namespace         namespace.Namespace `json:"_namespace"`
	AutoDepth         int                 `json:"_auto_depth"`
	ActivationID      int                 `json:"_activation_id"`
	UnitIdx           *int                `json:"_unit_idx"`
	DeliverableAtMs   int64               `json:"_deliverable_at_ms"`
	IntentCreatedAtMs int64               `json:"_intent_created_at_ms"`
	Generation        int                 `json:"_generation"`
}

// dpDecode turns a queue payload back into the task a consumer would run.
func dpDecode(body []byte) (*engine.Task, namespace.Namespace, error) {
	var w dpWire
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, "", err
	}
	if w.UnitIdx == nil {
		return nil, "", fmt.Errorf("payload %.80s carries no _unit_idx", body)
	}
	t := w.Task
	t.AutoDepth, t.ActivationID, t.Generation, t.UnitIdx = w.AutoDepth, w.ActivationID, w.Generation, *w.UnitIdx
	if w.DeliverableAtMs > 0 {
		t.DeliverableAt = time.UnixMilli(w.DeliverableAtMs).UTC()
	}
	if w.IntentCreatedAtMs > 0 {
		t.IntentCreatedAt = time.UnixMilli(w.IntentCreatedAtMs).UTC()
	}
	return &t, w.Namespace, nil
}

// dpRun is one assembled process of the default path: a distributed backend
// on real Redis that only produces (it starts no consumer of its own), an
// engine whose queue records the outbox before each delivery, and a test-side
// asynq server that reads each payload before handing the task to the same
// embedded dispatcher the backend's own consumer would call.
type dpRun struct {
	ns    namespace.Namespace
	addr  string
	b     *distributed.Backend
	eng   *engine.Engine
	cap   *dpCapture
	flaky *dpFlaky
}

// newDPRun assembles the stack the way sdk/xflow's NewCluster does, with the
// consumer swapped for dpRun.serve so the payloads can be read.
func newDPRun(t *testing.T, addr string) *dpRun {
	t.Helper()
	// "ns-<uuid>" is one of the random-id shapes the normalizer masks, so the
	// _namespace field of every queue payload normalizes to a fixed text.
	ns := namespace.Namespace("ns-" + uuid.NewString())
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	r := &dpRun{ns: ns, addr: addr, cap: newDPCapture(rdb, ns), flaky: &dpFlaky{}}
	t.Cleanup(func() { r.cleanup(t, rdb) })
	b, err := distributed.New(addr, nil,
		distributed.WithConcurrency(1),
		distributed.WithConsumer(false),
		distributed.WithOwnedNamespaces([]namespace.Namespace{ns}),
	)
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("registry %T is not a HandlerRegistrar", b.Registry())
	}
	reg.RegisterGlobal("dp.flaky", r.flaky)
	r.b = b
	r.eng = engine.New(b.State(), &dpRecordingQueue{next: b.Queue(), cap: r.cap},
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithNamespace(ns),
		engine.WithBatchBodyExecutor(dpBodyExecutor(t, b)),
	)
	stop, err := b.StartBinding(r.eng) // consumer off: this only closes what the backend owns
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	t.Cleanup(stop)
	// The backend starts no outbox dispatcher without a consumer, and the
	// delayed retry intent is delivered by nothing else, so the run starts the
	// engine's own dispatcher with the interval the backend would use.
	dctx, stopDispatch := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.NewOutboxDispatcher(r.eng, time.Second).Run(dctx)
	}()
	t.Cleanup(func() { stopDispatch(); <-done })
	return r
}

// dpBodyExecutor builds the executor that runs a map body per item inside the
// batch task, the same wiring local_map_batch_roundtrip_test.go uses.
func dpBodyExecutor(t *testing.T, b *distributed.Backend) *subgraph.MapBodyExecutor {
	t.Helper()
	reg, ok := b.Registry().(*execution.Registry)
	if !ok {
		t.Fatalf("registry %T is not *execution.Registry", b.Registry())
	}
	exec := subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
		func() subgraph.Backend {
			return backendlocal.New(backendlocal.WithRegistry(reg), backendlocal.WithConcurrency(1))
		})
	return subgraph.NewMapBodyExecutor(exec, true, time.Time{})
}

// dpTaskType and the queue names are the transport's own; the transport keeps
// them unexported, and a payload read from any other queue would not be one.
const (
	dpTaskType     = "xflow:node"
	dpDefaultQueue = "xflow:default:ns:"
	dpBatchQueue   = "xflow:batch:ns:"
)

// serve starts the consumer of the run: an asynq server on the run's two
// queues whose handler records the payload it was given and then dispatches the
// task exactly as the backend's consumer does. One worker keeps the order in
// which tasks are handled stable.
func (r *dpRun) serve(t *testing.T) {
	t.Helper()
	disp := execution.NewEmbeddedDispatcher(r.eng, r.b.Registry())
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(dpTaskType, func(ctx context.Context, at *asynqlib.Task) error {
		id, _ := asynqlib.GetTaskID(ctx)
		r.cap.payload(id, at.Payload())
		task, ns, err := dpDecode(at.Payload())
		if err != nil {
			r.cap.fail(err)
			return fmt.Errorf("%w: %w", asynqlib.SkipRetry, err)
		}
		err = disp.HandleTask(namespace.WithNamespace(ctx, ns), task)
		if err != nil {
			r.cap.fail(fmt.Errorf("handle %s/%s: %w", task.ExecutionID, task.NodeName, err))
		}
		return err
	})
	srv := asynqlib.NewServer(asynqlib.RedisClientOpt{Addr: r.addr}, asynqlib.Config{
		Concurrency: 1,
		Queues:      map[string]int{dpDefaultQueue + string(r.ns): 8, dpBatchQueue + string(r.ns): 1},
	})
	if err := srv.Start(mux); err != nil {
		t.Fatalf("start asynq server: %v", err)
	}
	t.Cleanup(srv.Shutdown)
}

// cleanup removes what this run left in Redis, and only that: the keys whose
// name carries the run's random namespace, its entry in the namespace set, and
// its queues' entries in asynq's queue set. It never flushes a database.
func (r *dpRun) cleanup(t *testing.T, rdb *redis.Client) {
	t.Helper()
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*"+string(r.ns)+"*", 500).Result()
		if err != nil {
			t.Errorf("scan redis keys for %s: %v", r.ns, err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("del redis keys for %s: %v", r.ns, err)
				return
			}
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if err := rdb.SRem(ctx, "xflow:namespaces", string(r.ns)).Err(); err != nil {
		t.Errorf("srem xflow:namespaces %s: %v", r.ns, err)
	}
	queues, err := rdb.SMembers(ctx, "asynq:queues").Result()
	if err != nil {
		t.Errorf("smembers asynq:queues: %v", err)
		return
	}
	for _, q := range queues {
		if strings.Contains(q, string(r.ns)) {
			if err := rdb.SRem(ctx, "asynq:queues", q).Err(); err != nil {
				t.Errorf("srem asynq:queues %s: %v", q, err)
			}
		}
	}
}

// dpPollEvery is how often dpUntil looks at the store while it waits.
const dpPollEvery = 50 * time.Millisecond

// dpUntil polls cond until it holds or ctx ends.
func dpUntil(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	tick := time.NewTicker(dpPollEvery)
	defer tick.Stop()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		case <-tick.C:
		}
	}
}

// drive runs the workflow to completion: submit, wait for the wait node to
// suspend, deliver its signal, and wait for the terminal state. The fan node
// splits dpItems into batches, the flaky node retries once, and the signal
// resumes the suspended node, so every task kind of the default path is hit.
func (r *dpRun) drive(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespace.WithNamespace(context.Background(), r.ns), 90*time.Second)
	defer cancel()
	g, err := graph.Compile(dpWorkflowDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	items := make([]any, dpItems)
	for i := range items {
		items[i] = map[string]any{"id": i}
	}
	id, err := r.eng.Submit(ctx, g, map[string]any{"items": items})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	st := r.b.State()
	dpUntil(ctx, t, "w to suspend", func() bool {
		n, _ := st.GetNode(ctx, id, "w")
		return n != nil && n.Status == types.NodeStatusSuspended
	})
	if err := r.eng.DeliverSignal(ctx, id, "go", map[string]any{"v": 1}); err != nil {
		t.Fatalf("DeliverSignal: %v", err)
	}
	res := waitForCompletion(ctx, t, st, id, "fan", "flaky", "w")
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success; outputs: %v", res.Status, res.Output)
	}
	if got := r.flaky.calls.Load(); got != 2 {
		t.Fatalf("flaky node ran %d times, want 2 (one failure, one retry)", got)
	}
	r.settle(ctx, t)
}

// settle waits until both queues of the run hold nothing, so every payload that
// was going to be handled has been handled before the capture is read.
func (r *dpRun) settle(ctx context.Context, t *testing.T) {
	t.Helper()
	insp := asynqlib.NewInspector(asynqlib.RedisClientOpt{Addr: r.addr})
	defer func() { _ = insp.Close() }()
	names := []string{dpDefaultQueue + string(r.ns), dpBatchQueue + string(r.ns)}
	dpUntil(ctx, t, "the run's queues to drain", func() bool {
		for _, q := range names {
			info, err := insp.GetQueueInfo(q)
			if errors.Is(err, asynqlib.ErrQueueNotFound) {
				continue
			}
			if err != nil || info.Size != 0 {
				return false
			}
		}
		return true
	})
}
