package distributed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// swRecoveryHandler counts how many times a node of its type really ran. A
// child rebuilt under the same derived id by the parent's reclaim shows up here
// as a second run of the child's handler.
type swRecoveryHandler struct {
	nodeType string
	runs     atomic.Int32
	data     map[string]any
}

func (h *swRecoveryHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.nodeType} }

func (h *swRecoveryHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.runs.Add(1)
	return &types.Output{Data: h.data}, nil
}

type swRecoveryResolver struct{ target graph.SubWorkflowTarget }

func (r swRecoveryResolver) ResolveSubWorkflow(context.Context, graph.SubWorkflowRef) (graph.SubWorkflowTarget, error) {
	return r.target, nil
}

// swRecoveryTransport is an in-process queue.Transport: it delivers every
// enqueued task to the bound handler, so the whole path (outbox flush, embedded
// dispatcher, system-task handling) runs without a broker. A broker is not what
// this test is about, and asynq would only add redelivery timing to it.
type swRecoveryTransport struct {
	tasks chan *engine.Task
	once  sync.Once

	mu       sync.Mutex
	handlers sync.WaitGroup
	errs     []error
}

func newSWRecoveryTransport() *swRecoveryTransport {
	return &swRecoveryTransport{tasks: make(chan *engine.Task, 256)}
}

func (tr *swRecoveryTransport) Enqueue(_ context.Context, task *engine.Task) error {
	tr.tasks <- task
	return nil
}

func (tr *swRecoveryTransport) EnqueueDelayed(ctx context.Context, task *engine.Task, delay time.Duration) error {
	time.AfterFunc(delay, func() { tr.tasks <- task })
	return nil
}

func (tr *swRecoveryTransport) StartConsumer(_ queue.ConsumerConfig, handler queue.TaskHandler) (func(), error) {
	done := make(chan struct{})
	for range 4 {
		tr.handlers.Add(1)
		go func() {
			defer tr.handlers.Done()
			for {
				select {
				case <-done:
					return
				case task := <-tr.tasks:
					if err := handler(context.Background(), task); err != nil {
						tr.mu.Lock()
						tr.errs = append(tr.errs, fmt.Errorf("%s/%s: %w", task.ExecutionID, task.NodeName, err))
						tr.mu.Unlock()
					}
				}
			}
		}()
	}
	return func() {
		tr.once.Do(func() { close(done) })
		tr.handlers.Wait()
	}, nil
}

func (tr *swRecoveryTransport) Close() error { return nil }

func (tr *swRecoveryTransport) deliveryErrors() []error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]error(nil), tr.errs...)
}

// errSWRecoveryRefused is the injected refusal of the parent's commit. A
// delivery that hits it fails on purpose; the test tolerates exactly this error
// and no other.
var errSWRecoveryRefused = errors.New("test: parent commit refused")

// swRecoveryState refuses the parent's "sub" commit while armed: the stand-in
// for the notifier dying between recording the child's conclusion and
// committing the parent. It embeds the concrete store so every optional
// capability the engine probes for stays visible.
type swRecoveryState struct {
	*rstate.Store
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *swRecoveryState) CommitNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	if s.armed.Load() && req.NodeName == "sub" {
		s.refused.Add(1)
		return engine.CommitNodeResult{}, errSWRecoveryRefused
	}
	return s.Store.CommitNode(ctx, req)
}

// executionKeys lists every key of one execution in the default namespace. All
// of an execution's keys share one hash tag, so the pattern is exactly that
// execution's slot and nothing of any other execution.
func executionKeys(t *testing.T, rdb redis.UniversalClient, id types.ExecutionID) []string {
	t.Helper()
	pattern := fmt.Sprintf("xflow:ns:%s:exec:{%s}:*", namespace.Default, id)
	var keys []string
	iter := rdb.Scan(context.Background(), 0, pattern, 100).Iterator()
	for iter.Next(context.Background()) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("scan %s: %v", pattern, err)
	}
	return keys
}

// deleteExecutionKeys is the recovery injection: it removes every key of one
// execution and nothing else, the state a retention policy leaves behind once
// it has expired a terminal child while the parent that waited on it is alive.
func deleteExecutionKeys(t *testing.T, rdb redis.UniversalClient, id types.ExecutionID) int {
	t.Helper()
	keys := executionKeys(t, rdb, id)
	for _, key := range keys {
		if err := rdb.Del(context.Background(), key).Err(); err != nil {
			t.Fatalf("DEL %s: %v", key, err)
		}
	}
	return len(keys)
}

// runSubWorkflowConclusionRecovery drives the guarantee the recorded conclusion
// exists for on the Redis state store: the child runs once and concludes, its
// conclusion is recorded in the parent's slot, the parent's commit does not
// happen, the child's keys are deleted, and the parent's reclaim must finish the
// node from the conclusion without running the child's handler again.
func runSubWorkflowConclusionRecovery(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	work := &swRecoveryHandler{nodeType: "test.sw_recovery_work", data: map[string]any{"total": float64(7)}}
	after := &swRecoveryHandler{nodeType: "test.sw_recovery_after", data: map[string]any{"done": true}}

	childGraph, err := graph.Compile(&types.WorkflowDef{
		Name:    "child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "work", Type: work.nodeType}},
	})
	if err != nil {
		t.Fatalf("compile child: %v", err)
	}
	resolver := swRecoveryResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          childGraph,
	}}
	parentGraph, err := graph.CompileWithResolver(ctx, &types.WorkflowDef{
		Name: "parent",
		Nodes: []types.NodeDef{
			{Name: "sub", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "child"}},
			{Name: "after", Type: after.nodeType},
		},
		Connections: types.Connections{
			"sub": {"main": {Targets: []types.Connection{{Node: "after"}}}},
		},
	}, resolver)
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	transport := newSWRecoveryTransport()
	b, err := New(addr, nil, WithTransport(transport), WithConcurrency(4))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	b.registry.RegisterGlobal(work.nodeType, work)
	b.registry.RegisterGlobal(after.nodeType, after)

	state := &swRecoveryState{Store: b.state}
	state.armed.Store(true)
	eng := engine.New(state, b.Queue(),
		engine.WithSubWorkflowResolver(resolver),
		engine.WithDefaultLeaseTTL(time.Minute))
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding() error = %v", err)
	}
	defer stop()

	// The backend closes its own client when it stops, so cleanup and the
	// injection use a client of their own.
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	parentID, err := eng.Submit(ctx, parentGraph, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var childID types.ExecutionID
	t.Cleanup(func() {
		cleanup := redis.NewClient(&redis.Options{Addr: addr})
		defer func() { _ = cleanup.Close() }()
		for _, id := range []types.ExecutionID{parentID, childID} {
			if id != "" {
				deleteExecutionKeys(t, cleanup, id)
			}
		}
	})

	// The child concludes, the conclusion is recorded, and the parent's commit
	// is refused: the crash window between the two writes.
	waitUntil(t, 20*time.Second, "the notifier to try the parent's commit", func() bool { return state.refused.Load() > 0 })
	link, found, err := state.GetSubWorkflowLink(ctx, parentID, "sub")
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowLink: found=%v err=%v, want the parked link", found, err)
	}
	childID = link.ChildExecutionID
	conclusion, found, err := state.GetSubWorkflowConclusion(ctx, parentID, "sub", childID)
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowConclusion: found=%v err=%v, want the conclusion recorded before the commit", found, err)
	}
	if conclusion.Status != types.ExecutionStatusSuccess {
		t.Fatalf("recorded conclusion = %+v, want success", conclusion)
	}
	if got, ok := conclusion.Output["work"].(map[string]any); !ok || got["total"] != float64(7) {
		t.Fatalf("recorded conclusion output = %#v, want the child's public node outputs", conclusion.Output)
	}
	if n := work.runs.Load(); n != 1 {
		t.Fatalf("child handler ran %d times before the reclaim, want 1", n)
	}
	if node, _ := state.GetNode(ctx, parentID, "sub"); node == nil || node.Status != types.NodeStatusWaiting {
		t.Fatalf("parent node = %+v, want it still parked", node)
	}

	// Recovery injection: only the child's keys go. The parent's keys, which
	// hold the link and the conclusion, are untouched.
	if deleted := deleteExecutionKeys(t, rdb, childID); deleted == 0 {
		t.Fatalf("the child %q had no keys to delete: the injection did nothing", childID)
	}
	if snap, err := state.GetExecution(ctx, childID); err != nil || snap != nil {
		t.Fatalf("GetExecution(child) = %+v, %v after the injection, want absent", snap, err)
	}
	if _, found, err := state.GetSubWorkflowConclusion(ctx, parentID, "sub", childID); err != nil || !found {
		t.Fatalf("the conclusion did not survive the child's deletion: found=%v err=%v", found, err)
	}

	// The wait's lease expires and the sweeper reclaims it.
	state.armed.Store(false)
	expired, err := state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	var target *engine.ExpiredLease
	for i := range expired {
		if expired[i].ExecutionID == parentID && expired[i].NodeName == "sub" {
			target = &expired[i]
		}
	}
	if target == nil {
		t.Fatalf("expired leases = %+v, want the parked sub node's", expired)
	}
	if reclaimed, err := eng.ReclaimLease(ctx, *target); err != nil || !reclaimed {
		t.Fatalf("ReclaimLease reclaimed=%v err=%v, want true/nil", reclaimed, err)
	}

	waitUntil(t, 30*time.Second, "the parent to succeed", func() bool {
		snap, err := state.GetExecution(ctx, parentID)
		return err == nil && snap != nil && snap.Status == types.ExecutionStatusSuccess
	})
	if n := work.runs.Load(); n != 1 {
		t.Fatalf("child handler ran %d times, want 1: the reclaim rebuilt and reran the child", n)
	}
	if n := after.runs.Load(); n != 1 {
		t.Fatalf("downstream handler ran %d times, want 1", n)
	}
	if keys := executionKeys(t, rdb, childID); len(keys) != 0 {
		t.Fatalf("the child has keys again after the reclaim (%v): it was rebuilt", keys)
	}
	output, err := state.GetOutput(ctx, parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	if got, ok := output["work"].(map[string]any); !ok || got["total"] != float64(7) {
		t.Fatalf("sub node output = %#v, want the recorded child output", output)
	}
	// Which delivery meets the armed refusal depends on timing: the child's
	// completion notification, or the parent's own sub task finding the child
	// already concluded. Either fails on purpose; any other failure is real.
	for _, err := range transport.deliveryErrors() {
		if !errors.Is(err, errSWRecoveryRefused) {
			t.Fatalf("task delivery failed: %v", err)
		}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// TestMiniredisSubWorkflowReclaimConcludesFromTheRecordedConclusion runs the
// recovery injection on the Redis state store over miniredis.
func TestMiniredisSubWorkflowReclaimConcludesFromTheRecordedConclusion(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	runSubWorkflowConclusionRecovery(t, srv.Addr())
}

// TestRedisSubWorkflowReclaimConcludesFromTheRecordedConclusion runs the same
// recovery injection on a real Redis when XFLOW_TEST_REDIS_ADDR is set. It
// deletes only the keys of the executions it created and never flushes the
// database.
func TestRedisSubWorkflowReclaimConcludesFromTheRecordedConclusion(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping real-Redis recovery injection")
	}
	probe := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := probe.Ping(pingCtx).Err()
	_ = probe.Close()
	if err != nil {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_REDIS_INTEGRATION=1: redis unavailable at %s: %v", addr, err)
		}
		t.Skipf("real Redis at %s unreachable: %v", addr, err)
	}
	runSubWorkflowConclusionRecovery(t, addr)
}
