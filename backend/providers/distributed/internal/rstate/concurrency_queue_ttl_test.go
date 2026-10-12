package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// queueTTLStore builds a store with a short, known execution retention so the
// TTL of a parked execution can be read as a number rather than compared to a
// default.
func queueTTLStore(t *testing.T, execTTL time.Duration) (*miniredis.Miniredis, *Store) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return srv, New(rdb, nil, execTTL)
}

// queueTTLGraph is a QUEUE-mode workflow whose queue_timeout is set by the
// caller; the limit of 1 is what forces the second submission into the line.
func queueTTLGraph(t *testing.T, name string, timeout time.Duration) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit:        1,
			Mode:         types.ConcurrencyModeQueue,
			QueueTimeout: timeout,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// queueTTLTwoExecutions fills the graph's only slot with one execution and
// parks a second one behind it, returning both. It goes through the real
// admission path: a plain CreateExecution charges nothing, and the slot it
// would not hold is exactly what makes the next submission queue.
func queueTTLTwoExecutions(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, wait time.Duration) (types.ExecutionID, types.ExecutionID) {
	t.Helper()
	holder := admitForSweep(t, ctx, state, g, 1)
	admission := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue, QueueTimeout: wait,
	}
	queued := engine.NewExecutionID()
	res, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: queued, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID: "root/" + string(queued) + "/start/0",
		Task: engine.Task{
			ExecutionID: queued, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec,
		},
	}}, admission)
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("submission result = %+v, want it parked", res)
	}
	return holder, queued
}

// TestQueuedExecutionRetentionCoversItsWait pins the requirement that a parked
// execution outlives its own queue_timeout.
//
// A queued execution's keys are written once and then sit untouched until
// promotion, so the retention they are written with is the only thing keeping
// them alive while the execution waits. If it is the ordinary execution
// retention — which knows nothing about the line in front of it — an execution
// whose wait is longer than its retention expires mid-queue: it silently
// disappears, and its place in line becomes a member that promotes nothing.
func TestQueuedExecutionRetentionCoversItsWait(t *testing.T) {
	ctx := context.Background()
	const (
		execTTL = time.Minute
		wait    = 10 * time.Minute
	)
	srv, state := queueTTLStore(t, execTTL)
	g := queueTTLGraph(t, "conc-queue-ttl", wait)
	_, queued := queueTTLTwoExecutions(t, ctx, state, g, wait)

	statusKey := execKey(namespace.Default, queued, "status")
	ttl := srv.TTL(statusKey)
	if ttl < wait {
		t.Fatalf("queued execution status TTL = %v, want at least its %v queue_timeout: "+
			"the execution would expire while still waiting", ttl, wait)
	}
	if want := execTTL + wait; ttl != want {
		t.Fatalf("queued execution status TTL = %v, want %v (retention plus the wait)", ttl, want)
	}

	// The control: an execution that did not wait keeps the plain retention.
	// Without this the assertion above would also pass if every execution were
	// simply written with an extended TTL.
	plain := engine.NewExecutionID()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: plain, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(plain) error = %v", err)
	}
	if got := srv.TTL(execKey(namespace.Default, plain, "status")); got != execTTL {
		t.Fatalf("unparked execution status TTL = %v, want %v", got, execTTL)
	}
}

// TestQueuedExecutionBufferedSignalOutlivesItsWait is the same requirement one
// key further out. A signal that arrives while an execution is queued is not
// part of the run — nothing has executed — so the only thing that keeps it for
// the promoted execution to consume is the retention it is stored under. Stored
// under the plain retention it expires mid-queue for exactly the waits this
// retention rule exists to cover, and the loss is silent: the node that waits
// for it parks at promotion, and the caller that delivered the signal is never
// told it evaporated.
func TestQueuedExecutionBufferedSignalOutlivesItsWait(t *testing.T) {
	ctx := context.Background()
	const (
		execTTL = time.Minute
		wait    = 10 * time.Minute
	)
	srv, state := queueTTLStore(t, execTTL)
	g := queueTTLGraph(t, "conc-queue-signal-ttl", wait)
	_, queued := queueTTLTwoExecutions(t, ctx, state, g, wait)

	node, _, committed, err := state.DeliverSignalWithOutbox(ctx, queued, "approval",
		map[string]any{"approved_by": "ops"}, engine.ResumeIntent{})
	if err != nil {
		t.Fatalf("DeliverSignalWithOutbox(queued) error = %v", err)
	}
	if node != "" || committed {
		t.Fatalf("delivery to a queued execution = (node %q, committed %v), want it stored", node, committed)
	}
	sigKey := signalKey(namespace.Default, queued, "approval")
	sigTTL := srv.TTL(sigKey)
	if sigTTL < wait {
		t.Fatalf("buffered signal TTL = %v, want at least its %v queue_timeout: the "+
			"promoted execution would find nothing to consume", sigTTL, wait)
	}
	if want := execTTL + wait; sigTTL != want {
		t.Fatalf("buffered signal TTL = %v, want %v (retention plus the wait)", sigTTL, want)
	}

	// The control: a signal delivered to an execution that is not waiting keeps
	// the plain retention.
	plain := engine.NewExecutionID()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: plain, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(plain) error = %v", err)
	}
	if _, _, _, err := state.DeliverSignalWithOutbox(ctx, plain, "approval",
		map[string]any{"approved_by": "ops"}, engine.ResumeIntent{}); err != nil {
		t.Fatalf("DeliverSignalWithOutbox(plain) error = %v", err)
	}
	if got := srv.TTL(signalKey(namespace.Default, plain, "approval")); got != execTTL {
		t.Fatalf("signal TTL on an unparked execution = %v, want %v", got, execTTL)
	}
}
