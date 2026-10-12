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

	// The holder fills the only slot, so the next submission has to park. It is
	// admitted through the real admission path: a plain CreateExecution charges
	// nothing, and the slot it would not hold is exactly what makes the next
	// submission queue.
	admitForSweep(t, ctx, state, g, 1)
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
