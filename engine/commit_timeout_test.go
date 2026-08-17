package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestCommitTaskTimeoutRespectsOnErrorContinue: a node with on_error "continue"
// terminated by the SERVER must get the same outcome as one terminated by the
// runner -- Continued, not a fatal Failed. This is the split CommitTaskFailure
// would have introduced.
func TestCommitTaskTimeoutRespectsOnErrorContinue(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "timeout-onerror",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", OnError: "continue"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("expected start task, got %v", taskNames(tasks))
	}

	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}

	cause := types.NewPermanentError("node.timeout", "node execution exceeded its deadline")
	err = eng.CommitTaskTimeout(ctx, lease, cause)
	if err != nil {
		t.Fatalf("CommitTaskTimeout() error = %v", err)
	}

	// The node should be Continued (on_error: continue), NOT Failed.
	node, _ := state.GetNode(ctx, id, "start")
	if node == nil {
		t.Fatal("node snapshot is nil after CommitTaskTimeout")
	}
	if node.Status != types.NodeStatusContinued {
		t.Fatalf("node status = %q, want %q (on_error: continue must produce Continued, not Failed)",
			node.Status, types.NodeStatusContinued)
	}

	// The execution must still be running (non-fatal), and downstream must
	// have been scheduled.
	exec, _ := state.GetExecution(ctx, id)
	if exec.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution status = %q, want Running (on_error: continue is non-fatal)", exec.Status)
	}
}

// TestCommitTaskTimeoutDoesNotRetry: a node with Retry{MaxAttempts:3}
// terminated by timeout must not be re-enqueued. Permanent short-circuits it.
func TestCommitTaskTimeoutDoesNotRetry(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "timeout-noretry",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", Retry: &types.RetrySettings{MaxAttempts: 3, InitialInterval: 1000}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}

	cause := types.NewPermanentError("node.timeout", "node execution exceeded its deadline")
	err = eng.CommitTaskTimeout(ctx, lease, cause)
	if err != nil {
		t.Fatalf("CommitTaskTimeout() error = %v", err)
	}

	// A permanent error must not be retried, even when retry is configured.
	node, _ := state.GetNode(ctx, id, "start")
	if node == nil {
		t.Fatal("node snapshot is nil")
	}
	if node.Status == types.NodeStatusPending || node.Status == types.NodeStatusRunning {
		t.Fatalf("node status = %q, expected terminal (permanent should skip retry)", node.Status)
	}

	// The queue must not have received a retry task.
	retryTasks := queue.Drain()
	for _, rt := range retryTasks {
		if rt.NodeName == "start" && rt.Type == TaskTypeNodeExec {
			t.Fatalf("found retry task for start node, but permanent timeout must not retry")
		}
	}
}

// TestCommitTaskTimeoutIsIdempotent: committing twice with the same lease token
// is not an error (the runner's report and the server's backstop can race).
func TestCommitTaskTimeoutIsIdempotent(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "timeout-idempotent",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	state.InitInDegrees(id, g)

	tasks := queue.Drain()
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatal(err)
	}

	cause := types.NewPermanentError("node.timeout", "node execution exceeded its deadline")
	// First commit should succeed.
	if err := eng.CommitTaskTimeout(ctx, lease, cause); err != nil {
		t.Fatalf("first CommitTaskTimeout() error = %v", err)
	}

	// Second commit with the same lease: should NOT return an error.
	// The race between runner and server is expected; the loser is not an alarm.
	err = eng.CommitTaskTimeout(ctx, lease, cause)
	if err != nil {
		t.Fatalf("second CommitTaskTimeout() error = %v, want nil (idempotent)", err)
	}

	// Node must still be in terminal state.
	node, _ := state.GetNode(ctx, id, "start")
	if node == nil || !types.IsTerminalNodeStatus(node.Status) {
		t.Fatalf("node not terminal after double commit: %+v", node)
	}
}
