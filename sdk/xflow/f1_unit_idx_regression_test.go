package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestClusterMixedGraphResolvesUnitIndexAfterDurableRoundTrip covers F1: a
// linear (ungrouped-only) cluster execution round-trips every task through
// the Asynq queue codec, which — before F1 — silently dropped UnitIdx to its
// Go zero value on every hop. This test proves that a multi-node execution
// completes fully (every node runs, not just the entry) after the graph JSON
// round-trip fix (F9/T1) and the F1 durable envelope fix are both in place.
// It is the SDK-level regression companion to the lower-level codec unit
// tests in backend/providers/distributed/internal/queue.
func TestClusterMixedGraphResolvesUnitIndexAfterDurableRoundTrip(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	hooks := &transientHookRecorder{}
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, WithHooks(hooks), WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Stop)

	wf := Workflow("f1_mixed_graph_regression")
	start := wf.Node("start", node.Start())
	step1 := wf.Node("step1", node.Expr("1"))
	step2 := wf.Node("step2", node.Expr("2"))
	wf.Connect(start, step1)
	wf.Connect(step1, step2)

	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(context.Background(), workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	waitForTransientCondition(t, 10*time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(context.Background(), id)
		if err != nil || snap == nil {
			return false
		}
		if !isTerminalStatus(snap.Status) {
			return false
		}
		// Terminal in the store and the hooks do not land at the same instant:
		// the commit script finalizes the execution inside Redis, and the
		// OnNodeComplete/OnExecutionComplete callbacks fire on the engine's
		// post-commit path after the script returns. Waiting only for the store
		// state races that gap and reads an empty node status while the run
		// itself succeeded, so wait for the last hook too.
		return hooks.nodeCompleteStatus("step2") != "" && hooks.executionCompleteStatus(id) != ""
	})

	for _, name := range []string{"start", "step1", "step2"} {
		if hooks.nodeCompleteStatus(name) != types.NodeStatusSuccess {
			t.Fatalf("node %q status = %q, want success (the execution reached a terminal state without this node succeeding)", name, hooks.nodeCompleteStatus(name))
		}
	}
	if hooks.executionCompleteStatus(id) != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %q, want success", hooks.executionCompleteStatus(id))
	}
}
