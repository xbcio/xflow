package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestClusterTimesOutAParkedExecutionWithoutAControlPlane is the end-to-end
// check of the gap NewCluster used to have: an execution parked on a wait node
// holds no lease, so only the execution deadline index can find it once
// settings.timeout passes, and a cluster engine has no control plane to scan
// that index. Only a NewCluster engine over Redis is running here: no server,
// no runner process.
//
// The workflow builder has no setting for the execution budget, so the graph is
// compiled from the built definition with settings.timeout added, and submitted
// through the core engine the facade wraps.
func TestClusterTimesOutAParkedExecutionWithoutAControlPlane(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	hooks := &transientHookRecorder{}
	recorder := &deadlineExpiredObserverRecorder{}
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, WithHooks(hooks), WithConcurrency(2), WithDeadlineExpiredObserver(recorder))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Stop)

	wf := Workflow("cluster_deadline_sweep")
	start := wf.Node("start", node.Start())
	park := wf.Node("park", node.Wait("never-signalled"))
	wf.Connect(start, park)
	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition() error = %v", err)
	}
	def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: 4 * time.Second}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	ctx := context.Background()
	id, err := eng.eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Park first: a budget that ran out while a node still held its lease would
	// be failed by that node's own deadline, which is not what this test is
	// about. The wait node has released its lease once it reports suspended.
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	waitForNodeStatus(t, waitCtx, eng, id, "park", types.NodeStatusSuspended)

	// The sweeper polls every few seconds, so allow several periods.
	waitForTransientCondition(t, 30*time.Second, func() bool {
		snap, err := eng.eng.State().GetExecution(ctx, id)
		return err == nil && snap != nil && snap.Status == types.ExecutionStatusTimeout &&
			hooks.executionCompleteStatus(id) != ""
	})

	snap, err := eng.eng.State().GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if snap.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("execution error = %q, want %q", snap.Error, engine.ExecutionDeadlineExceededError)
	}
	if got := hooks.executionCompleteStatus(id); got != types.ExecutionStatusTimeout {
		t.Fatalf("completion hook status = %q, want timeout", got)
	}
	// The observer is called after the timeout write, so wait for it rather
	// than reading a count that may not have landed yet.
	waitForTransientCondition(t, 5*time.Second, func() bool { return len(recorder.snapshot()) > 0 })
	calls := recorder.snapshot()
	if len(calls) != 1 {
		t.Fatalf("observer called %d times, want exactly 1: %+v", len(calls), calls)
	}
	if calls[0].source != engine.DeadlineExpiredSourceEmbedded {
		t.Fatalf("observer source = %q, want %q", calls[0].source, engine.DeadlineExpiredSourceEmbedded)
	}
}
