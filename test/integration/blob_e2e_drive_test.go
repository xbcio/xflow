//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// blobE2EOutcome is what a finished run left behind: the terminal result with
// the outputs of every node, and the snapshot of the map node.
type blobE2EOutcome struct {
	id  types.ExecutionID
	res types.Result
	fan *engine.NodeSnapshot
}

// blobE2ENodes are the nodes of blobE2EWorkflow whose outputs are collected.
var blobE2ENodes = []string{"start", "fan", "done"}

// submit compiles the workflow and starts an execution of it. The context
// carries the run's namespace and ends the wait after blobE2EWait, so a run that
// never settles fails the test instead of hanging it.
func (r *blobE2ERun) submit(t *testing.T, batchSize int) (context.Context, types.ExecutionID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespace.WithNamespace(context.Background(), r.ns), blobE2EWait)
	t.Cleanup(cancel)
	g, err := graph.Compile(blobE2EWorkflow(batchSize))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id, err := r.eng.Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return ctx, id
}

// finish waits for the execution to reach a terminal state, then for the run's
// queues to drain, and returns what it left behind. Both waits are bounded by
// ctx, so neither can outlive blobE2EWait.
func (r *blobE2ERun) finish(ctx context.Context, t *testing.T, id types.ExecutionID) blobE2EOutcome {
	t.Helper()
	st := r.b.State()
	res := waitForCompletion(ctx, t, st, id, blobE2ENodes...)
	(&dpRun{ns: r.ns, addr: r.addr}).settle(ctx, t)
	fan, err := st.GetNode(ctx, id, "fan")
	if err != nil || fan == nil {
		t.Fatalf("GetNode(fan) = %v, %v", fan, err)
	}
	return blobE2EOutcome{id: id, res: res, fan: fan}
}

// run submits the workflow and returns once the execution is settled.
func (r *blobE2ERun) run(t *testing.T, batchSize int) blobE2EOutcome {
	t.Helper()
	ctx, id := r.submit(t, batchSize)
	return r.finish(ctx, t, id)
}
