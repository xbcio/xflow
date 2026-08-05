package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// batchLeaseWorkflow is a map node with one downstream node. loopHandler
// produces three batches, so "done" firing early is unambiguous evidence that
// the all-batches-done barrier was bypassed.
func batchLeaseWorkflow(t *testing.T) (*Engine, *fakeState, *fakeQueue, types.ExecutionID, *graph.Graph) {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "batch-lease",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &loopHandler{},
		"test.echo": &echoHandler{},
	}}
	eng := newTestEngine(t, state, queue, reg)
	id, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return eng, state, queue, id, g
}

// drainBatchTasks runs the map node and returns the batch tasks it expanded to.
func drainBatchTasks(t *testing.T, eng *Engine, queue *fakeQueue) []*Task {
	t.Helper()
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "loop" {
		t.Fatalf("root tasks = %v, want one \"loop\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])
	batches := queue.Drain()
	if len(batches) < 2 {
		t.Fatalf("map expanded to %d batches, want at least 2 for a barrier to be observable", len(batches))
	}
	return batches
}

// A batch task names a synthetic node ("loop/_batch/0") that exists only in the
// expansion payload — the compiled graph has no such node. Routing it through
// the ordinary node-lease path makes the engine write live lease state under
// that name, inventing a node the graph never declared. Whatever reclaims,
// sweeps, or reports on nodes then has an entry it cannot resolve back to the
// graph.
func TestBuildSubgraphLeaseDoesNotInventANodeOutsideTheGraph(t *testing.T) {
	eng, state, queue, execID, g := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	if _, _, err := eng.BuildSubgraphLease(ctx, batches[0]); err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}

	if _, inGraph := g.NodeIndex(batches[0].NodeName); inGraph {
		t.Fatalf("test premise broken: %q is a real graph node", batches[0].NodeName)
	}
	phantom, err := state.GetNode(ctx, execID, batches[0].NodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if phantom != nil {
		t.Errorf("building a batch lease wrote node state for %q, which does not exist in the graph: %+v",
			batches[0].NodeName, phantom)
	}
}

// The parent map node stays Waiting under its own fence for the whole
// expansion; the batch lease is a delivery vehicle, not a claim on the parent.
// A batch lease that reset the parent's lease token would strand the fence that
// makes a stale batch harmless after recovery.
func TestBuildSubgraphLeaseLeavesTheParentFenceIntact(t *testing.T) {
	eng, state, queue, execID, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	before, err := state.GetNode(ctx, execID, "loop")
	if err != nil || before == nil {
		t.Fatalf("parent node before = %+v err=%v", before, err)
	}

	if _, _, err := eng.BuildSubgraphLease(ctx, batches[0]); err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}

	after, err := state.GetNode(ctx, execID, "loop")
	if err != nil || after == nil {
		t.Fatalf("parent node after = %+v err=%v", after, err)
	}
	if after.LeaseToken != before.LeaseToken || after.LeaseID != before.LeaseID {
		t.Errorf("batch lease moved the parent fence: lease %q->%q token %q->%q",
			before.LeaseID, after.LeaseID, before.LeaseToken, after.LeaseToken)
	}
	if after.Status != before.Status {
		t.Errorf("batch lease changed parent status %v -> %v, want it left Waiting", before.Status, after.Status)
	}
}

// A batch result committed through the ordinary node path terminalizes the map
// node on the first batch, so downstream fires while other batches are still in
// flight. The barrier lives in CompleteExpandedSubExecution, which only advances
// once every child generation reports.
func TestCommitSubgraphResultHoldsDownstreamUntilEveryBatchReports(t *testing.T) {
	eng, _, queue, _, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	for i, bt := range batches {
		lease, payload, err := eng.BuildSubgraphLease(ctx, bt)
		if err != nil {
			t.Fatalf("BuildSubgraphLease(batch %d) error = %v", i, err)
		}
		if payload == nil {
			t.Fatalf("BuildSubgraphLease(batch %d) returned no payload", i)
		}
		result := TaskResult{Output: &types.Output{Data: map[string]any{
			"items": payload.Items,
			"count": len(payload.Items),
		}}}
		if err := eng.CommitTaskResult(ctx, lease, result); err != nil {
			t.Fatalf("CommitTaskResult(batch %d) error = %v", i, err)
		}

		fired := false
		for _, tk := range queue.Drain() {
			if tk.NodeName == "done" {
				fired = true
			}
		}
		last := i == len(batches)-1
		if fired && !last {
			t.Fatalf("\"done\" fired after batch %d of %d: the all-batches-done barrier was bypassed",
				i+1, len(batches))
		}
		if !fired && last {
			t.Fatalf("\"done\" never fired after all %d batches reported", len(batches))
		}
	}
}

// A batch inherits the map node's routing because the body runs on behalf of
// that node — the selector, node type, and capability requirements are the map
// node's own. TaskRouting has no NodeBatch branch; it falls through to the
// default node branch keyed on NodeIdx, which is the map node. That is the
// wanted behavior, but it happens by coincidence of indexing rather than by
// intent, so it is asserted rather than assumed.
func TestTaskRoutingForABatchUsesTheMapNodesPlacement(t *testing.T) {
	eng, _, queue, _, g := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	routing, err := eng.TaskRouting(ctx, batches[0])
	if err != nil {
		t.Fatalf("TaskRouting(batch) error = %v", err)
	}
	mapIdx, _ := g.NodeIndex("loop")
	want := g.NodeAt(mapIdx)
	if routing.NodeType != want.Type {
		t.Errorf("routing.NodeType = %q, want the map node's %q", routing.NodeType, want.Type)
	}
	if len(routing.Requirements) == 0 {
		t.Error("routing carries no capability requirements, so any runner would match")
	}
}

// RecoverTaskLease lumps NodeBatch with NodeAdvance/NodeSkip as unrecoverable.
// That was right while batches were control-plane-internal. Now that a batch is
// leased to a runner, a response-loss replay must be able to rebuild it — and
// it can, because BuildSubgraphLease is a pure function of the task payload and
// the graph: it mutates nothing, so rebuilding is not "issuing a second lease".
func TestRecoverTaskLeaseRebuildsABatchLease(t *testing.T) {
	eng, _, queue, _, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	original, _, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}

	recovered, err := eng.RecoverTaskLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("RecoverTaskLease(batch) error = %v, want a rebuilt lease", err)
	}
	if recovered.SubgraphPayload == nil {
		t.Fatal("recovered batch lease carries no SubgraphPayload, so the runner has no items to run")
	}
	if recovered.LeaseToken != original.LeaseToken || recovered.LeaseID != original.LeaseID {
		t.Errorf("recovered lease identity = %q/%q, want the parent fence %q/%q",
			recovered.LeaseID, recovered.LeaseToken, original.LeaseID, original.LeaseToken)
	}
}
