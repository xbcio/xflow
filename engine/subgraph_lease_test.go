package engine

import (
	"context"
	"errors"
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
			{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()},
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

// A batch that reports an error must not terminalize the map node as a success.
// CompleteExpandedSubExecution already records the batch as Failed — the status
// argument has always been there — but completeLoopSplit committed
// NodeStatusSuccess unconditionally, so the map node ended up "success" with a
// failed batch's empty result silently occupying a slot in its results array,
// and downstream fired on that data.
//
// This asserts the node's own OnError decides, exactly as it does for any other
// failing node: the default "stop" strategy fails the node and aborts.
func TestCommitSubgraphResultFailsTheMapNodeWhenABatchFails(t *testing.T) {
	eng, state, queue, execID, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	// The FIRST batch fails and the rest succeed. Other batches are not
	// canceled by design (no cross-sub-execution cancellation), so every batch
	// still reports and the verdict is only reached at the all-done barrier.
	var downstream []string
	for i, bt := range batches {
		lease, _, err := eng.BuildSubgraphLease(ctx, bt)
		if err != nil {
			t.Fatalf("BuildSubgraphLease(batch %d) error = %v", i, err)
		}
		result := TaskResult{Output: &types.Output{Data: map[string]any{"count": 1}}}
		if i == 0 {
			result = TaskResult{Error: errors.New("body item blew up")}
		}
		if _, err := eng.CommitSubgraphResult(ctx, lease, result); err != nil {
			t.Fatalf("CommitSubgraphResult(batch %d) error = %v", i, err)
		}
		for _, tk := range queue.Drain() {
			if tk.NodeName == "done" {
				downstream = append(downstream, tk.NodeName)
			}
		}
	}

	node, err := state.GetNode(ctx, execID, "loop")
	if err != nil || node == nil {
		t.Fatalf("GetNode(loop) = %+v err=%v", node, err)
	}
	if node.Status != types.NodeStatusFailed {
		t.Errorf("map node status = %v, want failed: one of its batches reported an error", node.Status)
	}
	if node.Error == "" {
		t.Error("map node carries no error message, so nothing records why the expansion failed")
	}
	if len(downstream) != 0 {
		t.Errorf("downstream %q fired on a failed expansion's results", downstream)
	}
	snap, err := state.GetExecution(ctx, execID)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v err=%v", snap, err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Errorf("execution status = %v, want failed under the map node's default \"stop\" strategy", snap.Status)
	}
}

// The map node's OnError governs a failed generation, so a node that declares
// error_output routes there instead of aborting. What makes this worth its own
// test is the results array: ApplyOnError's non-fatal strategies rebuild the
// output from scratch, which would drop it, and then a downstream error branch
// would have no way to see which batches DID produce items.
func TestCommitSubgraphResultRoutesAFailedGenerationThroughOnError(t *testing.T) {
	ctx := context.Background()
	def := &types.WorkflowDef{
		Name: "batch-lease-onerror",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", OnError: string(types.OnErrorOutput),
				Parameters: mapBodyParamsForTest()},
			{Name: "recover", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {"error": {Targets: []types.Connection{{Node: "recover", Input: "main"}}}},
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
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)

	var routed []string
	for i, bt := range batches {
		lease, _, err := eng.BuildSubgraphLease(ctx, bt)
		if err != nil {
			t.Fatalf("BuildSubgraphLease(batch %d) error = %v", i, err)
		}
		result := TaskResult{Output: &types.Output{Data: map[string]any{"count": 1}}}
		if i == 0 {
			result = TaskResult{Error: errors.New("body item blew up")}
		}
		if _, err := eng.CommitSubgraphResult(ctx, lease, result); err != nil {
			t.Fatalf("CommitSubgraphResult(batch %d) error = %v", i, err)
		}
		for _, tk := range queue.Drain() {
			routed = append(routed, tk.NodeName)
		}
	}

	node, err := state.GetNode(ctx, execID, "loop")
	if err != nil || node == nil {
		t.Fatalf("GetNode(loop) = %+v err=%v", node, err)
	}
	if node.Status != types.NodeStatusSuccess {
		t.Errorf("map node status = %v, want success: error_output handles the failure gracefully", node.Status)
	}
	if _, ok := node.Output["results"]; !ok {
		t.Errorf("error_output dropped the results array, so the error branch cannot see which batches produced items: %+v", node.Output)
	}
	if node.Output["error"] == nil {
		t.Errorf("error_output carries no error payload: %+v", node.Output)
	}
	found := false
	for _, name := range routed {
		if name == "recover" {
			found = true
		}
	}
	if !found {
		t.Errorf("nothing routed to the error branch; queued %q", routed)
	}
	snap, err := state.GetExecution(ctx, execID)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v err=%v", snap, err)
	}
	if snap.Status == types.ExecutionStatusFailed {
		t.Error("execution failed despite error_output, which is supposed to keep it running")
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

// A remote runner has no compiled graph: it never saw the workflow definition,
// so it cannot look up the map node's body, its batch size, or the whole items
// array. Everything the body needs must travel on the lease, or the runner
// path can only ever be the pass-through it started as.
func TestBuildSubgraphLeaseCarriesEverythingTheBodyNeeds(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-lease-body",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": &loopHandler{}}}
	eng := New(state, queue, WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(ctx, batches[1])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if payload.Package == nil {
		t.Error("payload carries no body package: a runner cannot compile a body it was never sent")
	}
	if payload.PackageHash == "" {
		t.Error("payload carries no package hash: without it the runner's package cache " +
			"cannot key the body, so it recompiles per batch and cannot verify what it got")
	}
	// loopHandler produces three single-item batches, so batch 1 holds the
	// SECOND item. Its global index is only derivable from batch_size.
	if payload.BatchSize != 1 {
		t.Errorf("payload BatchSize = %d, want 1: without it the runner cannot compute a global $index",
			payload.BatchSize)
	}
	if len(payload.AllItems) != 3 {
		t.Errorf("payload carries %d AllItems, want all 3 for $items", len(payload.AllItems))
	}
}

// A batch lease is the only route the submission's Runtime.Vars have to a
// runner. $vars is the union of the workflow's static Context.Vars -- which
// travel inside the projected body package -- and the submission's
// Runtime.Vars, which do not: a runner never saw the submission, and a batch
// task carries no input of its own. Without this field a body member reading
// $vars.<per-submission key> got nil on a runner while the same expression
// resolved fine one level up, in the outer graph.
//
// Asserted on the payload rather than end-to-end because this is the wire
// boundary: service/runner/subgraph_runtime.go copies the field straight into
// BatchBodyRequest, and the in-process half is covered by
// TestMapBodyMemberSeesWorkflowVarsAndConfig.
func TestBuildSubgraphLeaseCarriesTheSubmissionRuntime(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-lease-runtime",
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()},
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
	}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil, &types.Runtime{Vars: map[string]any{"tenant": "acme"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)

	_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if payload.Runtime == nil {
		t.Fatalf("batch payload dropped the submission runtime; a body member's $vars would be missing its per-submission half")
	}
	if got := payload.Runtime.Vars["tenant"]; got != "acme" {
		t.Errorf("payload.Runtime.Vars[\"tenant\"] = %v, want \"acme\"", got)
	}
}

// The clone must be a copy, not an alias: a runner-facing payload that shares
// the snapshot's map lets any mutation on either side show up on the other,
// across every batch of the expansion.
func TestBuildSubgraphLeaseClonesTheSubmissionRuntime(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "batch-lease-runtime-clone",
		Nodes: []types.NodeDef{{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": &loopHandler{}}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()
	execID, err := eng.Submit(ctx, g, nil, &types.Runtime{Vars: map[string]any{"tenant": "acme"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)
	_, payload, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}

	payload.Runtime.Vars["tenant"] = "mutated"
	snap, err := state.GetExecution(ctx, execID)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v err=%v", snap, err)
	}
	if got := snap.Runtime.Vars["tenant"]; got != "acme" {
		t.Errorf("mutating the payload changed the stored snapshot: tenant = %v, want \"acme\"", got)
	}
}
