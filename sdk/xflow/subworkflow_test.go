package xflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// subWorkflowChildNode is the child's work node: a portable typed node with one
// stable public output for the parent-side projection and the result assertions
// to agree on. Portable, not local, so the same child runs on both the local
// and the cluster engine.
var subWorkflowChildNode = node.Define("test.subworkflow_child", func(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"answer": float64(42)}}, nil
})

// subWorkflowChildBuilder compiles the child workflow: start -> work, ending at
// work so a successful run needs no downstream synchronization.
func subWorkflowChildBuilder(t *testing.T) *WorkflowBuilder {
	t.Helper()
	wf := Workflow("sdk-sw-child")
	start := wf.Node("start", node.Start())
	work := wf.Node("work", subWorkflowChildNode.New(nil))
	wf.Connect(start, work)
	return wf
}

// subWorkflowParentBuilder compiles the parent: start -> sub(childName). The
// sub node is terminal, so the parent completes with the sub node's output.
func subWorkflowParentBuilder(childName string) *WorkflowBuilder {
	wf := Workflow("sdk-sw-parent")
	start := wf.Node("start", node.Start())
	sub := wf.Node("sub", node.SubWorkflow(childName))
	wf.Connect(start, sub)
	return wf
}

// subWorkflowWaitingChildBuilder compiles a child that parks on a signal, so a
// parent's wait on it stays live until the test cancels.
func subWorkflowWaitingChildBuilder(t *testing.T) *WorkflowBuilder {
	t.Helper()
	wf := Workflow("sdk-sw-waiting-child")
	start := wf.Node("start", node.Start())
	wait := wf.Node("ApprovalWait", node.Wait("approval"))
	wf.Connect(start, wait)
	return wf
}

// waitForExecutionStatus polls Inspect until the execution reaches status.
func waitForExecStatus(t *testing.T, ctx context.Context, eng *Engine, id types.ExecutionID, status types.ExecutionStatus) enginecore.ExecutionDetail {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s to reach %s: %v", id, status, ctx.Err())
		case <-ticker.C:
			detail, err := eng.Inspect(ctx, id)
			if err != nil {
				t.Fatalf("Inspect(%s) error: %v", id, err)
			}
			if detail.Status == status {
				return detail
			}
		}
	}
}

// TestSubWorkflowRegistrationPinsTheReference is the registration-time half of
// "latest drifts only at registration": a version-less reference is resolved
// once, against the registry, and the compiled node records the concrete
// (name, version, definition hash) the child was pinned to.
func TestSubWorkflowRegistrationPinsTheReference(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	childID, err := eng.AddWorkflow(ctx, subWorkflowChildBuilder(t))
	if err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-child"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}

	childRec, err := eng.workflowRegistry.GetWorkflow(ctx, childID)
	if err != nil {
		t.Fatalf("GetWorkflow(child) error = %v", err)
	}
	parentRec, err := eng.workflowRegistry.GetWorkflow(ctx, parentID)
	if err != nil {
		t.Fatalf("GetWorkflow(parent) error = %v", err)
	}
	idx, ok := parentRec.Graph.NodeIndex("sub")
	if !ok {
		t.Fatal("the persisted parent graph has no sub node")
	}
	binding := parentRec.Graph.NodeAt(idx).SubWorkflow
	if binding == nil {
		t.Fatal("the persisted sub node carries no sub-workflow binding")
	}
	if binding.Name != "sdk-sw-child" {
		t.Fatalf("binding name = %q, want %q", binding.Name, "sdk-sw-child")
	}
	if binding.Version != "v1" {
		t.Fatalf("binding version = %q, want the builder default %q", binding.Version, "v1")
	}
	if binding.DefinitionHash != childRec.DefinitionHash {
		t.Fatalf("binding pins hash %q, want the child record's %q", binding.DefinitionHash, childRec.DefinitionHash)
	}
}

// TestSubWorkflowRegistrationRefusesUnresolvableReference: a reference to a
// workflow that is not registered is refused at AddWorkflow rather than stored
// as a node that could never start.
func TestSubWorkflowRegistrationRefusesUnresolvableReference(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	_, err = eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-missing"))
	if err == nil {
		t.Fatal("AddWorkflow(parent) error = nil, want the unresolvable reference refused")
	}
	if !strings.Contains(err.Error(), "sdk-sw-missing") {
		t.Fatalf("AddWorkflow(parent) error = %v, want it to name the missing workflow", err)
	}
	if _, err := eng.workflowRegistry.GetWorkflowByKey(ctx, "default/sdk-sw-parent@v1"); err == nil {
		t.Fatal("the refused parent was registered anyway")
	}
}

// TestSubWorkflowLocalEndToEnd runs parent -> child on the local engine end to
// end: the child executes as its own execution, the parent's node concludes
// with the child's public outputs, and both drill-down directions resolve.
func TestSubWorkflowLocalEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	childID, err := eng.AddWorkflow(ctx, subWorkflowChildBuilder(t))
	if err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-child"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}

	id, err := eng.Invoke(ctx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q error %q, want success", result.Status, result.Error)
	}
	subOutput, ok := result.Output["sub"].(map[string]any)
	if !ok {
		t.Fatalf("result output has no sub node output: %#v", result.Output)
	}
	workOutput, ok := subOutput["work"].(map[string]any)
	if !ok || workOutput["answer"] != float64(42) {
		t.Fatalf("sub node output = %#v, want the child's public node outputs", subOutput)
	}

	// Parent -> child: the finished node still names the child it waited on.
	parent, err := eng.Inspect(ctx, id, "sub")
	if err != nil {
		t.Fatalf("Inspect(parent) error = %v", err)
	}
	if len(parent.Nodes) != 1 || parent.Nodes[0].ChildExecutionID == "" {
		t.Fatalf("Inspected parent nodes = %+v, want the sub node naming its child", parent.Nodes)
	}
	childExecID := parent.Nodes[0].ChildExecutionID

	// Child -> parent: the child names the node that started it, distinctly
	// from the rerun link.
	child, err := eng.Inspect(ctx, childExecID)
	if err != nil {
		t.Fatalf("Inspect(child) error = %v", err)
	}
	if child.ParentID != id || child.ParentNode != "sub" {
		t.Fatalf("child linkage = (%q, %q), want (%q, %q)", child.ParentID, child.ParentNode, id, "sub")
	}
	if child.RerunFrom != "" {
		t.Fatalf("child RerunFrom = %q, want empty: a sub-workflow child is not a rerun", child.RerunFrom)
	}
	if child.Status != types.ExecutionStatusSuccess {
		t.Fatalf("child status = %q, want success", child.Status)
	}
	// The child runs the definition the resolver read, so it must name that
	// record. The registry compiles the graph before the record carries its id,
	// which is why the identity cannot be read off the graph.
	if child.WorkflowID != childID {
		t.Fatalf("child WorkflowID = %q, want the registered child id %q", child.WorkflowID, childID)
	}
	if child.WorkflowVersion != "v1" {
		t.Fatalf("child WorkflowVersion = %q, want %q", child.WorkflowVersion, "v1")
	}
}

// TestSubWorkflowClusterEndToEnd is the same round trip on the cluster engine
// (Redis state + asynq queue over miniredis): the parent's node must wait, the
// child must be admitted through the queue, and the conclusion must land back
// on the parent's node.
func TestSubWorkflowClusterEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer eng.Stop()

	childID, err := eng.AddWorkflow(ctx, subWorkflowChildBuilder(t))
	if err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-child"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}

	id, err := eng.Invoke(ctx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q error %q, want success", result.Status, result.Error)
	}
	subOutput, ok := result.Output["sub"].(map[string]any)
	if !ok {
		t.Fatalf("result output has no sub node output: %#v", result.Output)
	}
	workOutput, ok := subOutput["work"].(map[string]any)
	if !ok || workOutput["answer"] != float64(42) {
		t.Fatalf("sub node output = %#v, want the child's public node outputs", subOutput)
	}

	parent, err := eng.Inspect(ctx, id, "sub")
	if err != nil {
		t.Fatalf("Inspect(parent) error = %v", err)
	}
	if len(parent.Nodes) != 1 || parent.Nodes[0].ChildExecutionID == "" {
		t.Fatalf("Inspected parent nodes = %+v, want the sub node naming its child", parent.Nodes)
	}
	child, err := eng.Inspect(ctx, parent.Nodes[0].ChildExecutionID)
	if err != nil {
		t.Fatalf("Inspect(child) error = %v", err)
	}
	if child.ParentID != id || child.ParentNode != "sub" {
		t.Fatalf("child linkage = (%q, %q), want (%q, %q)", child.ParentID, child.ParentNode, id, "sub")
	}
	// The cluster registry round-trips the compiled graph through JSON, which
	// does not preserve its definition; the identity must still come from the
	// resolved record or every production child is unattributed.
	if child.WorkflowID != childID {
		t.Fatalf("child WorkflowID = %q, want the registered child id %q", child.WorkflowID, childID)
	}
	if child.WorkflowVersion != "v1" {
		t.Fatalf("child WorkflowVersion = %q, want %q", child.WorkflowVersion, "v1")
	}
}

// TestSubWorkflowCancelReachesTheChild is the SDK-level cancel cascade: a
// parent parked on a live child is canceled, and the child it was waiting on —
// discovered only through the parent's own inspect — is canceled with it.
func TestSubWorkflowCancelReachesTheChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	if _, err := eng.AddWorkflow(ctx, subWorkflowWaitingChildBuilder(t)); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-waiting-child"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}
	id, err := eng.Invoke(ctx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	parent := waitForNodeStatus(t, ctx, eng, id, "sub", types.NodeStatusWaiting)
	if parent.Nodes[0].ChildExecutionID == "" {
		t.Fatal("the parked sub node names no child")
	}
	childID := parent.Nodes[0].ChildExecutionID

	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	waitForExecStatus(t, ctx, eng, id, types.ExecutionStatusCanceled)
	waitForExecStatus(t, ctx, eng, childID, types.ExecutionStatusCanceled)
}

// TestSubWorkflowRerunMintsAFreshChild: a canceled parent can be rerun from its
// sub node, and the rerun starts its own child rather than adopting the
// canceled one or reusing its identity.
func TestSubWorkflowRerunMintsAFreshChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	if _, err := eng.AddWorkflow(ctx, subWorkflowWaitingChildBuilder(t)); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-waiting-child"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}
	id, err := eng.Invoke(ctx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	parent := waitForNodeStatus(t, ctx, eng, id, "sub", types.NodeStatusWaiting)
	firstChild := parent.Nodes[0].ChildExecutionID
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	waitForExecStatus(t, ctx, eng, id, types.ExecutionStatusCanceled)

	rerun, err := eng.Rerun(ctx, id, "sub")
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	rerunParent := waitForNodeStatus(t, ctx, eng, rerun.ExecutionID, "sub", types.NodeStatusWaiting)
	secondChild := rerunParent.Nodes[0].ChildExecutionID
	if secondChild == "" {
		t.Fatal("the rerun's parked sub node names no child")
	}
	if secondChild == firstChild {
		t.Fatalf("the rerun adopted child %q, want a fresh child", firstChild)
	}
	rerunDetail, err := eng.Inspect(ctx, rerun.ExecutionID)
	if err != nil {
		t.Fatalf("Inspect(rerun) error = %v", err)
	}
	if rerunDetail.ParentID != id || rerunDetail.RerunFrom != "sub" {
		t.Fatalf("rerun linkage = (%q, %q), want (%q, %q)", rerunDetail.ParentID, rerunDetail.RerunFrom, id, "sub")
	}
	// The abandoned child stays canceled: a rerun must not revive what the
	// operator stopped.
	abandoned, err := eng.Inspect(ctx, firstChild)
	if err != nil {
		t.Fatalf("Inspect(first child) error = %v", err)
	}
	if abandoned.Status != types.ExecutionStatusCanceled {
		t.Fatalf("first child status = %q, want it left canceled", abandoned.Status)
	}

	// Clean up the live child the rerun started.
	if err := eng.Cancel(ctx, rerun.ExecutionID); err != nil {
		t.Fatalf("Cancel(rerun) error = %v", err)
	}
	waitForExecStatus(t, ctx, eng, secondChild, types.ExecutionStatusCanceled)
}
