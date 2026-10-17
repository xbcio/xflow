package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// subWorkflowMidBuilder compiles a child that itself references the plain child
// (start -> sub(sdk-sw-child)). It exists to exercise namespace inheritance one
// level deeper: the mid execution's own dispatch must resolve the grandchild by
// the namespace it inherited from the parent.
func subWorkflowMidBuilder(t *testing.T) *WorkflowBuilder {
	t.Helper()
	wf := Workflow("sdk-sw-mid")
	start := wf.Node("start", node.Start())
	sub := wf.Node("sub", node.SubWorkflow("sdk-sw-child"))
	wf.Connect(start, sub)
	return wf
}

// runExplicitRegistrationNamespaceCase registers child and parent under
// tenant-x (explicitly) on an engine whose own namespace is tenant-x, and runs
// the pair end to end. Registration-time resolution already passes; the failure
// this pins is at dispatch time, where the queue ctx carries the deployment
// namespace instead of the registration namespace.
func runExplicitRegistrationNamespaceCase(t *testing.T, eng *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Registration resolves references against the caller's namespace, as the
	// server does for an authenticated principal. The dispatch-time case this
	// test pins is the one that runs later, off the queue ctx.
	regCtx := namespace.WithNamespace(ctx, namespace.Namespace("tenant-x"))

	if _, err := eng.AddWorkflow(regCtx, subWorkflowChildBuilder(t).Namespace("tenant-x")); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(regCtx, subWorkflowParentBuilder("sdk-sw-child").Namespace("tenant-x"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}

	id, err := eng.Invoke(regCtx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := eng.Wait(regCtx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q error %q, want success: the child is registered under "+
			"tenant-x and the parent was submitted under tenant-x", result.Status, result.Error)
	}
	if result.Output["sub"] == nil {
		t.Fatalf("result output has no sub node output: %#v", result.Output)
	}
}

// TestSubWorkflowExplicitRegistrationNamespaceLocal: engine namespace and
// registration namespace agree on a non-default value. The parent's sub node
// must resolve its child even though the dispatch-time queue ctx starts from
// context.Background().
func TestSubWorkflowExplicitRegistrationNamespaceLocal(t *testing.T) {
	eng, err := NewLocal(WithNamespace(namespace.Namespace("tenant-x")))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()
	runExplicitRegistrationNamespaceCase(t, eng)
}

// TestSubWorkflowExplicitRegistrationNamespaceCluster is the same case on the
// cluster engine: it additionally proves the registration namespace survives
// the Redis snapshot round trip, which is where dispatch reads it.
func TestSubWorkflowExplicitRegistrationNamespaceCluster(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()}, WithNamespace(namespace.Namespace("tenant-x")))
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer eng.Stop()
	runExplicitRegistrationNamespaceCase(t, eng)
}

// TestSubWorkflowNestedRegistrationNamespace runs parent -> mid -> child, all
// registered under tenant-x. The mid execution's dispatch resolves the
// grandchild from the namespace it inherited from its parent; without the
// inheritance the lookup falls back to the queue ctx and misses.
func TestSubWorkflowNestedRegistrationNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng, err := NewLocal(WithNamespace(namespace.Namespace("tenant-x")))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	if _, err := eng.AddWorkflow(ctx, subWorkflowChildBuilder(t).Namespace("tenant-x")); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	if _, err := eng.AddWorkflow(ctx, subWorkflowMidBuilder(t).Namespace("tenant-x")); err != nil {
		t.Fatalf("AddWorkflow(mid) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-mid").Namespace("tenant-x"))
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
		t.Fatalf("status = %q error %q, want success: the grandchild is registered "+
			"under tenant-x and every level inherits it", result.Status, result.Error)
	}
}

// TestSubWorkflowRerunKeepsTheRegistrationNamespace: a rerun is created through
// the seed path, where no definition is on the ctx. It must inherit the
// registration namespace from the parent; otherwise its sub node resolves
// against the queue ctx and misses, and the rerun fails where the original run
// succeeded.
func TestSubWorkflowRerunKeepsTheRegistrationNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eng, err := NewLocal(WithNamespace(namespace.Namespace("tenant-x")))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	if _, err := eng.AddWorkflow(ctx, subWorkflowWaitingChildBuilder(t).Namespace("tenant-x")); err != nil {
		t.Fatalf("AddWorkflow(child) error = %v", err)
	}
	parentID, err := eng.AddWorkflow(ctx, subWorkflowParentBuilder("sdk-sw-waiting-child").Namespace("tenant-x"))
	if err != nil {
		t.Fatalf("AddWorkflow(parent) error = %v", err)
	}
	id, err := eng.Invoke(ctx, parentID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	// Park on the sub node before canceling: a rerun from "sub" copies the
	// results upstream of it, so the parent must have run through "start" and
	// the child must be live, or the rerun is refused for a reason unrelated to
	// the namespace this test is about.
	waitForNodeStatus(t, ctx, eng, id, "sub", types.NodeStatusWaiting)
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	waitForExecStatus(t, ctx, eng, id, types.ExecutionStatusCanceled)

	rerun, err := eng.Rerun(ctx, id, "sub")
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	rerunDetail := waitForNodeStatus(t, ctx, eng, rerun.ExecutionID, "sub", types.NodeStatusWaiting)
	if len(rerunDetail.Nodes) != 1 || rerunDetail.Nodes[0].ChildExecutionID == "" {
		t.Fatalf("the rerun's parked sub node names no child: %+v", rerunDetail.Nodes)
	}

	if err := eng.Cancel(ctx, rerun.ExecutionID); err != nil {
		t.Fatalf("Cancel(rerun) error = %v", err)
	}
	waitForExecStatus(t, ctx, eng, rerun.ExecutionID, types.ExecutionStatusCanceled)
}
