package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// subWorkflowEchoChildGraph compiles a child whose single node has a real
// handler in the test registry, so the child can be driven through its own
// commit path — the path that observes the child's conclusion and notifies the
// parent.
func subWorkflowEchoChildGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "echo", Type: "test.echo", Kind: types.NodeKindAction}},
	})
	if err != nil {
		t.Fatalf("compile echo child graph: %v", err)
	}
	return g
}

// TestSubWorkflowChildCompletionNotifiesTheParent is the notification end to
// end: the child concludes through its own commit path, that finalization
// notifies the waiting parent, and the parent node concludes without anything
// prompting it — not the reclaim sweeper, not a manual call.
func TestSubWorkflowChildCompletionNotifiesTheParent(t *testing.T) {
	ctx := context.Background()
	child := subWorkflowEchoChildGraph(t)
	resolver := stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
		Version:        "v1",
		DefinitionHash: "sha256:child",
		Graph:          child,
	}}
	g, err := graph.CompileWithResolver(ctx, subWorkflowParentDef(), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, nil, resolver)
	testRegistries.Store(rig.eng, &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}})
	t.Cleanup(func() { testRegistries.Delete(rig.eng) })

	rig.dispatch(t, rig.task)
	link := rig.link(t)
	childTasks := rig.queue.Drain()
	if len(childTasks) != 1 {
		t.Fatalf("queued %d child tasks, want the child's single root", len(childTasks))
	}

	// The child's node runs to completion in the engine that holds the parent's
	// wait — the ordinary case for an embedded engine, and the case the
	// notification exists for.
	executeTask(t, rig.eng, childTasks[0])

	if child := rig.child(t, link.ChildExecutionID); child.Status != types.ExecutionStatusSuccess {
		t.Fatalf("child status = %s, want success", child.Status)
	}
	node := rig.node(t)
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %s, want success: the child's conclusion must notify the waiting parent", node.Status)
	}
	if exec := rig.child(t, rig.parentID); exec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("parent status = %s, want success", exec.Status)
	}
}

// TestSubWorkflowChildCancelNotifiesTheParent pins the cancel half of the
// notification and its mapping: a child killed from outside did not deliver a
// result, so the parent node fails — with the reason naming the child — and the
// node's own retry/on_error decide what happens next.
func TestSubWorkflowChildCancelNotifiesTheParent(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	if err := rig.eng.Cancel(context.Background(), link.ChildExecutionID); err != nil {
		t.Fatalf("Cancel(child): %v", err)
	}

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed: a canceled child delivers no result", node.Status)
	}
	if !strings.Contains(node.Error, "was canceled") || !strings.Contains(node.Error, string(link.ChildExecutionID)) {
		t.Fatalf("node error = %q, want the child's cancellation and identity", node.Error)
	}
}
