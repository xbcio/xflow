package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// subWorkflowPrivateChildGraph compiles a child whose "work" node keeps its
// output private, so the parent-side mapping's projection can be observed.
func subWorkflowPrivateChildGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "child",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "xflow.noop", Output: &types.NodeOutputPolicy{Private: true}},
			{Name: "report", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "work"}}}},
			"work":  {"main": {Targets: []types.Connection{{Node: "report"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile private child graph: %v", err)
	}
	return g
}

// subWorkflowParentWithDownstream compiles the resolved parent with a node
// downstream of the sub-workflow node, so the completion's advance along main
// is observable instead of finalizing the execution on the spot.
func subWorkflowParentWithDownstream(t *testing.T) *graph.Graph {
	t.Helper()
	def := subWorkflowParentDef()
	def.Nodes = append(def.Nodes, types.NodeDef{Name: "after", Type: "xflow.noop"})
	def.Connections = types.Connections{
		"sub": {"main": {Targets: []types.Connection{{Node: "after"}}}},
	}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

// concludeChild drives a dispatched child to a terminal status with the given
// node outputs, the two writes a real child's own engine would have made.
func concludeChild(t *testing.T, rig *subWorkflowRig, childID types.ExecutionID, status types.ExecutionStatus, errMsg string,
	outputs map[string]map[string]any) {
	t.Helper()
	ctx := context.Background()
	for name, output := range outputs {
		if err := rig.state.PutOutput(ctx, childID, name, output); err != nil {
			t.Fatalf("PutOutput(%s): %v", name, err)
		}
	}
	if err := rig.state.UpdateExecutionStatus(ctx, childID, status, errMsg); err != nil {
		t.Fatalf("UpdateExecutionStatus(%s): %v", status, err)
	}
}

// notifyCompletion delivers the completion notification for one child and
// requires it to have been applied to the wait it names.
func notifyCompletion(t *testing.T, rig *subWorkflowRig, childID types.ExecutionID) {
	t.Helper()
	handled, err := rig.eng.completeSubWorkflowNode(context.Background(), rig.parentID, "sub", childID)
	if err != nil {
		t.Fatalf("completeSubWorkflowNode: %v", err)
	}
	if !handled {
		t.Fatal("the completion was not applied to the wait it names")
	}
}

// TestSubWorkflowCompletionConcludesWithTheChildResult is the completion happy
// path: the child's public node outputs become the node's output, and the node
// advances along main into its downstream node.
func TestSubWorkflowCompletionConcludesWithTheChildResult(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})
	notifyCompletion(t, rig, link.ChildExecutionID)

	node := rig.node(t)
	if node.Status != types.NodeStatusSuccess || node.Port != "main" {
		t.Fatalf("node = (%s, port %q), want (success, main)", node.Status, node.Port)
	}
	// The child outlives the wait it ran: a finished node must still name the
	// child execution its result was projected from, through Inspect as well as
	// on the record, or the drill-down would close exactly when an operator
	// reads it.
	if node.ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("child id after the commit = %q, want %q kept", node.ChildExecutionID, link.ChildExecutionID)
	}
	detail, err := rig.eng.Inspect(ctx, rig.parentID, "sub")
	if err != nil {
		t.Fatalf("Inspect(parent): %v", err)
	}
	if len(detail.Nodes) != 1 || detail.Nodes[0].ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("Inspected node after the commit = %+v, want child %q kept", detail.Nodes, link.ChildExecutionID)
	}
	output, err := rig.state.GetOutput(ctx, rig.parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	work, ok := output["work"].(map[string]any)
	if !ok || work["total"] != float64(7) {
		t.Fatalf("node output = %#v, want the child's public node outputs", output)
	}

	// The advance is resolved in-process during the commit's own flush, so what
	// lands on the queue is the downstream node's exec intent — which is the
	// evidence that the node advanced along main rather than only finalizing.
	tasks := rig.queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "after" || tasks[0].Type != TaskTypeNodeExec {
		t.Fatalf("delivered tasks = %+v, want the downstream node's exec intent", tasks)
	}
	if exec := rig.child(t, rig.parentID); exec.Status != types.ExecutionStatusRunning {
		t.Fatalf("parent status = %s, want running: the downstream node has not run yet", exec.Status)
	}
}

// TestSubWorkflowCompletionDropsAnotherGenerationsNotification is the
// stale-notification guard: after a retry the wait belongs to a later
// generation, and an answer to the abandoned one must not decide the node.
func TestSubWorkflowCompletionDropsAnotherGenerationsNotification(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	stale := subWorkflowChildID(rig.parentID, "sub", 1, 2)
	handled, err := rig.eng.completeSubWorkflowNode(context.Background(), rig.parentID, "sub", stale)
	if err != nil {
		t.Fatalf("completeSubWorkflowNode: %v", err)
	}
	if handled {
		t.Fatal("a notification naming another generation was applied")
	}
	if node := rig.node(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want the wait untouched", node.Status)
	}
	if current := rig.link(t); current != link {
		t.Fatalf("the stale notification rewrote the link: %+v -> %+v", link, current)
	}
}

// TestSubWorkflowCompletionFailsTheNodeWithTheChildReason pins the failure
// mapping: a child that failed fails the node, and the message names the child
// execution so an operator can go read it.
func TestSubWorkflowCompletionFailsTheNodeWithTheChildReason(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusFailed, "charge declined", nil)
	notifyCompletion(t, rig, link.ChildExecutionID)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed", node.Status)
	}
	if !strings.Contains(node.Error, string(link.ChildExecutionID)) || !strings.Contains(node.Error, "charge declined") {
		t.Fatalf("node error = %q, want the child's identity and reason", node.Error)
	}
	if exec := rig.child(t, rig.parentID); exec.Status != types.ExecutionStatusFailed {
		t.Fatalf("parent status = %s, want failed", exec.Status)
	}
}

// TestSubWorkflowReclaimConcludesAFinishedChild is the lost-notification
// backstop: the child concluded while nothing delivered the notification, the
// wait lease expired, and the reclaim redelivery concludes the node under its
// fresh claim instead of parking again.
func TestSubWorkflowReclaimConcludesAFinishedChild(t *testing.T) {
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %s, want success: the redelivery must conclude the finished child", node.Status)
	}
	if len(rig.state.executions) != 2 {
		t.Fatalf("executions = %d, want the parent and its one child: the redelivery must not start a second",
			len(rig.state.executions))
	}
	if tasks := rig.queue.Drain(); len(tasks) != 1 || tasks[0].NodeName != "after" {
		t.Fatalf("delivered tasks = %+v, want the downstream node's exec intent", tasks)
	}
}

// TestSubWorkflowOutputExpressionsMapTheChildResult pins the override: a
// declared output map is evaluated with $child as its root, and its shape is
// the node's output rather than the child's result map.
func TestSubWorkflowOutputExpressionsMapTheChildResult(t *testing.T) {
	ctx := context.Background()
	def := subWorkflowParentDef()
	def.Nodes[0].Parameters["output"] = map[string]any{
		"receipt": "${{ $child['work'].total }}",
		"origin":  "fixed",
	}
	g, err := graph.CompileWithResolver(ctx, def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})
	notifyCompletion(t, rig, link.ChildExecutionID)

	output, err := rig.state.GetOutput(ctx, rig.parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	switch n := output["receipt"].(type) {
	case int:
		if n != 7 {
			t.Fatalf("receipt = %v, want the child's total", n)
		}
	case float64:
		if n != 7 {
			t.Fatalf("receipt = %v, want the child's total", n)
		}
	default:
		t.Fatalf("receipt = %#v (%T), want the child's numeric total", output["receipt"], output["receipt"])
	}
	if output["origin"] != "fixed" {
		t.Fatalf("origin = %#v, want the untouched literal", output["origin"])
	}
	if _, leaked := output["work"]; leaked {
		t.Fatalf("output = %#v, want the declared mapping only, not the child's result map", output)
	}
}

// TestSubWorkflowOutputOmitsPrivateChildNodes pins the projection: a private
// node's output is withheld from both the default mapping and the $child root,
// the same fail-closed read the audit surface applies.
func TestSubWorkflowOutputOmitsPrivateChildNodes(t *testing.T) {
	ctx := context.Background()
	child := subWorkflowPrivateChildGraph(t)
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
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{
			"work":   {"secret": "hidden"},
			"report": {"ok": true},
		})
	notifyCompletion(t, rig, link.ChildExecutionID)

	output, err := rig.state.GetOutput(ctx, rig.parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	if _, leaked := output["work"]; leaked {
		t.Fatalf("output = %#v, want the private node's output withheld", output)
	}
	report, ok := output["report"].(map[string]any)
	if !ok || report["ok"] != true {
		t.Fatalf("output = %#v, want the public node's output carried", output)
	}
}

// TestSubWorkflowChildFailureRetriesAtTheNextGeneration pins the retry rule on
// the completion path: a failed child is retried, and the retry intent carries
// the next generation, so the retry starts a new child instead of resuming the
// failed one.
func TestSubWorkflowChildFailureRetriesAtTheNextGeneration(t *testing.T) {
	g := subWorkflowParentWithRetry(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusFailed, "boom", nil)
	handled, err := rig.eng.completeSubWorkflowNode(context.Background(), rig.parentID, "sub", link.ChildExecutionID)
	if err != nil || !handled {
		t.Fatalf("completeSubWorkflowNode: handled=%v err=%v", handled, err)
	}

	if node := rig.node(t); node.Status != types.NodeStatusPending {
		t.Fatalf("node status = %s, want the retry's pending reset", node.Status)
	}
	entries, err := rig.state.ListOutbox(context.Background(), rig.parentID, time.Now().UTC().Add(time.Minute), 16)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("recorded %d retry intents, want 1", len(entries))
	}
	if entries[0].Task.Type != TaskTypeSubWorkflow || entries[0].Task.Generation != 2 {
		t.Fatalf("retry intent = (%d, generation %d), want the sub-workflow task at generation 2",
			entries[0].Task.Type, entries[0].Task.Generation)
	}
}
