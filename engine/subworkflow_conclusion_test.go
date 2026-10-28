package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// expireChild removes every record of a child execution, the state a retention
// policy leaves behind once it has expired the child while its parent is still
// waiting. The parent's own records, including anything recorded in its slot,
// are untouched — that asymmetry is the situation these tests exist for.
func expireChild(rig *subWorkflowRig, childID types.ExecutionID) {
	rig.state.mu.Lock()
	defer rig.state.mu.Unlock()
	delete(rig.state.executions, childID)
	prefix := string(childID) + "/"
	for key := range rig.state.nodes {
		if strings.HasPrefix(key, prefix) {
			delete(rig.state.nodes, key)
		}
	}
	for key := range rig.state.outputs {
		if strings.HasPrefix(key, prefix) {
			delete(rig.state.outputs, key)
		}
	}
}

// childTasks counts queued tasks addressed to the child: the engine-level
// equivalent of "the child's handlers ran (again)", because a child that is not
// started delivers no task for any handler to consume.
func childTasks(tasks []*Task, childID types.ExecutionID) int {
	count := 0
	for _, task := range tasks {
		if task.ExecutionID == childID {
			count++
		}
	}
	return count
}

// recordConclusionOnly performs exactly the half of a completion notification
// that writes the conclusion, and stops: the crash window between the child's
// terminal commit and the parent's commit.
func recordConclusionOnly(t *testing.T, rig *subWorkflowRig, childID types.ExecutionID) SubWorkflowConclusion {
	t.Helper()
	conclusion, concluded, err := rig.eng.resolveSubWorkflowConclusion(context.Background(), rig.parentID, "sub", childID)
	if err != nil || !concluded {
		t.Fatalf("resolveSubWorkflowConclusion: concluded=%v err=%v", concluded, err)
	}
	return conclusion
}

// TestSubWorkflowCompletionRecordsTheChildConclusion pins the write: concluding
// the parent from a live child leaves the child's verdict, error and public
// output in the parent's slot, keyed by the child.
func TestSubWorkflowCompletionRecordsTheChildConclusion(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	if _, found, err := rig.state.GetSubWorkflowConclusion(ctx, rig.parentID, "sub", link.ChildExecutionID); err != nil || found {
		t.Fatalf("a conclusion exists before the child concluded: found=%v err=%v", found, err)
	}

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})
	notifyCompletion(t, rig, link.ChildExecutionID)

	got, found, err := rig.state.GetSubWorkflowConclusion(ctx, rig.parentID, "sub", link.ChildExecutionID)
	if err != nil || !found {
		t.Fatalf("GetSubWorkflowConclusion: found=%v err=%v, want the recorded conclusion", found, err)
	}
	if got.ChildExecutionID != link.ChildExecutionID || got.Status != types.ExecutionStatusSuccess || got.Error != "" {
		t.Fatalf("conclusion = %+v, want success for child %q", got, link.ChildExecutionID)
	}
	work, ok := got.Output["work"].(map[string]any)
	if !ok || work["total"] != float64(7) {
		t.Fatalf("conclusion output = %#v, want the child's public node outputs", got.Output)
	}
}

// TestSubWorkflowCompletionDoesNotRecordAnUnfinishedChild pins the other side of
// the write: a running child has no verdict, so nothing is recorded for the
// parent to later mistake for one.
func TestSubWorkflowCompletionDoesNotRecordAnUnfinishedChild(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	handled, err := rig.eng.completeSubWorkflowNode(context.Background(), rig.parentID, "sub", link.ChildExecutionID)
	if err != nil || handled {
		t.Fatalf("completeSubWorkflowNode(running child) handled=%v err=%v, want false/nil", handled, err)
	}
	if _, found, _ := rig.state.GetSubWorkflowConclusion(context.Background(), rig.parentID, "sub", link.ChildExecutionID); found {
		t.Fatal("a conclusion was recorded for a child that has not concluded")
	}
}

// TestSubWorkflowReclaimConcludesFromTheRecordedConclusion is the guarantee this
// capability exists for. The child concluded and the conclusion was recorded,
// but the parent was never committed (the notifier crashed after the record);
// then the child's records expired. The reclaim redelivery must finish the node
// from the recorded conclusion — without rebuilding the child, which would run
// it a second time under the same derived id.
func TestSubWorkflowReclaimConcludesFromTheRecordedConclusion(t *testing.T) {
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})
	recordConclusionOnly(t, rig, link.ChildExecutionID)
	expireChild(rig, link.ChildExecutionID)

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %s, want success: the reclaim must conclude from the recorded conclusion", node.Status)
	}
	if node.ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("concluded node child = %q, want %q kept for the drill-down", node.ChildExecutionID, link.ChildExecutionID)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want only the parent: the child must not be rebuilt", len(rig.state.executions))
	}
	output, err := rig.state.GetOutput(context.Background(), rig.parentID, "sub")
	if err != nil {
		t.Fatalf("GetOutput(sub): %v", err)
	}
	if work, ok := output["work"].(map[string]any); !ok || work["total"] != float64(7) {
		t.Fatalf("node output = %#v, want the recorded child output", output)
	}
	tasks := rig.queue.Drain()
	if n := childTasks(tasks, link.ChildExecutionID); n != 0 {
		t.Fatalf("%d task(s) were delivered to the child: it must not run again", n)
	}
	if len(tasks) != 1 || tasks[0].NodeName != "after" {
		t.Fatalf("delivered tasks = %+v, want only the downstream node's exec intent", tasks)
	}
}

// TestSubWorkflowNotificationConcludesFromTheRecordedConclusion is the same
// guarantee through the fast path: the wait is still live, the child's records
// are gone, and a (late) completion notification finishes the node from the
// recorded conclusion instead of dropping it as "child missing".
func TestSubWorkflowNotificationConcludesFromTheRecordedConclusion(t *testing.T) {
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "",
		map[string]map[string]any{"work": {"total": float64(7)}})
	recordConclusionOnly(t, rig, link.ChildExecutionID)
	expireChild(rig, link.ChildExecutionID)

	notifyCompletion(t, rig, link.ChildExecutionID)

	if node := rig.node(t); node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %s, want success", node.Status)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want only the parent", len(rig.state.executions))
	}
}

// TestSubWorkflowRecordedFailureFailsTheNodeWithTheChildReason pins that the
// recorded conclusion is judged by the same mapping as a live child: a failed
// child's reason, with its identity, reaches the parent node's error.
func TestSubWorkflowRecordedFailureFailsTheNodeWithTheChildReason(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusFailed, "charge declined", nil)
	recordConclusionOnly(t, rig, link.ChildExecutionID)
	expireChild(rig, link.ChildExecutionID)

	rig.revoke(t)
	rig.dispatch(t, rig.task)

	node := rig.node(t)
	if node.Status != types.NodeStatusFailed {
		t.Fatalf("node status = %s, want failed", node.Status)
	}
	if !strings.Contains(node.Error, string(link.ChildExecutionID)) || !strings.Contains(node.Error, "charge declined") {
		t.Fatalf("node error = %q, want the child's identity and reason", node.Error)
	}
	if len(rig.state.executions) != 1 {
		t.Fatalf("executions = %d, want only the parent: a failed child must not be rebuilt either", len(rig.state.executions))
	}
}

// TestSubWorkflowRecordedConclusionFeedsTheOutputMapping pins that the node's
// `output:` mapping is evaluated at commit time over the recorded output, so a
// result read from a conclusion maps exactly like one read from a live child.
func TestSubWorkflowRecordedConclusionFeedsTheOutputMapping(t *testing.T) {
	ctx := context.Background()
	def := subWorkflowParentDef()
	def.Nodes[0].Parameters["output"] = map[string]any{
		"receipt": "${{ $child['work'].total }}",
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
	recordConclusionOnly(t, rig, link.ChildExecutionID)
	expireChild(rig, link.ChildExecutionID)

	rig.revoke(t)
	rig.dispatch(t, rig.task)

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
}

// TestSubWorkflowReclaimWithoutAConclusionStillRebuildsAMissingChild pins the
// path that must NOT change: a child whose records are gone and whose
// conclusion was never recorded (a wait that predates conclusions, or a child
// that vanished before it concluded) is rebuilt under the same derived id by the
// reclaim, exactly as before. There is no transition gate and no dual write —
// absence of the record simply is the old behavior.
func TestSubWorkflowReclaimWithoutAConclusionStillRebuildsAMissingChild(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	expireChild(rig, link.ChildExecutionID)
	rig.revoke(t)
	rig.dispatch(t, rig.task)

	if rig.node(t).Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want waiting on the rebuilt child", rig.node(t).Status)
	}
	if rebuilt := rig.child(t, link.ChildExecutionID); rebuilt.Status != types.ExecutionStatusRunning {
		t.Fatalf("rebuilt child status = %s, want running", rebuilt.Status)
	}
	if n := childTasks(rig.queue.Drain(), link.ChildExecutionID); n != 1 {
		t.Fatalf("the rebuilt child delivered %d root task(s), want 1", n)
	}
}

// TestSubWorkflowConclusionFromAnotherGenerationIsInvisible pins the per-child
// key: after a retry the node waits on the next generation's child, and the
// previous generation's recorded conclusion must neither suppress that child's
// creation nor decide the node.
func TestSubWorkflowConclusionFromAnotherGenerationIsInvisible(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	first := rig.link(t)

	concludeChild(t, rig, first.ChildExecutionID, types.ExecutionStatusFailed, "boom", nil)
	recordConclusionOnly(t, rig, first.ChildExecutionID)
	expireChild(rig, first.ChildExecutionID)
	rig.queue.Drain()

	rig.revoke(t)
	retryTask := *rig.task
	retryTask.Generation = 2
	rig.dispatch(t, &retryTask)

	second := rig.link(t)
	if second.Generation != 2 || second.ChildExecutionID == first.ChildExecutionID {
		t.Fatalf("link = %+v, want generation 2 on a fresh child", second)
	}
	if node := rig.node(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want waiting on the fresh child: the old conclusion must not decide it", node.Status)
	}
	if n := childTasks(rig.queue.Drain(), second.ChildExecutionID); n != 1 {
		t.Fatalf("the fresh child delivered %d root task(s), want 1", n)
	}
}

// TestSubWorkflowReclaimDoesNotCommitWhenTheConclusionCannotBeRecorded pins the
// write-before-commit order. If the conclusion cannot be made durable, the
// parent must not act on the child's result: committing anyway would be the
// window where the parent is done but neither the child's records nor a
// conclusion can later be read. The redelivery fails, and the next one — with
// the store back — concludes.
func TestSubWorkflowReclaimDoesNotCommitWhenTheConclusionCannotBeRecorded(t *testing.T) {
	g := subWorkflowParentWithDownstream(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "", nil)
	outage := errors.New("conclusion store unavailable")
	rig.state.recordError = outage

	rig.revoke(t)
	handled, err := rig.eng.handleSystemTask(context.Background(), rig.task, true)
	if !handled || !errors.Is(err, outage) {
		t.Fatalf("handleSystemTask handled=%v err=%v, want the recording failure surfaced", handled, err)
	}
	if node := rig.node(t); node.Status == types.NodeStatusSuccess {
		t.Fatal("the parent node committed although the conclusion could not be recorded")
	}

	rig.state.recordError = nil
	rig.revoke(t)
	rig.dispatch(t, rig.task)
	if node := rig.node(t); node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %s, want success once the store is back", node.Status)
	}
	if _, found, _ := rig.state.GetSubWorkflowConclusion(context.Background(), rig.parentID, "sub", link.ChildExecutionID); !found {
		t.Fatal("the concluding redelivery left no recorded conclusion")
	}
}

// TestSubWorkflowNotificationRecordFailureLeavesTheWaitToTheReclaim pins that
// the fast path stays best effort: a conclusion that cannot be recorded is an
// error for the notifier to log, the node is left parked, and nothing is
// committed from an unrecorded verdict.
func TestSubWorkflowNotificationRecordFailureLeavesTheWaitToTheReclaim(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)

	concludeChild(t, rig, link.ChildExecutionID, types.ExecutionStatusSuccess, "", nil)
	outage := errors.New("conclusion store unavailable")
	rig.state.recordError = outage

	handled, err := rig.eng.completeSubWorkflowNode(context.Background(), rig.parentID, "sub", link.ChildExecutionID)
	if handled || !errors.Is(err, outage) {
		t.Fatalf("completeSubWorkflowNode handled=%v err=%v, want the recording failure and no commit", handled, err)
	}
	if node := rig.node(t); node.Status != types.NodeStatusWaiting {
		t.Fatalf("node status = %s, want the wait left standing", node.Status)
	}
}
