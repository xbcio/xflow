package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// subWorkflowDeadlineGraph is a parent with settings.timeout whose sub-workflow
// node declares its own timeout (so the dispatch's wait-expiry branch is live)
// and every on_error route a failed node could take: a main target, and an
// error target for error_output.
func subWorkflowDeadlineGraph(t *testing.T, onError string, nodeTimeout time.Duration) *graph.Graph {
	t.Helper()
	def := subWorkflowParentDef()
	def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	def.Nodes[0].Timeout = nodeTimeout
	def.Nodes[0].OnError = onError
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "next", Type: "test.echo"},
		types.NodeDef{Name: "onerr", Type: "test.echo"},
	)
	def.Connections = types.Connections{
		"sub": {
			"main":  {Targets: []types.Connection{{Node: "next", Input: "main"}}},
			"error": {Targets: []types.Connection{{Node: "onerr", Input: "main"}}},
		},
	}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

// redeliverSubWorkflowNode simulates the lease sweeper's reclaim of the parked
// node and hands the same intent to the engine again.
func redeliverSubWorkflowNode(t *testing.T, rig *expireSubWorkflowRig, g *graph.Graph) {
	t.Helper()
	ctx := context.Background()
	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil {
		t.Fatalf("read the sub node: node=%v err=%v", node, err)
	}
	if revoked, err := rig.state.RevokeLease(ctx, rig.parentID, "sub", node.LeaseToken); err != nil || !revoked {
		t.Fatalf("RevokeLease: revoked=%v err=%v", revoked, err)
	}
	idx, ok := g.NodeIndex("sub")
	if !ok {
		t.Fatal(`the graph has no node named "sub"`)
	}
	handled, err := rig.eng.handleSystemTask(ctx, &Task{
		ExecutionID:  rig.parentID,
		NodeName:     "sub",
		NodeIdx:      idx,
		UnitIdx:      g.UnitIndexForNode(idx),
		Type:         TaskTypeSubWorkflow,
		ActivationID: 1,
	}, true)
	if err != nil || !handled {
		t.Fatalf("redeliver the sub-workflow node: handled=%v err=%v", handled, err)
	}
}

// TestSubWorkflowWaitEndingAtTheExecutionDeadlineExpiresTheExecution pins the
// tie rule for the dispatch's own wait expiry. The wait is parked clamped to the
// execution deadline, so a resumed dispatch that finds the wait over cannot tell
// the node's budget from the execution's by the wait deadline alone. When that
// deadline is the execution's, the execution has timed out: no on_error route
// (stop, continue, error_output) may absorb it, and the child is stopped by the
// execution's own cascade.
func TestSubWorkflowWaitEndingAtTheExecutionDeadlineExpiresTheExecution(t *testing.T) {
	for _, onError := range []string{"", string(types.OnErrorContinue), string(types.OnErrorOutput)} {
		name := onError
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g := subWorkflowDeadlineGraph(t, onError, time.Minute)
			// The rig parks the node under an already-reached execution deadline,
			// so the stored wait end is the execution's.
			rig := newExpireSubWorkflowRigOf(t, g)
			if !rig.link.WaitDeadline.Equal(pastDeadline) {
				t.Fatalf("parked wait deadline = %s, want the execution deadline %s", rig.link.WaitDeadline, pastDeadline)
			}

			redeliverSubWorkflowNode(t, rig, g)

			parent, err := rig.state.GetExecution(ctx, rig.parentID)
			if err != nil || parent == nil {
				t.Fatalf("read the parent: parent=%v err=%v", parent, err)
			}
			if parent.Status != types.ExecutionStatusTimeout || parent.Error != ExecutionDeadlineExceededError {
				t.Fatalf("parent = %s %q, want timeout %q: the node's on_error must not absorb the execution's deadline",
					parent.Status, parent.Error, ExecutionDeadlineExceededError)
			}
			if node, err := rig.state.GetNode(ctx, rig.parentID, "sub"); err != nil || node == nil || node.Status == types.NodeStatusFailed {
				t.Fatalf("sub node = %+v (err %v), want it not failed through on_error", node, err)
			}
			child, err := rig.state.GetExecution(ctx, rig.link.ChildExecutionID)
			if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
				t.Fatalf("child = %+v (err %v), want canceled", child, err)
			}
			if got := rig.hooks.completions(); len(got) != 1 || got[0] != types.ExecutionStatusTimeout {
				t.Fatalf("completion hooks = %v, want exactly [timeout]", got)
			}
		})
	}
}

// TestSubWorkflowWaitTimeoutBeforeTheExecutionDeadlineStillFollowsOnError is the
// other side of the tie rule (R3, same family): a wait that ran out of its node
// timeout while the execution deadline is still ahead is the node's failure, and
// it goes through the ordinary on_error pipeline. The execution is not expired.
func TestSubWorkflowWaitTimeoutBeforeTheExecutionDeadlineStillFollowsOnError(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowDeadlineGraph(t, "", time.Minute)
	rig := newExpireSubWorkflowRigOf(t, g)

	// The rig seeds an already-reached execution deadline; move it ahead and
	// drop the engine's memo of the old one. The parked wait end stays in the
	// past, so only the node's budget has run out.
	rig.state.mu.Lock()
	rig.state.executions[rig.parentID].Deadline = time.Now().Add(time.Hour)
	rig.state.mu.Unlock()
	rig.eng.EvictExecution(rig.parentID)

	redeliverSubWorkflowNode(t, rig, g)

	parent, err := rig.state.GetExecution(ctx, rig.parentID)
	if err != nil || parent == nil {
		t.Fatalf("read the parent: parent=%v err=%v", parent, err)
	}
	if parent.Status == types.ExecutionStatusTimeout {
		t.Fatalf("parent = %s %q, want the execution left to on_error: its deadline is still ahead", parent.Status, parent.Error)
	}
	node, err := rig.state.GetNode(ctx, rig.parentID, "sub")
	if err != nil || node == nil || node.Status != types.NodeStatusFailed {
		t.Fatalf("sub node = %+v (err %v), want failed through the node timeout", node, err)
	}
	child, err := rig.state.GetExecution(ctx, rig.link.ChildExecutionID)
	if err != nil || child == nil || child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child = %+v (err %v), want canceled by the node-timeout path", child, err)
	}
}
