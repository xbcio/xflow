package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestSubWorkflowCancelCascadesToTheChild is the inline half of the cascade: a
// canceled parent must not be outlived by the child its node started, so Cancel
// cancels the child before it finalizes itself — and it must finalize the
// parked node too.
//
// The node-status assertion is the ordering pin. The child's own cancellation
// notifies the parent — the notification funnel has no way to know the cancel is
// in flight — and the parent's canceling status is the only thing that keeps
// that notification from concluding the node with a failure the cancel already
// supersedes. That is also why the node ends up canceled by the cancel path
// rather than by the notification: a node left Waiting keeps its lease, and the
// sweeper then reclaims it once per wait TTL, each redelivery dropped as
// inactive until the execution keys expire.
func TestSubWorkflowCancelCascadesToTheChild(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	if err := rig.eng.Cancel(context.Background(), rig.parentID); err != nil {
		t.Fatalf("Cancel(parent): %v", err)
	}

	if child := rig.child(t, link.ChildExecutionID); child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child status = %s, want canceled: the parent's cancel must cascade", child.Status)
	}
	node := rig.node(t)
	if node.Status != types.NodeStatusCanceled {
		t.Fatalf("node status = %s, want canceled: the cancel path itself must finalize the parked wait", node.Status)
	}
	// The canceled node keeps the child it waited on: the drill-down is the
	// same for a canceled wait as for a finished one, and the cascade above
	// just canceled that exact child.
	if node.ChildExecutionID != link.ChildExecutionID {
		t.Fatalf("canceled node child = %q, want %q kept", node.ChildExecutionID, link.ChildExecutionID)
	}
	expired, err := rig.state.ListExpiredLeases(context.Background(), time.Now().UTC().Add(subWorkflowWaitTTL+time.Minute))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	for _, lease := range expired {
		if lease.ExecutionID == rig.parentID && lease.NodeName == "sub" {
			t.Fatalf("the sweeper still sees the canceled wait's lease: %+v", lease)
		}
	}
	if exec := rig.child(t, rig.parentID); exec.Status != types.ExecutionStatusCanceled {
		t.Fatalf("parent status = %s, want canceled", exec.Status)
	}
}

// TestSubWorkflowCancelWithoutALinkCancelsAnyway pins the loop's no-link path:
// a sub-workflow node that was never dispatched has no child to find, and that
// absence must leave the cancel itself untouched.
func TestSubWorkflowCancelWithoutALinkCancelsAnyway(t *testing.T) {
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))

	if err := rig.eng.Cancel(context.Background(), rig.parentID); err != nil {
		t.Fatalf("Cancel(parent): %v", err)
	}
	if exec := rig.child(t, rig.parentID); exec.Status != types.ExecutionStatusCanceled {
		t.Fatalf("parent status = %s, want canceled", exec.Status)
	}
}

// TestSubWorkflowDispatchBackstopCancelsTheChild is the reclaim half of the
// cascade, the window the inline half cannot cover: the parent was marked
// canceling and the cancel never got to its cascade (a crash between the two
// status writes), so the parked node's lease is the only thing left holding the
// child's fate. When the sweeper redelivers that node, the dispatch must stop
// the child and release its claim — parking again would hand the sweeper a
// lease it can only reclaim into the same abandoned path, once per TTL.
func TestSubWorkflowDispatchBackstopCancelsTheChild(t *testing.T) {
	ctx := context.Background()
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.dispatch(t, rig.task)
	link := rig.link(t)
	rig.queue.Drain()

	if err := rig.state.UpdateExecutionStatus(ctx, rig.parentID, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("mark the parent canceling: %v", err)
	}
	rig.revoke(t)
	rig.dispatch(t, rig.task)

	// The derived id is a function of the dispatch key, so the redelivery looks
	// at exactly the child the first dispatch started — canceling it here is the
	// redelivery's proof that it did not mint a second one.
	if child := rig.child(t, link.ChildExecutionID); child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child status = %s, want canceled: the redelivery must stop the child", child.Status)
	}
	node := rig.node(t)
	if node.Status != types.NodeStatusPending || node.LeaseToken != "" {
		t.Fatalf("node status = %s lease token %q, want pending with no lease: the dispatch must release its claim",
			node.Status, node.LeaseToken)
	}
}
