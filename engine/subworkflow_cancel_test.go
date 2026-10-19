package engine

import (
	"context"
	"errors"
	"strings"
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

// TestSubWorkflowCancelLandedAfterChildCreationDoesNotPark is the create-then-
// park window in its worst form: the cancel reaches the parent while the child
// record exists but before the dispatch writes the link, so the inline cascade
// finds no link to walk and no parked wait to finalize. No reclaim runs in this
// test — the dispatch's own post-start re-read is the only actor that can stop
// the child now. Without it the node parks and the child runs until the
// sweeper's reclaim rediscovers the parent, up to subWorkflowWaitTTL later.
func TestSubWorkflowCancelLandedAfterChildCreationDoesNotPark(t *testing.T) {
	ctx := context.Background()
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.state.onExecutionCreated = func(id types.ExecutionID) {
		if id == rig.parentID {
			return // the parent was seeded before the hook was installed
		}
		// The real cancel API, landed exactly in the window: its cascade cannot
		// see the child it was just created for. The parent ends fully canceled,
		// not merely canceling, which is the harsher half of the re-read.
		if err := rig.eng.Cancel(ctx, rig.parentID); err != nil {
			t.Fatalf("Cancel inside the create-then-park window: %v", err)
		}
	}

	rig.dispatch(t, rig.task)

	childID := subWorkflowChildID(rig.parentID, "sub", 1, 1)
	if child := rig.child(t, childID); child.Status != types.ExecutionStatusCanceled {
		t.Fatalf("child status = %s, want canceled: the dispatch must stop the child it was canceling for", child.Status)
	}
	node := rig.node(t)
	if node.Status != types.NodeStatusPending || node.LeaseToken != "" {
		t.Fatalf("node status = %s lease token %q, want pending with no lease: the canceling path must release the claim",
			node.Status, node.LeaseToken)
	}
	if _, ok, err := rig.state.GetSubWorkflowLink(ctx, rig.parentID, "sub"); err != nil || ok {
		t.Fatalf("the dispatch parked on a canceled parent: link found=%v err=%v", ok, err)
	}
}

// TestSubWorkflowDispatchFailsWhenThePostStartParentReadFails pins the failure
// preference on the re-read: an unreadable parent status must fail the delivery
// so the reclaim retries, never park the node on a status nobody could read.
// Released here instead of left claimed, the retry would have nothing to
// revisit; parked, it would take the reclaim to undo.
func TestSubWorkflowDispatchFailsWhenThePostStartParentReadFails(t *testing.T) {
	ctx := context.Background()
	g := resolvedSubWorkflowParent(t)
	rig := newSubWorkflowRig(t, g, nil, subWorkflowStubResolver(t))
	rig.state.onExecutionCreated = func(id types.ExecutionID) {
		if id == rig.parentID {
			return
		}
		rig.state.readError = func(readID types.ExecutionID) error {
			if readID != rig.parentID {
				return nil
			}
			return errors.New("parent status unavailable")
		}
	}

	handled, err := rig.eng.handleSystemTask(ctx, rig.task, true)
	if !handled {
		t.Fatal("the engine did not claim its own sub-workflow task")
	}
	if err == nil || !strings.Contains(err.Error(), "parent status unavailable") {
		t.Fatalf("dispatch error = %v, want the failed parent re-read", err)
	}

	if _, ok, linkErr := rig.state.GetSubWorkflowLink(ctx, rig.parentID, "sub"); linkErr != nil || ok {
		t.Fatalf("the dispatch parked on an unreadable parent status: link found=%v err=%v", ok, linkErr)
	}
	node := rig.node(t)
	if node.Status != types.NodeStatusRunning || node.LeaseToken == "" {
		t.Fatalf("node status = %s lease token %q, want the claim kept for the reclaim to retry",
			node.Status, node.LeaseToken)
	}
	childID := subWorkflowChildID(rig.parentID, "sub", 1, 1)
	if child := rig.child(t, childID); child.Status != types.ExecutionStatusRunning {
		t.Fatalf("child status = %s, want running: the failed re-read decides nothing", child.Status)
	}
}
