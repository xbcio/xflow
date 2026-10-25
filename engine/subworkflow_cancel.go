package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// errSubWorkflowParentCanceling marks a dispatch that found its parent
// execution canceling. It is not a node failure: there is nothing to report and
// nothing to retry, only a child to stop so the cancel is not outlived by the
// work it canceled.
var errSubWorkflowParentCanceling = errors.New("parent execution is canceling")

// subWorkflowParentCanceling re-reads the parent's lifecycle status after its
// child has been started or resumed. It narrows the create-then-park window:
// the check inside startSubWorkflowChild happens before the child exists, and
// the inline cancel cascade (cancelSubWorkflowChildren) walks recorded links,
// which this dispatch has not written yet, so a cancel landing in between is
// visible to neither. The remaining gap is between this read and the park
// write, which still relies on the wait's reclaim; parking without any read
// would leave the child running for up to subWorkflowWaitTTL.
//
// An execution that is gone reads as canceling. Only the parent can conclude
// this wait, so an execution that no longer exists can never consume the
// child's result; stopping the child is the fail-closed answer.
//
// A read failure is returned rather than folded into "not canceling". Parking
// on an unreadable status is exactly the blind wait this re-read exists to
// prevent; the delivery can be retried, and the retry re-reads.
func (e *Engine) subWorkflowParentCanceling(ctx context.Context, id types.ExecutionID) (bool, error) {
	status, found, err := e.executionStatus(ctx, id)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	return status == types.ExecutionStatusCanceling || status == types.ExecutionStatusCanceled, nil
}

// cancelSubWorkflowChildren cancels the child executions this execution's
// sub-workflow nodes are waiting on, so a canceled parent is not outlived by
// the children its nodes started.
//
// This is the inline half of the cascade; the reclaim backstop is the other one
// (executeSubWorkflow cancels the child when a redelivery finds the parent
// canceling). Neither is a reconciler: a crash between the cancel and this loop
// leaves a child running until its own node leases expire and it reaches a
// terminal state on its own, which is the same window Cancel already accepts
// for in-flight runner leases.
//
// A child that already concluded or is gone (Cancel answers ErrExecutionInactive)
// is exactly the state this wants and counts as success; a child already
// canceling is resumed by Cancel and finishes the same way. Every other failure
// (a link read, a child read, any step of the child's own cancel) is logged
// where it happens and returned, joined across the children: one failing child
// does not stop the others from being canceled.
//
// Cancel discards the result. A failure costs the child's cancellation, not the
// parent's: the execution is still marked canceled, and the child's own
// wait/lease machinery remains in charge of it. ExpireExecution returns it,
// because its deadline entry may only be withdrawn once the children are
// stopped. The replay that follows repeats the cascade and finds each failed
// child in whatever state the failure left it: still running when the link or
// the child could not be read, canceling when its own cancel failed half way,
// or concluded when it finished on its own meanwhile.
// A failure inside a grandchild's cancel is not seen here: the child's own
// Cancel discards it by the same rule.
func (e *Engine) cancelSubWorkflowChildren(ctx context.Context, id types.ExecutionID, g *graph.Graph) error {
	store, ok := e.state.(SubWorkflowLinkStore)
	if !ok {
		// No links can exist without the capability, and a graph this engine
		// could run has none of these nodes.
		return nil
	}
	var errs []error
	for _, idx := range g.SubWorkflowNodeIndexes() {
		nodeName := g.NodeName(idx)
		link, found, err := store.GetSubWorkflowLink(ctx, id, nodeName)
		if err != nil {
			if e.logger != nil {
				e.logger.Error("read sub-workflow link for cancel failed",
					"execution_id", string(id), "node_name", nodeName, "err", err)
			}
			errs = append(errs, fmt.Errorf("read sub-workflow link %q/%q for cancel: %w", id, nodeName, err))
			continue
		}
		if !found {
			continue
		}
		if err := e.cancelSubWorkflowChild(ctx, link.ChildExecutionID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// cancelSubWorkflowChild cancels one child execution, tolerating the benign
// answers: a child that already concluded or is gone (inactive) is exactly the
// state this wants, and a child already canceling or canceled is the same answer
// reached twice. Any other failure is logged and returned. The dispatch paths
// call it directly and discard the result; Cancel reaches it through
// cancelSubWorkflowChildren and discards that function's joined result.
func (e *Engine) cancelSubWorkflowChild(ctx context.Context, childID types.ExecutionID) error {
	if childID == "" {
		return nil
	}
	err := e.Cancel(ctx, childID)
	if err == nil || errors.Is(err, ErrExecutionInactive) {
		return nil
	}
	if e.logger != nil {
		e.logger.Error("cancel sub-workflow child failed",
			"child_execution_id", string(childID), "err", err)
	}
	return fmt.Errorf("cancel sub-workflow child %q: %w", childID, err)
}

// finalizeParkedSubWorkflowNodes marks this execution's still-parked
// sub-workflow nodes canceled. The completion path deliberately refuses to
// decide these nodes while the parent is canceling (the notification the
// child's own cancellation fires carries no way to know this cancel
// supersedes it — see completeSubWorkflowNode), so the cancel is the only path
// left to close the wait. A node left Waiting would keep its lease, and the
// sweeper would then reclaim it once per wait TTL, each redelivery dropped as
// inactive until the execution keys expire.
//
// A node is finalized only while it is still Waiting: a concurrent completion
// or a sweeper reclaim that already moved it (committed, pending) is left
// alone, and the losing race is benign — a reclaim whose revoke lands after
// this write finds no claimable status and answers false, and a redelivery of
// a canceled node's task finds the execution inactive. The snapshot keeps the
// lease identity and the child id for the same reason the suspended-node
// rewrite keeps them: the write must not erase the record it is closing, and
// the terminal status is what fences later work off.
func (e *Engine) finalizeParkedSubWorkflowNodes(ctx context.Context, id types.ExecutionID, g *graph.Graph) error {
	for _, idx := range g.SubWorkflowNodeIndexes() {
		nodeName := g.NodeName(idx)
		current, err := e.state.GetNode(ctx, id, nodeName)
		if err != nil {
			return fmt.Errorf("read parked sub-workflow node %q/%q for cancel: %w", id, nodeName, err)
		}
		if current == nil || current.Status != types.NodeStatusWaiting {
			continue
		}
		cancelSnap := *current
		cancelSnap.Status = types.NodeStatusCanceled
		if err := e.state.UpsertNode(ctx, &cancelSnap); err != nil {
			return fmt.Errorf("mark parked sub-workflow node %q/%q canceled: %w", id, nodeName, err)
		}
	}
	return nil
}
