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
// Every refusal is benign and swallowed: the child may already be terminal (its
// own completion raced the cancel), and a store read failure costs the child's
// cancellation, not this cancel — the execution is still marked canceled, and
// the child's own wait/lease machinery remains in charge of it.
func (e *Engine) cancelSubWorkflowChildren(ctx context.Context, id types.ExecutionID, g *graph.Graph) {
	store, ok := e.state.(SubWorkflowLinkStore)
	if !ok {
		// No links can exist without the capability, and a graph this engine
		// could run has none of these nodes.
		return
	}
	for _, idx := range g.SubWorkflowNodeIndexes() {
		nodeName := g.NodeName(idx)
		link, found, err := store.GetSubWorkflowLink(ctx, id, nodeName)
		if err != nil {
			if e.logger != nil {
				e.logger.Error("read sub-workflow link for cancel failed",
					"execution_id", string(id), "node_name", nodeName, "err", err)
			}
			continue
		}
		if !found {
			continue
		}
		e.cancelSubWorkflowChild(ctx, link.ChildExecutionID)
	}
}

// cancelSubWorkflowChild cancels one child execution, tolerating the benign
// answers: a child that already concluded (inactive) is exactly the state this
// wants, and a child already canceling or canceled is the same answer reached
// twice.
func (e *Engine) cancelSubWorkflowChild(ctx context.Context, childID types.ExecutionID) {
	if childID == "" {
		return
	}
	if err := e.Cancel(ctx, childID); err != nil && !errors.Is(err, ErrExecutionInactive) && e.logger != nil {
		e.logger.Error("cancel sub-workflow child failed",
			"child_execution_id", string(childID), "err", err)
	}
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
