package engine

import (
	"context"
	"errors"

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
