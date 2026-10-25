package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// Cancel marks an execution as canceled. The transition is driven in five
// steps:
//
//  1. mark the execution canceling;
//  2. cancel every suspended node it still has;
//  3. cancel the child executions its sub-workflow nodes are waiting on;
//  4. finalize those parked sub-workflow nodes (Waiting → Canceled);
//  5. mark the execution canceled, notify completion, and evict its graph.
//
// Cancel is idempotent and RESUMABLE. Every step is fenced at the store and
// tolerates already having been applied by an earlier attempt: the canceling
// write is a no-op on a canceling execution, the suspended-node transition is a
// fenced compare-and-set that skips nodes no longer Suspended, the child
// cascade tolerates a child that already concluded, and the parked-node
// finalization only rewrites nodes still Waiting. The final write into canceled
// is likewise accepted from canceling. A second call on an execution left in
// canceling therefore carries on from wherever the interrupted call stopped
// rather than starting over or refusing.
//
// The one thing Cancel cannot do is run after the caller that started it is
// gone. Steps 2–5 each leave the execution in canceling with intermediate state
// half-reconciled, and nothing in the request path retries. That recovery is
// the leader-driven canceling-execution reaper's job (see
// engine.CancelingExecutionLister and service/control's LeaseSweeper): it finds
// executions that stayed canceling past a stuck threshold and calls Cancel
// again, which resumes exactly as above. Because the reaper's call is subject
// to the same concurrency as any other, the steps are also safe to run while an
// original Cancel is still in flight — each store transition decides its own
// race.
//
// Completion notification (step 5) is deliberately after the durable canceled
// write and remains best-effort: a crash between them loses the hook, not the
// cancellation. The execution is terminal either way, so it is not a state
// the reaper can or should revisit.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	// The graph cache is shared across namespaces, so a cache hit alone is not
	// sufficient to authorize a cancel. loadActiveGraph confirms the execution
	// exists in the caller's namespace namespace via GetExecution; a cross-namespace
	// or inactive ID is reported as not-found so we do not leak existence or
	// trigger side effects against another namespace's execution.
	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return fmt.Errorf("load graph for canceled execution %q: %w", id, err)
	}
	if !active {
		return fmt.Errorf("execution %q: %w", id, ErrExecutionInactive)
	}

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		return fmt.Errorf("mark execution %q canceling: %w", id, err)
	}

	if err := e.cancelSuspendedNodes(ctx, id, g); err != nil {
		return err
	}

	// The children must stop too, and they stop BEFORE the parent is marked
	// canceled: each child's own cancellation notifies this execution's node,
	// and the canceling status set above is what keeps that notification from
	// concluding the node with a failure this cancel already supersedes.
	//
	// A child that could not be stopped costs that child's cancellation, not this
	// cancel: the parent is still marked canceled and the child's own wait/lease
	// machinery stays in charge of it. cancelSubWorkflowChildren returns that
	// failure for the callers that need it (ExpireExecution); Cancel stays
	// best-effort, so it keeps only the log line the helper already wrote.
	_ = e.cancelSubWorkflowChildren(ctx, id, g)

	// With the children stopping, the nodes they were parked under have no
	// completion left to wait for: the completion path refuses them while this
	// execution is canceling, so the cancel finalizes them here rather than
	// leaving a waiting node for the sweeper to reclaim once per wait TTL.
	if err := e.finalizeParkedSubWorkflowNodes(ctx, id, g); err != nil {
		return err
	}

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		return fmt.Errorf("mark execution %q canceled: %w", id, err)
	}
	e.notifyExecutionComplete(ctx, id, types.ExecutionStatusCanceled)
	e.EvictExecution(id)
	return nil
}

// cancelSuspendedNodes cancels every node the execution still has suspended. It
// does not touch the execution's own status, so Cancel (which has already
// marked the execution canceling) and the execution-level timeout (which
// drives its own terminal status) share it. It is idempotent: a node no longer
// Suspended, whether an earlier attempt already canceled it or a concurrent
// resume moved it on, is skipped rather than rewritten.
func (e *Engine) cancelSuspendedNodes(ctx context.Context, id types.ExecutionID, g *graph.Graph) error {
	suspendedNodes, err := e.state.ListSuspendedNodes(ctx, id)
	if err != nil {
		return fmt.Errorf("list suspended nodes for %q: %w", id, err)
	}
	for _, nodeName := range suspendedNodes {
		nodeIdx, ok := g.NodeIndex(nodeName)
		if !ok {
			return fmt.Errorf("suspended node %q is not in execution graph", nodeName)
		}
		// Prefer a backend fenced compare-and-set: cancel the node ONLY IF it is
		// still Suspended, atomically. A concurrent resume that already moved the
		// node to Running keeps its live lease (canceled=false is a benign skip).
		// This closes the read-then-write TOCTOU window the fallback below cannot.
		if canceler, ok := e.state.(SuspendedNodeCanceler); ok {
			if _, err := canceler.CancelSuspendedNode(ctx, id, nodeName); err != nil {
				return fmt.Errorf("cancel suspended node %q/%q: %w", id, nodeName, err)
			}
			continue
		}
		// Fallback (backends without a fenced cancel primitive): re-read the
		// current snapshot to avoid overwriting a concurrent resume that already
		// transitioned the node out of Suspended. This minimizes but does not
		// close the TOCTOU window between GetNode and UpsertNode.
		current, err := e.state.GetNode(ctx, id, nodeName)
		if err != nil {
			return fmt.Errorf("read node %q/%q for cancel: %w", id, nodeName, err)
		}
		// If the node exists and is no longer Suspended (e.g. a concurrent
		// resume moved it to Running), skip the cancel to avoid clobbering.
		if current != nil && current.Status != types.NodeStatusSuspended {
			continue
		}
		cancelSnap := &NodeSnapshot{
			ExecutionID: id,
			Name:        nodeName,
			NodeIdx:     nodeIdx,
			Status:      types.NodeStatusCanceled,
		}
		if current != nil {
			// Preserve existing fields from the snapshot so we don't zero out
			// lease/activation/output metadata.
			cancelSnap.LeaseID = current.LeaseID
			cancelSnap.LeaseToken = current.LeaseToken
			cancelSnap.Attempt = current.Attempt
			cancelSnap.ActivationID = current.ActivationID
			cancelSnap.AutoDepth = current.AutoDepth
			cancelSnap.Output = current.Output
			cancelSnap.PrivateOutput = current.PrivateOutput
			cancelSnap.Port = current.Port
			cancelSnap.Error = current.Error
		}
		if err := e.state.UpsertNode(ctx, cancelSnap); err != nil {
			return fmt.Errorf("mark suspended node %q/%q canceled: %w", id, nodeName, err)
		}
	}
	return nil
}
