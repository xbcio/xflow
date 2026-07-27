package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
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
			cancelSnap.Port = current.Port
			cancelSnap.Error = current.Error
		}
		if err := e.state.UpsertNode(ctx, cancelSnap); err != nil {
			return fmt.Errorf("mark suspended node %q/%q canceled: %w", id, nodeName, err)
		}
	}

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		return fmt.Errorf("mark execution %q canceled: %w", id, err)
	}
	e.notifyExecutionComplete(ctx, id, types.ExecutionStatusCanceled)
	e.EvictExecution(id)
	return nil
}
