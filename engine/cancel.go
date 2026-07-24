package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	// The graph cache is shared across tenants, so a cache hit alone is not
	// sufficient to authorize a cancel. loadActiveGraph confirms the execution
	// exists in the caller's tenant namespace via GetExecution; a cross-tenant
	// or inactive ID is reported as not-found so we do not leak existence or
	// trigger side effects against another tenant's execution.
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
		if err := e.state.UpsertNode(ctx, &NodeSnapshot{
			ExecutionID: id,
			Name:        nodeName,
			NodeIdx:     nodeIdx,
			Status:      types.NodeStatusCanceled,
		}); err != nil {
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
