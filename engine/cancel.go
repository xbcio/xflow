package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()
	if g == nil {
		var err error
		g, err = e.state.LoadGraph(ctx, id)
		if err != nil {
			return fmt.Errorf("load graph for canceled execution %q: %w", id, err)
		}
		if g == nil {
			return ErrExecutionInactive
		}
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
