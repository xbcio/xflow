package engine

import (
	"context"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// OnNodeComplete is called after a node finishes (success, error-routed, or continued).
// It decrements in-degrees for all downstream nodes and either enqueues ready nodes,
// cascades skips for nodes that received no active input, or checks for execution completion.
func (e *Engine) OnNodeComplete(ctx context.Context, id types.ExecutionID, g *graph.Graph, completedIdx int, activePort string, output map[string]any) error {
	if g.AllowCycles {
		return e.onCyclicNodeComplete(ctx, id, g, completedIdx, activePort)
	}

	outEdges := g.OutEdges[completedIdx]
	if len(outEdges) == 0 {
		// Leaf node — check if the whole execution is done.
		return e.tryComplete(ctx, id, g)
	}

	anyEnqueued := false
	for _, edge := range outEdges {
		portActive := edge.SrcPort == activePort
		remaining, arrivedActive, err := e.state.DecrementInDegree(ctx, id, edge.DstIdx, portActive)
		if err != nil {
			return err
		}

		dstMeta := g.Nodes[edge.DstIdx]

		// wait_any merge: trigger as soon as the first active input arrives.
		if dstMeta.MergeMode == "wait_any" && portActive && arrivedActive == 1 {
			task := &Task{
				ExecutionID: id,
				NodeName:    dstMeta.Name,
				NodeIdx:     edge.DstIdx,
				Type:        TaskTypeNodeExec,
			}
			if err := e.queue.Enqueue(ctx, task); err != nil {
				return err
			}
			anyEnqueued = true
			continue
		}

		if remaining > 0 {
			// Still waiting for other upstream nodes.
			continue
		}

		// All upstream edges have arrived for this node.
		if arrivedActive > 0 {
			// At least one active (non-skipped) input arrived — execute the node.
			task := &Task{
				ExecutionID: id,
				NodeName:    dstMeta.Name,
				NodeIdx:     edge.DstIdx,
				Type:        TaskTypeNodeExec,
			}
			if err := e.queue.Enqueue(ctx, task); err != nil {
				return err
			}
			anyEnqueued = true
		} else {
			// All inputs were skipped — cascade the skip.
			if err := e.skipCascade(ctx, id, g, edge.DstIdx); err != nil {
				return err
			}
		}
	}

	if !anyEnqueued {
		return e.tryComplete(ctx, id, g)
	}
	return nil
}

func (e *Engine) onCyclicNodeComplete(ctx context.Context, id types.ExecutionID, g *graph.Graph, completedIdx int, activePort string) error {
	outEdges := g.OutEdges[completedIdx]
	if len(outEdges) == 0 {
		return e.completeExecution(ctx, id, types.ExecutionStatusSuccess, "")
	}

	completed := g.Nodes[completedIdx]
	ns, err := e.state.GetNode(ctx, id, completed.Name)
	if err != nil {
		return err
	}
	activationID := 0
	autoDepth := 0
	if ns != nil {
		activationID = ns.ActivationID
		autoDepth = ns.AutoDepth
	}
	nextActivationID := activationID + 1

	anyEnqueued := false
	for _, edge := range outEdges {
		if edge.SrcPort != activePort {
			continue
		}
		nextDepth := autoDepth + 1
		if nextDepth > g.MaxAutoDepth {
			return e.completeExecution(ctx, id, types.ExecutionStatusFailed, "max auto execution depth exceeded")
		}
		dstMeta := g.Nodes[edge.DstIdx]
		task := &Task{
			ExecutionID:  id,
			NodeName:     dstMeta.Name,
			NodeIdx:      edge.DstIdx,
			Type:         TaskTypeNodeExec,
			AutoDepth:    nextDepth,
			ActivationID: nextActivationID,
		}
		if err := e.queue.Enqueue(ctx, task); err != nil {
			return err
		}
		anyEnqueued = true
	}

	if !anyEnqueued {
		return e.completeExecution(ctx, id, types.ExecutionStatusSuccess, "")
	}
	return nil
}

// skipCascade marks a node as skipped and propagates the skip to its descendants.
func (e *Engine) skipCascade(ctx context.Context, id types.ExecutionID, g *graph.Graph, nodeIdx int) error {
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id,
		Name:        g.Nodes[nodeIdx].Name,
		NodeIdx:     nodeIdx,
		Status:      types.NodeStatusSkipped,
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, id, g.Nodes[nodeIdx].Name, types.NodeStatusSkipped)
	}

	for _, edge := range g.OutEdges[nodeIdx] {
		remaining, arrivedActive, err := e.state.DecrementInDegree(ctx, id, edge.DstIdx, false)
		if err != nil {
			return err
		}
		if remaining > 0 {
			continue
		}
		if arrivedActive > 0 {
			task := &Task{
				ExecutionID: id,
				NodeName:    g.Nodes[edge.DstIdx].Name,
				NodeIdx:     edge.DstIdx,
				Type:        TaskTypeNodeExec,
			}
			if err := e.queue.Enqueue(ctx, task); err != nil {
				return err
			}
		} else {
			if err := e.skipCascade(ctx, id, g, edge.DstIdx); err != nil {
				return err
			}
		}
	}
	return nil
}

// tryComplete checks whether all nodes have reached a terminal state and,
// if so, marks the execution as success or failed.
func (e *Engine) tryComplete(ctx context.Context, id types.ExecutionID, g *graph.Graph) error {
	allDone, hasFailed, err := e.state.CheckCompletion(ctx, id, len(g.Nodes))
	if err != nil {
		return err
	}
	if !allDone {
		return nil
	}

	status := types.ExecutionStatusSuccess
	errMsg := ""
	if hasFailed {
		status = types.ExecutionStatusFailed
		errMsg = "one or more nodes failed"
	}

	return e.completeExecution(ctx, id, status, errMsg)
}

func (e *Engine) completeExecution(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	_ = e.state.UpdateExecutionStatus(ctx, id, status, errMsg)
	if e.hooks != nil {
		e.hooks.OnExecutionComplete(ctx, id, status)
	}
	e.mu.Lock()
	delete(e.graphs, id)
	e.mu.Unlock()
	return nil
}
