package engine

import (
	"context"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// OnNodeComplete is called after a node finishes (success, error-routed, or continued).
// It routes cyclic completion: acyclic graphs never reach here because commitLegacyNode
// redirects acyclic commits to the atomic CommitNode/AdvanceNode path, which owns its
// own in-degree decrement and downstream enqueue.
func (e *Engine) OnNodeComplete(ctx context.Context, id types.ExecutionID, g *graph.Graph, completedIdx int, activePort string) error {
	return e.onCyclicNodeComplete(ctx, id, g, completedIdx, activePort)
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

func (e *Engine) completeExecution(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	if err := e.state.UpdateExecutionStatus(ctx, id, status, errMsg); err != nil {
		return err
	}
	// The status write is cancel-aware (CAS-fenced in durable backends): if a
	// concurrent Cancel already drove the execution to canceling/canceled, our
	// success/failed write was skipped. In that case do not emit a completion
	// notification or evict — Cancel owns the terminal transition.
	if e.isCancelingOrCanceled(ctx, id) {
		return nil
	}
	e.notifyExecutionComplete(ctx, id, status)
	e.EvictExecution(id)
	return nil
}

// isCancelingOrCanceled reports whether the execution is in (or past) the
// cancellation path. Used to decide whether completeExecution should yield the
// terminal transition to an in-flight Cancel.
func (e *Engine) isCancelingOrCanceled(ctx context.Context, id types.ExecutionID) bool {
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return false
	}
	return snap.Status == types.ExecutionStatusCanceling || snap.Status == types.ExecutionStatusCanceled
}
