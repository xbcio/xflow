package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// isLoopSplitOutput detects if a node output signals loop/split expansion.
func isLoopSplitOutput(data map[string]any) bool {
	if data == nil {
		return false
	}
	if _, ok := data["_loop"]; ok {
		return true
	}
	if _, ok := data["_split"]; ok {
		return true
	}
	return false
}

// expandLoopSplit handles the sub-execution expansion for loop/split nodes.
// It creates a child execution per batch and suspends the parent node until all complete.
func (e *Engine) expandLoopSplit(ctx context.Context, t *Task, g *graph.Graph, data map[string]any) error {
	batches, ok := data["batches"].([][]any)
	if !ok || len(batches) == 0 {
		// No items to iterate — complete immediately with empty results.
		return e.completeLoopSplit(ctx, t, g, []map[string]any{})
	}

	// Mark parent node as "waiting" (sub-executions in progress).
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "waiting",
	})

	for i, batch := range batches {
		childID := types.ExecutionID(fmt.Sprintf("%s/sub/%s/%d", t.ExecutionID, t.NodeName, i))

		sub := &SubExecution{
			ParentExecID: t.ExecutionID,
			ParentNode:   t.NodeName,
			ChildExecID:  childID,
			BatchIndex:   i,
			Status:       types.StatusRunning,
		}
		if err := e.state.CreateSubExecution(ctx, sub); err != nil {
			return fmt.Errorf("create sub-execution %d: %w", i, err)
		}

		// Enqueue a task that executes the batch items inline.
		// For now, each batch result is the batch itself (pass-through).
		// A full implementation would compile and run a body sub-graph.
		batchTask := &Task{
			ExecutionID: t.ExecutionID,
			NodeName:    fmt.Sprintf("%s/_batch/%d", t.NodeName, i),
			NodeIdx:     t.NodeIdx,
			Type:        TaskTypeNodeExec,
			Payload: &node.SignalPayload{
				Data: map[string]any{
					"_batch_exec":    true,
					"parent_exec_id": string(t.ExecutionID),
					"parent_node":    t.NodeName,
					"child_exec_id":  string(childID),
					"batch_index":    i,
					"items":          batch,
				},
			},
		}
		if err := e.queue.Enqueue(ctx, batchTask); err != nil {
			return fmt.Errorf("enqueue batch %d: %w", i, err)
		}
	}

	return nil
}

// ExecuteBatch processes a single batch of a loop/split expansion.
// Called by the queue consumer when it receives a batch task.
func (e *Engine) ExecuteBatch(ctx context.Context, t *Task) error {
	if t.Payload == nil || t.Payload.Data == nil {
		return fmt.Errorf("batch task missing payload")
	}

	data := t.Payload.Data
	parentExecID := types.ExecutionID(data["parent_exec_id"].(string))
	parentNode := data["parent_node"].(string)
	childExecID := types.ExecutionID(data["child_exec_id"].(string))
	items, _ := data["items"].([]any)

	// Execute: for now, pass-through the batch items as result.
	// TODO: when body sub-graph is supported, compile and run it here.
	result := map[string]any{
		"items": items,
		"count": len(items),
	}

	allDone, err := e.state.CompleteSubExecution(ctx, parentExecID, parentNode, childExecID, types.StatusSuccess, result)
	if err != nil {
		return err
	}

	if allDone {
		// All batches complete — finalize the parent loop/split node.
		e.mu.RLock()
		g := e.graphs[parentExecID]
		e.mu.RUnlock()
		if g == nil {
			g, _ = e.state.LoadGraph(ctx, parentExecID)
		}
		if g == nil {
			return nil
		}

		nodeIdx := g.Index[parentNode]
		parentTask := &Task{
			ExecutionID: parentExecID,
			NodeName:    parentNode,
			NodeIdx:     nodeIdx,
		}

		results, err := e.state.GetSubExecutionResults(ctx, parentExecID, parentNode)
		if err != nil {
			return err
		}

		return e.completeLoopSplit(ctx, parentTask, g, results)
	}

	return nil
}

// completeLoopSplit finalizes a loop/split node after all sub-executions complete.
func (e *Engine) completeLoopSplit(ctx context.Context, t *Task, g *graph.Graph, results []map[string]any) error {
	output := map[string]any{
		"results": results,
		"count":   len(results),
	}

	_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, output)
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "success",
		Output:      output,
		Port:        "main",
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, "success")
	}

	return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, "main", output)
}

// newSubExecutionID generates a unique sub-execution ID.
func newSubExecutionID() types.ExecutionID {
	return types.ExecutionID("sub-" + uuid.New().String())
}
