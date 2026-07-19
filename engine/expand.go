package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
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

// expandLoopSplit starts one lease-fenced child generation. The parent stays
// waiting with its original lease metadata, so the normal sweeper can reclaim
// a crash between child creation, batch delivery, and parent finalization.
func (e *Engine) expandLoopSplit(ctx context.Context, lease *TaskLease, g *graph.Graph, data map[string]any) error {
	if lease == nil {
		return ErrInvalidLeaseToken
	}
	batches, err := loopSplitBatches(data)
	if err != nil {
		return fmt.Errorf("decode loop/split batches for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(batches) == 0 {
		return e.completeLoopSplit(ctx, lease, g, []map[string]any{})
	}

	expander, ok := e.state.(DurableLeaseExpander)
	if !ok {
		return ErrAtomicCommitUnsupported
	}
	children := make([]SubExecution, 0, len(batches))
	entries := make([]OutboxEntry, 0, len(batches))
	for i, batch := range batches {
		childID := expansionChildID(lease, i)
		children = append(children, SubExecution{
			ParentExecID: lease.Task.ExecutionID,
			ParentNode:   lease.Task.NodeName,
			ChildExecID:  childID,
			BatchIndex:   i,
			Status:       types.ExecutionStatusRunning,
		})
		entries = append(entries, OutboxEntry{
			ID:   expansionOutboxID(lease, i),
			Task: expansionBatchTask(lease, childID, i, batch),
		})
	}
	started, err := expander.BeginTaskExpansionWithOutbox(ctx, lease, children, entries)
	if err != nil {
		return fmt.Errorf("begin durable loop/split expansion %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if !started {
		return ErrInvalidLeaseToken
	}
	if err := e.FlushOutbox(ctx, lease.Task.ExecutionID); err != nil {
		return fmt.Errorf("deliver loop/split batches for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}

// loopSplitBatches accepts both native Go batches and the []any representation
// produced when a runner result is decoded from JSON.
func loopSplitBatches(data map[string]any) ([][]any, error) {
	raw, exists := data["batches"]
	if !exists || raw == nil {
		return nil, nil
	}
	if batches, ok := raw.([][]any); ok {
		return batches, nil
	}
	rows, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("batches has type %T, want array", raw)
	}
	batches := make([][]any, 0, len(rows))
	for index, row := range rows {
		if row == nil {
			batches = append(batches, nil)
			continue
		}
		items, ok := row.([]any)
		if !ok {
			return nil, fmt.Errorf("batch %d has type %T, want array", index, row)
		}
		batches = append(batches, items)
	}
	return batches, nil
}

func expansionBatchTask(lease *TaskLease, childID types.ExecutionID, batchIndex int, items []any) Task {
	return Task{
		ExecutionID: lease.Task.ExecutionID,
		NodeName:    fmt.Sprintf("%s/_batch/%d", lease.Task.NodeName, batchIndex),
		NodeIdx:     lease.Task.NodeIdx,
		Type:        TaskTypeNodeBatch,
		Payload: &types.SignalPayload{Data: map[string]any{
			"_batch_exec":          true,
			"parent_exec_id":       string(lease.Task.ExecutionID),
			"parent_node":          lease.Task.NodeName,
			"parent_node_idx":      lease.Task.NodeIdx,
			"parent_lease_id":      string(lease.LeaseID),
			"parent_lease_token":   string(lease.LeaseToken),
			"parent_attempt":       lease.Attempt,
			"parent_activation_id": lease.Task.ActivationID,
			"parent_auto_depth":    lease.Task.AutoDepth,
			"child_exec_id":        string(childID),
			"batch_index":          batchIndex,
			"items":                items,
		}},
	}
}

func expansionOutboxID(lease *TaskLease, batchIndex int) string {
	return fmt.Sprintf("expand/%s/%s/%s/%d", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID, batchIndex)
}

func expansionChildID(lease *TaskLease, batchIndex int) types.ExecutionID {
	return types.ExecutionID(fmt.Sprintf("%s/sub/%s/%s/%d", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID, batchIndex))
}

// ExecuteBatch processes a single batch of an experimental loop/split
// expansion. Its parent lease fence makes delayed or duplicate old batches
// harmless after recovery has issued a newer parent lease.
func (e *Engine) ExecuteBatch(ctx context.Context, t *Task) error {
	lease, childExecID, items, err := expansionBatchLease(t)
	if err != nil {
		return err
	}

	// EXPERIMENTAL: pass-through stub. When body sub-graph execution lands,
	// compile and run the body graph here. See .claude/specs/expand-gate.md.
	result := map[string]any{
		"items": items,
		"count": len(items),
	}

	expander, ok := e.state.(LeaseExpander)
	if !ok {
		return ErrAtomicCommitUnsupported
	}
	allDone, accepted, results, err := expander.CompleteExpandedSubExecution(ctx, lease, childExecID, types.ExecutionStatusSuccess, result)
	if err != nil {
		return err
	}
	if !accepted || !allDone {
		return nil
	}

	g, active, err := e.loadActiveGraph(ctx, lease.Task.ExecutionID)
	if err != nil {
		return fmt.Errorf("load graph for loop/split parent %q: %w", lease.Task.ExecutionID, err)
	}
	if !active {
		return ErrExecutionInactive
	}
	return e.completeLoopSplit(ctx, lease, g, results)
}

func expansionBatchLease(t *Task) (*TaskLease, types.ExecutionID, []any, error) {
	if t == nil || t.Payload == nil || t.Payload.Data == nil {
		return nil, "", nil, fmt.Errorf("batch task missing payload")
	}
	data := t.Payload.Data
	stringField := func(name string) (string, error) {
		value, ok := data[name].(string)
		if !ok || value == "" {
			return "", fmt.Errorf("batch task has invalid %s", name)
		}
		return value, nil
	}
	intField := func(name string) (int, error) {
		switch value := data[name].(type) {
		case int:
			return value, nil
		case int64:
			return int(value), nil
		case float64:
			return int(value), nil
		default:
			return 0, fmt.Errorf("batch task has invalid %s", name)
		}
	}

	parentExecID, err := stringField("parent_exec_id")
	if err != nil {
		return nil, "", nil, err
	}
	parentNode, err := stringField("parent_node")
	if err != nil {
		return nil, "", nil, err
	}
	childID, err := stringField("child_exec_id")
	if err != nil {
		return nil, "", nil, err
	}
	leaseID, err := stringField("parent_lease_id")
	if err != nil {
		return nil, "", nil, err
	}
	leaseToken, err := stringField("parent_lease_token")
	if err != nil {
		return nil, "", nil, err
	}
	nodeIdx, err := intField("parent_node_idx")
	if err != nil {
		return nil, "", nil, err
	}
	attempt, err := intField("parent_attempt")
	if err != nil {
		return nil, "", nil, err
	}
	activationID, err := intField("parent_activation_id")
	if err != nil {
		return nil, "", nil, err
	}
	autoDepth, err := intField("parent_auto_depth")
	if err != nil {
		return nil, "", nil, err
	}
	items, _ := data["items"].([]any)
	return &TaskLease{
		LeaseID:    LeaseID(leaseID),
		LeaseToken: LeaseToken(leaseToken),
		Attempt:    attempt,
		Task: Task{
			ExecutionID:  types.ExecutionID(parentExecID),
			NodeName:     parentNode,
			NodeIdx:      nodeIdx,
			Type:         TaskTypeNodeBatch,
			ActivationID: activationID,
			AutoDepth:    autoDepth,
		},
	}, types.ExecutionID(childID), items, nil
}

// completeLoopSplit terminalizes a fully completed child generation through
// the same token-fenced commit path as other node results. In an acyclic graph
// that also writes the durable downstream advance intent.
func (e *Engine) completeLoopSplit(ctx context.Context, lease *TaskLease, g *graph.Graph, results []map[string]any) error {
	output := map[string]any{
		"results": results,
		"count":   len(results),
	}
	outcome, err := e.commitLegacyNode(ctx, lease, types.NodeStatusSuccess, output, "main", "", false)
	if outcome == CommitOutcomeStaleToken || outcome == CommitOutcomeDuplicateTerminal || outcome == CommitOutcomeExecutionInactive {
		return nil
	}
	if err != nil {
		return fmt.Errorf("finalize loop/split node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}
