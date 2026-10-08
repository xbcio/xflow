package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// handlePinnedNode serves a TaskTypeNodeExec task from the workflow's pin_data
// instead of leasing it (DSL-SPECIFICATION §7.4): the engine commits the pinned
// mock output with status pinned on the "main" port and advances downstream in
// the same fenced transition, exactly like the skip cascade but carrying data.
// No lease is issued and no handler or runner ever sees the task.
//
// It returns handled=false for every task it does not pin, so the caller falls
// through to the ordinary lease path. The graph read is cache-first and skips
// the activeness probe: an unpinned graph -- every graph without pin_data --
// costs one map lookup here, and the probe the lease path is about to make is
// not duplicated. Only a pinned candidate pays for loadActiveGraph and, under
// test_only, one snapshot read for ExecutionSnapshot.TestRun.
//
// The decision is a pure function of the compiled graph and the persisted
// snapshot, so a redelivered task decides the same way: a second delivery
// lands on the terminal pinned node and resolves as a duplicate.
func (e *Engine) handlePinnedNode(ctx context.Context, task *Task, flush bool) (bool, error) {
	g, err := e.loadGraph(ctx, task.ExecutionID)
	if err != nil || g == nil || g.PinDataMode() == "" {
		// A load error or a missing graph is the lease path's to classify.
		return false, nil
	}
	if g.AllowCycles() || task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
		return false, nil
	}
	mock, ok := g.PinnedOutput(task.NodeIdx)
	if !ok || g.NodeAt(task.NodeIdx).Name != task.NodeName {
		return false, nil
	}
	// The unit is derived from the graph rather than trusted from the task: the
	// Invoke entry task is built without a UnitIdx, and the backend resolves the
	// scheduling-marker fence by this value (see CommitNodeRequest.UnitIdx).
	unitIdx := g.UnitIndexForNode(task.NodeIdx)
	if unitIdx < 0 || g.UnitKindAt(unitIdx) != graph.UnitNode {
		return false, nil
	}
	if _, active, err := e.loadActiveGraph(ctx, task.ExecutionID); err != nil || !active {
		return false, nil
	}
	if apply, err := e.pinAppliesTo(ctx, g, task.ExecutionID); err != nil {
		return true, err
	} else if !apply {
		return false, nil
	}

	port := "main"
	advance := &Task{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		UnitIdx:      unitIdx,
		Type:         TaskTypeNodeAdvance,
		ActivationID: task.ActivationID,
		AutoDepth:    task.AutoDepth,
		Port:         &port,
	}
	req := CommitNodeRequest{
		ExecutionID:   task.ExecutionID,
		NodeName:      task.NodeName,
		NodeIdx:       task.NodeIdx,
		UnitIdx:       unitIdx,
		ActivationID:  task.ActivationID,
		AutoDepth:     task.AutoDepth,
		Status:        types.NodeStatusPinned,
		Output:        mock,
		StoreOutput:   true,
		PrivateOutput: privateOutputForTask(g, task),
		Port:          port,
		System:        true,
		AdvanceTask:   advance,
	}
	result, err := e.commitNode(ctx, req)
	if err != nil {
		return true, fmt.Errorf("commit pinned node %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	return true, e.afterAtomicCommitWithFlush(ctx, req, result, flush)
}

// pinAppliesTo evaluates pin_data_mode for one execution: always applies to
// every execution, test_only only to one persisted as a test run.
func (e *Engine) pinAppliesTo(ctx context.Context, g *graph.Graph, id types.ExecutionID) (bool, error) {
	switch g.PinDataMode() {
	case types.PinDataModeAlways:
		return true, nil
	case types.PinDataModeTestOnly:
		snap, err := e.state.GetExecution(ctx, id)
		if err != nil {
			return false, fmt.Errorf("read execution %q for pin_data_mode: %w", id, err)
		}
		return snap != nil && snap.TestRun, nil
	default:
		return false, nil
	}
}
