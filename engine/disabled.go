package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// handleDisabledNode serves a TaskTypeNodeExec task for a definition-disabled
// node instead of leasing it (DSL-SPECIFICATION §3.1): the engine commits the
// node as skipped on the "main" port and advances downstream in the same fenced
// transition, exactly like the pin hook but without an output.
//
// The "main" port is what separates this from the skip cascade
// (TaskTypeNodeSkip): the cascade commits with no active port so that every
// downstream node is skipped too, while a disabled node's downstream is a
// normal dependency -- it still runs, and reads nil for the disabled node.
//
// Unlike pin_data there is no per-execution gate: disabled is unconditional.
// The flag is assigned at compile time (assignDisabledNodes) and every shape
// this hook cannot intercept was rejected there, so a task that reaches this
// point for a disabled node is always interceptable.
//
// It returns handled=false for every task it does not own, so the caller falls
// through to the pin hook and then to the ordinary lease path. The graph read
// is cache-first: a graph without disabled nodes -- every graph but the ones
// that use the flag -- costs one bool read here.
func (e *Engine) handleDisabledNode(ctx context.Context, task *Task, flush bool) (bool, error) {
	g, err := e.loadGraph(ctx, task.ExecutionID)
	if err != nil || g == nil || !g.HasDisabledNodes() {
		// A load error or a missing graph is the lease path's to classify.
		return false, nil
	}
	if g.AllowCycles() || task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
		return false, nil
	}
	if !g.NodeDisabled(task.NodeIdx) || g.NodeAt(task.NodeIdx).Name != task.NodeName {
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
		Status:        types.NodeStatusSkipped,
		PrivateOutput: privateOutputForTask(g, task),
		Port:          port,
		System:        true,
		AdvanceTask:   advance,
	}
	result, err := e.commitNode(ctx, req)
	if err != nil {
		return true, fmt.Errorf("commit disabled node %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	return true, e.afterAtomicCommitWithFlush(ctx, req, result, flush)
}
