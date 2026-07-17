package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// cyclicPlan is the deterministic downstream decision for one completed cyclic
// node. Exactly one branch holds: either the active output port fans out to
// downstream tasks (entries non-empty, persisted as durable outbox intents in
// the same fenced commit), or the branch terminates / exceeds MaxAutoDepth and
// the execution is finalized atomically (complete true).
type cyclicPlan struct {
	entries     []OutboxEntry
	complete    bool
	finalStatus types.ExecutionStatus
	finalError  string
}

// planCyclicDownstream computes the downstream scheduling intent for a cyclic
// node that completed on activePort. It reads only the immutable graph and the
// completed node's lease-carried activation/depth, so the result is fully
// deterministic and safe to recompute identically on a fenced-commit retry —
// which is what lets the intent be persisted atomically with the terminal
// commit and replayed from the outbox after a crash (#7).
func (e *Engine) planCyclicDownstream(g *graph.Graph, task *Task, activePort string) cyclicPlan {
	outEdges := g.OutEdges[task.NodeIdx]
	if len(outEdges) == 0 {
		return cyclicPlan{complete: true, finalStatus: types.ExecutionStatusSuccess}
	}

	nextActivationID := task.ActivationID + 1
	nextDepth := task.AutoDepth + 1
	var entries []OutboxEntry
	seen := make(map[string]struct{})
	for _, edge := range outEdges {
		if edge.SrcPort != activePort {
			continue
		}
		if nextDepth > g.MaxAutoDepth {
			return cyclicPlan{complete: true, finalStatus: types.ExecutionStatusFailed, finalError: "max auto execution depth exceeded"}
		}
		dstMeta := g.Nodes[edge.DstIdx]
		id := cyclicOutboxID(task.ExecutionID, dstMeta.Name, nextActivationID)
		if _, dup := seen[id]; dup {
			// Parallel edges to the same destination in the same activation
			// collapse to a single deterministic delivery intent.
			continue
		}
		seen[id] = struct{}{}
		entries = append(entries, OutboxEntry{
			ID: id,
			Task: Task{
				ExecutionID:  task.ExecutionID,
				NodeName:     dstMeta.Name,
				NodeIdx:      edge.DstIdx,
				Type:         TaskTypeNodeExec,
				AutoDepth:    nextDepth,
				ActivationID: nextActivationID,
			},
		})
	}
	if len(entries) == 0 {
		// No edge on the active port → this branch terminates the execution.
		return cyclicPlan{complete: true, finalStatus: types.ExecutionStatusSuccess}
	}
	return cyclicPlan{entries: entries}
}

// cyclicOutboxID is the deterministic durable delivery-intent key for one
// cyclic downstream activation. Keying by destination + next activation makes
// replay after a crash idempotent (HSETNX in the backend dedupes).
func cyclicOutboxID(id types.ExecutionID, dstName string, activationID int) string {
	return fmt.Sprintf("cyclic/%s/%s/%d", id, dstName, activationID)
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
