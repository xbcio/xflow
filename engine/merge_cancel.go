package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// mergeCancelReasonPrefix is the Error text a cancel commit writes on the nodes
// a fired merge retires. The design keeps it a neutral cause, not a failure.
const mergeCancelReasonPrefix = "canceled by merge "

// MergeCancelObserver observes the nodes a fired merge's on_others: cancel
// scan could not retire — the residue the cancellation leaves behind.
//
// A cancel commit refused by the backend fence is NOT residue: a refusal means
// the node already committed naturally, was never scheduled (its ancestors in
// the closure will skip it once they resolve), or carries a skip marker (that
// cascade is already in flight). Those are the fence's three benign answers
// and they need no signal. Waiting nodes hanging on a sub-workflow or an
// expansion, and nodes under a group unit, are deliberately NOT attempted in
// v1 — canceling them would race the child completion callback or the group
// lease, and the coordinated teardown is a later design. They are the only
// cases where an execution can still hang after its merge fired, so they are
// reported here: this counter is the thing a host treating "no stuck
// executions" as a requirement can alert on.
//
// Implementations must be non-blocking and must not affect scheduling.
type MergeCancelObserver interface {
	// OnMergeCancelIncomplete reports one closure node the scan did not
	// attempt. node is that node's name; count is always 1 (one call per
	// unresolved node, so the label carries the node attribution an aggregate
	// cannot).
	OnMergeCancelIncomplete(ctx context.Context, node string, count int)
}

// WithMergeCancelObserver installs an observer for the nodes a merge-cancel
// scan leaves unresolved. A nil observer leaves the counting disabled.
func WithMergeCancelObserver(o MergeCancelObserver) Option {
	return func(e *Engine) {
		if o != nil {
			e.mergeCancelObserver = o
		}
	}
}

func (e *Engine) notifyMergeCancelIncomplete(ctx context.Context, node string, count int) {
	if count <= 0 || e.mergeCancelObserver == nil {
		return
	}
	safeHook(ctx, e.logger, func(observerCtx context.Context) {
		e.mergeCancelObserver.OnMergeCancelIncomplete(observerCtx, node, count)
	})
}

// handleMergeCancelTask consumes one durable mergecancel/ intent: a wait_any
// merge committed successfully with on_others: cancel, and the branches that
// have not reached it must now be retired. The intent is the whole trigger —
// the scan below is idempotent and replayable, so a redelivered intent is a
// no-op rather than a second cancellation.
//
// The per-node work is one fenced System commit with Status canceled, walked
// in the closure's topological order (ancestors first) so that every node's
// own scheduling question is already settled — by its canceled ancestor's
// folded skip or by its natural commit — before the scan reaches it. The
// backend's fence decides per node whether the cancellation applies; the
// engine deliberately does NOT pre-judge "is this node still cancellable",
// because a fence refusal is one of the benign answers, and a late natural
// commit racing the cancel loses cleanly to the same fence.
func (e *Engine) handleMergeCancelTask(ctx context.Context, task *Task, flush bool) (bool, error) {
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return true, err
	}
	if !active {
		// A merge intent arriving after the execution concluded (or was
		// canceled) is classified, not handled: the same contract as the skip
		// branch, so the caller can settle the intent as a benign drop
		// instead of dead-lettering it.
		return true, e.inactiveExecutionError(ctx, task.ExecutionID, task.provabilityAnchor())
	}
	if task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
		return true, fmt.Errorf("merge-cancel node index %d is out of range", task.NodeIdx)
	}
	mergeName := g.NodeAt(task.NodeIdx).Name
	closure := g.CancelClosure(task.NodeIdx)
	foldArrivals := e.supportsArrivalFolding()
	for _, nodeIdx := range closure {
		residue, err := e.cancelClosureNode(ctx, task, g, nodeIdx, mergeName, foldArrivals, flush)
		if err != nil {
			// Leave the intent unacknowledged: the outbox redelivery replays the
			// whole scan, and every node this pass settled is a terminal row the
			// next pass skips.
			return true, err
		}
		if residue {
			e.notifyMergeCancelIncomplete(ctx, g.NodeAt(nodeIdx).Name, 1)
		}
	}
	return true, nil
}

// cancelClosureNode drives one closure node through the cancel fence and
// reports whether the node is RESIDUE — a v1-excluded case the scan does not
// attempt (a group unit, or a Waiting row hanging on a sub-workflow or an
// expansion). Everything else is settled by this scan or by the fence: an
// accepted commit applied the cancellation; a refused one (StaleToken /
// DuplicateTerminal) means the node already needs no cancellation. A transient
// store error is returned to the caller so the intent's outbox redelivery
// replays the whole scan idempotently.
func (e *Engine) cancelClosureNode(ctx context.Context, task *Task, g *graph.Graph, nodeIdx int, mergeName string, foldArrivals, flush bool) (bool, error) {
	meta := g.NodeAt(nodeIdx)
	// Group units are v1-excluded: their members are scheduled under one group
	// lease the scan does not own, and UnitMergeMode reads them as wait_all.
	// Canceling the representative row would fight the group runtime.
	if g.UnitKindAt(g.UnitIndexForNode(nodeIdx)) == graph.UnitGroup {
		return true, nil
	}
	node, err := e.state.GetNode(ctx, task.ExecutionID, meta.Name)
	if err != nil {
		// Transient read error: the redelivery replays the scan. Not residue.
		if e.logger != nil {
			e.logger.Debug("merge-cancel node read errored; leaving it to outbox retry",
				"execution_id", string(task.ExecutionID), "node_name", meta.Name, "err", err)
		}
		return false, err
	}
	if node != nil && types.IsTerminalNodeStatus(node.Status) {
		return false, nil
	}
	if node != nil && node.Status == types.NodeStatusWaiting {
		// Waiting rows hang on a sub-workflow or an expansion; writing Canceled
		// under them would race the child's completion callback into a spurious
		// failed parent. v1 leaves them to the execution timeout and reports
		// them instead.
		return true, nil
	}
	req := CommitNodeRequest{
		ExecutionID: task.ExecutionID,
		NodeName:    meta.Name,
		NodeIdx:     nodeIdx,
		UnitIdx:     g.UnitIndexForNode(nodeIdx),
		// A row keeps its own activation, so the fence can reject a cancel
		// that lost an activation bump; a node with no row yet takes the
		// merge's.
		ActivationID: activationForCancelCommit(node, task.ActivationID),
		Status:       types.NodeStatusCanceled,
		System:       true,
		Error:        mergeCancelReasonPrefix + mergeName,
	}
	if foldArrivals {
		// The empty active port gives every out-edge ActiveCount 0, which is
		// exactly the skip cascade the cancellation wants downstream of the
		// retired node.
		req.Arrivals = downstreamArrivals(g, nodeIdx, "")
	}
	result, err := e.commitNode(ctx, req)
	if err != nil {
		if e.logger != nil {
			e.logger.Debug("merge-cancel commit errored; leaving it to outbox retry",
				"execution_id", string(task.ExecutionID), "node_name", meta.Name, "err", err)
		}
		return false, err
	}
	if result.Outcome != CommitOutcomeAccepted || !result.Applied {
		// Refused is the common benign answer: the node committed naturally,
		// was never scheduled, or is mid-skip. No log at Info and above — the
		// scan can refuse the majority of a wide closure on replay.
		if e.logger != nil {
			e.logger.Debug("merge-cancel commit refused; the node needs no cancellation",
				"execution_id", string(task.ExecutionID), "node_name", meta.Name,
				"outcome", string(result.Outcome))
		}
	}
	// The shared funnel fires OnNodeComplete for an applied cancel, notifies
	// waiters when this commit finalizes the execution, and flushes the
	// execution's outbox when the caller asked for it. flush is false on the
	// inline path, where the delivery loop re-polls the outbox after this
	// handled intent anyway.
	if err := e.afterAtomicCommitWithFlush(ctx, req, result, flush); err != nil {
		return false, err
	}
	// The folded arrivals this commit resolved downstream are skips and are
	// reported from this transition, exactly as the skip branch reports its
	// own. flowAdvance is the right label by that branch's precedent: the
	// decision means the same thing an advance would have decided — this
	// node's outbound routes carry no active port.
	if foldArrivals && result.Outcome == CommitOutcomeAccepted {
		e.notifySkip(ctx, flowAdvance, result.Skipped)
	}
	return false, nil
}

// activationForCancelCommit picks the ActivationID a cancel commit carries:
// the node row's own when one exists, otherwise the merge's, which any first
// row write in this activation satisfies.
func activationForCancelCommit(node *NodeSnapshot, mergeActivation int) int {
	if node != nil && node.ActivationID > 0 {
		return node.ActivationID
	}
	return mergeActivation
}
