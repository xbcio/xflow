package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// subWorkflowConclusion translates a concluded child execution into the parent
// node's verdict.
//
// The mapping is deliberately total over the terminal statuses and partial
// elsewhere: ok=false hands the caller "not concluded yet" (running, queued,
// paused, canceling) so the wait stands and nothing is committed. A canceled
// child fails the node: from this node's point of view a child that was killed
// did not deliver its result, and the way to recover is the node's own retry or
// on_error — unless the CANCEL came from the parent's own cancel cascade, which
// the completion is skipped for before it ever reaches this mapping.
func subWorkflowConclusion(child *ExecutionSnapshot) (types.NodeStatus, string, bool) {
	switch child.Status {
	case types.ExecutionStatusSuccess:
		return types.NodeStatusSuccess, "", true
	case types.ExecutionStatusFailed:
		return types.NodeStatusFailed, childExecutionReason(child, "failed"), true
	case types.ExecutionStatusTimeout:
		return types.NodeStatusFailed, childExecutionReason(child, "timed out"), true
	case types.ExecutionStatusCanceled:
		return types.NodeStatusFailed, childExecutionReason(child, "was canceled"), true
	}
	return "", "", false
}

// childExecutionReason renders the node failure message: the child's terminal
// state and its identity, so an operator reading the parent's node error can
// tell which execution to go look at.
func childExecutionReason(child *ExecutionSnapshot, outcome string) string {
	reason := fmt.Sprintf("child execution %q %s", child.ID, outcome)
	if child.Error != "" {
		reason += ": " + child.Error
	}
	return reason
}

// completeSubWorkflowNode concludes a parked parent node from the child the
// notifier names.
//
// childID is the notifier's claim about which child concluded, and it is
// checked against the link rather than trusted. After a retry the node waits on
// a LATER generation, and a late notification for the generation that failed
// must be a no-op: concluding from it would commit the parent to an answer its
// current wait does not own. The commit fence alone would not catch that — the
// re-parked link carries a newer lease, so the stale commit WOULD be refused,
// but by then the node's output would have been built from the wrong child, and
// a refusal is only correct here because the comparison dropped it first.
//
// The handled return reports whether the notification was recognized as THIS
// wait's and handed to the commit fence (which may still drop it as stale).
// false is not a failure: the child may not have concluded yet, the wait may
// belong to another generation, or the parent may already be canceling. Every
// one of those is a delivery with nothing left for this path to do, and none of
// them needs a redelivery — the wait's lease and the reclaim redelivery are the
// backstop for anything that does.
func (e *Engine) completeSubWorkflowNode(ctx context.Context, parentID types.ExecutionID, nodeName string, childID types.ExecutionID) (bool, error) {
	g, active, err := e.loadActiveGraph(ctx, parentID)
	if err != nil {
		return false, err
	}
	if !active {
		return false, nil
	}
	snap, err := e.state.GetExecution(ctx, parentID)
	if err != nil {
		return false, fmt.Errorf("read parent execution %q: %w", parentID, err)
	}
	if snap == nil || snap.Status == types.ExecutionStatusCanceling {
		// Canceling: the cascade owns the node now, and a completion racing it
		// must not decide the node's outcome from a child the cancel is about to
		// kill. The cancel path finalizes the node itself
		// (finalizeParkedSubWorkflowNodes).
		return false, nil
	}
	idx, ok := g.NodeIndex(nodeName)
	if !ok {
		// A notification for a node this execution's graph does not have is a
		// leftover of a superseded definition; there is no node to conclude.
		return false, nil
	}
	store, ok := e.state.(SubWorkflowLinkStore)
	if !ok {
		// Unreachable: the plan checked the capability before the child existed.
		return false, fmt.Errorf("state store does not implement SubWorkflowLinkStore: %w", ErrSubWorkflowUnsupported)
	}
	link, found, err := store.GetSubWorkflowLink(ctx, parentID, nodeName)
	if err != nil {
		return false, fmt.Errorf("read sub-workflow link %q/%q: %w", parentID, nodeName, err)
	}
	if !found || link.ChildExecutionID != childID {
		return false, nil
	}
	child, err := e.state.GetExecution(ctx, childID)
	if err != nil {
		return false, fmt.Errorf("read child execution %q: %w", childID, err)
	}
	if child == nil {
		// The child's records are gone — retention expired them between its
		// terminal transition and this delivery. Nothing can be concluded from
		// a result that no longer exists; the wait's lease still stands, and its
		// reclaim redelivery re-creates the child under the same derived id
		// rather than minting a second one.
		return false, nil
	}
	status, reason, concluded := subWorkflowConclusion(child)
	if !concluded {
		return false, nil
	}
	lease := subWorkflowLinkLease(parentID, nodeName, idx, g, link)
	return true, e.commitSubWorkflowConclusion(ctx, lease, g, g.NodeAt(idx), childID, status, reason)
}

// concludeTerminalChild is the reclaim side of the completion: a redelivery
// that finds its child already concluded commits the node under the CURRENT
// claim instead of parking again. This is the backstop a lost completion
// notification falls back to (after the wait lease expires and the sweeper
// redelivers the task), and it is why a lost notification costs latency rather
// than correctness.
//
// It commits under the fresh claim rather than the link's identity because the
// link — if one exists at all — was written by the claim the sweeper just
// revoked, and a commit fenced on it would be refused forever, livelocking the
// node between reclaims.
func (e *Engine) concludeTerminalChild(ctx context.Context, lease *TaskLease, g *graph.Graph, meta graph.NodeMeta, child subWorkflowChild) (bool, error) {
	snap, err := e.state.GetExecution(ctx, child.ID)
	if err != nil {
		return false, fmt.Errorf("read child execution %q: %w", child.ID, err)
	}
	if snap == nil {
		return false, nil
	}
	status, reason, concluded := subWorkflowConclusion(snap)
	if !concluded {
		return false, nil
	}
	return true, e.commitSubWorkflowConclusion(ctx, lease, g, meta, child.ID, status, reason)
}

// commitSubWorkflowConclusion applies the child's verdict to the parent node
// through the same pipeline any node result takes: success commits the mapped
// output and advances along main, failure takes the retry → on_error →
// classified-failure route, where a retry mints the next child generation
// (engine/retry.go) and the commit fence decides whether this completion is
// still the node's to apply.
//
// The committers are the loop/split pair (commitLegacyNode /
// commitLegacyNodeWithClassification), not the acyclic-only ones: a parent graph
// may be cyclic, and those helpers are the ones that redirect by graph shape
// instead of assuming it.
func (e *Engine) commitSubWorkflowConclusion(ctx context.Context, lease *TaskLease, g *graph.Graph, meta graph.NodeMeta, childID types.ExecutionID, status types.NodeStatus, reason string) error {
	privateOutput := privateOutputForTask(g, &lease.Task)
	if status == types.NodeStatusSuccess {
		output, err := e.subWorkflowNodeOutput(ctx, meta, childID)
		if err != nil {
			// A declared output map that cannot be evaluated is this node's
			// failure, not a crash: routing it through the failure pipeline
			// keeps on_error and retry (a fresh child, whose result may map
			// differently) available to the author.
			status, reason = types.NodeStatusFailed, err.Error()
		} else {
			outcome, err := e.commitLegacyNode(ctx, lease, privateOutput, types.NodeStatusSuccess, output, "main", "", false)
			if subWorkflowCommitDropped(outcome) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("conclude sub-workflow node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
			}
			return nil
		}
	}

	outcome, err := e.commitNodeErrorOutcome(ctx, lease, meta, privateOutput, errors.New(reason), nil, nil,
		e.commitLegacyNodeWithClassification)
	if subWorkflowCommitDropped(outcome) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("conclude failed sub-workflow node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}

// subWorkflowCommitDropped reports whether a commit outcome means another owner
// already decided this node: a revoked lease (the sweeper took it), a duplicate
// of a terminal transition, or an execution that is gone. Every one of those is
// a benign end for the delivery that produced it.
func subWorkflowCommitDropped(outcome CommitOutcome) bool {
	return outcome == CommitOutcomeStaleToken || outcome == CommitOutcomeDuplicateTerminal || outcome == CommitOutcomeExecutionInactive
}

// subWorkflowLinkLease rebuilds the claim a parked node committed under, from
// the link that recorded it. The completion path must commit with exactly this
// identity: it is what makes a notification delivered after a reclaim a
// stale-token refusal rather than a second commit.
//
// The generation is restored too, and it is load-bearing rather than
// decorative: a retry scheduled from here advances generation so the retry
// starts a NEW child, and a lease whose generation read zero would always
// schedule generation 1 — sending the retry back to the child that just failed.
func subWorkflowLinkLease(parentID types.ExecutionID, nodeName string, nodeIdx int, g *graph.Graph, link SubWorkflowLink) *TaskLease {
	return &TaskLease{
		LeaseID:    link.LeaseID,
		LeaseToken: link.LeaseToken,
		Attempt:    link.Attempt,
		Task: Task{
			ExecutionID:  parentID,
			NodeName:     nodeName,
			NodeIdx:      nodeIdx,
			UnitIdx:      g.UnitIndexForNode(nodeIdx),
			Type:         TaskTypeSubWorkflow,
			ActivationID: link.ActivationID,
			Generation:   link.Generation,
		},
	}
}

// subWorkflowNodeOutput renders the parent node's output from the child
// execution.
//
// The default is the child's public node-output map — node name to output, the
// same shape the SDK reports as Result.Output — built through Inspect so the
// privacy projection is the one the audit surface applies (a private node's
// output is withheld rather than leaked through the parent). A declared
// `output:` map replaces it, evaluated with $child as its only root: the
// mapping is a pure function of the child's public result, and giving it a
// wider environment would make the same expression mean different things
// depending on where in the parent the node happened to sit.
func (e *Engine) subWorkflowNodeOutput(ctx context.Context, meta graph.NodeMeta, childID types.ExecutionID) (map[string]any, error) {
	detail, err := e.Inspect(ctx, childID)
	if err != nil {
		return nil, fmt.Errorf("read child execution result %q: %w", childID, err)
	}
	child := make(map[string]any, len(detail.Nodes))
	for _, node := range detail.Nodes {
		if node.Output != nil {
			child[node.Name] = node.Output
		}
	}
	if len(child) == 0 {
		// Absent rather than empty, matching the SDK's result shape: a child
		// whose nodes produced nothing has no output map, not an empty one.
		child = nil
	}
	raw, present := meta.Parameters["output"]
	if !present || raw == nil {
		return child, nil
	}
	evaluated, err := subWorkflowEvaluateParam(raw, map[string]any{"$child": child})
	if err != nil {
		return nil, fmt.Errorf("node %q: evaluate output map: %w", meta.Name, err)
	}
	out, ok := evaluated.(map[string]any)
	if !ok {
		// The compiler requires a map, so this is a malformed compiled artifact
		// rather than author error; fail closed instead of publishing a
		// template literal as the node's result.
		return nil, fmt.Errorf("node %q: parameters.output evaluated to %T, want a map: %w", meta.Name, evaluated, types.ErrPermanent)
	}
	return out, nil
}
