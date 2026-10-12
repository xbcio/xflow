package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ExecutionDetail is an audit snapshot for a workflow execution.
type ExecutionDetail struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	Error       string                `json:"error,omitempty"`
	// QueuePosition is where a queued execution stands in line, 1-based, or 0
	// when it is not queued — which includes every execution of a workflow that
	// never queues and every store that cannot report a position. Reporting is
	// best effort: a store that was asked and failed leaves this at 0 rather
	// than failing the inspection, because the status and the nodes are the
	// audit answer and the position is an extra.
	QueuePosition int          `json:"queue_position,omitempty"`
	Nodes         []NodeDetail `json:"nodes,omitempty"`
}

// NodeDetail is an audit snapshot for a workflow node.
type NodeDetail struct {
	Name    string           `json:"name"`
	Status  types.NodeStatus `json:"status"`
	Attempt int              `json:"attempt,omitempty"`
	Port    string           `json:"port,omitempty"`
	Error   string           `json:"error,omitempty"`
	// ErrorDetails carries the structured failure detail (see
	// engine.NodeSnapshot.ErrorDetails) to the consumer of this API — the SDK's
	// Inspect, and GET /v1/executions/{id}, which serializes this struct
	// directly. Without it a caller saw the rendered message and had to
	// reverse-engineer the machine-readable part out of prose.
	//
	// Subject to the same fail-closed private-output policy as Output, and
	// deliberately NOT covered by Output's own redaction: see inspectNode.
	ErrorDetails map[string]any `json:"error_details,omitempty"`
	Output       map[string]any `json:"output,omitempty"`
}

// Inspect returns execution status and, when requested or discoverable from the
// stored graph, per-node status/output details for audit and approval flows.
func (e *Engine) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (ExecutionDetail, error) {
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return ExecutionDetail{}, fmt.Errorf("inspect execution %q: %w", id, err)
	}
	if snap == nil {
		return ExecutionDetail{}, fmt.Errorf("inspect execution %q: %w", id, ErrExecutionNotFound)
	}

	detail := ExecutionDetail{
		ExecutionID: id,
		Status:      snap.Status,
		Error:       snap.Error,
	}
	if snap.Status == types.ExecutionStatusQueued {
		// Best effort, and deliberately not an error when it fails: the caller
		// asked to inspect an execution and the status is the answer to that.
		// A position it could not read is a missing extra, not a failed read.
		if inspector, ok := e.state.(ConcurrencyQueueInspector); ok {
			if position, err := inspector.ConcurrencyQueuePosition(ctx, id); err == nil {
				detail.QueuePosition = position
			}
		}
	}

	g, err := e.state.LoadGraph(ctx, id)
	if err != nil {
		return ExecutionDetail{}, fmt.Errorf("inspect graph %q: %w", id, err)
	}

	names := nodeNames
	if len(names) == 0 {
		if g == nil {
			return detail, nil
		}
		names = make([]string, 0, g.NodeCount())
		for i := 0; i < g.NodeCount(); i++ {
			names = append(names, g.NodeAt(i).Name)
		}
	}

	detail.Nodes = make([]NodeDetail, 0, len(names))
	for _, name := range names {
		graphPolicyResolved := false
		graphPrivateOutput := false
		if g != nil {
			if idx, ok := g.NodeIndex(name); ok && idx >= 0 && idx < g.NodeCount() {
				graphPolicyResolved = true
				output := g.NodeAt(idx).Output
				graphPrivateOutput = output != nil && output.Private
			}
		}

		node, err := e.inspectNode(ctx, id, name, graphPolicyResolved, graphPrivateOutput)
		if err != nil {
			return ExecutionDetail{}, err
		}
		if err := e.markExpiredTransientGroupNode(ctx, id, g, snap.Status, &node); err != nil {
			return ExecutionDetail{}, err
		}
		detail.Nodes = append(detail.Nodes, node)
	}
	return detail, nil
}

// markExpiredTransientGroupNode rewrites "we know nothing about this node" into
// "its records expired" for a member of a transient group (groups[].mode:
// transient) whose artifacts have passed the mode's completion TTL.
//
// The two states it separates are indistinguishable from a node-level read. A
// group writes no per-member node status — members run inside the runner's own
// embedded engine — so a member with no exit output reads the same whether the
// group has not run yet or its keys expired. What distinguishes them is the
// group unit's own record plus the execution's state: the execution is terminal,
// the unit has no record left, and the mode is what removed it. Leaving that as
// "pending" would assert the node never ran, which is the one answer the mode
// makes unknowable; "expired" states what is actually known.
//
// Scoped deliberately:
//   - only a node with no status verdict of its own is rewritten, so positive
//     evidence (a snapshot, an output) always wins;
//   - only a terminal execution: while the run is live, a member with no record
//     is genuinely not-yet-run, and nothing shortens a transient group's keys
//     before the execution ends;
//   - only a transient group: a durable group's artifacts live as long as the
//     execution's, so an absent record there is the pre-existing group-readback
//     gap, not expiry, and rewriting it would be a new lie;
//   - only when the store can answer. A store without the readback capability
//     leaves the previous answer in place rather than guessing.
func (e *Engine) markExpiredTransientGroupNode(
	ctx context.Context,
	id types.ExecutionID,
	g *graph.Graph,
	execStatus types.ExecutionStatus,
	node *NodeDetail,
) error {
	if g == nil || !types.IsTerminalExecutionStatus(execStatus) {
		return nil
	}
	if node.Status != types.NodeStatusPending && node.Status != "" {
		return nil
	}
	if node.Output != nil {
		return nil
	}
	idx, ok := g.NodeIndex(node.Name)
	if !ok {
		return nil
	}
	unitIdx := g.UnitIndexForNode(idx)
	if unitIdx < 0 || unitIdx >= g.UnitCount() || g.UnitKindAt(unitIdx) != graph.UnitGroup {
		return nil
	}
	if !g.GroupMetaAt(unitIdx).Transient() {
		return nil
	}
	reader, ok := e.state.(GroupUnitPhaseReader)
	if !ok {
		return nil
	}
	// A store error propagates like every other read on this path
	// (inspectNode's GetNode/GetOutput): reporting a wrong "pending" silently
	// would be worse than failing the inspect.
	if _, found, err := reader.GetGroupUnitPhase(ctx, id, unitIdx); err != nil {
		return fmt.Errorf("inspect group unit %q/#%d: %w", id, unitIdx, err)
	} else if found {
		return nil
	}
	node.Status = types.NodeStatusExpired
	return nil
}

func (e *Engine) inspectNode(
	ctx context.Context,
	id types.ExecutionID,
	name string,
	graphPolicyResolved bool,
	graphPrivateOutput bool,
) (NodeDetail, error) {
	snap, err := e.state.GetNode(ctx, id, name)
	if err != nil {
		return NodeDetail{}, fmt.Errorf("inspect node %q/%q: %w", id, name, err)
	}

	detail := NodeDetail{Name: name, Status: types.NodeStatusPending}
	if snap != nil {
		detail.Status = snap.Status
		detail.Attempt = snap.Attempt
		detail.Port = snap.Port
		detail.Error = snap.Error
	}

	// A resolved graph is authoritative. If it is unavailable or does not know
	// this node, fail closed: neither a missing snapshot nor a stale public
	// marker may turn a potentially private runtime output into an audit value.
	privateOutput := !graphPolicyResolved || graphPrivateOutput
	if privateOutput {
		return detail, nil
	}

	// ErrorDetails is projected here, AFTER the fail-closed return, and not in
	// the block above that copies Error. Two different visibility judgments are
	// in play and conflating them would be wrong in opposite directions:
	//
	//   - Error is a rendered string that the engine already publishes for
	//     private nodes (it is the execution-level reason, it reaches the
	//     audit row, and it is what an operator has to go on). Withholding it
	//     would break every existing private-output diagnosis.
	//   - ErrorDetails is an arbitrary runner-supplied map with no bound and no
	//     producer-side contract, i.e. a channel the node can widen at will.
	//     A node whose output the graph marked private is exactly the node
	//     whose error detail could quote that output, so it is withheld —
	//     failing closed, like every other decision on this path.
	//
	// The cost is bounded: the plain Error text survives, so the failure is
	// still diagnosable, just not machine-readably.
	if snap != nil {
		detail.ErrorDetails = snap.ErrorDetails
	}

	output, err := e.state.GetOutput(ctx, id, name)
	if err != nil {
		return NodeDetail{}, fmt.Errorf("inspect output %q/%q: %w", id, name, err)
	}
	if output != nil {
		// The engine's node-state slot is stripped so the inspected output is the
		// same map a downstream node and a $nodes reference see. Leaving it in
		// would make the public result surface the one place the slot appears,
		// and it would disagree with $nodes['name'] about the same node.
		detail.Output = StripNodeState(output)
	}
	return detail, nil
}
