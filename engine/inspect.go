package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ExecutionDetail is an audit snapshot for a workflow execution.
type ExecutionDetail struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	// WorkflowID and WorkflowVersion name the definition this execution ran,
	// captured at submission (see ExecutionSnapshot.WorkflowID). Both are
	// omitted when the submission carried no identity — an inline definition
	// with no id, or an execution recorded before the fields existed — because
	// an unattributed run must not be reported under a guessed workflow.
	WorkflowID      types.WorkflowID `json:"workflow_id,omitempty"`
	WorkflowVersion string           `json:"workflow_version,omitempty"`
	// TraceID is the correlation id of the external tracing system that
	// accepted the submission — the trace id captured when the execution was
	// submitted (see ExecutionSnapshot.TraceID). It is diagnostic metadata, not
	// business data: it names a span tree rather than carrying a payload, and it
	// is what lets a reader jump from an execution to the trace that produced
	// its logs. Empty means the submission carried no trace context, which is
	// unknown — a client must not reconstruct or guess one.
	TraceID string `json:"trace_id,omitempty"`
	// CreatedAt / UpdatedAt describe the execution's lifetime: creation and the
	// latest status transition, both taken from the backend's clock. Pointers
	// rather than time.Time because omitempty does not omit a zero struct, and
	// an execution recorded before the backends stamped these must report
	// absence — the same "unknown, never guess" rule the run bar relies on —
	// instead of the year 1.
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	Error     string     `json:"error,omitempty"`
	// QueuePosition is where a queued execution stands in line, 1-based, or 0
	// when it is not queued — which includes every execution of a workflow that
	// never queues and every store that cannot report a position. Reporting is
	// best effort: a store that was asked and failed leaves this at 0 rather
	// than failing the inspection, because the status and the nodes are the
	// audit answer and the position is an extra.
	QueuePosition int `json:"queue_position,omitempty"`
	// ParentID and RerunFrom are the rerun link: the execution this one was
	// derived from and the node it starts at. Both are empty for an execution an
	// engine started itself (and for a sub-execution, whose source is not a
	// node). They are the only durable answer to "what is this execution a
	// rerun of" once the parent's own keys have expired.
	ParentID  types.ExecutionID `json:"parent_execution_id,omitempty"`
	RerunFrom string            `json:"rerun_from,omitempty"`
	// ParentNode is the parent execution's node that started this execution,
	// non-empty only on a sub-workflow child. It shares ParentID with the rerun
	// link and is what tells the two apart: a rerun carries RerunFrom, a
	// sub-workflow child carries ParentNode. It is the child-side half of the
	// drill-down whose parent-side half is NodeDetail.ChildExecutionID.
	ParentNode string       `json:"parent_node,omitempty"`
	Nodes      []NodeDetail `json:"nodes,omitempty"`
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
	// ChildExecutionID is the child execution a sub-workflow node waited on,
	// empty on every other node. It survives the node's commit, so a finished
	// node's child is still drillable, and it is metadata rather than node
	// content: it is published under the same policy as Status and Error, not
	// under Output's.
	ChildExecutionID types.ExecutionID `json:"child_execution_id,omitempty"`
	// StartedAt / FinishedAt are the node's timing facts (see
	// NodeSnapshot.StartedAt): the first lease it was given and the moment it
	// first reached a terminal status. Omitted while unknown, so a client can
	// tell "never started" from a real timestamp without parsing a sentinel.
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// InspectStatus returns the execution-level half of Inspect: status, error and,
// for a queued execution, its place in line — without loading the graph or any
// node.
//
// It exists for callers that need one status and nothing else. The submission
// path is what made the split necessary: it has just written the execution and
// has to tell the caller whether the work started or is waiting for a slot, and
// Inspect's per-node reads would put a whole graph's worth of reads on that
// request to answer a one-field question.
func (e *Engine) InspectStatus(ctx context.Context, id types.ExecutionID) (ExecutionDetail, error) {
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return ExecutionDetail{}, fmt.Errorf("inspect execution %q: %w", id, err)
	}
	if snap == nil {
		return ExecutionDetail{}, fmt.Errorf("inspect execution %q: %w", id, ErrExecutionNotFound)
	}

	detail := ExecutionDetail{
		ExecutionID:     id,
		Status:          snap.Status,
		WorkflowID:      snap.WorkflowID,
		WorkflowVersion: snap.WorkflowVersion,
		TraceID:         snap.TraceID,
		CreatedAt:       timePtr(snap.CreatedAt),
		UpdatedAt:       timePtr(snap.UpdatedAt),
		Error:           snap.Error,
		ParentID:        snap.ParentID,
		RerunFrom:       snap.RerunFrom,
		ParentNode:      snap.ParentNode,
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
	return detail, nil
}

// timePtr projects a backend timestamp into the inspect payload, where a zero
// value means "unknown" and must be omitted rather than serialized as the year
// 1 (encoding/json's omitempty does not treat a zero time.Time as empty).
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Inspect returns execution status and, when requested or discoverable from the
// stored graph, per-node status/output details for audit and approval flows.
func (e *Engine) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (ExecutionDetail, error) {
	detail, err := e.InspectStatus(ctx, id)
	if err != nil {
		return ExecutionDetail{}, err
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
		if err := e.markExpiredTransientGroupNode(ctx, id, g, detail.Status, &node); err != nil {
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
		// The child a sub-workflow node waited on. Not gated by the privacy
		// policy below: it is an execution id, not node output, and it is the
		// parent-side handle the child-side ExecutionDetail.ParentNode exists to
		// complete — withholding it would make a privacy-marked wait
		// undrillable with nothing gained, since the id names no content.
		detail.ChildExecutionID = snap.ChildExecutionID
		// Timing is metadata, not node content: it is published before the
		// private-output fence below for the same reason Status and Error are.
		// Withholding it would make a privacy-marked node's run untimed while
		// revealing nothing, since the values name no payload.
		detail.StartedAt = timePtr(snap.StartedAt)
		detail.FinishedAt = timePtr(snap.FinishedAt)
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
