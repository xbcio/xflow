package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// ExecutionDetail is an audit snapshot for a workflow execution.
type ExecutionDetail struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	Error       string                `json:"error,omitempty"`
	Nodes       []NodeDetail          `json:"nodes,omitempty"`
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
		detail.Nodes = append(detail.Nodes, node)
	}
	return detail, nil
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
		detail.Output = output
	}
	return detail, nil
}
