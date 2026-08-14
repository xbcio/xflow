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
	Output  map[string]any   `json:"output,omitempty"`
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

	names := nodeNames
	if len(names) == 0 {
		g, err := e.state.LoadGraph(ctx, id)
		if err != nil {
			return ExecutionDetail{}, fmt.Errorf("inspect graph %q: %w", id, err)
		}
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
		node, err := e.inspectNode(ctx, id, name)
		if err != nil {
			return ExecutionDetail{}, err
		}
		detail.Nodes = append(detail.Nodes, node)
	}
	return detail, nil
}

func (e *Engine) inspectNode(ctx context.Context, id types.ExecutionID, name string) (NodeDetail, error) {
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

	output, err := e.state.GetOutput(ctx, id, name)
	if err != nil {
		return NodeDetail{}, fmt.Errorf("inspect output %q/%q: %w", id, name, err)
	}
	if output != nil {
		detail.Output = output
	}
	return detail, nil
}
