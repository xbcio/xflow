package engine

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// buildInput assembles the types.Input from graph metadata and upstream outputs.
// Backend read failures are authoritative failures: they must never be treated
// as an empty execution or absent upstream business data.
func (e *Engine) buildInput(ctx context.Context, t *Task, g *graph.Graph) (*types.Input, error) {
	snap, err := e.state.GetExecution(ctx, t.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("get execution %q: %w", t.ExecutionID, err)
	}
	if snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
		return nil, ErrExecutionInactive
	}

	runtime := snap.Runtime
	input := &types.Input{
		Params:      cloneMap(g.Nodes[t.NodeIdx].Parameters),
		Vars:        mergeVars(g.Vars, runtimeVars(runtime)),
		Config:      cloneMap(g.Config),
		Runtime:     cloneRuntime(runtime),
		ExecutionID: string(t.ExecutionID),
		NodeName:    t.NodeName,
		TraceID:     snap.TraceID,
		SpanID:      snap.SpanID,
	}

	if t.Type == TaskTypeNodeResume {
		data, err := e.state.GetOutput(ctx, t.ExecutionID, t.NodeName)
		if err != nil {
			return nil, fmt.Errorf("get resumed node output %q/%q: %w", t.ExecutionID, t.NodeName, err)
		}
		input.Data = cloneMap(data)
		return input, nil
	}

	inEdges := g.InEdges[t.NodeIdx]
	if g.AllowCycles && t.NodeIdx == g.StartIdx && t.ActivationID == 1 {
		input.Data = cloneMap(snap.Params)
		return input, nil
	}
	switch len(inEdges) {
	case 0:
		// Root node — inject workflow-level submission params as input.Data so
		// source handlers can read them (mirrors ClusterRunner behaviour).
		input.Data = cloneMap(snap.Params)
	case 1:
		data, err := e.state.GetOutput(ctx, t.ExecutionID, g.Nodes[inEdges[0].SrcIdx].Name)
		if err != nil {
			return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, g.Nodes[inEdges[0].SrcIdx].Name, err)
		}
		input.Data = cloneMap(data)
	default:
		// Fan-in: expose all upstream outputs keyed by node name.
		inputs := make(map[string]any, len(inEdges))
		for _, edge := range inEdges {
			name := g.Nodes[edge.SrcIdx].Name
			data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
			if err != nil {
				return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, name, err)
			}
			inputs[name] = cloneMap(data)
		}
		input.Inputs = inputs
	}
	return input, nil
}

func cloneRuntime(runtime *types.Runtime) *types.Runtime {
	if runtime == nil {
		return nil
	}
	// NOTE: types.Runtime currently only has Vars. Any new field added to
	// types.Runtime MUST be explicitly copied here — otherwise the clone will
	// silently drop it, leading to shared-aliasing or lost-data bugs across
	// snapshots. Do not switch to a value copy without auditing deep-clone
	// semantics for any new map/slice/pointer fields.
	cp := &types.Runtime{}
	if runtime.Vars != nil {
		cp.Vars = cloneMap(runtime.Vars)
	}
	return cp
}

func runtimeVars(runtime *types.Runtime) map[string]any {
	if runtime == nil {
		return nil
	}
	return runtime.Vars
}

func mergeVars(staticVars map[string]any, runtimeVars map[string]any) map[string]any {
	if staticVars == nil && runtimeVars == nil {
		return nil
	}
	merged := cloneMap(staticVars)
	if merged == nil {
		merged = make(map[string]any, len(runtimeVars))
	}
	for k, v := range runtimeVars {
		merged[k] = v
	}
	return merged
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
