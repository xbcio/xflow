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
		// NodeAt returns a defensive deep copy of Parameters; the engine owns
		// that copy, so no further clone is needed before handing it to the
		// handler. (The handler is untrusted; the Graph stays isolated because
		// the copy is independent.)
		Params:      g.NodeAt(t.NodeIdx).Parameters,
		Vars:        mergeVars(g.Vars(), runtimeVars(runtime)),
		Config:      g.Config(),
		Runtime:     cloneRuntime(runtime),
		ExecutionID: string(t.ExecutionID),
		NodeName:    t.NodeName,
		TraceID:     snap.TraceID,
		SpanID:      snap.SpanID,
		// Graph identity, exposed to expressions as $workflow. Read from the
		// graph being executed, so inside a sub-graph these are the INNER
		// graph's values -- see the Input.WorkflowName field comment.
		WorkflowName:    g.Name(),
		WorkflowVersion: g.WorkflowVersion(),
	}

	if t.Type == TaskTypeNodeResume {
		data, err := e.state.GetOutput(ctx, t.ExecutionID, t.NodeName)
		if err != nil {
			return nil, fmt.Errorf("get resumed node output %q/%q: %w", t.ExecutionID, t.NodeName, err)
		}
		input.Data = cloneMap(data)
		applyExecutionScope(input, snap.Scope)
		// A resumed node's parameters may also contain $nodes references that
		// need resolving — the resume re-enters handler Execute with the same
		// parameters, so $nodes must be available for template evaluation.
		if err := prefetchNodesRefs(ctx, e, t, g, input); err != nil {
			return nil, err
		}
		return input, nil
	}

	inEdges := g.NodeInEdges(t.NodeIdx)
	// A cyclic workflow's start node on its first activation takes the
	// submission params directly rather than reading its in-edges: the only
	// edges into it come from around the cycle, and none of them have fired yet.
	//
	// The $nodes prefetch still runs. A start node CAN carry a $nodes reference
	// that compiles: buildNodesRefs rejects only references reachable FROM the
	// node, so a reference to a peer root passes. Returning without prefetching
	// left input.Nodes nil, which is the "key absent" row of the Input.Nodes
	// contract -- $nodes['x'].field then fails outright instead of yielding nil
	// for ?? to catch, so the guard the spec recommends for an unexecuted node
	// could not fire and the whole node failed to evaluate its parameters.
	if g.AllowCycles() && t.NodeIdx == g.StartIndex() && t.ActivationID == 1 {
		input.Data = cloneMap(snap.Params)
		applyExecutionScope(input, snap.Scope)
		if err := prefetchNodesRefs(ctx, e, t, g, input); err != nil {
			return nil, err
		}
		return input, nil
	}
	switch len(inEdges) {
	case 0:
		// Root node — inject workflow-level submission params as input.Data so
		// source handlers can read them (mirrors ClusterRunner behaviour).
		input.Data = cloneMap(snap.Params)
	case 1:
		name := g.NodeName(inEdges[0].SrcIdx)
		data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
		if err != nil {
			return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, name, err)
		}
		input.Data = cloneMap(data)
	default:
		// Fan-in: expose all upstream outputs keyed by node name.
		inputs := make(map[string]any, len(inEdges))
		for _, edge := range inEdges {
			name := g.NodeName(edge.SrcIdx)
			data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
			if err != nil {
				return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, name, err)
			}
			inputs[name] = cloneMap(data)
		}
		input.Inputs = inputs
	}
	applyExecutionScope(input, snap.Scope)
	if err := prefetchNodesRefs(ctx, e, t, g, input); err != nil {
		return nil, err
	}
	return input, nil
}

// applyExecutionScope merges the execution-wide expression roots (a map body's
// $item/$index/$items) into this node's Data, which is what BuildExprEnv
// spreads into the expression environment's top level.
//
// It runs on EVERY node of the execution, after Data was assembled from
// whichever source that node's position dictates. Shipping the roots as
// submission params instead reached only nodes with zero in-edges -- the body's
// entry member -- and every other member failed to compile its parameters.
//
// The scope wins over a same-named upstream key. The three roots are "$"-
// prefixed and the "$" prefix is reserved (a node output cannot introduce one
// through the DSL), so the collision this resolves is not reachable today; the
// rule is stated because the alternative -- letting an upstream output shadow a
// promised loop root -- would be silent and item-dependent.
func applyExecutionScope(input *types.Input, scope map[string]any) {
	if len(scope) == 0 {
		return
	}
	if input.Data == nil {
		input.Data = make(map[string]any, len(scope))
	}
	for k, v := range scope {
		input.Data[k] = v
	}
}

// prefetchNodesRefs populates input.Nodes from the compile-time reference set.
func prefetchNodesRefs(ctx context.Context, e *Engine, t *Task, g *graph.Graph, input *types.Input) error {
	refs := g.NodesRefsFor(t.NodeIdx)
	if len(refs) == 0 {
		return nil
	}
	nodes := make(map[string]any, len(refs))
	for _, name := range refs {
		data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
		if err != nil {
			return fmt.Errorf("get $nodes output %q/%q: %w", t.ExecutionID, name, err)
		}
		// data is map[string]any. On miss (node not executed) both backends
		// return nil, nil — the static type is map[string]any so this assignment
		// produces a typed nil map, which is the required form (see Input.Nodes
		// field comment for why).
		nodes[name] = data
	}
	input.Nodes = nodes
	return nil
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
