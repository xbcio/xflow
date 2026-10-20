package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ExecutionScopeNodesKey is the execution-scope key carrying a map body's
// snapshot of the OUTER graph's node outputs.
//
// $nodes rides the scope alongside $item/$index/$items because it is the same
// kind of thing -- a root that belongs to the whole sub-execution rather than to
// one node -- and reusing that channel means it inherits an already-tested
// round trip (snapshot field, Redis codec, statestore contract) instead of
// growing a parallel one.
//
// What it does NOT share is the destination. BuildExprEnv assigns env["$nodes"]
// from Input.Nodes AFTER spreading Input.Data, so a "$nodes" key left in Data is
// overwritten by the node's own (empty) Nodes and reads as absent.
// applyExecutionScope therefore routes this one key to Input.Nodes and every
// other key to Input.Data.
const ExecutionScopeNodesKey = "$nodes"

// resolveNodeTimeout normalizes NodeDef's three-state timeout into the single
// convention the rest of the system reads: a positive duration is a bound, and
// zero means unbounded. That matches the two pre-existing readers
// (input.Timeout > 0 in action/http.go, gm.Timeout > 0 in group_lease.go), so
// no second convention enters the codebase.
func (e *Engine) resolveNodeTimeout(raw time.Duration) time.Duration {
	switch {
	case raw < 0:
		return 0 // explicit opt-out
	case raw == 0:
		return e.defaultNodeTimeout // may itself be 0, i.e. default disabled
	default:
		return raw
	}
}

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
	// NodeAt returns a defensive deep copy of NodeMeta, so the engine owns this
	// copy. Call it once and reuse it for both Parameters and Timeout -- calling
	// twice would mint a second independent copy whose Timeout we would then
	// throw away.
	meta := g.NodeAt(t.NodeIdx)
	input := &types.Input{
		// NodeAt's copy is independent, so no further clone is needed before
		// handing Params to the handler. (The handler is untrusted; the Graph
		// stays isolated because the copy is independent.)
		Params:      meta.Parameters,
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
		// Resolved here (not at graph compile time) so the engine's
		// WithDefaultNodeTimeout option is honoured; the graph package cannot
		// see that option. > 0 means bounded, 0 means unbounded, matching the
		// two pre-existing readers (action/http.go, group_lease.go).
		Timeout: e.resolveNodeTimeout(meta.Timeout),
	}

	if t.Type == TaskTypeNodeResume {
		// This is the one read that recovers the node's own private state. It
		// comes from the node's own stored output, which only this node's commits
		// write, so it is the one input channel no upstream node and no caller can
		// seed -- see types.Input.State.
		//
		// A resumed node's parameters may also contain $nodes references that
		// need resolving — the resume re-enters handler Execute with the same
		// parameters, so $nodes must be available for template evaluation. Both
		// reads come from the same store and the same execution, so they merge
		// into one batch when the backend supports it.
		refs := g.NodesRefsFor(t.NodeIdx)
		names := make([]string, 0, 1+len(refs))
		names = append(names, t.NodeName)
		names = append(names, refs...)
		outputs, err := e.readOutputs(ctx, t.ExecutionID, names)
		if err != nil {
			return nil, fmt.Errorf("get resumed node output %q/%q: %w", t.ExecutionID, t.NodeName, err)
		}
		input.State, input.Data = splitNodeState(outputs[t.NodeName])
		applyExecutionScope(input, snap.Scope)
		mergeNodesRefs(input, refs, outputs)
		stripNodeStateFromInput(input)
		return input, nil
	}

	inEdges := g.NodeInEdges(t.NodeIdx)
	refs := g.NodesRefsFor(t.NodeIdx)
	// refOutputs holds a read that already covers refs, so the post-switch
	// prefetch does not read them a second time. Nil means "not read yet".
	var refOutputs map[string]map[string]any
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
		input.Data = StripNodeState(cloneMap(snap.Params))
		applyExecutionScope(input, snap.Scope)
		if err := prefetchNodesRefs(ctx, e, t, g, input); err != nil {
			return nil, err
		}
		stripNodeStateFromInput(input)
		return input, nil
	}
	switch len(inEdges) {
	case 0:
		// Root node — inject workflow-level submission params as input.Data so
		// source handlers can read them (mirrors ClusterRunner behaviour).
		input.Data = StripNodeState(cloneMap(snap.Params))
	case 1:
		name := g.NodeName(inEdges[0].SrcIdx)
		// With no $nodes references this is one read either way, so it keeps
		// the plain GetOutput every one-edge node has always made. When the
		// node also references peers, the upstream payload and those references
		// ride the same batch instead of paying a second round trip.
		if len(refs) == 0 {
			data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
			if err != nil {
				return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, name, err)
			}
			input.Data = StripNodeState(cloneMap(data))
			break
		}
		names := make([]string, 0, 1+len(refs))
		names = append(names, name)
		names = append(names, refs...)
		outputs, err := e.readOutputs(ctx, t.ExecutionID, names)
		if err != nil {
			return nil, fmt.Errorf("get upstream output %q: %w", t.ExecutionID, err)
		}
		input.Data = StripNodeState(cloneMap(outputs[name]))
		refOutputs = outputs
	default:
		// Fan-in: expose every upstream output under the input port its edge
		// targets, which is what $inputs is defined as -- the connection's
		// `input:` label, falling back to the implicit main port. Two edges that
		// name no port therefore share one key; that is the case the compiler
		// warns about, because only one of the two payloads can be read.
		//
		// All upstream outputs -- and the node's $nodes references, when it has
		// any -- are read before any of them is placed, so the fan-in costs one
		// batch read instead of one round trip per inbound edge. The merge below
		// still walks the edges in graph order, which is what keeps the "first
		// non-nil payload wins a shared port" rule independent of the order the
		// backend returned the map in.
		//
		// A nil output never overwrites a value already collected under the same
		// key. An upstream that did not run has no output (GetOutput returns nil
		// for it), and letting that nil win would turn "one arm of the fan-in was
		// skipped" into "this port carries nothing". The key stays present either
		// way, so $inputs.x still resolves -- to nil -- for a port nothing
		// produced.
		names := make([]string, 0, len(inEdges)+len(refs))
		for _, edge := range inEdges {
			names = append(names, g.NodeName(edge.SrcIdx))
		}
		names = append(names, refs...)
		outputs, err := e.readOutputs(ctx, t.ExecutionID, names)
		if err != nil {
			return nil, fmt.Errorf("get upstream output %q: %w", t.ExecutionID, err)
		}
		inputs := make(map[string]any, len(inEdges))
		for _, edge := range inEdges {
			port := edge.DstPort
			if port == "" {
				port = types.DefaultInputPort
			}
			value := StripNodeState(cloneMap(outputs[g.NodeName(edge.SrcIdx)]))
			if prev, seen := inputs[port]; seen && prev != nil && value == nil {
				continue
			}
			inputs[port] = value
		}
		input.Inputs = inputs
		// main is the port $input reads, so a fan-in whose edges name no port
		// still hands its data to the node's main input. Without this the node
		// would see $inputs.main while $input stayed nil, and $input is the name
		// every handler authored before ports existed reads.
		if main, ok := inputs[types.DefaultInputPort].(map[string]any); ok {
			input.Data = cloneMap(main)
		}
		if len(refs) > 0 {
			refOutputs = outputs
		}
	}
	applyExecutionScope(input, snap.Scope)
	// The local $nodes merge runs after applyExecutionScope because a local name
	// wins over a snapshotted outer one (see prefetchNodesRefs); a read the
	// switch already performed is simply merged here instead of being repeated.
	if len(refs) > 0 {
		if refOutputs != nil {
			mergeNodesRefs(input, refs, refOutputs)
		} else if err := prefetchNodesRefs(ctx, e, t, g, input); err != nil {
			return nil, err
		}
	}
	stripNodeStateFromInput(input)
	return input, nil
}

// applyExecutionScope merges the execution-wide expression roots (a map body's
// $item/$index/$items, and its $nodes snapshot of the outer graph) into this
// node's input.
//
// It runs on EVERY node of the execution, after Data was assembled from
// whichever source that node's position dictates. Shipping the roots as
// submission params instead reached only nodes with zero in-edges -- the body's
// entry member -- and every other member failed to compile its parameters.
//
// Most roots land in Data, which is what BuildExprEnv spreads into the
// expression environment's top level. ExecutionScopeNodesKey is the exception
// and lands in Nodes -- see that constant's doc for why Data cannot work for it.
//
// The scope wins over a same-named upstream key. The roots are "$"-prefixed and
// the "$" prefix belongs to the engine, so the collision this resolves is not
// reachable through a workflow definition; the rule is stated because the
// alternative -- letting an upstream output shadow a promised loop root -- would
// be silent and item-dependent.
//
// The "$" prefix is a naming convention, not an enforced boundary: an output map
// is untyped, so a node can publish a "$"-prefixed key and a caller can put one
// in submission params. Nothing here relies on its absence. buildInput strips
// the engine's own NodeStateKey from Data after this runs, which is why the
// convention's limits cost nothing.
func applyExecutionScope(input *types.Input, scope map[string]any) {
	if len(scope) == 0 {
		return
	}
	for k, v := range scope {
		if k == ExecutionScopeNodesKey {
			outer, ok := v.(map[string]any)
			if !ok || len(outer) == 0 {
				continue
			}
			if input.Nodes == nil {
				input.Nodes = make(map[string]any, len(outer))
			}
			for name, data := range outer {
				input.Nodes[name] = data
			}
			continue
		}
		if input.Data == nil {
			input.Data = make(map[string]any, len(scope))
		}
		input.Data[k] = v
	}
}

// prefetchNodesRefs populates input.Nodes from the compile-time reference set.
//
// It MERGES rather than assigns, because applyExecutionScope may already have
// put a map body's outer-graph snapshot there. Assigning dropped it for exactly
// the members that had a local $nodes reference too -- a two-member body whose
// second member read both a sibling and an outer ancestor saw only the sibling.
//
// A local name always wins over a snapshotted outer one: these are read from
// THIS execution's state store, and the compile-time sets are disjoint anyway
// (buildNodesRefs excludes the outer names from g.nodesRefs precisely so the
// inner prefetch never chases them against the inner store).
func prefetchNodesRefs(ctx context.Context, e *Engine, t *Task, g *graph.Graph, input *types.Input) error {
	refs := g.NodesRefsFor(t.NodeIdx)
	if len(refs) == 0 {
		return nil
	}
	outputs, err := e.readOutputs(ctx, t.ExecutionID, refs)
	if err != nil {
		return fmt.Errorf("get $nodes output %q: %w", t.ExecutionID, err)
	}
	mergeNodesRefs(input, refs, outputs)
	return nil
}

// mergeNodesRefs copies the referenced outputs into input.Nodes. A name with no
// stored output maps to a typed nil map, which is the form the $nodes contract
// requires -- see Input.Nodes and TestInputNodes_UnexecutedIsTypedNilMap.
func mergeNodesRefs(input *types.Input, refs []string, outputs map[string]map[string]any) {
	if len(refs) == 0 {
		return
	}
	if input.Nodes == nil {
		input.Nodes = make(map[string]any, len(refs))
	}
	for _, name := range refs {
		// outputs[name] is a typed nil map when the backend reported the output
		// as missing (or omitted the name), which is exactly what GetOutput's
		// nil, nil used to produce here.
		input.Nodes[name] = StripNodeState(outputs[name])
	}
}

// readOutputs reads the named node outputs of one execution, in a single batch
// when the backend implements BatchOutputReader and one GetOutput per name
// otherwise. The returned map omits names with no stored output; callers look
// values up by name and therefore see a missing entry as a nil payload, which
// is what GetOutput's nil, nil means.
//
// Duplicate names are read once. The caller's ordering never depends on the
// returned map's iteration order: every caller walks the fixed sequence it
// built (in-edges, or the sorted compile-time reference set) and addresses the
// map by name.
func (e *Engine) readOutputs(ctx context.Context, id types.ExecutionID, names []string) (map[string]map[string]any, error) {
	if len(names) == 0 {
		return map[string]map[string]any{}, nil
	}
	if reader, ok := e.state.(BatchOutputReader); ok {
		// A backend that declares the capability is responsible for naming the
		// failing output in its error; the caller adds the operation context.
		return reader.GetOutputs(ctx, id, names)
	}
	outputs := make(map[string]map[string]any, len(names))
	for _, name := range names {
		if _, seen := outputs[name]; seen {
			continue
		}
		data, err := e.state.GetOutput(ctx, id, name)
		if err != nil {
			return nil, fmt.Errorf("get output %q/%q: %w", id, name, err)
		}
		outputs[name] = data
	}
	return outputs, nil
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
