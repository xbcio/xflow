package xflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// WorkflowBuilder is a CDK-style builder for workflow definitions.
// It holds no runtime state — only the definition.
type WorkflowBuilder struct {
	name    string
	nodes   []*nodeEntry
	refs    []*NodeRef
	edges   []edge
	direct  map[string]node.TaskHandler // direct handlers (local mode only)
}

type nodeEntry struct {
	name             string
	builder          node.Builder     // nil when using the direct TaskHandler path
	handler          node.TaskHandler // local-only direct handler
	onError          node.OnError
	normalizedParams map[string]any
}

type edge struct {
	srcNode string
	srcPort string
	dstNode string
	dstPort string
}

// NewWorkflow creates a new WorkflowBuilder with the given name.
func NewWorkflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{
		name:   name,
		direct: make(map[string]node.TaskHandler),
	}
}

// NodeRef is returned by AddNode and used to reference output/input ports in Connect.
type NodeRef struct {
	name string
	body *WorkflowBuilder
}

// Out returns a reference to the named output port of this node.
func (n *NodeRef) Out(port string) node.OutputPort {
	return node.OutputPort{Node: n.name, Port: port}
}

// In returns a reference to the named input port of this node.
func (n *NodeRef) In(port string) node.InputPort {
	return node.InputPort{Node: n.name, Port: port}
}

// SetBody attaches a sub-workflow as the loop/split body.
// The sub-workflow is compiled and stored in the node's parameters at build time.
func (n *NodeRef) SetBody(body *WorkflowBuilder) *NodeRef {
	n.body = body
	return n
}

// AddNode adds a node to the workflow and returns a NodeRef.
//
// builder may be:
//   - node.Builder (recommended; works with all engine types)
//   - node.TaskHandler (local mode only; bypasses the global registry)
func (w *WorkflowBuilder) AddNode(name string, builder any) *NodeRef {
	entry := &nodeEntry{name: name}
	switch b := builder.(type) {
	case node.Builder:
		entry.builder = b
		entry.onError = b.OnErrorStrategy()
	case node.TaskHandler:
		entry.handler = b
		w.direct[name] = b
	default:
		panic(fmt.Sprintf("AddNode %q: builder must implement node.Builder or node.TaskHandler, got %T", name, builder))
	}
	w.nodes = append(w.nodes, entry)
	ref := &NodeRef{name: name}
	w.refs = append(w.refs, ref)
	return ref
}

// Connect establishes a directed edge from src to dst.
//
// dst may be:
//   - *NodeRef       — connects to the dst node's "main" input port
//   - node.InputPort — connects to the specified input port
func (w *WorkflowBuilder) Connect(src node.OutputPort, dst any) *WorkflowBuilder {
	e := edge{srcNode: src.Node, srcPort: src.Port}
	switch d := dst.(type) {
	case *NodeRef:
		e.dstNode = d.name
		e.dstPort = "main"
	case node.InputPort:
		e.dstNode = d.Node
		e.dstPort = d.Port
	default:
		panic(fmt.Sprintf("Connect: dst must be *NodeRef or node.InputPort, got %T", dst))
	}
	w.edges = append(w.edges, e)
	return w
}

// Build validates the workflow and returns a *types.WorkflowDef.
func (w *WorkflowBuilder) Build() (*types.WorkflowDef, error) {
	// Compile body sub-workflows and inject into node params.
	for i, ref := range w.refs {
		if ref.body != nil {
			bodyDef, err := ref.body.Build()
			if err != nil {
				return nil, fmt.Errorf("node %q body: %w", ref.name, err)
			}
			entry := w.nodes[i]
			if entry.builder != nil {
				params, err := normalizeParams(entry.builder.RawParams())
				if err != nil {
					return nil, fmt.Errorf("node %q: %w", entry.name, err)
				}
				params["body"] = bodyDef
				entry.normalizedParams = params
			}
		}
	}

	// Schema validation for Builder-based nodes.
	for _, entry := range w.nodes {
		if entry.builder == nil {
			continue // direct handler — no registry entry, skip validation
		}
		if entry.normalizedParams != nil {
			// Already normalized (e.g. body was injected above).
			continue
		}
		h, found := node.Lookup(entry.builder.NodeType())
		if !found {
			return nil, fmt.Errorf("node %q: handler type %q not found in registry", entry.name, entry.builder.NodeType())
		}
		dp, ok := h.(node.DescriptorProvider)
		if !ok {
			continue
		}
		desc := dp.Descriptor()
		params, err := normalizeParams(entry.builder.RawParams())
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", entry.name, err)
		}
		if err := validateParams(entry.name, desc.Params, params); err != nil {
			return nil, err
		}
		entry.normalizedParams = params
	}

	// Cycle detection.
	if err := detectCycle(w.name, w.nodes, w.edges); err != nil {
		return nil, err
	}

	// Assemble WorkflowDef.
	def := &types.WorkflowDef{
		Name:        w.name,
		Spec:        "1.0",
		Connections: make(types.Connections),
	}

	for _, entry := range w.nodes {
		nodeType := ""
		nodeVersion := 0
		params := entry.normalizedParams
		if params == nil {
			params = map[string]any{}
		}
		if entry.builder != nil {
			nodeType = entry.builder.NodeType()
			if v, ok := entry.builder.(interface{ NodeVersion() int }); ok {
				nodeVersion = v.NodeVersion()
			}
		} else {
			nodeType = "__direct__/" + entry.name
		}
		def.Nodes = append(def.Nodes, types.NodeDef{
			Name:       entry.name,
			Type:       nodeType,
			Version:    nodeVersion,
			Parameters: params,
			OnError:    string(entry.onError),
		})
	}

	for _, e := range w.edges {
		if def.Connections[e.srcNode] == nil {
			def.Connections[e.srcNode] = make(map[string][]types.Connection)
		}
		def.Connections[e.srcNode][e.srcPort] = append(
			def.Connections[e.srcNode][e.srcPort],
			types.Connection{Node: e.dstNode, Input: e.dstPort},
		)
	}

	return def, nil
}

// directHandlers returns the map of node name → direct TaskHandler.
func (w *WorkflowBuilder) directHandlers() map[string]node.TaskHandler {
	return w.direct
}

// Run is a convenience method that executes the workflow synchronously in a
// temporary local engine. Suitable for testing and scripting.
func (w *WorkflowBuilder) Run(ctx context.Context, params map[string]any) (types.Result, error) {
	eng, err := NewLocal()
	if err != nil {
		return types.Result{}, err
	}
	defer eng.Stop()

	id, err := eng.Submit(ctx, w, params)
	if err != nil {
		return types.Result{}, err
	}
	return eng.Wait(ctx, id)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validateParams(nodeName string, specs []node.ParamSpec, params map[string]any) error {
	for _, spec := range specs {
		val, exists := params[spec.Name]
		if !exists || val == nil {
			if spec.Required {
				return fmt.Errorf("node %q: required param %q is missing", nodeName, spec.Name)
			}
			if spec.Default != nil {
				params[spec.Name] = spec.Default
			}
		}
	}
	return nil
}

func normalizeParams(raw any) (map[string]any, error) {
	if raw == nil {
		return map[string]any{}, nil
	}
	if m, ok := raw.(map[string]any); ok {
		return m, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}
	result := map[string]any{}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("unmarshal params: %w", err)
	}
	return result, nil
}

func detectCycle(wfName string, nodes []*nodeEntry, edges []edge) error {
	known := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		known[n.name] = struct{}{}
	}
	inDegree := make(map[string]int, len(nodes))
	adj := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		inDegree[n.name] = 0
	}
	for _, e := range edges {
		if _, ok := known[e.srcNode]; !ok {
			continue
		}
		if _, ok := known[e.dstNode]; !ok {
			continue
		}
		adj[e.srcNode] = append(adj[e.srcNode], e.dstNode)
		inDegree[e.dstNode]++
	}
	queue := make([]string, 0, len(nodes))
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[cur] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(nodes) {
		return fmt.Errorf("workflow %q contains a cycle", wfName)
	}
	return nil
}
