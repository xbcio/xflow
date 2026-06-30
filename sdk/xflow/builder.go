package xflow

import (
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// WorkflowBuilder is a CDK-style builder for workflow definitions.
// It holds no runtime state — only the definition.
type WorkflowBuilder struct {
	namespace string
	name      string
	version   string
	nodes     []*nodeEntry
	refs      []*NodeRef
	edges     []edge
	direct    map[string]types.ActionHandler  // direct handlers (local mode only)
	handlers  map[string]types.ActionHandler  // portable typed action handlers
	triggers  map[string]types.TriggerHandler // portable typed trigger handlers
	options   *types.WorkflowOptions
}

type nodeEntry struct {
	name             string
	builder          node.Builder        // nil when using the direct ActionHandler path
	handler          types.ActionHandler // local-only direct handler
	kind             types.NodeKind
	onError          node.OnError
	normalizedParams map[string]any
}

type edge struct {
	srcNode string
	srcPort string
	dstNode string
	dstPort string
}

// Workflow creates a workflow builder with a concise user-facing name.
//
// The builder is definition-only: it does not start execution, own runtime
// state, or talk to a backend until passed to Engine.Submit. Use Node for
// portable typed nodes and LocalNode only for single-process local examples.
func Workflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{
		name:     name,
		direct:   make(map[string]types.ActionHandler),
		handlers: make(map[string]types.ActionHandler),
		triggers: make(map[string]types.TriggerHandler),
	}
}

// AllowCycles opts this workflow into cyclic execution mode.
//
// Cyclic mode is an explicit escape hatch for approval/rework flows such as
// "reject -> revise -> review again". It disables builder-side cycle rejection
// and asks the engine to schedule along the active output edge instead of using
// DAG in-degree counters.
//
// Requirements and behavior:
//   - the workflow must contain exactly one node.Start() / xflow.start node;
//   - repeated node execution overwrites the node's latest state/output;
//   - external systems own business history, custom-node idempotency, and
//     side-effect consistency;
//   - maxAutoDepth limits one automatic chain to prevent infinite unattended
//     loops; values <= 0 use the engine default; signal/timeout resumes reset
//     the counter.
func (w *WorkflowBuilder) AllowCycles(maxAutoDepth int) *WorkflowBuilder {
	w.options = &types.WorkflowOptions{
		AllowCycles:  true,
		MaxAutoDepth: maxAutoDepth,
	}
	return w
}

func (w *WorkflowBuilder) Namespace(namespace string) *WorkflowBuilder {
	w.namespace = namespace
	return w
}

func (w *WorkflowBuilder) Version(version string) *WorkflowBuilder {
	w.version = version
	return w
}

// NodeRef references a node's input and output ports in Connect.
type NodeRef struct {
	name string
	body *WorkflowBuilder
}

// Output returns a reference to the named output port of this node.
func (n *NodeRef) Output(port string) types.OutputPort {
	return types.OutputPort{Node: n.name, Port: port}
}

// Input returns a reference to the named input port of this node.
func (n *NodeRef) Input(port string) types.InputPort {
	return types.InputPort{Node: n.name, Port: port}
}

// Body attaches a sub-workflow as the loop/split body.
func (n *NodeRef) Body(body *WorkflowBuilder) *NodeRef {
	n.body = body
	return n
}

// Node adds a portable typed node to the workflow and returns its reference.
//
// This is the production path for both local and cluster execution. A typed
// node stores only its type/version/params in the workflow definition. Its
// handler is registered in the current process for local execution; cluster
// consumers that may execute workflows submitted by other processes should
// also declare the same definitions with xflow.WithNodes.
func (w *WorkflowBuilder) Node(name string, builder node.Builder) *NodeRef {
	entry := &nodeEntry{
		name:    name,
		builder: builder,
		onError: builder.OnErrorStrategy(),
	}
	if hb, ok := builder.(node.HandlerCarrier); ok {
		h := hb.Handler()
		if h != nil {
			w.handlers[builder.NodeType()] = h
		}
	}
	if hb, ok := builder.(node.TriggerHandlerCarrier); ok {
		h := hb.TriggerHandler()
		if h != nil {
			w.triggers[builder.NodeType()] = h
		}
	}
	return w.addNode(entry)
}

// LocalNode adds a local-only direct handler node to the workflow.
//
// LocalNode embeds a Go handler instance in the in-process registry by node
// name. It is convenient for tests and examples, but it is not portable and is
// rejected by NewCluster submissions. Use Node with node.Define for production
// or distributed execution.
func (w *WorkflowBuilder) LocalNode(name string, handler types.ActionHandler) *NodeRef {
	entry := &nodeEntry{name: name, handler: handler}
	w.direct[name] = handler
	return w.addNode(entry)
}

func (w *WorkflowBuilder) addNode(entry *nodeEntry) *NodeRef {
	w.nodes = append(w.nodes, entry)
	ref := &NodeRef{name: entry.name}
	w.refs = append(w.refs, ref)
	return ref
}

// Connect establishes a directed edge from src to dst.
//
// dst may be:
//   - *NodeRef       — connects to the dst node's "main" input port
//   - types.InputPort — connects to the specified input port
func (w *WorkflowBuilder) Connect(src any, dst any) *WorkflowBuilder {
	e := edge{}
	switch s := src.(type) {
	case *NodeRef:
		e.srcNode = s.name
		e.srcPort = "main"
	case types.OutputPort:
		e.srcNode = s.Node
		e.srcPort = s.Port
	default:
		panic(fmt.Sprintf("Connect: src must be *NodeRef or types.OutputPort, got %T", src))
	}
	switch d := dst.(type) {
	case *NodeRef:
		e.dstNode = d.name
		e.dstPort = "main"
	case types.InputPort:
		e.dstNode = d.Node
		e.dstPort = d.Port
	default:
		panic(fmt.Sprintf("Connect: dst must be *NodeRef or types.InputPort, got %T", dst))
	}
	w.edges = append(w.edges, e)
	return w
}

// build validates the workflow and returns a *types.WorkflowDef.
func (w *WorkflowBuilder) build() (*types.WorkflowDef, error) {
	// Compile body sub-workflows and inject into node params.
	for i, ref := range w.refs {
		if ref.body != nil {
			bodyDef, err := ref.body.build()
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
		dp, ok := entry.builder.(types.DescriptorProvider)
		if !ok {
			h, found := node.Lookup(entry.builder.NodeType())
			if !found {
				return nil, fmt.Errorf("node %q: handler type %q not found in registry", entry.name, entry.builder.NodeType())
			}
			dp = h
		}
		desc := dp.Descriptor()
		if desc.Kind != "" {
			entry.kind = desc.Kind
		} else {
			entry.kind = types.NodeKindAction
		}
		params, err := normalizeParams(entry.builder.RawParams())
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", entry.name, err)
		}
		if err := validateParams(entry.name, desc.Params, params); err != nil {
			return nil, err
		}
		entry.normalizedParams = params
	}

	if w.options == nil || !w.options.AllowCycles {
		if err := detectCycle(w.name, w.nodes, w.edges); err != nil {
			return nil, err
		}
	}

	namespace := w.namespace
	if namespace == "" {
		namespace = "default"
	}
	version := w.version
	if version == "" {
		version = "v1"
	}

	// Assemble WorkflowDef.
	def := &types.WorkflowDef{
		Namespace:   namespace,
		Name:        w.name,
		Version:     version,
		Spec:        "1.0",
		Options:     w.options,
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
			if entry.kind == "" {
				entry.kind = types.NodeKindAction
			}
			if v, ok := entry.builder.(interface{ NodeVersion() int }); ok {
				nodeVersion = v.NodeVersion()
			}
		} else {
			nodeType = "__direct__/" + entry.name
			entry.kind = types.NodeKindAction
		}
		def.Nodes = append(def.Nodes, types.NodeDef{
			Name:       entry.name,
			Type:       nodeType,
			Kind:       entry.kind,
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

// directHandlers returns the map of node name → direct ActionHandler.
func (w *WorkflowBuilder) directHandlers() map[string]types.ActionHandler {
	return w.direct
}

// workflowHandlers returns portable typed handlers declared by this workflow.
func (w *WorkflowBuilder) workflowHandlers() map[string]types.ActionHandler {
	handlers := make(map[string]types.ActionHandler)
	w.collectWorkflowHandlers(handlers)
	return handlers
}

func (w *WorkflowBuilder) workflowTriggerHandlers() map[string]types.TriggerHandler {
	handlers := make(map[string]types.TriggerHandler)
	w.collectWorkflowTriggerHandlers(handlers)
	return handlers
}

func (w *WorkflowBuilder) collectWorkflowHandlers(handlers map[string]types.ActionHandler) {
	if w == nil {
		return
	}
	for nodeType, h := range w.handlers {
		handlers[nodeType] = h
	}
	for _, ref := range w.refs {
		if ref.body != nil {
			ref.body.collectWorkflowHandlers(handlers)
		}
	}
}

func (w *WorkflowBuilder) collectWorkflowTriggerHandlers(handlers map[string]types.TriggerHandler) {
	if w == nil {
		return
	}
	for nodeType, h := range w.triggers {
		handlers[nodeType] = h
	}
	for _, ref := range w.refs {
		if ref.body != nil {
			ref.body.collectWorkflowTriggerHandlers(handlers)
		}
	}
}

func validateParams(nodeName string, specs []types.ParamSpec, params map[string]any) error {
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
