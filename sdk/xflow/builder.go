package xflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// WorkflowBuilder is a CDK-style builder for workflow definitions.
// It holds no runtime state — only the definition.
type WorkflowBuilder struct {
	namespace      string
	name           string
	version        string
	nodes          []*nodeEntry
	refs           []*NodeRef
	edges          []edge
	direct         map[string]types.ActionHandler  // direct handlers (local mode only)
	handlers       map[string]types.ActionHandler  // portable typed action handlers
	triggers       map[string]types.TriggerHandler // portable typed trigger handlers
	options        *types.WorkflowOptions
	runnerSelector *types.RunnerSelector
	groups         []*groupEntry
	depEdges       []depEdge
}

type nodeEntry struct {
	name             string
	builder          types.Builder       // nil when using the direct ActionHandler path
	handler          types.ActionHandler // local-only direct handler
	kind             types.NodeKind
	onError          types.OnError
	normalizedParams map[string]any
	runnerSelector   *types.RunnerSelector
}

type edge struct {
	srcNode string
	srcPort string
	dstNode string
	dstPort string
}

// depEdge is a dependency edge: consumer reads the shared data maintained by
// supply. It is deliberately kept out of w.edges so that neither detectCycle
// nor assembleConnections treats it as a dataflow edge.
type depEdge struct {
	consumer string
	supply   string
}

// Workflow creates a workflow builder with a concise user-facing name.
//
// The builder is definition-only: it does not start execution, own runtime
// state, or talk to a backend until passed to Engine.AddWorkflow. Use Node for
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
	opts := w.ensureOptions()
	opts.AllowCycles = true
	opts.MaxAutoDepth = maxAutoDepth
	return w
}

// Transient opts this single workflow into transient (fire-and-forget)
// execution mode, independently of the engine-wide setting.
//
// Executions of a transient workflow skip the SQL audit projection entirely:
// no execution row, no node rows, no payloads. This is the switch to reach for
// when a workflow carries data that must not be persisted -- raw third-party
// traffic, credentials in flight, anything a durable audit trail would turn
// into a disclosure. Redis state is TTL-bounded rather than retained for the
// durable window.
//
// ttl slides while the execution is active; completionTTL replaces it once the
// execution reaches a terminal state. Zero for either means "use the
// engine-wide transient TTL", per types.WorkflowOptions.
//
// Unlike the store-wide transient mode this does NOT disable suspend: the
// control plane stays fully durable, so signal/revoke/inspect all still work
// and a parked waiter can be woken. The mode promises only "no SQL projection,
// short TTL".
func (w *WorkflowBuilder) Transient(ttl, completionTTL time.Duration) *WorkflowBuilder {
	opts := w.ensureOptions()
	opts.Transient = true
	opts.TransientTTL = ttl
	opts.TransientCompletionTTL = completionTTL
	return w
}

// ensureOptions returns the workflow's option block, allocating it on first
// use. Every option setter must go through here: assigning w.options wholesale
// makes the last setter win and silently discards the earlier ones -- which for
// Transient would turn the SQL projection back on for a workflow that declared
// itself ephemeral.
func (w *WorkflowBuilder) ensureOptions() *types.WorkflowOptions {
	if w.options == nil {
		w.options = &types.WorkflowOptions{}
	}
	return w.options
}

func (w *WorkflowBuilder) Namespace(namespace string) *WorkflowBuilder {
	w.namespace = namespace
	return w
}

func (w *WorkflowBuilder) Version(version string) *WorkflowBuilder {
	w.version = version
	return w
}

func (w *WorkflowBuilder) RunnerSelector(selector types.RunnerSelector) *WorkflowBuilder {
	w.runnerSelector = cloneRunnerSelector(&selector)
	return w
}

func RunnerSelector(matchLabels map[string]string) types.RunnerSelector {
	return types.RunnerSelector{MatchLabels: cloneStringMap(matchLabels)}
}

func DefaultRunnerSelector(matchLabels map[string]string) types.RunnerSelector {
	return types.RunnerSelector{
		Mode:        types.RunnerSelectorModeDefault,
		MatchLabels: cloneStringMap(matchLabels),
	}
}

func RequiredRunnerSelector(matchLabels map[string]string) types.RunnerSelector {
	return types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: cloneStringMap(matchLabels),
	}
}

// NodeRef references a node's input and output ports in Connect.
type NodeRef struct {
	name  string
	entry *nodeEntry
	body  *WorkflowBuilder
}

// Output returns a reference to the named output port of this node.
func (n *NodeRef) Output(port string) types.OutputPort {
	return types.OutputPort{Node: n.name, Port: port}
}

// Input returns a reference to the named input port of this node.
func (n *NodeRef) Input(port string) types.InputPort {
	return types.InputPort{Node: n.name, Port: port}
}

// NodePort returns the node name and "main" port — the default endpoint
// when a NodeRef is used directly in Connect. For multi-port nodes, use
// NodeRef.Output("portName") to target a specific port.
func (n *NodeRef) NodePort() (string, string) { return n.name, "main" }

// Body attaches a sub-workflow as the loop/split body.
func (n *NodeRef) Body(body *WorkflowBuilder) *NodeRef {
	n.body = body
	return n
}

func (n *NodeRef) RunnerSelector(selector types.RunnerSelector) *NodeRef {
	if n.entry != nil {
		n.entry.runnerSelector = cloneRunnerSelector(&selector)
	}
	return n
}

// Node adds a portable typed node to the workflow and returns its reference.
//
// This is the production path for both local and cluster execution. A typed
// node stores only its type/version/params in the workflow definition. Its
// handler is registered in the current process for local execution; cluster
// consumers that may execute workflows submitted by other processes should
// also declare the same definitions with xflow.WithNodes.
func (w *WorkflowBuilder) Node(name string, builder types.Builder) *NodeRef {
	entry := &nodeEntry{
		name:    name,
		builder: builder,
		onError: builder.OnErrorStrategy(),
	}
	if hb, ok := builder.(types.HandlerProvider); ok {
		h := hb.Handler()
		if h != nil {
			w.handlers[builder.NodeType()] = h
		}
	}
	if hb, ok := builder.(types.TriggerHandlerProvider); ok {
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
	ref := &NodeRef{name: entry.name, entry: entry}
	w.refs = append(w.refs, ref)
	return ref
}

// Connect establishes a directed edge from src to dst. Both endpoints must
// satisfy types.EdgeEndpoint — typically *NodeRef (defaults to "main" port)
// or OutputPort/InputPort from NodeRef.Output/Input.
func (w *WorkflowBuilder) Connect(src, dst types.EdgeEndpoint) *WorkflowBuilder {
	sn, sp := src.NodePort()
	dn, dp := dst.NodePort()
	w.edges = append(w.edges, edge{srcNode: sn, srcPort: sp, dstNode: dn, dstPort: dp})
	return w
}

// DependsOn declares that consumer reads the long-lived shared data maintained
// by the supply node. It creates no dataflow edge: nothing is passed from
// supply to consumer at execution time, and the supply node never advances the
// execution. The compiler rejects a supply target that is not a supply node.
func (w *WorkflowBuilder) DependsOn(consumer, supply *NodeRef) *WorkflowBuilder {
	if consumer == nil || supply == nil {
		return w
	}
	w.depEdges = append(w.depEdges, depEdge{consumer: consumer.name, supply: supply.name})
	return w
}

// build validates the workflow and returns a *types.WorkflowDef.
func (w *WorkflowBuilder) build() (*types.WorkflowDef, error) {
	return w.buildInternal(map[*WorkflowBuilder]bool{})
}

// buildInternal is build with cycle detection across attached body
// sub-workflows. visited holds the builders on the current compilation path; a
// builder that reappears on its own path is a cyclic (e.g. self-referencing)
// body and is rejected instead of overflowing the stack. Reuse of the same
// builder across sibling branches is still allowed.
func (w *WorkflowBuilder) buildInternal(visited map[*WorkflowBuilder]bool) (*types.WorkflowDef, error) {
	if visited[w] {
		return nil, fmt.Errorf("workflow %q: cyclic body reference detected", w.name)
	}
	visited[w] = true
	defer delete(visited, w)

	if err := w.compileBodies(visited); err != nil {
		return nil, err
	}
	if err := w.validateAndNormalizeParams(); err != nil {
		return nil, err
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

	def := &types.WorkflowDef{
		Namespace:      namespace,
		Name:           w.name,
		Version:        version,
		Spec:           "1.0",
		RunnerSelector: cloneRunnerSelector(w.runnerSelector),
		Options:        w.options,
		Connections:    make(types.Connections),
	}
	w.assembleNodes(def)
	w.assembleConnections(def)
	w.assembleGroups(def)
	w.assembleDependencyEdges(def)

	return def, nil
}

// compileBodies recursively compiles body sub-workflows attached to node refs
// and injects the resulting body into the node's normalized params.
// visited carries the cycle-detection set from buildInternal down into each
// body's own build.
func (w *WorkflowBuilder) compileBodies(visited map[*WorkflowBuilder]bool) error {
	for i, ref := range w.refs {
		if ref.body == nil {
			continue
		}
		bodyDef, err := ref.body.buildInternal(visited)
		if err != nil {
			return fmt.Errorf("node %q body: %w", ref.name, err)
		}
		entry := w.nodes[i]
		if entry.builder == nil {
			continue
		}
		params, err := normalizeParams(entry.builder.RawParams())
		if err != nil {
			return fmt.Errorf("node %q: %w", entry.name, err)
		}
		params["body"], err = subgraphBodyParam(bodyDef)
		if err != nil {
			return fmt.Errorf("node %q: body: %w", entry.name, err)
		}
		entry.normalizedParams = params
	}
	return nil
}

// subgraphBodyParam reshapes a built sub-workflow into the body shape the
// compiler requires: a node definition of type xflow.subgraph whose own
// parameters carry {nodes, connections}.
//
// A WorkflowDef cannot be used directly. It carries workflow-level fields
// (version as a string, namespace, settings) that decode into a NodeDef's
// same-named int fields and fail, and the compiler asserts body.type
// (engine/graph/compile.go:479) — which a WorkflowDef has no field for at all.
// Only the members and their wiring are meaningful for a body: everything else
// on a workflow describes a deployment, and a body is not deployed.
//
// The members round-trip through JSON rather than being embedded as structs.
// Parameters must hold immutable value types only (the value-domain guard
// rejects the *Position pointers a NodeDef carries), and the body reaches the
// compiler as opaque JSON anyway — decodeSubgraphMembers re-marshals whatever
// is here.
func subgraphBodyParam(def *types.WorkflowDef) (map[string]any, error) {
	shape := map[string]any{
		"nodes":       def.Nodes,
		"connections": def.Connections,
	}
	data, err := json.Marshal(shape)
	if err != nil {
		return nil, fmt.Errorf("encode body members: %w", err)
	}
	var params map[string]any
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("decode body members: %w", err)
	}
	return map[string]any{
		"type":       subgraphBodyNodeType,
		"parameters": params,
	}, nil
}

// subgraphBodyNodeType is the body-only node type. It mirrors
// engine/graph's subgraphNodeType, which is unexported.
const subgraphBodyNodeType = "xflow.subgraph"

// validateAndNormalizeParams resolves each Builder-based node's descriptor,
// sets its kind, and validates/normalizes its params. Direct-handler nodes and
// nodes already normalized by compileBodies are skipped.
func (w *WorkflowBuilder) validateAndNormalizeParams() error {
	for _, entry := range w.nodes {
		if entry.builder == nil || entry.normalizedParams != nil {
			continue
		}
		dp, ok := entry.builder.(types.DescriptorProvider)
		if !ok {
			h, found := registry.Lookup(entry.builder.NodeType())
			if !found {
				return fmt.Errorf("node %q: handler type %q not found in registry", entry.name, entry.builder.NodeType())
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
			return fmt.Errorf("node %q: %w", entry.name, err)
		}
		if err := validateParams(entry.name, desc.Params, params); err != nil {
			return err
		}
		entry.normalizedParams = params
	}
	return nil
}

// assembleNodes fills def.Nodes from w.nodes, finalizing type/kind/version/params
// for both builder-based and direct-handler nodes.
func (w *WorkflowBuilder) assembleNodes(def *types.WorkflowDef) {
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
			Name:           entry.name,
			Type:           nodeType,
			Kind:           entry.kind,
			Version:        nodeVersion,
			Parameters:     params,
			OnError:        string(entry.onError),
			RunnerSelector: cloneRunnerSelector(entry.runnerSelector),
		})
	}
}

// assembleConnections fills def.Connections from w.edges.
func (w *WorkflowBuilder) assembleConnections(def *types.WorkflowDef) {
	for _, e := range w.edges {
		if def.Connections[e.srcNode] == nil {
			def.Connections[e.srcNode] = make(map[string]types.PortConnections)
		}
		// A map index expression yields a non-addressable struct, so the slice
		// has to be lifted out, appended to, and written back.
		pc := def.Connections[e.srcNode][e.srcPort]
		pc.Targets = append(pc.Targets, types.Connection{Node: e.dstNode, Input: e.dstPort})
		def.Connections[e.srcNode][e.srcPort] = pc
	}
}

// assembleDependencyEdges fills def.DependencyEdges from w.depEdges.
func (w *WorkflowBuilder) assembleDependencyEdges(def *types.WorkflowDef) {
	for _, e := range w.depEdges {
		def.DependencyEdges = append(def.DependencyEdges,
			types.DependencyEdge{Node: e.consumer, Supply: e.supply})
	}
}

// directHandlers returns the map of node name → direct ActionHandler.
// directHandlers returns the name-scoped handlers this workflow declares,
// including those declared inside a node's body sub-workflow.
//
// The recursion matters: a body's members are dispatched by the same registry
// as the outer graph's, so a body's LocalNode handler must be registered
// alongside the outer ones. Returning only w.direct left the body's handlers
// unregistered, and a map body assembled from LocalNode members failed package
// validation ("handler not available: type=__direct__/<name>") on every batch.
func (w *WorkflowBuilder) directHandlers() map[string]types.ActionHandler {
	handlers := make(map[string]types.ActionHandler, len(w.direct))
	w.collectDirectHandlers(handlers, map[*WorkflowBuilder]bool{})
	return handlers
}

func (w *WorkflowBuilder) collectDirectHandlers(handlers map[string]types.ActionHandler, visited map[*WorkflowBuilder]bool) {
	if w == nil || visited[w] {
		return
	}
	visited[w] = true
	for nodeName, h := range w.direct {
		handlers[nodeName] = h
	}
	for _, ref := range w.refs {
		if ref.body != nil {
			ref.body.collectDirectHandlers(handlers, visited)
		}
	}
}

// workflowHandlers returns portable typed handlers declared by this workflow.
func (w *WorkflowBuilder) workflowHandlers() map[string]types.ActionHandler {
	handlers := make(map[string]types.ActionHandler)
	w.collectWorkflowHandlers(handlers, map[*WorkflowBuilder]bool{})
	return handlers
}

func (w *WorkflowBuilder) workflowTriggerHandlers() map[string]types.TriggerHandler {
	handlers := make(map[string]types.TriggerHandler)
	w.collectWorkflowTriggerHandlers(handlers, map[*WorkflowBuilder]bool{})
	return handlers
}

func (w *WorkflowBuilder) collectWorkflowHandlers(handlers map[string]types.ActionHandler, visited map[*WorkflowBuilder]bool) {
	if w == nil || visited[w] {
		return
	}
	visited[w] = true
	for nodeType, h := range w.handlers {
		handlers[nodeType] = h
	}
	for _, ref := range w.refs {
		if ref.body != nil {
			ref.body.collectWorkflowHandlers(handlers, visited)
		}
	}
}

func (w *WorkflowBuilder) collectWorkflowTriggerHandlers(handlers map[string]types.TriggerHandler, visited map[*WorkflowBuilder]bool) {
	if w == nil || visited[w] {
		return
	}
	visited[w] = true
	for nodeType, h := range w.triggers {
		handlers[nodeType] = h
	}
	for _, ref := range w.refs {
		if ref.body != nil {
			ref.body.collectWorkflowTriggerHandlers(handlers, visited)
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
		// Return a shallow copy so callers that write defaults/body into the
		// result do not mutate the builder's shared RawParams map.
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out, nil
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

func cloneRunnerSelector(selector *types.RunnerSelector) *types.RunnerSelector {
	if selector == nil {
		return nil
	}
	out := &types.RunnerSelector{
		Mode:        selector.Mode,
		MatchLabels: cloneStringMap(selector.MatchLabels),
	}
	if out.Mode == "" && len(out.MatchLabels) == 0 {
		return nil
	}
	return out
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
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
