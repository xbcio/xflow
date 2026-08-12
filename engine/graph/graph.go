package graph

import "github.com/xbcio/xflow/types"

// DefaultMaxAutoDepth is used for cyclic workflows when max_auto_depth is not
// set or is non-positive.
const DefaultMaxAutoDepth = 100

// Graph is the immutable compiled representation of a workflow definition.
// It is built once via Compile and shared across concurrent executions.
// All fields are private; use the accessor methods for read-only access.
type Graph struct {
	name string
	// graphHash identifies the exact compiled graph snapshot.
	graphHash string
	// workflowVersion is the WorkflowDef version captured at compilation.
	workflowVersion string
	// compilerVersion identifies the graph compiler format used to create this snapshot.
	compilerVersion string
	nodes           []NodeMeta
	index           map[string]int // node name → slice index
	// entryIndexes contains explicit execution entry nodes such as xflow.start
	// and trigger nodes.
	entryIndexes map[string]int
	outEdges     [][]Edge
	inEdges      [][]Edge
	inDegree     []int
	vars         map[string]any
	config       map[string]any
	// allowCycles switches the compiled graph from DAG scheduling to cyclic
	// active-port scheduling. False preserves DAG in-degree behavior.
	allowCycles bool
	// startIdx is the index of the required xflow.start node in cyclic mode.
	startIdx int
	// maxAutoDepth limits one uninterrupted automatic scheduling chain.
	maxAutoDepth int

	// Two-layer IR: durable scheduling topology (P0-2). When no groups are
	// defined the unit graph mirrors the node graph 1:1.
	groups       []GroupMeta
	units        []UnitMeta
	unitOutEdges [][]UnitEdge
	unitInEdges  [][]UnitEdge
	unitInDegree []int
	nodeUnit     []int // nodeIdx → unitIdx mapping

	// warnings holds non-fatal compile diagnostics. It is deliberately NOT part of
	// MarshalJSON: a warning is a property of one compilation, not of the graph, and
	// putting it on the wire would shift every persisted graph's hash.
	warnings []string

	// supplyIndexes maps a supply node's name to its index in g.nodes. Supply
	// nodes live in the node layer only — never in g.units.
	supplyIndexes map[string]int
	// supplyRefs maps a consumer node index to the sorted names of the supply
	// nodes it declares a dependency on. Absent key means no dependency. This
	// one is genuinely graph-level: it comes from WorkflowDef.DependencyEdges
	// and describes a relation BETWEEN nodes, not a property of any one of
	// them. Contrast NodeMeta.Body, which is a node's own compiled artifact.
	supplyRefs map[int][]string

	// nodesRefs maps a node index to the sorted, deduplicated names of other
	// nodes it references via $nodes['name'] in its parameters. Unlike
	// supplyRefs (which comes from explicitly declared DependencyEdges),
	// nodesRefs is DERIVED from parameter text during compilation. It must
	// still be persisted on the graph and enter the hash because buildInput
	// (in another process / another load) uses it to prefetch $nodes outputs,
	// and re-extracting from parameters would be maintaining a second
	// implementation of the extraction logic.
	nodesRefs map[int][]string
}

// BodyAt returns the projected body package for the node at nodeIdx, or nil
// when that node declares no body.
func (g *Graph) BodyAt(nodeIdx int) *NodeBodyPackage {
	if nodeIdx < 0 || nodeIdx >= len(g.nodes) {
		return nil
	}
	return g.nodes[nodeIdx].Body
}

// Name returns the workflow name.
func (g *Graph) Name() string { return g.name }

// Hash returns the compiled graph snapshot hash (e.g. "sha256:...").
func (g *Graph) Hash() string { return g.graphHash }

// WorkflowVersion returns the WorkflowDef version captured at compilation.
func (g *Graph) WorkflowVersion() string { return g.workflowVersion }

// CompilerVersion returns the graph compiler format identifier.
func (g *Graph) CompilerVersion() string { return g.compilerVersion }

// NodeCount returns the number of nodes in the graph.
func (g *Graph) NodeCount() int { return len(g.nodes) }

// NodeName returns the name of the node at position i without copying the rest
// of the node's mutable fields. It is the hot-path accessor for code that only
// needs the (immutable, string) name — e.g. resolving upstream node names in
// fan-in scheduling — so it pays no defensive-copy cost.
func (g *Graph) NodeName(i int) string { return g.nodes[i].Name }

// NodeAt returns a defensive deep copy of the NodeMeta at position i. Every
// mutable reference field (Parameters, PortOuts, RunnerSelector, Retry) is
// recursively cloned so the caller cannot mutate the Graph's internal state
// through the returned value. Callers that only need the node name should use
// NodeName to avoid the copy.
func (g *Graph) NodeAt(i int) NodeMeta {
	n := g.nodes[i] // struct value copy; strings and value fields are isolated
	n.Parameters = cloneStringAnyMap(n.Parameters)
	n.PortOuts = cloneStringSlice(n.PortOuts)
	n.RunnerSelector = cloneRunnerSelector(n.RunnerSelector)
	n.Retry = cloneRetry(n.Retry)
	return n
}

// NodeIndex looks up a node by name and returns its slice index.
func (g *Graph) NodeIndex(name string) (int, bool) {
	idx, ok := g.index[name]
	return idx, ok
}

// EntryIndex looks up an entry node by name and returns its slice index.
func (g *Graph) EntryIndex(name string) (int, bool) {
	idx, ok := g.entryIndexes[name]
	return idx, ok
}

// NodeOutEdges returns a defensive copy of the outgoing edges for nodeIdx.
func (g *Graph) NodeOutEdges(nodeIdx int) []Edge {
	src := g.outEdges[nodeIdx]
	out := make([]Edge, len(src))
	copy(out, src)
	return out
}

// NodeOutEdgeCount returns the number of outgoing edges for nodeIdx.
func (g *Graph) NodeOutEdgeCount(nodeIdx int) int { return len(g.outEdges[nodeIdx]) }

// NodeInEdges returns a defensive copy of the incoming edges for nodeIdx.
func (g *Graph) NodeInEdges(nodeIdx int) []Edge {
	src := g.inEdges[nodeIdx]
	out := make([]Edge, len(src))
	copy(out, src)
	return out
}

// InDegreeAt returns the static in-degree for nodeIdx.
func (g *Graph) InDegreeAt(nodeIdx int) int { return g.inDegree[nodeIdx] }

// Vars returns a defensive deep copy of the workflow-level variable map.
// Nested maps and slices are recursively cloned so the caller cannot mutate
// the Graph's internal state through the returned map.
func (g *Graph) Vars() map[string]any {
	return cloneStringAnyMap(g.vars)
}

// Config returns a defensive deep copy of the workflow-level config map.
// Nested maps and slices are recursively cloned so the caller cannot mutate
// the Graph's internal state through the returned map.
func (g *Graph) Config() map[string]any {
	return cloneStringAnyMap(g.config)
}

// AllowCycles reports whether this graph uses cyclic active-port scheduling.
func (g *Graph) AllowCycles() bool { return g.allowCycles }

// StartIndex returns the index of the xflow.start node in cyclic mode (-1 for DAG).
func (g *Graph) StartIndex() int { return g.startIdx }

// MaxAutoDepth returns the maximum uninterrupted automatic scheduling depth.
func (g *Graph) MaxAutoDepth() int { return g.maxAutoDepth }

// UnitCount returns the number of scheduling units in the two-layer IR.
func (g *Graph) UnitCount() int { return len(g.units) }

// UnitAt returns the UnitMeta at position i.
func (g *Graph) UnitAt(i int) UnitMeta { return g.units[i] }

// Units returns a defensive copy of all UnitMeta entries.
func (g *Graph) Units() []UnitMeta {
	out := make([]UnitMeta, len(g.units))
	copy(out, g.units)
	return out
}

// Groups returns a defensive copy of all GroupMeta entries.
func (g *Graph) Groups() []GroupMeta {
	out := make([]GroupMeta, len(g.groups))
	copy(out, g.groups)
	return out
}

// UnitInDegreeAt returns the in-degree for unit i in the scheduling topology.
func (g *Graph) UnitInDegreeAt(i int) int { return g.unitInDegree[i] }

// UnitOutEdges returns a defensive copy of outgoing unit edges for unit i.
func (g *Graph) UnitOutEdges(i int) []UnitEdge {
	out := make([]UnitEdge, len(g.unitOutEdges[i]))
	copy(out, g.unitOutEdges[i])
	return out
}

// UnitIndexForNode returns the unit index that the given node index belongs to.
func (g *Graph) UnitIndexForNode(nodeIdx int) int { return g.nodeUnit[nodeIdx] }

// UnitKindAt returns the UnitKind for the unit at index i.
func (g *Graph) UnitKindAt(i int) UnitKind { return g.units[i].Kind }

// UnitNodeIndex returns the node index for a UnitNode unit. Panics if the unit
// is not UnitNode.
func (g *Graph) UnitNodeIndex(i int) int {
	if g.units[i].Kind != UnitNode {
		panic("UnitNodeIndex called on non-UnitNode unit")
	}
	return g.units[i].NodeIdx
}

// GroupMetaAt returns the GroupMeta for a UnitGroup unit. Panics if the unit is
// not UnitGroup.
func (g *Graph) GroupMetaAt(unitIdx int) GroupMeta {
	if g.units[unitIdx].Kind != UnitGroup {
		panic("GroupMetaAt called on non-UnitGroup unit")
	}
	return g.groups[g.units[unitIdx].GroupIdx]
}

// UnitMergeMode returns the downstream merge semantic for a unit. For a
// UnitNode it returns the member node's MergeMode. For a UnitGroup it returns
// "" (default wait_all; group fan-in semantics belong to Milestone B).
func (g *Graph) UnitMergeMode(unitIdx int) string {
	u := g.units[unitIdx]
	if u.Kind == UnitGroup {
		return ""
	}
	return g.nodes[u.NodeIdx].MergeMode
}

// UnitDisplayName returns a human-readable name for the unit (node name for
// UnitNode, group name for UnitGroup).
func (g *Graph) UnitDisplayName(unitIdx int) string { return g.units[unitIdx].Name }

// NodeMeta holds the static metadata for a single node extracted from NodeDef.
type NodeMeta struct {
	Name           string
	Type           string
	Kind           types.NodeKind
	Version        int
	OnError        string
	RunnerSelector *types.RunnerSelector
	MergeMode      string // "wait_all" or "wait_any"; empty means normal node
	Parameters     map[string]any
	PortOuts       []string // distinct output port names that have outgoing edges
	// Retry, when non-nil and MaxAttempts>0, instructs the engine to
	// re-enqueue this node with an exponential backoff after a transient
	// handler failure. Nil means no retries.
	Retry *types.RetrySettings
	// GroupIdx is the index of the co-location group this node belongs to;
	// -1 means the node is ungrouped.
	GroupIdx int
	// Body is this node's projected body sub-graph, or nil when it declares
	// none. Today only xflow.map grows one (and not in its expression form),
	// but nothing here is map-specific: any node type that comes to embed a
	// "body" sub-graph stores it in this same field, and the wire format, the
	// graph hash, and the executor all keep working unchanged.
	//
	// Projected once at compile time, so N batches of the same node share one
	// package and one hash — which is what lets the executor's cache compile
	// the body exactly once no matter how the items were batched.
	//
	// It lives on the node rather than in a graph-level map because it is the
	// node's own compiled artifact, exactly like GroupIdx — and because that
	// placement is what makes it travel: NodeMeta round-trips through
	// wireNodeMeta as a unit, so a body cannot be silently dropped from a
	// snapshot the way a separate graph-level field could be (and was).
	//
	// The explicit omitempty is load-bearing: NodeMeta is hashed field-by-field
	// with no json tags (graphHashPayload.Nodes is []NodeMeta), so without it
	// every node in every graph would contribute a "Body":null and every
	// already-persisted graph's hash would move — even for workflows with no
	// body anywhere. Same reason graphHashPayload.SupplyRefs carries one.
	Body *NodeBodyPackage `json:",omitempty"`
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	SrcIdx, DstIdx   int
	SrcPort, DstPort string
}

// SupplyRefsFor returns the sorted supply node names that the node at nodeIdx
// depends on, or nil when it declares none. The returned slice is a copy.
func (g *Graph) SupplyRefsFor(nodeIdx int) []string {
	names := g.supplyRefs[nodeIdx]
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	copy(out, names)
	return out
}

// NodesRefsFor returns the sorted, deduplicated names of nodes referenced via
// $nodes['name'] in the parameters of the node at nodeIdx, or nil when it
// references none. The returned slice is a defensive copy.
func (g *Graph) NodesRefsFor(nodeIdx int) []string {
	names := g.nodesRefs[nodeIdx]
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	copy(out, names)
	return out
}

// BodyOuterRefsFor returns the OUTER-graph $nodes references the body declared
// on nodeIdx makes, or nil when it declares no body or reads none. The returned
// slice is a defensive copy (BodyOuterRef itself is all-string, so a shallow
// copy fully isolates it).
//
// It is deliberately a SEPARATE set from NodesRefsFor. That one drives
// buildInput's prefetch for the node's OWN parameters; this one drives the
// snapshot shipped into the body's sub-execution. Merging them would make the
// map node prefetch names it never mentions and would let a body's reference
// silently satisfy the map node's own $nodes lookups.
func (g *Graph) BodyOuterRefsFor(nodeIdx int) []BodyOuterRef {
	body := g.BodyAt(nodeIdx)
	if body == nil || len(body.OuterNodeRefs) == 0 {
		return nil
	}
	out := make([]BodyOuterRef, len(body.OuterNodeRefs))
	copy(out, body.OuterNodeRefs)
	return out
}

// SupplyNodeIndexes returns a copy of the supply node name → node index map.
func (g *Graph) SupplyNodeIndexes() map[string]int {
	out := make(map[string]int, len(g.supplyIndexes))
	for k, v := range g.supplyIndexes {
		out[k] = v
	}
	return out
}

// Warnings returns the non-fatal diagnostics collected during compilation.
func (g *Graph) Warnings() []string { return g.warnings }

func (g *Graph) addWarning(msg string) { g.warnings = append(g.warnings, msg) }
