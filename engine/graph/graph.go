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

// NodeAt returns a copy of the NodeMeta at position i.
func (g *Graph) NodeAt(i int) NodeMeta { return g.nodes[i] }

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

// Vars returns a defensive copy of the workflow-level variable map.
func (g *Graph) Vars() map[string]any {
	out := make(map[string]any, len(g.vars))
	for k, v := range g.vars {
		out[k] = v
	}
	return out
}

// Config returns a defensive copy of the workflow-level config map.
func (g *Graph) Config() map[string]any {
	out := make(map[string]any, len(g.config))
	for k, v := range g.config {
		out[k] = v
	}
	return out
}

// AllowCycles reports whether this graph uses cyclic active-port scheduling.
func (g *Graph) AllowCycles() bool { return g.allowCycles }

// StartIndex returns the index of the xflow.start node in cyclic mode (-1 for DAG).
func (g *Graph) StartIndex() int { return g.startIdx }

// MaxAutoDepth returns the maximum uninterrupted automatic scheduling depth.
func (g *Graph) MaxAutoDepth() int { return g.maxAutoDepth }

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
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	SrcIdx, DstIdx   int
	SrcPort, DstPort string
}
