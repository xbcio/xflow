package graph

import "github.com/xbcio/xflow/types"

// DefaultMaxAutoDepth is used for cyclic workflows when max_auto_depth is not
// set or is non-positive.
const DefaultMaxAutoDepth = 100

// Graph is the immutable compiled representation of a workflow definition.
// It is built once via Compile and shared across concurrent executions.
type Graph struct {
	Name  string
	Nodes []NodeMeta
	Index map[string]int // node name → slice index
	// EntryIndexes contains explicit execution entry nodes such as xflow.start
	// and trigger nodes.
	EntryIndexes map[string]int
	OutEdges     [][]Edge
	InEdges      [][]Edge
	InDegree     []int
	Vars         map[string]any
	Config       map[string]any
	// AllowCycles switches the compiled graph from DAG scheduling to cyclic
	// active-port scheduling. False preserves DAG in-degree behavior.
	AllowCycles bool
	// StartIdx is the index of the required xflow.start node in cyclic mode.
	StartIdx int
	// MaxAutoDepth limits one uninterrupted automatic scheduling chain.
	MaxAutoDepth int
}

// NodeMeta holds the static metadata for a single node extracted from NodeDef.
type NodeMeta struct {
	Name       string
	Type       string
	Kind       types.NodeKind
	Version    int
	OnError    string
	MergeMode  string // "wait_all" or "wait_any"; empty means normal node
	Parameters map[string]any
	PortOuts   []string // distinct output port names that have outgoing edges
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	SrcIdx, DstIdx   int
	SrcPort, DstPort string
}
