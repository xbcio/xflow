package graph

// Graph is the immutable compiled representation of a workflow definition.
// It is built once via Compile and shared across concurrent executions.
type Graph struct {
	Name     string
	Nodes    []NodeMeta
	Index    map[string]int // node name → slice index
	OutEdges [][]Edge
	InEdges  [][]Edge
	InDegree []int
	Vars     map[string]any
	Config   map[string]any
}

// NodeMeta holds the static metadata for a single node extracted from NodeDef.
type NodeMeta struct {
	Name       string
	Type       string
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
