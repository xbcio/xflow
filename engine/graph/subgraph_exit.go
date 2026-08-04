package graph

// SubgraphExitResult is one fired boundary output produced by executing a
// sub-graph package (a node group's body or a map body -- the executor that
// runs either cannot tell which). It intentionally carries no outer-graph
// node index: NodeIdx only makes sense relative to the graph that embeds the
// sub-graph as a unit, and a sub-graph execution has no view of that outer
// graph. A caller that needs the outer index recomputes it against its own
// graph (see engine.GroupExitResult, the outer-graph-indexed counterpart used
// for in-process/mid-graph commits).
type SubgraphExitResult struct {
	NodeName string
	Port     string
	Data     map[string]any
}
