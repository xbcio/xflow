package engine

import "github.com/xbcio/xflow/engine/graph"

// transientOutputReclaimsForCommit returns the source outputs a normal node may
// release only after its own fenced terminal CommitNode transition succeeds.
//
// It deliberately supports a narrow topology rather than pretending a generic
// output read is a consumption acknowledgement:
//   - transient execution only;
//   - a group-boundary exit is the source, because this is the cross-process
//     collect -> sink handoff that motivated the feature;
//   - the source has exactly one outgoing edge and exactly one normal-node
//     consumer; and
//   - no ordinary $nodes reference can observe the source outside that edge.
//
// Any retry, duplicate, stale lease, non-successful terminal commit, fan out,
// group consumer, dynamic reference, or unsupported shape leaves output
// retention to the execution TTL. The backend performs the actual delete after
// accepting the same terminal fence, so this planner never creates a
// read/delete crash window.
func transientOutputReclaimsForCommit(g *graph.Graph, nodeIdx int) []string {
	if g == nil || !g.Transient() || g.AllowCycles() || nodeIdx < 0 || nodeIdx >= g.NodeCount() {
		return nil
	}
	consumerUnit := g.UnitIndexForNode(nodeIdx)
	if consumerUnit < 0 || consumerUnit >= g.UnitCount() || g.UnitKindAt(consumerUnit) != graph.UnitNode {
		return nil
	}

	var sourceIdx = -1
	for _, edge := range g.NodeInEdges(nodeIdx) {
		sourceUnit := g.UnitIndexForNode(edge.SrcIdx)
		if sourceUnit < 0 || sourceUnit >= g.UnitCount() || g.UnitKindAt(sourceUnit) != graph.UnitGroup {
			return nil
		}
		if sourceIdx >= 0 && sourceIdx != edge.SrcIdx {
			return nil
		}
		sourceIdx = edge.SrcIdx
	}
	if sourceIdx < 0 || g.NodeOutEdgeCount(sourceIdx) != 1 || hasExternalNodesReference(g, sourceIdx, nodeIdx) {
		return nil
	}

	out := g.NodeOutEdges(sourceIdx)
	if len(out) != 1 || out[0].DstIdx != nodeIdx {
		return nil
	}
	return []string{g.NodeName(sourceIdx)}
}

// hasExternalNodesReference rejects an output that another node may read
// through $nodes. The compiler keeps static reference names on the graph, but
// a computed subscript has no statically derivable target. Treat every dynamic
// reference outside the producing group and completing consumer as a possible
// read of sourceName; reclaiming less aggressively is safe, while guessing its
// target would let a later consumer observe a prematurely deleted output.
//
// The supported group exit source is never its own normal consumer. References
// within the producing group are irrelevant because that group completed before
// publishing its boundary exit; the completing consumer has already finished
// its only supported read when this helper is consulted for its terminal commit.
func hasExternalNodesReference(g *graph.Graph, sourceIdx, consumerIdx int) bool {
	sourceName := g.NodeName(sourceIdx)
	for idx := 0; idx < g.NodeCount(); idx++ {
		if idx == consumerIdx || g.UnitIndexForNode(idx) == g.UnitIndexForNode(sourceIdx) {
			continue
		}
		if g.HasDynamicNodesRefFor(idx) || g.BodyHasDynamicNodesRefFor(idx) {
			return true
		}
		if g.BodyMayReadNode(idx, sourceName) {
			return true
		}
		for _, name := range g.NodesRefsFor(idx) {
			if name == sourceName {
				return true
			}
		}
	}
	return false
}
