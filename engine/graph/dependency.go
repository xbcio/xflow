package graph

import (
	"errors"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// ErrSupplyInDataflow is returned when a supply node appears on either side of a
// Connections edge. A supply node is deliberately excluded from the unit layer,
// so g.nodeUnit maps it to -1; letting it into the dataflow would make
// buildUnitEdges index unitOutEdges[-1] and panic.
var ErrSupplyInDataflow = errors.New("supply node cannot participate in dataflow connections")

// buildDependencyEdges materializes WorkflowDef.DependencyEdges into
// g.supplyRefs (consumer nodeIdx -> sorted supply node names) and records the
// supply nodes in g.supplyIndexes.
//
// It runs after buildEdges so it can reject a supply node that also appears in
// Connections, and before buildUnits so no invalid graph reaches the unit pass.
func buildDependencyEdges(def *types.WorkflowDef, g *Graph) error {
	if g.supplyIndexes == nil {
		g.supplyIndexes = map[string]int{}
	}
	for i := range g.nodes {
		if g.nodes[i].Kind != types.NodeKindSupply {
			continue
		}
		if len(g.outEdges[i]) > 0 || len(g.inEdges[i]) > 0 {
			return fmt.Errorf("%w: %s", ErrSupplyInDataflow, g.nodes[i].Name)
		}
		g.supplyIndexes[g.nodes[i].Name] = i
	}

	if len(def.DependencyEdges) == 0 {
		return nil
	}

	refs := make(map[int]map[string]struct{}, len(def.DependencyEdges))
	for _, e := range def.DependencyEdges {
		consumerIdx, ok := g.index[e.Node]
		if !ok {
			return fmt.Errorf("dependency edge references unknown consumer node: %s", e.Node)
		}
		supplyIdx, ok := g.index[e.Supply]
		if !ok {
			return fmt.Errorf("dependency edge references unknown supply node: %s", e.Supply)
		}
		if g.nodes[supplyIdx].Kind != types.NodeKindSupply {
			return fmt.Errorf("dependency edge target %q is not a supply node", e.Supply)
		}
		if g.nodes[consumerIdx].Kind == types.NodeKindSupply {
			return fmt.Errorf("supply node may not depend on another supply: %s -> %s", e.Node, e.Supply)
		}
		if refs[consumerIdx] == nil {
			refs[consumerIdx] = map[string]struct{}{}
		}
		refs[consumerIdx][e.Supply] = struct{}{}
	}

	g.supplyRefs = make(map[int][]string, len(refs))
	for consumerIdx, set := range refs {
		names := make([]string, 0, len(set))
		for name := range set {
			names = append(names, name)
		}
		sort.Strings(names)
		g.supplyRefs[consumerIdx] = names
	}
	return nil
}
