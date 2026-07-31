package graph

import (
	"errors"
	"fmt"
	"regexp"
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
		return validateSupplyUsage(g)
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
	return validateSupplyUsage(g)
}

// suppliesRefPattern matches a static $supplies.<name> reference. The name is
// restricted to the identifier characters a node name may use, which is exactly
// what makes the reference statically derivable.
var suppliesRefPattern = regexp.MustCompile(`\$supplies\.([A-Za-z_][A-Za-z0-9_-]*)`)

// suppliesDynamicPattern matches any use of $supplies that is NOT a static
// dotted name: a bracket subscript, or a bare $supplies with no member access.
// Such a use is rejected at compile time — if the name cannot be derived
// statically, neither the dependency-edge check nor the server-side reverse
// index can be built.
var suppliesDynamicPattern = regexp.MustCompile(`\$supplies\s*\[|\$supplies(?:$|[^.A-Za-z0-9_])`)

// deriveSupplyRefs walks a node's parameter tree and returns the distinct supply
// names referenced via $supplies.<name>, sorted. It mirrors extractNodeRefs in
// group_portability.go — same recursion, different pattern.
func deriveSupplyRefs(params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	seen := map[string]bool{}
	walkForSupplyRefs(params, seen)
	if len(seen) == 0 {
		return nil
	}
	refs := make([]string, 0, len(seen))
	for name := range seen {
		refs = append(refs, name)
	}
	sort.Strings(refs)
	return refs
}

func walkForSupplyRefs(v any, seen map[string]bool) {
	switch val := v.(type) {
	case string:
		for _, m := range suppliesRefPattern.FindAllStringSubmatch(val, -1) {
			seen[m[1]] = true
		}
	case map[string]any:
		for _, child := range val {
			walkForSupplyRefs(child, seen)
		}
	case []any:
		for _, child := range val {
			walkForSupplyRefs(child, seen)
		}
	}
}

// hasDynamicSupplyRef reports whether any string in the parameter tree uses
// $supplies with a non-literal name.
func hasDynamicSupplyRef(v any) bool {
	switch val := v.(type) {
	case string:
		return suppliesDynamicPattern.MatchString(val)
	case map[string]any:
		for _, child := range val {
			if hasDynamicSupplyRef(child) {
				return true
			}
		}
	case []any:
		for _, child := range val {
			if hasDynamicSupplyRef(child) {
				return true
			}
		}
	}
	return false
}

// validateSupplyUsage enforces the two compile-time rules for $supplies:
//   - the name must be a static literal (otherwise the dependency is not
//     derivable and both the edge check and the reverse index break);
//   - every referenced supply must be reachable through a declared dependency
//     edge (the dependency must be visible on the graph, not implicit).
func validateSupplyUsage(g *Graph) error {
	for i := range g.nodes {
		params := g.nodes[i].Parameters
		if len(params) == 0 {
			continue
		}
		if hasDynamicSupplyRef(params) {
			return fmt.Errorf("node %q: $supplies must be a static literal name (e.g. $supplies.rules); "+
				"a computed name cannot be resolved at compile time", g.nodes[i].Name)
		}
		refs := deriveSupplyRefs(params)
		if len(refs) == 0 {
			continue
		}
		declared := map[string]bool{}
		for _, name := range g.supplyRefs[i] {
			declared[name] = true
		}
		for _, ref := range refs {
			if !declared[ref] {
				return fmt.Errorf("node %q references $supplies.%s but has no dependency edge to it",
					g.nodes[i].Name, ref)
			}
		}
	}
	return nil
}
