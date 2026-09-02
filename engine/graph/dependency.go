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

// dependencyPort is one dependency-typed port collected by buildEdges: the
// supply node's name and the consumers declared as its targets. srcName is
// the supply, targets[].Node are the consumers -- the inverse of the legacy
// top-level DependencyEdge{Node, Supply} shape, where Node is the consumer
// declaring what it depends on.
type dependencyPort struct {
	srcName string
	targets []types.Connection
}

// buildDependencyEdges materializes dependency edges into g.supplyRefs
// (consumer nodeIdx -> sorted supply node names) and records the supply nodes
// in g.supplyIndexes. Two sources feed the same internal refs structure so
// behavior is identical either way:
//   - depPorts: dependency-typed ports collected by buildEdges from
//     def.Connections (the current, preferred form). Here the supply node is
//     the source and its targets are the consumers.
//   - def.DependencyEdges: the deprecated top-level form, where Node is the
//     consumer declaring what it depends on (Supply) -- the reverse mapping.
//
// nodeSupplyRefs is forwarded unchanged to validateSupplyUsage; see that
// function's doc for its semantics. It has nothing to do with g.supplyRefs
// above despite the similar name: g.supplyRefs is derived from THIS graph's
// own edges, nodeSupplyRefs is the per-node correction a caller supplies from
// OUTSIDE this graph (the enclosing group's own analysis) for the projected
// case where this Def cannot carry those edges at all.
//
// It runs after buildEdges so buildEdges has already rejected any supply node
// that also appears as an endpoint of a data edge (see ErrSupplyInDataflow),
// and before buildUnits so no invalid graph reaches the unit pass.
func buildDependencyEdges(def *types.WorkflowDef, depPorts []dependencyPort, g *Graph, extraAllowedSupplies []string, nodeSupplyRefs map[string][]string) error {
	if g.supplyIndexes == nil {
		g.supplyIndexes = map[string]int{}
	}
	for i := range g.nodes {
		if g.nodes[i].Kind != types.NodeKindSupply {
			continue
		}
		g.supplyIndexes[g.nodes[i].Name] = i
	}

	if len(depPorts) == 0 && len(def.DependencyEdges) == 0 {
		return validateSupplyUsage(g, extraAllowedSupplies, nodeSupplyRefs)
	}

	refs := make(map[int]map[string]struct{}, len(depPorts)+len(def.DependencyEdges))

	// New, port-level form: srcName is the supply, targets[].Node are the
	// consumers -- the inverse of the legacy DependencyEdge{Node, Supply}
	// mapping below.
	for _, dp := range depPorts {
		// Existence only. The node's supply Kind was already enforced where these
		// ports were collected (compile.go rejects a dependency-typed port on a
		// non-supply node), which is why this branch has no Kind check of its own
		// while the deprecated form below does — that form's supply name comes
		// straight from the definition, unvalidated.
		if _, ok := g.index[dp.srcName]; !ok {
			return fmt.Errorf("dependency edge references unknown supply node: %s", dp.srcName)
		}
		for _, t := range dp.targets {
			consumerIdx, ok := g.index[t.Node]
			if !ok {
				return fmt.Errorf("dependency edge references unknown consumer node: %s", t.Node)
			}
			if g.nodes[consumerIdx].Kind == types.NodeKindSupply {
				return fmt.Errorf("supply node may not depend on another supply: %s -> %s",
					t.Node, dp.srcName)
			}
			if refs[consumerIdx] == nil {
				refs[consumerIdx] = map[string]struct{}{}
			}
			refs[consumerIdx][dp.srcName] = struct{}{}
		}
	}

	// Deprecated top-level form: e.Node is the consumer declaring what it
	// depends on, e.Supply is the supply node -- normalized into the same
	// refs structure so behavior matches the port-level form exactly.
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
	return validateSupplyUsage(g, extraAllowedSupplies, nodeSupplyRefs)
}

// suppliesRefPattern matches a static $supplies.<name> dotted reference. The
// name's character class here is deliberately WIDER than what expr-lang's own
// dotted member-access syntax accepts (letters, digits, underscore only —
// confirmed with a throwaway probe against exprx.EvalExpr: evaluating
// `$supplies.my-supply.field` against an env containing that exact key fails
// with `compile expression: unknown name supply (1:14)`, because expr
// tokenizes the hyphen as a subtraction operator and re-parses "supply.field"
// as a second, unrelated operand rather than treating "my-supply" as one
// name). The pattern here still captures across the hyphen, on purpose: that
// lets firstInvalidDotSupplyName below name the whole intended supply
// ("my-supply") in its error, instead of a strict extraction silently
// truncating at "my" and producing either a confusing "no dependency edge"
// error (if "my" doesn't happen to match one) or, worse, a graph that
// compiles clean and only fails once the workflow runs.
var suppliesRefPattern = regexp.MustCompile(`\$supplies\.([A-Za-z_][A-Za-z0-9_-]*)`)

// exprDotNamePattern is the character class expr-lang's dotted member-access
// actually accepts. Anything suppliesRefPattern captures that falls outside
// this class (in practice: contains a hyphen) has no valid dot-form syntax at
// all — see suppliesRefPattern's comment for the probed failure mode.
var exprDotNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// suppliesLiteralIndexPattern matches a $supplies[...] subscript whose index
// is a single- or double-quoted string literal, e.g. $supplies["my-supply"]
// or $supplies['my-supply']. Probed directly against exprx.EvalExpr: both
// quote styles evaluate correctly against a map key containing a hyphen —
// expr-lang treats ' and " as interchangeable for a string literal, unlike
// the dotted form, which cannot express a hyphenated name at all (see
// suppliesRefPattern above). Unlike a computed subscript
// ($supplies[name], $supplies[$vars.env + "-rules"]), the literal's text IS
// the supply name: nothing to evaluate, nothing that can differ between
// compile time and run time. That makes it exactly as statically derivable
// as a suppliesRefPattern dotted name, so it gets the same treatment below:
// exempted from the dynamic-use rejection, and its name feeds
// deriveSupplyRefs like any other reference.
var suppliesLiteralIndexPattern = regexp.MustCompile(`\$supplies\[\s*(?:"([^"]*)"|'([^']*)')\s*\]`)

// suppliesDynamicPattern matches a $supplies use that cannot be resolved at
// compile time: a bracket subscript, or a bare $supplies with no member
// access at all. Such a use is rejected at compile time — if the name cannot
// be derived statically, neither the dependency-edge check nor the
// server-side reverse index can be built.
//
// This pattern alone can't distinguish a literal subscript
// ($supplies["my-supply"]) from a computed one ($supplies[name]): Go's RE2
// engine has no lookahead/lookbehind to express "a bracket whose content is
// NOT a string literal" in one pattern. hasDynamicSupplyRef below works
// around that by stripping suppliesLiteralIndexPattern matches out of the
// string first, so only a genuinely computed subscript (or a bare
// $supplies) is left for this pattern to catch.
var suppliesDynamicPattern = regexp.MustCompile(`\$supplies\s*\[|\$supplies(?:$|[^.A-Za-z0-9_])`)

// deriveSupplyRefs walks a node's parameter tree and returns the distinct
// supply names referenced via $supplies.<name> or via a quoted bracket
// subscript ($supplies["<name>"] / $supplies['<name>']), sorted. It mirrors
// extractNodeRefs in group_portability.go — same recursion, different
// pattern.
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
		for _, m := range suppliesLiteralIndexPattern.FindAllStringSubmatch(val, -1) {
			// Exactly one of the two quote-style alternatives participates per
			// match; the other reports back as "" from Go's regexp package,
			// indistinguishable from a genuinely empty literal. An empty name
			// isn't a valid supply name either way, so falling back to it here
			// is safe.
			name := m[1]
			if name == "" {
				name = m[2]
			}
			seen[name] = true
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
// $supplies with a name that cannot be resolved at compile time.
func hasDynamicSupplyRef(v any) bool {
	switch val := v.(type) {
	case string:
		// Strip literal-quoted subscripts first (see suppliesDynamicPattern's
		// comment) so only a genuinely dynamic use is left for the pattern
		// below to match.
		return suppliesDynamicPattern.MatchString(suppliesLiteralIndexPattern.ReplaceAllString(val, ""))
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

// firstInvalidDotSupplyName returns the first dot-form supply name in the
// parameter tree that expr-lang's member-access syntax cannot parse — i.e. one
// containing a character outside exprDotNamePattern, which in practice means a
// hyphen — or "" if every dot-form name is valid. See suppliesRefPattern's
// comment for why the extraction itself stays permissive enough to find this.
func firstInvalidDotSupplyName(v any) string {
	switch val := v.(type) {
	case string:
		for _, m := range suppliesRefPattern.FindAllStringSubmatch(val, -1) {
			if !exprDotNamePattern.MatchString(m[1]) {
				return m[1]
			}
		}
	case map[string]any:
		for _, child := range val {
			if name := firstInvalidDotSupplyName(child); name != "" {
				return name
			}
		}
	case []any:
		for _, child := range val {
			if name := firstInvalidDotSupplyName(child); name != "" {
				return name
			}
		}
	}
	return ""
}

// validateSupplyUsage enforces the compile-time rules for $supplies:
//   - the name must be a static literal, dotted or a quoted bracket subscript
//     (otherwise the dependency is not derivable and both the edge check and
//     the reverse index break);
//   - a dot-form name must be one expr-lang can actually parse as a member
//     name — no hyphen — or the graph would compile clean and only fail once
//     the workflow runs and the expression is evaluated (see
//     suppliesRefPattern's comment for the probed failure mode); the fix is
//     always the same (switch to bracket indexing), so the error says so
//     directly instead of surfacing as an unrelated "no dependency edge";
//   - every referenced supply must be reachable through a declared dependency
//     edge (the dependency must be visible on the graph, not implicit).
//
// extraAllowedSupplies widens the declared set for every node with names that
// are known-visible but do not appear as a per-node dependency edge in this
// graph -- this is how a projected group package (whose Def carries no
// dependency edges at all, only the flattened VisibleSupplies name list)
// re-establishes what validateSupplyUsage needs to see. It is nil for the
// ordinary Compile path.
//
// nodeSupplyRefs is the per-node correction to that widening. extraAllowedSupplies
// is a FLAT union across every member of the enclosing group -- correct for a
// member that legitimately shares access with its siblings, but too wide for
// one that does not: without nodeSupplyRefs, a member with zero dependency
// edges of its own would be validated against every OTHER member's edges too,
// letting it read (and its body's members read, via projectNodeBodies'
// parallel use of the same lookup) a sibling-declared supply it was never
// granted. When nodeSupplyRefs is non-nil, this function looks up each node's
// OWN entry via visibleSuppliesForNode instead of using extraAllowedSupplies
// directly; see that function's doc for the nil-map-vs-missing-key distinction
// that makes the correction actually hold once triggered.
func validateSupplyUsage(g *Graph, extraAllowedSupplies []string, nodeSupplyRefs map[string][]string) error {
	for i := range g.nodes {
		params := g.nodes[i].Parameters
		if len(params) == 0 {
			continue
		}
		if hasDynamicSupplyRef(params) {
			return fmt.Errorf("node %q: $supplies must be a static literal name (e.g. $supplies.rules or "+
				"$supplies[\"rules\"]); a computed name cannot be resolved at compile time", g.nodes[i].Name)
		}
		if name := firstInvalidDotSupplyName(params); name != "" {
			return fmt.Errorf("node %q: $supplies.%s is not a valid reference; expr-lang parses a hyphen "+
				"in a dotted member name as subtraction, not a name character, and fails at evaluation "+
				"time with \"compile expression: unknown name ...\"; use bracket indexing instead: "+
				"$supplies[%q]", g.nodes[i].Name, name, name)
		}
		refs := deriveSupplyRefs(params)
		if len(refs) == 0 {
			continue
		}
		declared := map[string]bool{}
		for _, name := range g.supplyRefs[i] {
			declared[name] = true
		}
		for _, name := range visibleSuppliesForNode(g.nodes[i].Name, nodeSupplyRefs, extraAllowedSupplies) {
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
