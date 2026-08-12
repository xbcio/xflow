package graph

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// nodesRefPattern matches $nodes['name'] or $nodes["name"] in expression strings.
var nodesRefPattern = regexp.MustCompile(`\$nodes\[['"]([^'"]+)['"]\]`)

// nodesDynamicPattern matches a $nodes subscript whose first non-space character
// is not a quote — i.e. a computed name such as $nodes[$vars.which]. Only the
// quoted form is statically derivable, and only the derivable form reaches the
// prefetch set, so this pattern is what the compile-time warning keys on.
var nodesDynamicPattern = regexp.MustCompile(`\$nodes\s*\[\s*[^'"\s]`)

// hasDynamicNodesRef reports whether a node's parameters use $nodes with a
// computed name. It walks through walkParams so a sub-graph body is skipped for
// the same reason extractNodeRefs skips it: the body's references belong to the
// inner graph and are diagnosed when the body itself compiles.
func hasDynamicNodesRef(params map[string]any) bool {
	if len(params) == 0 {
		return false
	}
	hasBody := declaresSubgraphBody(params)
	for key, val := range params {
		if hasBody && key == subgraphBodyKey {
			continue
		}
		if walkForDynamicNodesRef(val) {
			return true
		}
	}
	return false
}

func walkForDynamicNodesRef(v any) bool {
	switch val := v.(type) {
	case string:
		return nodesDynamicPattern.MatchString(val)
	case map[string]any:
		for _, child := range val {
			if walkForDynamicNodesRef(child) {
				return true
			}
		}
	case []any:
		for _, child := range val {
			if walkForDynamicNodesRef(child) {
				return true
			}
		}
	}
	return false
}

// validateGroupPortability checks that all members of a group are portable:
// they must not reference nodes outside the group, use reserved types, or
// contain patterns that cannot be executed in an isolated runner context.
// Called during compileGroups after members are resolved.
func validateGroupPortability(g *Graph, gm *GroupMeta) error {
	names := make([]string, 0, len(gm.Members))
	for _, idx := range gm.Members {
		names = append(names, g.nodes[idx].Name)
	}
	return validatePortability(g, "group", gm.Name, names)
}

// validatePortability is the construct-agnostic core validateGroupPortability
// and the body path (compileBodyMembers) both call, generalized to take a
// member-name set rather than a *GroupMeta so a body — which has no GroupMeta,
// only its own compiled *Graph and member names — can reuse it verbatim. kind
// is "group" or "body": it is what lets the emitted error say WHICH construct
// failed, since both paths would otherwise raise byte-identical messages and
// an operator debugging a rejected deploy would have no way to tell them apart.
func validatePortability(g *Graph, kind, name string, members []string) error {
	_, err := checkPortability(g, kind, name, members, false)
	return err
}

// checkPortability is validatePortability with the external-reference verdict
// made a parameter. collectExternal=false rejects any $nodes reference leaving
// the member set; true collects those references and returns them instead.
//
// The two answers are both correct, for different constructs. A GROUP is
// co-located and scheduled as one unit, so a member pointing outside it has no
// ordering that guarantees the target ran, and there is no later pass that
// could give it one -- reject. A BODY runs as a sub-execution of a node whose
// own ancestors have already completed, and the DSL spec grants it read access
// to exactly those (docs/design/DSL-SPECIFICATION.md, 跨域引用): whether a
// given reference is one of them is a question about the OUTER graph's
// topology, which this function cannot see -- bg here is the body's own
// two-pass graph, in which no outer node exists at all. So the body path
// collects and validateBodyOuterRefs adjudicates.
//
// Collecting is not the same as permitting. Every collected reference is
// checked by validateBodyOuterRefs before compilation succeeds; relaxing here
// WITHOUT that pass would turn a compile error into a runtime nil silently
// absorbed by the spec's ?? guard, which is strictly worse than rejecting.
func checkPortability(g *Graph, kind, name string, members []string, collectExternal bool) ([]BodyOuterRef, error) {
	memberSet := make(map[string]bool, len(members))
	for _, n := range members {
		memberSet[n] = true
	}

	var external []BodyOuterRef
	for _, memberName := range members {
		idx, ok := g.index[memberName]
		if !ok {
			continue
		}
		n := g.nodes[idx]

		if isNonPortableType(n.Type) {
			return nil, fmt.Errorf("%s %q: non-portable member %q: type %q is not portable (local/closure types cannot be distributed)",
				kind, name, n.Name, n.Type)
		}

		refs := extractNodeRefs(n.Parameters)
		for _, ref := range refs {
			if memberSet[ref] {
				continue
			}
			if !collectExternal {
				return nil, fmt.Errorf("%s %q: non-portable member %q: references external node %q via $nodes",
					kind, name, n.Name, ref)
			}
			external = append(external, BodyOuterRef{Member: n.Name, Node: ref})
		}
	}
	// members is iterated in the caller's order and extractNodeRefs already
	// sorts each member's refs, but the member order itself is the body's
	// authoring order at one call site. Sort so the set that lands in
	// NodeBodyPackage -- and therefore in the graph hash -- does not depend on
	// how the author listed the members.
	sort.Slice(external, func(i, j int) bool {
		if external[i].Member != external[j].Member {
			return external[i].Member < external[j].Member
		}
		return external[i].Node < external[j].Node
	})
	return external, nil
}

// isNonPortableType returns true for node types that are inherently local and
// cannot run on a remote runner (closures, local function nodes).
func isNonPortableType(nodeType string) bool {
	switch nodeType {
	case "xflow.local", "xflow.closure", "xflow.inline":
		return true
	}
	return false
}

// extractNodeRefs recursively walks a parameters value tree and returns all
// distinct node names referenced via $nodes['name'] patterns, SKIPPING a
// sub-graph body.
//
// The body is skipped because its names belong to the INNER graph. Attributing
// them to the outer node made a map node inside a group fail as "references
// external node <body member>" while the byte-identical map compiled fine
// outside a group -- the two paths disagreed about the same definition.
//
// Skipping does not leave a body unchecked. ProjectNodeBodyPackage runs
// validatePortability a second time over the body's own compiled graph
// (node_body_package.go), with the body's members as the member set, so a body
// member reaching outside the body is still rejected -- only the message's
// stamp changes, from the group naming the map node to the body naming the
// member.
func extractNodeRefs(params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	seen := map[string]bool{}
	walkParamsForRefs(params, seen)
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

// walkParamsForRefs walks a node's top-level parameter map, skipping the
// sub-graph body. It is the one place the "skip the body" rule lives for both
// consumers of this file's walker (extractNodeRefs here, deriveNodesRefs in
// nodes_refs.go), which previously carried two copies of it -- and did not
// carry the same one, which is exactly how the group path came to reject what
// the solo path accepted.
//
// Body detection is delegated to declaresSubgraphBody, which keys on the
// VALUE's shape (a map whose type is xflow.subgraph), never on the parameter
// name: xflow.http's "body" is a request payload whose $nodes references are
// real outer-graph references and must keep being seen.
func walkParamsForRefs(params map[string]any, seen map[string]bool) {
	hasBody := declaresSubgraphBody(params)
	for key, val := range params {
		if hasBody && key == subgraphBodyKey {
			continue
		}
		walkForRefs(val, seen)
	}
}

// subgraphBodyKey is the parameter name a sub-graph body lives under. It is
// only ever consulted together with declaresSubgraphBody -- the name alone
// means nothing.
const subgraphBodyKey = "body"

func walkForRefs(v any, seen map[string]bool) {
	switch val := v.(type) {
	case string:
		for _, m := range nodesRefPattern.FindAllStringSubmatch(val, -1) {
			seen[m[1]] = true
		}
	case map[string]any:
		for _, child := range val {
			walkForRefs(child, seen)
		}
	case []any:
		for _, child := range val {
			walkForRefs(child, seen)
		}
	}
}

// validateGroupsAllowCyclesExclusion rejects workflows that define groups with
// AllowCycles=true. Cyclic scheduling and group co-location are mutually
// exclusive (spec §13).
func validateGroupsAllowCyclesExclusion(g *Graph, def_hasGroups bool) error {
	if g.allowCycles && def_hasGroups {
		return fmt.Errorf("groups are not supported in cyclic workflows (options.allow_cycles=true)")
	}
	return nil
}

// suspiciousSecretPattern detects common secret-like values in parameters.
// This is a heuristic: it matches patterns like API keys, tokens, etc.
var suspiciousSecretPattern = regexp.MustCompile(
	`(?i)` +
		`(^|\b)(sk[_-]live[_-][a-zA-Z0-9]{20,}` +
		`|AKIA[0-9A-Z]{16}` +
		`|ghp_[a-zA-Z0-9]{36,}` +
		`|glpat-[a-zA-Z0-9\-]{20,}` +
		`|xox[bpras]-[a-zA-Z0-9\-]{10,})(\b|$)`)

// validateNoSecretLiterals scans member parameters for suspicious secret-like
// values. Credentials should be referenced by name via the Credentials system,
// not embedded as literals.
func validateNoSecretLiterals(g *Graph, gm *GroupMeta) error {
	for _, idx := range gm.Members {
		n := g.nodes[idx]
		if secret := findSecretLiteral(n.Parameters); secret != "" {
			return fmt.Errorf("group %q: non-portable member %q: parameter contains suspected secret literal (use credentials reference instead)",
				gm.Name, n.Name)
		}
	}
	return nil
}

func findSecretLiteral(v any) string {
	switch val := v.(type) {
	case string:
		if suspiciousSecretPattern.MatchString(val) {
			return val
		}
	case map[string]any:
		for _, child := range val {
			if s := findSecretLiteral(child); s != "" {
				return s
			}
		}
	case []any:
		for _, child := range val {
			if s := findSecretLiteral(child); s != "" {
				return s
			}
		}
	}
	return ""
}

// validateGroupNameNotReserved rejects group names that conflict with reserved
// identity prefixes.
func validateGroupNameNotReserved(name string) error {
	if strings.HasPrefix(name, ReservedNodeTypePrefix) {
		return fmt.Errorf("group name %q conflicts with reserved prefix %q", name, ReservedNodeTypePrefix)
	}
	if strings.HasPrefix(name, "__") {
		return fmt.Errorf("group name %q conflicts with reserved prefix %q", name, "__")
	}
	return nil
}
