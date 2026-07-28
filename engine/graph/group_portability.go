package graph

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// nodesRefPattern matches $nodes['name'] or $nodes["name"] in expression strings.
var nodesRefPattern = regexp.MustCompile(`\$nodes\[['"]([^'"]+)['"]\]`)

// validateGroupPortability checks that all members of a group are portable:
// they must not reference nodes outside the group, use reserved types, or
// contain patterns that cannot be executed in an isolated runner context.
// Called during compileGroups after members are resolved.
func validateGroupPortability(g *Graph, gm *GroupMeta) error {
	memberSet := make(map[string]bool, len(gm.Members))
	for _, idx := range gm.Members {
		memberSet[g.nodes[idx].Name] = true
	}

	for _, idx := range gm.Members {
		n := g.nodes[idx]

		if isNonPortableType(n.Type) {
			return fmt.Errorf("group %q: non-portable member %q: type %q is not portable (local/closure types cannot be distributed)",
				gm.Name, n.Name, n.Type)
		}

		refs := extractNodeRefs(n.Parameters)
		for _, ref := range refs {
			if !memberSet[ref] {
				return fmt.Errorf("group %q: non-portable member %q: references external node %q via $nodes",
					gm.Name, n.Name, ref)
			}
		}
	}
	return nil
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
// distinct node names referenced via $nodes['name'] patterns.
func extractNodeRefs(params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	seen := map[string]bool{}
	walkForRefs(params, seen)
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
