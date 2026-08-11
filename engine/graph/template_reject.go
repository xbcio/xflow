package graph

import (
	"fmt"
	"strings"
)

// evaluableParams lists, per node type, the parameters where a "{{" is NOT a
// mistake -- either because the handler evaluates the value as an expression,
// or because the value is program text whose own syntax may contain braces
// (xflow.script's code never reaches exprx at all: script.go hands it to a
// script engine verbatim). A template in any OTHER parameter is rejected at
// compile time, because at runtime it is handed to the handler as-is -- and the
// worst forms of that are silent: xflow.http ships the literal template in a
// header or body and still reports HTTP 200 and node success, so nothing in the
// logs or metrics says anything went wrong.
//
// It is a literal table, not a lookup into node/registry, for the same reason
// transformNodeTypes and bannedBodyMemberTypes are literal: this package must
// not depend on which handlers happen to be linked into the current binary.
// That dependency is real and measured -- the server process sees a POPULATED
// registry today, but only incidentally, because service/control imports
// package node to install an observer. Read from engine/graph itself the same
// registry is empty. A compile-time rule whose verdict changes with the
// binary's import graph is worse than no rule.
//
// The key is (nodeType, paramName), never paramName alone: xflow.trigger.cron
// has a parameter called "expression" that holds a cron spec ("0 */5 * * *"),
// not an expr. A name-only table silently reclassifies it as evaluable.
//
// A node type absent from this table is NOT rejected -- see
// validateTemplateReachability. Absence means "unknown", not "nothing is
// evaluable", so a gap in this table degrades to a warning rather than to a
// false rejection of a third-party node that evaluates its own parameters.
var evaluableParams = map[string]map[string]bool{
	"xflow.if": {"condition": true},
	// NOT "rules": the handler evaluates rules[].condition (switch.go:119) but
	// reads rules[].output with cast.ToString and uses it as a port name
	// (switch.go:114). Exempting the whole subtree would let a template in
	// "output" through: not rejected here, not evaluated there, the literal
	// becomes the port name and the switch silently takes its default branch.
	// The condition sub-field is exempted by evaluableSubFields below.
	"xflow.switch": {"expression": true},
	"xflow.map":    {"items": true, "expression": true},
	"xflow.split":  {"items": true},
	// xflow.function's and xflow.script's "code" is the program itself, not a
	// template around one -- function.go:120 evaluates it as an expr, and
	// script.go hands it to a script engine verbatim. Either way "{{" inside it
	// is the program's own syntax. Task 1's exemption table is this data negated.
	"xflow.function":                    {"code": true},
	"xflow.script":                      {"code": true},
	"xflow.transform.set":               {"expressions": true},
	"xflow.transform.filter":            {"items": true, "condition": true},
	"xflow.transform.sort":              {"items": true},
	"xflow.transform.limit":             {"items": true},
	"xflow.transform.aggregate":         {"items": true},
	"xflow.transform.remove_duplicates": {"items": true},
	// The following types have NO evaluable parameters. A template in any of
	// their parameters is always a mistake -- the handler uses the value
	// verbatim (xflow.http ships it as a header/body, xflow.start ignores
	// params entirely, triggers use them as configuration specs). They are
	// registered explicitly so the compiler rejects rather than warns.
	"xflow.http":              {},
	"xflow.start":             {},
	"xflow.end":               {},
	"xflow.merge":             {},
	"xflow.trigger.cron":      {},
	"xflow.trigger.timer":     {},
	"xflow.trigger.webhook":   {},
	"xflow.trigger.kafka":     {},
	"xflow.trigger.redis_hub": {},
}

// evaluableSubFields exempts a path INSIDE a parameter for node types whose
// handler evaluates only part of a structured parameter. Only xflow.switch
// needs it today; the shape generalizes because "the whole parameter is
// evaluable" is the wrong granularity whenever a parameter is an object.
var evaluableSubFields = map[string]map[string][]string{
	// rules is []any of map[string]any; only "condition" of each element is
	// evaluated. "output" and any other key are literal.
	"xflow.switch": {"rules": {"condition"}},
}

// containsTemplate reports whether s carries either template form. It keys off
// "{{" alone: "${{" contains it, and a value with braces but no "{{" -- an
// xflow.http JSON body like {"a":1} -- is a static literal by spec rule 3.
func containsTemplate(s string) bool { return strings.Contains(s, "{{") }

// validateTemplateReachability rejects a template in a parameter no handler
// evaluates, and rejects the malformed "${{ }} with text around it" form that
// DSL-SPECIFICATION.md §4.1 declares a compile error. Both are decidable here
// and independent of every other gap in the expression layer, which is why this
// lands first. Doing the malformed-form check at compile time rather than at
// render time is load-bearing: RenderTemplate's rule-1 branch assumes the form
// is already excluded, and a rejected deploy beats a failed execution.
func validateTemplateReachability(g *Graph) error {
	for i := range g.nodes {
		n := g.nodes[i]
		evaluable, known := evaluableParams[n.Type]
		subFields := evaluableSubFields[n.Type]
		for param, value := range n.Parameters {
			// The malformed form is illegal everywhere, evaluable or not.
			if err := rejectMalformedTemplate(n, param, value); err != nil {
				return err
			}
			if evaluable[param] {
				continue
			}
			if paths, ok := subFields[param]; ok {
				if err := checkSubFields(n, param, value, paths); err != nil {
					return err
				}
				continue
			}
			if !treeHasTemplate(value) {
				continue
			}
			if !known {
				// The type is not in the table, so "no handler evaluates this"
				// is an assumption, not a fact -- a third-party node may
				// evaluate its own parameters. Warn instead of rejecting: a gap
				// in the table must not become a false rejection.
				g.addWarning(fmt.Sprintf("node %q (%s): parameter %q contains an expression "+
					"template but this node type is not registered as evaluating it; "+
					"verify the handler evaluates it, or the template ships verbatim",
					n.Name, n.Type, param))
				continue
			}
			// The message names the node, its type, and the parameter -- never
			// the value. A rejected value is frequently the very thing that
			// must not be logged (an Authorization header, a credential in a
			// request body); see the security policy's absolute blacklist.
			return fmt.Errorf("node %q (%s): parameter %q contains an expression template "+
				"but this parameter is never evaluated -- it would be sent verbatim at runtime; "+
				"remove the template or move the value into an evaluated parameter",
				n.Name, n.Type, param)
		}
	}
	return nil
}

// PLACEHOLDER_TREE_AND_HELPERS

// treeHasTemplate mirrors walkForRefs in group_portability.go: the same three
// shapes a compiled parameter tree can take after JSON decoding.
func treeHasTemplate(v any) bool {
	switch val := v.(type) {
	case string:
		return containsTemplate(val)
	case map[string]any:
		for _, child := range val {
			if treeHasTemplate(child) {
				return true
			}
		}
	case []any:
		for _, child := range val {
			if treeHasTemplate(child) {
				return true
			}
		}
	}
	return false
}

// checkSubFields applies the reachability rule inside a structured parameter:
// the named sub-fields of each element are evaluable, every other key is not.
// Used for xflow.switch's rules, where "condition" is evaluated and "output"
// becomes a port name verbatim.
func checkSubFields(n NodeMeta, param string, value any, evaluablePaths []string) error {
	elems, ok := value.([]any)
	if !ok {
		// Not the expected shape; fall back to the whole-parameter rule so a
		// malformed parameter cannot smuggle a template through.
		if treeHasTemplate(value) {
			return fmt.Errorf("node %q (%s): parameter %q contains an expression template "+
				"in an unexpected shape", n.Name, n.Type, param)
		}
		return nil
	}
	exempt := map[string]bool{}
	for _, p := range evaluablePaths {
		exempt[p] = true
	}
	for _, e := range elems {
		m, ok := e.(map[string]any)
		if !ok {
			if treeHasTemplate(e) {
				return fmt.Errorf("node %q (%s): parameter %q contains an expression template "+
					"in an unexpected shape", n.Name, n.Type, param)
			}
			continue
		}
		for key, child := range m {
			if exempt[key] {
				continue
			}
			if treeHasTemplate(child) {
				return fmt.Errorf("node %q (%s): parameter %q field %q contains an expression "+
					"template but only %v is evaluated -- the template would be used verbatim",
					n.Name, n.Type, param, key, evaluablePaths)
			}
		}
	}
	return nil
}

// PLACEHOLDER_MALFORMED

// rejectMalformedTemplate enforces DSL-SPECIFICATION.md §4.1's rule that "${{"
// must wrap the ENTIRE value: text before or after it is a compile error. Any
// value containing "${{" must both start with it and end with "}}", and must
// hold no second "{{". Enforced for evaluable parameters too -- RenderTemplate
// treats a value that fails this as interpolation mode, where the leading "$"
// silently becomes part of the output string.
func rejectMalformedTemplate(n NodeMeta, param string, value any) error {
	bad := ""
	walkStrings(value, func(s string) {
		if bad != "" || !strings.Contains(s, "${{") {
			return
		}
		trimmed := strings.TrimSpace(s)
		if !strings.HasPrefix(trimmed, "${{") || !strings.HasSuffix(trimmed, "}}") {
			bad = "text before or after ${{ }}"
			return
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(trimmed, "${{"), "}}")
		if strings.Contains(inner, "{{") {
			bad = "more than one template in a ${{ }} value"
		}
	})
	if bad == "" {
		return nil
	}
	// Again: name the parameter, never the value.
	return fmt.Errorf("node %q (%s): parameter %q is malformed -- %s; "+
		"${{ }} must wrap the whole value, use {{ }} to interpolate into text",
		n.Name, n.Type, param, bad)
}

// walkStrings visits every string leaf of a decoded parameter tree.
func walkStrings(v any, fn func(string)) {
	switch val := v.(type) {
	case string:
		fn(val)
	case map[string]any:
		for _, child := range val {
			walkStrings(child, fn)
		}
	case []any:
		for _, child := range val {
			walkStrings(child, fn)
		}
	}
}