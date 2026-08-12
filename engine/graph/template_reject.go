package graph

import (
	"fmt"
	"strings"
)

// validateTemplateForm rejects the malformed "${{ }} with text around it" form
// that DSL-SPECIFICATION.md §4.1 declares a compile error. This is the only
// template rule decidable at compile time.
//
// It used to also reject a template in a parameter no handler evaluates. That
// rule was removed deliberately: execution/params.go now evaluates every
// non-exempt parameter at the handler boundary, so "no handler evaluates this
// parameter" is no longer true of anything. Keeping the rule made the spec's
// own documented form (a template in xflow.http headers,
// DSL-SPECIFICATION.md:354/427/447) undeployable -- a false rejection of the
// very shape the boundary layer exists to support.
//
// Doing the malformed-form check at compile time rather than at render time is
// load-bearing: RenderTemplate's rule-1 branch assumes the form is already
// excluded, and a rejected deploy beats a failed execution.
func validateTemplateForm(g *Graph) error {
	for i := range g.nodes {
		n := g.nodes[i]
		for param, value := range n.Parameters {
			if err := rejectMalformedTemplate(n, param, value); err != nil {
				return err
			}
		}
	}
	return nil
}

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