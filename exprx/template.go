package exprx

import (
	"fmt"
	"strings"
)

const (
	exprOpen   = "${{"
	interpOpen = "{{"
	closeTag   = "}}"
)

// RenderTemplate applies the three parse rules from DSL-SPECIFICATION.md §4.1:
//
//  1. the value starts with "${{" and ends with "}}"  -> expression mode: the
//     whole value is one expr and the RESULT TYPE IS PRESERVED (a bool stays a
//     bool, a number stays a number, an object stays an object)
//  2. the value contains "{{ }}" but is not rule 1     -> interpolation mode:
//     each segment is evaluated and the pieces are concatenated into a string
//  3. the value contains no "{{"                       -> static literal,
//     returned unchanged
//
// The spec's fourth rule -- "${{ }}" must wrap the ENTIRE value, text before or
// after it is a compile error -- is enforced in engine/graph at compile time,
// not here: by the time a value reaches this function the workflow is already
// running, and a rejected deploy is strictly better than a failed execution.
func RenderTemplate(s string, env map[string]any) (any, error) {
	if !strings.Contains(s, interpOpen) {
		return s, nil // rule 3
	}
	if strings.HasPrefix(s, exprOpen) && strings.HasSuffix(s, closeTag) {
		inner := strings.TrimSuffix(strings.TrimPrefix(s, exprOpen), closeTag)
		// Only rule 1 if there is no further "{{" inside -- otherwise the value
		// is "${{ a }}{{ b }}", which compile time already rejected, and
		// treating it as rule 1 here would silently evaluate garbage.
		if !strings.Contains(inner, interpOpen) {
			return EvalExpr(strings.TrimSpace(inner), env, false)
		}
	}
	return renderInterpolated(s, env) // rule 2
}

// renderInterpolated evaluates every {{ ... }} segment and concatenates.
func renderInterpolated(s string, env map[string]any) (any, error) {
	var b strings.Builder
	rest := s
	for {
		start := strings.Index(rest, interpOpen)
		if start < 0 {
			b.WriteString(rest)
			return b.String(), nil
		}
		// A "${{" inside interpolation mode: the "$" belongs to the literal
		// prefix and the "{{" opens the segment. Compile time rejects the
		// mixed form, so this only runs for input that bypassed it.
		b.WriteString(rest[:start])
		rest = rest[start+len(interpOpen):]
		end := strings.Index(rest, closeTag)
		if end < 0 {
			// Unterminated segment. Report the position, never the content:
			// the tail of an unterminated template can hold a credential.
			return nil, fmt.Errorf("unterminated %q at offset %d", interpOpen, len(s)-len(rest)-len(interpOpen))
		}
		value, err := EvalExpr(strings.TrimSpace(rest[:end]), env, false)
		if err != nil {
			// EvalExpr's error already carries the expression SOURCE (which is
			// authored configuration, not runtime data) but never the values it
			// read. Do not add the rendered result here.
			return nil, err
		}
		fmt.Fprintf(&b, "%v", value)
		rest = rest[end+len(closeTag):]
	}
}
