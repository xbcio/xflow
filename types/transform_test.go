package types

import "testing"

// An empty-string expression is not a declared expression.
//
// The two sides of the transform contract used to disagree on exactly this
// value: the compiler asked whether the "expression" KEY was present, while
// xflow.map's handler asked whether its VALUE was non-empty. For xflow.map the
// disagreement was masked — it is also a fan-out type, and that rule rejects a
// body-less node on the value. A transform that does not fan out (filter,
// reduce's accumulator, sort's key) has no such second net: it would compile
// with expression:"" + no body, and its handler would then take the body branch
// against a body that does not exist.
//
// ParseTransformSpec is the single answer both sides read, so the question is
// settled here rather than per node.
func TestParseTransformSpecTreatsAnEmptyExpressionAsAbsent(t *testing.T) {
	spec, err := ParseTransformSpec(map[string]any{"expression": ""})
	if err == nil {
		t.Fatalf("expression:%q was accepted as a declared expression (got %+v); the "+
			"handler would take the body branch against a body that is not there", "", spec)
	}
}

// The mutual exclusion itself, on values rather than keys.
func TestParseTransformSpecRejectsBothForms(t *testing.T) {
	_, err := ParseTransformSpec(map[string]any{
		"expression": "$item * 2",
		"body":       map[string]any{"type": "xflow.subgraph"},
	})
	if err == nil {
		t.Fatal("expression and body are mutually exclusive; both were accepted")
	}
}

// An expression:"" paired with a real body is the body form, not a conflict.
// Keying the exclusion off presence rejects this; keying it off the value
// accepts it. The handler has always accepted it, so the compiler must too —
// otherwise a definition that runs correctly fails to compile.
func TestParseTransformSpecAcceptsAnEmptyExpressionBesideABody(t *testing.T) {
	spec, err := ParseTransformSpec(map[string]any{
		"expression": "",
		"body":       map[string]any{"type": "xflow.subgraph"},
	})
	if err != nil {
		t.Fatalf("empty expression beside a real body must read as the body form: %v", err)
	}
	if spec.Expression != "" || spec.Body == nil {
		t.Fatalf("got %+v, want the body form", spec)
	}
}

// Neither form declared is rejected: exactly one is required.
func TestParseTransformSpecRejectsNeitherForm(t *testing.T) {
	if _, err := ParseTransformSpec(map[string]any{"items": "$input.rows"}); err == nil {
		t.Fatal("a transform node with neither expression nor body was accepted")
	}
	if _, err := ParseTransformSpec(nil); err == nil {
		t.Fatal("a parameterless transform node was accepted")
	}
}

// A body that decodes but is not a sub-graph is a typo, not a payload. The
// returned spec names what was found so the caller can say so.
func TestParseTransformSpecRejectsANonSubgraphBody(t *testing.T) {
	_, err := ParseTransformSpec(map[string]any{"body": map[string]any{"n": 1}})
	if err == nil {
		t.Fatal("a body that is not a sub-graph was accepted")
	}
}

// The expression form round-trips its value: callers read spec.Expression
// instead of reaching back into Parameters.
func TestParseTransformSpecReturnsTheExpression(t *testing.T) {
	spec, err := ParseTransformSpec(map[string]any{"expression": "$item * 2"})
	if err != nil {
		t.Fatalf("the expression form must parse: %v", err)
	}
	if spec.Expression != "$item * 2" {
		t.Fatalf("Expression = %q, want %q", spec.Expression, "$item * 2")
	}
	if spec.Body != nil {
		t.Fatalf("the expression form must carry no body, got %+v", spec.Body)
	}
}
