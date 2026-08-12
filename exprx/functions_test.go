package exprx

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestSprintfRegistered is the regression test for the missing-function gap.
//
// docs/design/DSL-SPECIFICATION.md §4.3 advertises sprintf, and
// docs/dsl-samples/purchase-approval.yaml uses it seven times, but expr-lang
// has no such builtin: every one of those expressions fails to compile with
// "unknown name sprintf". The failure surfaces at run time, not at submit
// time, because the parameter boundary is where templates are evaluated.
func TestSprintfRegistered(t *testing.T) {
	got, err := EvalExpr(`sprintf("order %s totals %d", id, n)`,
		map[string]any{"id": "A-1", "n": 3}, false)
	if err != nil {
		t.Fatalf("sprintf is advertised by DSL-SPECIFICATION.md §4.3 and used by "+
			"docs/dsl-samples/purchase-approval.yaml, so it must compile: %v", err)
	}
	if got != "order A-1 totals 3" {
		t.Fatalf("sprintf = %#v, want %q", got, "order A-1 totals 3")
	}
}

// TestSprintfNoArgs covers the degenerate call: a format string alone is a
// legal fmt.Sprintf call and must not panic on the variadic slice.
func TestSprintfNoArgs(t *testing.T) {
	got, err := EvalExpr(`sprintf("plain")`, map[string]any{}, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != "plain" {
		t.Fatalf("sprintf = %#v, want %q", got, "plain")
	}
}

// TestSprintfRejectsNonStringFormat guards the type assertion. expr passes
// arguments as any, so a non-string first argument must produce an error
// rather than a panic that takes down the whole node.
func TestSprintfRejectsNonStringFormat(t *testing.T) {
	_, err := EvalExpr(`sprintf(n)`, map[string]any{"n": 42}, false)
	if err == nil {
		t.Fatal("expected an error when the format argument is not a string")
	}
}

// TestSprintfNoArgumentsAtAll covers sprintf() with an empty argument list.
// expr allows it (the function is variadic), so the implementation must not
// index p[0] unguarded.
func TestSprintfNoArgumentsAtAll(t *testing.T) {
	_, err := EvalExpr(`sprintf()`, map[string]any{}, false)
	if err == nil {
		t.Fatal("expected an error when sprintf is called with no arguments")
	}
}

// TestSprintfSurvivesTheProgramCache is the reason the function is registered
// on CompileExpr rather than injected into the env map.
//
// A registered function is baked into the compiled *vm.Program, so it travels
// with the cache entry. An env-injected function would have to be present in
// every env map passed to Run, and BuildExprEnv's callers pass a dozen
// different maps -- one omission and a cached program built with the function
// would fail at Run against an env without it.
func TestSprintfSurvivesTheProgramCache(t *testing.T) {
	const code = `sprintf("v=%d", n)`
	p1, err := CompileExpr(code, map[string]any{"n": 1}, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	p2, err := CompileExpr(code, map[string]any{"n": 2}, false)
	if err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if p1 != p2 {
		t.Fatal("expected the cached program to be reused")
	}
	// Run the cached program against an env it was never compiled against.
	got, err := EvalExpr(code, map[string]any{"n": 9}, false)
	if err != nil {
		t.Fatalf("running a cached program against a fresh env: %v", err)
	}
	if got != "v=9" {
		t.Fatalf("got %#v, want %q", got, "v=9")
	}
}

// TestSprintfInTemplate is the end-to-end shape: sprintf inside a {{ }}
// interpolation, which is how purchase-approval.yaml uses it.
func TestSprintfInTemplate(t *testing.T) {
	input := &types.Input{Data: map[string]any{"amount": 1200}}
	env := BuildExprEnv(input, nil)
	got, err := RenderTemplate(`total: {{ sprintf("%.2f", float($input.amount)) }}`, env)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got != "total: 1200.00" {
		t.Fatalf("rendered = %#v, want %q", got, "total: 1200.00")
	}
}

// TestSprintfErrorCarriesNoValue pins the §7 constraint on the error path.
//
// The rejected format argument is a map holding a credential -- the shape a
// $credentials entry actually takes. Formatting it into the error with %v (the
// obvious way to write "got X") would print the secret. The error must name
// the TYPE only.
func TestSprintfErrorCarriesNoValue(t *testing.T) {
	const secret = "ulp-abcdef0123456789"
	env := map[string]any{"cred": map[string]any{"token": secret}}
	_, err := EvalExpr(`sprintf(cred)`, env, false)
	if err == nil {
		t.Fatal("expected an error when the format argument is not a string")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("the error leaked the argument value; a format argument can hold "+
			"a credential (DSL-SPECIFICATION.md §4.2 $credentials): %q", err.Error())
	}
}
