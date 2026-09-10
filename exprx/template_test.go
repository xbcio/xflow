package exprx

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestRenderTemplateThreeRules(t *testing.T) {
	env := BuildExprEnv(&types.Input{
		Params: map[string]any{"amount": 1500, "order_id": "A-1"},
	}, nil)

	// Rule 1: expression mode, preserves return type.
	got, err := RenderTemplate("${{ $params.amount > 1000 }}", env)
	if err != nil {
		t.Fatalf("rule 1: %v", err)
	}
	if got != true {
		t.Errorf("rule 1 must preserve the bool type; got %#v (%T)", got, got)
	}

	// Rule 2: interpolation mode, concatenates into string.
	got, err = RenderTemplate("order {{ $params.order_id }} ok", env)
	if err != nil {
		t.Fatalf("rule 2: %v", err)
	}
	if got != "order A-1 ok" {
		t.Errorf("rule 2 = %#v, want %q", got, "order A-1 ok")
	}

	// Rule 3: static literal returned unchanged, type preserved.
	got, err = RenderTemplate(`{"a":1}`, env)
	if err != nil {
		t.Fatalf("rule 3: %v", err)
	}
	if got != `{"a":1}` {
		t.Errorf("rule 3 must return the literal unchanged; got %#v", got)
	}
}

// Evaluation failure errors must not echo the runtime value or result.
func TestRenderTemplateErrorDoesNotEchoTheValue(t *testing.T) {
	env := BuildExprEnv(&types.Input{
		Params: map[string]any{"secret": "s3cr3t-token-value"},
	}, nil)
	_, err := RenderTemplate("${{ $params.secret.nonexistent.deeper }}", env)
	// This used to be a t.Skip("expr tolerated this; pick another failing
	// form"), which meant the one assertion in the test — that a failing
	// evaluation does not put the secret in the error — died silently the
	// moment expr stopped rejecting this expression. A skip reads as ok, so
	// nothing would have said the leak check was no longer running.
	//
	// $params.secret is a string; indexing it with .nonexistent must fail in
	// any expr version, and the whole point of the test is what the failure
	// says. If a future expr does tolerate it, that is a real change to
	// investigate, not something to step around.
	if err == nil {
		t.Fatal("expected ${{ $params.secret.nonexistent.deeper }} to fail: a string has no " +
			"such field. If expr now tolerates it, this test's leak assertion has no failure " +
			"to inspect — pick another failing form rather than letting it pass vacuously")
	}
	if strings.Contains(err.Error(), "s3cr3t") {
		t.Errorf("error must not echo the value; got %q", err.Error())
	}
}

func TestRenderTemplateInterpolationMultipleSegments(t *testing.T) {
	env := BuildExprEnv(&types.Input{
		Params: map[string]any{"host": "api.example.com", "port": 8080},
	}, nil)
	got, err := RenderTemplate("https://{{ $params.host }}:{{ $params.port }}/v1", env)
	if err != nil {
		t.Fatalf("interpolation multi: %v", err)
	}
	if got != "https://api.example.com:8080/v1" {
		t.Errorf("got %#v, want %q", got, "https://api.example.com:8080/v1")
	}
}

func TestRenderTemplateUnterminatedReportsPositionNotContent(t *testing.T) {
	env := BuildExprEnv(&types.Input{
		Params: map[string]any{"secret": "credential-value"},
	}, nil)
	_, err := RenderTemplate("prefix {{ $params.secret", env)
	if err == nil {
		t.Fatal("expected error for unterminated template")
	}
	if strings.Contains(err.Error(), "credential-value") {
		t.Errorf("error must not contain the runtime value; got %q", err.Error())
	}
	// The "ReportsPosition" half of this test's name had no assertion at all —
	// it was an `if` with an empty body, so the only thing checked was that the
	// value did not leak. An error reading "unterminated template" with no
	// position satisfies that, and leaves an operator with a multi-kilobyte
	// config and nowhere to look.
	//
	// Checking for the word "offset" was the first repair and it was still
	// toothless: "offset" is a literal in template.go's fmt.Errorf format
	// string, so it is present for every possible input, including a computed
	// position that is wrong. The offset has to be checked by value. "{{" opens
	// at index 7 of "prefix {{ $params.secret" — that is the byte an operator
	// would seek to.
	//
	// Only the offset is required, not the absence of the expression source:
	// "$params.secret" is authored config and a future change that includes it
	// for diagnostics would be an improvement, not a regression. The value is
	// the thing that must never appear, and that is checked above.
	if !strings.Contains(err.Error(), "at offset 7") {
		t.Errorf("error must locate the failure at the byte where %q opens (offset 7) so "+
			"a caller can seek to it in a large template; got %q", "{{", err.Error())
	}
}
