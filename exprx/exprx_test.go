package exprx

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestCompileExprCachesAndReuses(t *testing.T) {
	env := map[string]any{"x": 1}
	p1, err := CompileExpr("x + 1", env, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	p2, err := CompileExpr("x + 1", env, false)
	if err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if p1 != p2 {
		t.Fatal("expected cached program to be the same pointer")
	}
}

func TestCompileExprAsBoolFlagDistinct(t *testing.T) {
	env := map[string]any{"x": true}
	pBool, err := CompileExpr("x", env, true)
	if err != nil {
		t.Fatalf("compile bool: %v", err)
	}
	pAny, err := CompileExpr("x", env, false)
	if err != nil {
		t.Fatalf("compile any: %v", err)
	}
	if pBool == pAny {
		t.Fatal("expected different cache entries for asBool true/false")
	}
}

func TestCompileExprInvalidSyntax(t *testing.T) {
	if _, err := CompileExpr("x +", map[string]any{"x": 1}, false); err == nil {
		t.Fatal("expected compile error for invalid syntax")
	}
}

func TestEvalExpr(t *testing.T) {
	env := map[string]any{"x": 2, "y": 3}
	got, err := EvalExpr("x * y", env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 6 {
		t.Fatalf("got %v, want 6", got)
	}
}

func TestEvalExprBoolMode(t *testing.T) {
	env := map[string]any{"x": 5}
	got, err := EvalExpr("x > 3", env, true)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	b, ok := got.(bool)
	if !ok {
		t.Fatalf("expected bool, got %T", got)
	}
	if !b {
		t.Fatal("expected true")
	}
}

func TestEvalExprBoolModeRejectsNonBool(t *testing.T) {
	env := map[string]any{"x": 5}
	if _, err := EvalExpr("x", env, true); err == nil {
		t.Fatal("expected error when non-bool result required as bool")
	}
}

func TestEvalExprCompileError(t *testing.T) {
	if _, err := EvalExpr("(x", map[string]any{}, false); err == nil {
		t.Fatal("expected compile error")
	}
}

func TestEvalExprRuntimeError(t *testing.T) {
	// Accessing an undefined variable at runtime fails evaluation even though
	// the env used for compile-time inference is typed.
	if _, err := EvalExpr("nope", map[string]any{}, false); err == nil {
		t.Fatal("expected runtime error")
	}
}

func TestBuildExprEnv(t *testing.T) {
	input := &types.Input{
		Data:   map[string]any{"foo": "bar"},
		Inputs: map[string]any{"p1": "v1"},
		Vars:   map[string]any{"count": 1},
		Config: map[string]any{"region": "us"},
		Params: map[string]any{"limit": 10},
	}
	extra := map[string]any{"$credentials": map[string]any{"k": "v"}}
	env := BuildExprEnv(input, extra)

	if env["foo"] != "bar" {
		t.Fatalf("data spread missing: %v", env["foo"])
	}
	// Each root is checked by the value the fixture put in it, not by
	// non-nilness. Every root here is a non-nil map, so "!= nil" was green for
	// any wiring that assigned the wrong one -- env["$vars"] = input.Config is
	// a plausible copy/paste and makes every $vars.* expression in every
	// workflow read config data instead, with no diagnostic anywhere.
	roots := []struct {
		name string
		key  string
		val  any
	}{
		{"$input", "foo", "bar"},
		{"$inputs", "p1", "v1"},
		{"$vars", "count", 1},
		{"$config", "region", "us"},
		{"$params", "limit", 10},
		{"$credentials", "k", "v"},
	}
	for _, r := range roots {
		m, ok := env[r.name].(map[string]any)
		if !ok {
			t.Fatalf("%s = %#v, want map[string]any", r.name, env[r.name])
		}
		if m[r.key] != r.val {
			t.Errorf("%s[%q] = %#v, want %#v (a root is wired to the wrong field)",
				r.name, r.key, m[r.key], r.val)
		}
	}
	if env["$runtime"] == nil {
		t.Fatal("$runtime missing")
	}

	// Extra should be able to overwrite data-spread keys.
	env2 := BuildExprEnv(input, map[string]any{"foo": "overwritten"})
	if env2["foo"] != "overwritten" {
		t.Fatalf("extra did not overwrite spread key: %v", env2["foo"])
	}
}

func TestRuntimeEnv(t *testing.T) {
	t.Run("nil runtime", func(t *testing.T) {
		env := RuntimeEnv(&types.Input{})
		// RuntimeEnv initializes env["vars"] to map[string]any(nil) when
		// input.Runtime is nil. The interface holds a typed nil map, so it
		// is non-nil; assert emptiness by length instead.
		vars, ok := env["vars"].(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", env["vars"])
		}
		if len(vars) != 0 {
			t.Fatalf("expected empty vars, got %v", vars)
		}
	})
	t.Run("with runtime vars", func(t *testing.T) {
		rt := &types.Runtime{Vars: map[string]any{"a": 1}}
		env := RuntimeEnv(&types.Input{Runtime: rt})
		vars, ok := env["vars"].(map[string]any)
		if !ok {
			t.Fatalf("expected map, got %T", env["vars"])
		}
		if vars["a"] != 1 {
			t.Fatalf("expected a=1, got %v", vars["a"])
		}
	})
}

func TestEvalExprAgainstBuiltEnv(t *testing.T) {
	// Integration: BuildExprEnv feeds EvalExpr with $input access.
	input := &types.Input{Data: map[string]any{"count": 4}}
	env := BuildExprEnv(input, nil)
	got, err := EvalExpr("$input.count * 2", env, false)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 8 {
		t.Fatalf("got %v, want 8", got)
	}
}
