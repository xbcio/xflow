package js

import "testing"

func TestBuildGlobals_Keys(t *testing.T) {
	globals := map[string]any{
		"$input":       map[string]any{"x": 1.0},
		"$credentials": map[string]any{"k": map[string]any{"token": "t"}},
		"$credential":  map[string]any{"token": "t"},
	}
	g := BuildGlobals(globals)
	for _, k := range []string{"$input", "$credentials", "$credential"} {
		if _, ok := g[k]; !ok {
			t.Fatalf("missing global %q", k)
		}
	}
}
