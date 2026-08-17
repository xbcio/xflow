package js

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

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

func TestJSFamily_HelpersConsistent(t *testing.T) {
	code := `({enc: $helpers.base64Encode('xyz'), dec: $helpers.base64Decode('eHl6')})`
	results := map[string]map[string]any{}
	for _, rt := range []string{"goja", "qjs"} {
		e, ok := engine.Lookup("js", rt)
		if !ok {
			t.Fatalf("%s not registered", rt)
		}
		out, err := e.Execute(context.Background(), engine.Code(code), nil, engine.DefaultHelpers())
		if err != nil {
			t.Fatalf("%s exec error: %v", rt, err)
		}
		results[rt] = out.(map[string]any)
	}
	if results["goja"]["enc"] != results["qjs"]["enc"] || results["goja"]["dec"] != results["qjs"]["dec"] {
		t.Fatalf("js family inconsistent: goja=%v qjs=%v", results["goja"], results["qjs"])
	}
}
