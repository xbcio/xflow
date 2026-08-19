package script

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestRootsProjectionDropsBothCopies pins the property the whole feature exists
// for: a declared root must remove the record from BOTH places BuildExprEnv puts
// it — flattened at the env top level AND nested under $input.
//
// Removing only the top-level copy would look like it works (the key is gone
// from a shallow scan) while forfeiting the entire saving, because $input still
// carries the identical bytes. That is the failure stripItems documents and the
// one the cleansed credentials hit when they reached storage through their
// $input copy, so it gets a test that scans for the VALUE rather than the key.
func TestRootsProjectionDropsBothCopies(t *testing.T) {
	const marker = "SENTINEL-RECORD-BODY-8f3a"
	input := &types.Input{
		Data: map[string]any{
			"$item": map[string]any{"body": marker},
			"tags":  []any{"keep-me"},
		},
	}

	env := buildScriptGlobals(input, nil, nil)
	if !strings.Contains(marshal(t, env), marker) {
		t.Fatal("undeclared env lost the record — the probe is not measuring what it claims")
	}

	input.Params = map[string]any{"roots": []string{"tags"}}
	got := marshal(t, buildScriptGlobals(input, nil, nil))
	if strings.Contains(got, marker) {
		t.Errorf("declared roots=[tags] but the record survived in the payload: %s", got)
	}
	if !strings.Contains(got, "keep-me") {
		t.Errorf("declared root tags was dropped: %s", got)
	}
}

// A guest that declares $item gets it once, not twice. This is the SAS decode
// node's shape: it reads env["$item"] and nothing else.
func TestRootsProjectionKeepsDeclaredRootOnce(t *testing.T) {
	const marker = "SENTINEL-RECORD-BODY-8f3a"
	input := &types.Input{
		Data:   map[string]any{"$item": map[string]any{"body": marker}},
		Params: map[string]any{"roots": []string{"$item"}},
	}

	got := marshal(t, buildScriptGlobals(input, nil, nil))
	if n := strings.Count(got, marker); n != 1 {
		t.Errorf("record appears %d time(s) in payload, want 1: %s", n, got)
	}
}

// Declaring "$input" means "the whole input map", which is SAS's clean guest:
// it reads env["$input"] wholesale. Rebuilding $input from the allowlist would
// hand that guest an empty map and silently pass through every message.
func TestRootsProjectionInputDeclaredKeepsWholeMap(t *testing.T) {
	input := &types.Input{
		Data:   map[string]any{"method": "GET", "uri": "/v1/users"},
		Params: map[string]any{"roots": []string{"$input"}},
	}

	env := buildScriptGlobals(input, nil, nil)
	inner, ok := env["$input"].(map[string]any)
	if !ok {
		t.Fatalf("$input missing or wrong type: %T", env["$input"])
	}
	if inner["method"] != "GET" || inner["uri"] != "/v1/users" {
		t.Errorf("$input was rebuilt from the allowlist instead of kept whole: %v", inner)
	}
	// The flattened top-level copy is exactly what the declaration buys back:
	// $input already carries these keys, so shipping them again is the
	// duplication the projection exists to remove.
	if _, dup := env["method"]; dup {
		t.Error("top-level duplicate of input.Data survived a $input declaration")
	}
}

// Declaring nothing must change nothing. Every existing script — including
// wasm/testdata/tagger, which reads the flattened top-level keys as its record —
// runs without a declaration, so this is the compatibility gate.
func TestRootsProjectionAbsentDeclarationIsIdentity(t *testing.T) {
	input := &types.Input{Data: map[string]any{"$item": "x", "body": "y"}}

	env := buildScriptGlobals(input, nil, nil)
	for _, k := range []string{"$item", "body", "$input", "$vars", "$runtime"} {
		if _, ok := env[k]; !ok {
			t.Errorf("undeclared env is missing %q — declaring nothing must be identity", k)
		}
	}
}

// $config is consumed host-side by wasm.splitConfig before the payload is
// encoded, so a projection that drops it leaves the reactor pool unconfigured —
// a failure that surfaces as wrong results rather than an error. Credentials get
// the same protection: a Roots() declaration must not silently revoke what a
// Credentials() declaration delivered.
func TestRootsProjectionKeepsEngineRoots(t *testing.T) {
	input := &types.Input{
		Data:   map[string]any{"x": 1},
		Params: map[string]any{"roots": []string{"x"}},
	}
	creds := map[string]any{"api_token": "t"}

	env := buildScriptGlobals(input, creds, creds["api_token"])
	if _, ok := env["$credentials"]; !ok {
		t.Error("$credentials dropped by a roots declaration")
	}
	if _, ok := env["$credential"]; !ok {
		t.Error("$credential dropped by a roots declaration")
	}

	// $config arrives on the env rather than through input.Data, so it is checked
	// through projectRoots directly.
	out := projectRoots(map[string]any{"$config": "cfg", "junk": 1}, []string{"nothing"})
	if _, ok := out["$config"]; !ok {
		t.Error("$config dropped by a roots declaration — the reactor pool would go unconfigured")
	}
}

// projectRoots must not mutate what it was handed: env belongs to the engine's
// activation record, and an in-place delete would corrupt a retry of the same
// node — and would strip the roots from the js and expression paths that still
// promise them.
func TestRootsProjectionDoesNotMutateSource(t *testing.T) {
	inner := map[string]any{"keep": 1, "drop": 2}
	env := map[string]any{"keep": 1, "drop": 2, "$input": inner}

	out := projectRoots(env, []string{"keep"})

	if _, ok := env["drop"]; !ok {
		t.Error("projectRoots deleted from the source env in place")
	}
	if _, ok := env["$input"]; !ok {
		t.Error("projectRoots deleted $input from the source env in place")
	}
	if _, ok := out["$input"]; ok {
		t.Error("undeclared $input survived the projection — it is pure duplication")
	}
}

func marshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
