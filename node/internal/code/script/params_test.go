package script_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestScript_CodeNotEchoedIntoGlobals is the regression test for a defect that
// made every wasm reactor workflow fail in production while the entire wasm test
// suite stayed green.
//
// ScriptNode.Execute reads the script from input.Params["code"] and then passes
// the WHOLE Params map into the engine as the $params global. So the script's own
// source was handed back to it as part of its input — on every single message.
// For a base64-encoded wasm module that is ~9 MB per call: the guest's alloc
// export asked Go's allocator for a 9 MB slice inside a 32-bit linear memory and
// trapped, surfacing as the opaque `wasm reactor: alloc: wasm error: unreachable`
// with a stack ending in runtime.mallocgcLarge.
//
// Every pre-existing wasm reactor test missed this because they call the engine
// directly with hand-built globals, so no $params root ever existed. The defect
// lived exclusively in the seam between ScriptNode and the engine, which is why
// this test asserts on what the engine RECEIVES rather than on what it returns.
func TestScript_CodeNotEchoedIntoGlobals(t *testing.T) {
	captured := captureGlobals(t)

	// Stand in for a real wasm module: large enough that echoing it back is
	// unmistakable, and distinctive enough to find inside a nested map.
	bigCode := "MODULEBYTES" + strings.Repeat("A", 512<<10)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(bigCode).Language("js").Runtime(captureRuntime)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"path": "/admin/users"},
	}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	globals := captured.globals
	if globals == nil {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}

	// The node's own input must still be there — this test must not pass by
	// having emptied the environment.
	if globals["path"] != "/admin/users" {
		t.Fatalf("$input data missing from globals: %v keys=%v", globals["path"], keysOf(globals))
	}

	// The script source must not be reachable from ANY root. Checking the
	// serialized whole environment rather than just globals["$params"]["code"]
	// is deliberate: the payload is what gets encoded and handed to the guest, so
	// a fix that merely moved the code to another key would still ship 9 MB per
	// message and must still fail here.
	if where := findBigCode(globals, bigCode, ""); where != "" {
		t.Fatalf("script source is echoed back into the engine environment at %s;\n"+
			"this is %d bytes handed to the script as its own input on every message",
			where, len(bigCode))
	}
}

// TestScript_ParamsStillCarryNonCodeEntries pins that the fix removed ONLY the
// code, not the $params root itself. A script legitimately reads its own
// configuration through $params, so dropping the whole root would trade a
// memory defect for a silent behaviour change — and the test above would pass
// either way.
func TestScript_ParamsStillCarryNonCodeEntries(t *testing.T) {
	captured := captureGlobals(t)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script("code-body").Language("js").Runtime(captureRuntime)
	params := b.RawParams().(map[string]any)
	params["threshold"] = 42
	params["mode"] = "strict"

	input := &types.Input{Params: params, Data: map[string]any{}}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	p, ok := captured.globals["$params"].(map[string]any)
	if !ok {
		t.Fatalf("$params root is missing or not a map: %T", captured.globals["$params"])
	}
	if p["threshold"] != 42 || p["mode"] != "strict" {
		t.Fatalf("$params lost non-code entries: %v", p)
	}
	if _, present := p["code"]; present {
		t.Fatalf("$params still carries the code key: %v", keysOf(p))
	}
	// language and runtime are how a script can tell which engine it is on;
	// they are small and must survive.
	if p["language"] != "js" || p["runtime"] != captureRuntime {
		t.Fatalf("$params lost language/runtime: %v", p)
	}
}

// TestScript_CallerParamsNotMutated pins that stripping the code does not write
// through to the caller's map. input.Params is owned by the engine's activation
// record; clearing "code" in place would make the node unable to run a second
// time — the second Execute would fail "code parameter is required".
func TestScript_CallerParamsNotMutated(t *testing.T) {
	captureGlobals(t)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script("original-code").Language("js").Runtime(captureRuntime)
	params := b.RawParams().(map[string]any)
	input := &types.Input{Params: params, Data: map[string]any{}}

	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if params["code"] != "original-code" {
		t.Fatalf("caller's Params was mutated: code = %v", params["code"])
	}
	// The real proof: a second run must still find its code.
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("second execute (code was consumed by the first): %v", err)
	}
}

// findBigCode reports the path at which needle appears anywhere in the
// environment, walking maps and slices. It returns "" when the source is
// genuinely absent.
func findBigCode(v any, needle, path string) string {
	switch t := v.(type) {
	case string:
		if strings.Contains(t, needle) {
			return path
		}
	case map[string]any:
		for k, vv := range t {
			if hit := findBigCode(vv, needle, path+"."+k); hit != "" {
				return hit
			}
		}
	case []any:
		for i, vv := range t {
			if hit := findBigCode(vv, needle, path+"[]"); hit != "" {
				return hit
			}
			_ = i
		}
	}
	return ""
}

func keysOf(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
