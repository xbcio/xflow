package js

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

func newQJS(t *testing.T) engine.Engine {
	t.Helper()
	e, ok := engine.Lookup("js", "qjs")
	if !ok {
		t.Fatal("qjs engine not registered")
	}
	return e
}

func TestQJS_ObjectCompletion(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({status: 'ok', len: $input.name.length})`,
		map[string]any{"$input": map[string]any{"name": "abcd"}},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["status"] != "ok" {
		t.Fatalf("status = %v", out)
	}
}

func TestQJS_ReadsCredential(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({t: $credential.token, k: $credentials.aes_key.key})`,
		map[string]any{
			"$credential":  map[string]any{"token": "t-1"},
			"$credentials": map[string]any{"aes_key": map[string]any{"key": "kk"}},
		},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	if m["t"] != "t-1" || m["k"] != "kk" {
		t.Fatalf("credential read wrong: %v", m)
	}
}

func TestQJS_HelpersBase64(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({enc: $helpers.base64Encode('hi')})`,
		nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["enc"] != "aGk=" {
		t.Fatalf("base64 helper wrong: %v", out)
	}
}

func TestQJS_SandboxNoIO(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({hasRequire: typeof require, hasFetch: typeof fetch, hasProcess: typeof process,
		   hasStd: typeof std, hasOs: typeof os, hasPrint: typeof print, hasScriptArgs: typeof scriptArgs,
		   hasConsole: typeof console, hasBjson: typeof bjson, hasPerformance: typeof performance,
		   hasNavigator: typeof navigator, hasGc: typeof gc, hasQueueMicrotask: typeof queueMicrotask,
		   hasSetTimeout: typeof setTimeout, hasSetInterval: typeof setInterval,
		   hasClearTimeout: typeof clearTimeout, hasClearInterval: typeof clearInterval})`,
		nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	for _, k := range []string{
		"hasRequire", "hasFetch", "hasProcess",
		"hasStd", "hasOs", "hasPrint", "hasScriptArgs",
		"hasConsole", "hasBjson", "hasPerformance", "hasNavigator", "hasGc",
		"hasQueueMicrotask", "hasSetTimeout", "hasSetInterval",
		"hasClearTimeout", "hasClearInterval",
	} {
		if m[k] != "undefined" {
			t.Fatalf("sandbox leak: %s = %v, want undefined", k, m[k])
		}
	}
}

// TestQJS_SandboxFileIOUnreachable proves the host filesystem capability is
// actually gone, not merely shadowed: os must be undefined so os.readdir cannot
// be reached at all from a user script.
func TestQJS_SandboxFileIOUnreachable(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({osType: typeof os, readdir: (typeof os !== 'undefined' && typeof os.readdir),
		   stdType: typeof std, open: (typeof std !== 'undefined' && typeof std.open)})`,
		nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	if m["osType"] != "undefined" {
		t.Fatalf("os still present: %v", m["osType"])
	}
	if m["stdType"] != "undefined" {
		t.Fatalf("std still present: %v", m["stdType"])
	}
	if m["readdir"] != false {
		t.Fatalf("os.readdir reachable: %v", m["readdir"])
	}
	if m["open"] != false {
		t.Fatalf("std.open reachable: %v", m["open"])
	}
}

func TestQJS_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := newQJS(t).Execute(ctx, `while(true){}`, nil, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// TestQJS_Warmup verifies the warmup hook runs to completion (the QuickJS
// wasm build cache is process-wide, so a strict cold-vs-hot timing assertion
// is flaky — this test guards the contract: Warmup returns nil and does not
// break subsequent Execute calls.
func TestQJS_Warmup(t *testing.T) {
	if err := Warmup(context.Background()); err != nil {
		t.Fatalf("Warmup: %v", err)
	}
	// After warmup, normal execution must still succeed.
	out, err := newQJS(t).Execute(context.Background(),
		`({ok: 1+1})`, nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("post-warmup execute: %v", err)
	}
	if out.(map[string]any)["ok"] != float64(2) {
		t.Fatalf("ok = %v", out.(map[string]any)["ok"])
	}
}
