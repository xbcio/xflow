package js

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/nodes/node/script"
)

func newQJS(t *testing.T) script.Engine {
	t.Helper()
	e, ok := script.Lookup("js", "qjs")
	if !ok {
		t.Fatal("qjs engine not registered")
	}
	return e
}

func TestQJS_ObjectCompletion(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({status: 'ok', len: $input.name.length})`,
		map[string]any{"$input": map[string]any{"name": "abcd"}},
		script.DefaultHelpers())
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
		script.DefaultHelpers())
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
		nil, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["enc"] != "aGk=" {
		t.Fatalf("base64 helper wrong: %v", out)
	}
}

func TestQJS_SandboxNoIO(t *testing.T) {
	out, err := newQJS(t).Execute(context.Background(),
		`({hasRequire: typeof require, hasFetch: typeof fetch, hasProcess: typeof process})`,
		nil, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	for _, k := range []string{"hasRequire", "hasFetch", "hasProcess"} {
		if m[k] != "undefined" {
			t.Fatalf("sandbox leak: %s = %v", k, m[k])
		}
	}
}

func TestQJS_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := newQJS(t).Execute(ctx, `while(true){}`, nil, script.DefaultHelpers())
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
