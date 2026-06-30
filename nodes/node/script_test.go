package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

func TestScript_Factory(t *testing.T) {
	b := node.Script(`({result: $input.x * 2})`).Language("js").Runtime("qjs").Credentials("aes_key", "api_token")
	if b.NodeType() != "xflow.script" {
		t.Fatalf("type = %s", b.NodeType())
	}
	p := b.RawParams().(map[string]any)
	if p["language"] != "js" {
		t.Fatalf("language = %v", p["language"])
	}
	if p["runtime"] != "qjs" {
		t.Fatalf("runtime = %v", p["runtime"])
	}
	creds := p["credentials"].([]string)
	if len(creds) != 2 || creds[0] != "aes_key" {
		t.Fatalf("credentials = %v", creds)
	}
}

// TestScript_NoImplicitDefaults asserts the builder does NOT silently fill in
// language/runtime when the caller omitted them — they must be set explicitly
// so the security/perf tradeoff is always a conscious choice.
func TestScript_NoImplicitDefaults(t *testing.T) {
	p := node.Script(`({})`).RawParams().(map[string]any)
	if p["language"] != "" {
		t.Fatalf("expected empty language (no implicit default), got %v", p["language"])
	}
	if p["runtime"] != "" {
		t.Fatalf("expected empty runtime (no implicit default), got %v", p["runtime"])
	}
}

func TestScript_ExecExplicitJSGoja(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`({doubled: $input.x * 2})`).Language("js").Runtime("goja")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"x": 21.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" && out.Port != "" {
		t.Fatalf("expected main port, got %q", out.Port)
	}
	// goja Export() yields int64 for integer-valued JS numbers; compare
	// numerically rather than pinning a single Go numeric type.
	if got := asFloat(out.Data["doubled"]); got != 42.0 {
		t.Fatalf("doubled = %v (%T), want 42", out.Data["doubled"], out.Data["doubled"])
	}
}

func asFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

func TestScript_RuntimeError_ErrorPort(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`throw new Error('boom')`).Language("js").Runtime("goja")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("runtime error should route to port, not Go error: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
	if out.Data["error"] == nil {
		t.Fatal("error port should carry error message")
	}
}

func TestScript_MissingCode_ConfigError(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	input := &node.Input{Params: map[string]any{}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing code")
	}
}

func TestScript_UnknownRuntime_ConfigError(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	input := &node.Input{Params: map[string]any{"code": `({})`, "language": "js", "runtime": "v8"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for unknown runtime")
	}
}

// TestScript_MissingLanguage_ConfigError covers the "no implicit default"
// contract on the runtime path: an Input that supplies code but no language
// must surface a config error rather than silently picking js.
func TestScript_MissingLanguage_ConfigError(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	input := &node.Input{Params: map[string]any{"code": `({})`, "runtime": "goja"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing language")
	}
}

// TestScript_MissingRuntime_ConfigError is the runtime counterpart: language
// supplied but runtime omitted must also be a config error, not "guess goja
// because language is js".
func TestScript_MissingRuntime_ConfigError(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	input := &node.Input{Params: map[string]any{"code": `({})`, "language": "js"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing runtime")
	}
}

func TestScript_CredentialInjected(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`({token: $credential.token})`).Language("js").Runtime("goja").Credentials("api_token")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		if name == "api_token" {
			return map[string]any{"token": "secret-t"}
		}
		return nil
	})
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["token"] != "secret-t" {
		t.Fatalf("token = %v, want secret-t", out.Data["token"])
	}
}

func TestScript_UndeclaredCredentialInvisible(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`({seen: typeof $credentials.api_token})`).Language("js").Runtime("goja")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(string) map[string]any {
		return map[string]any{"token": "leak"}
	})
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["seen"] != "undefined" {
		t.Fatalf("gate violated: undeclared credential visible (%v)", out.Data["seen"])
	}
}

// TestScript_OutputSizeLimit verifies the node-layer cap routes oversize
// results to the error port instead of returning them to downstream nodes.
// A 1.5 MiB string blows past the 1 MiB default.
func TestScript_OutputSizeLimit(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	// 'A'.repeat(1572864) produces a ~1.5 MiB result; the JSON encoding adds
	// quoting overhead but the raw payload alone already exceeds 1 MiB.
	b := node.Script(`({blob: 'A'.repeat(1572864)})`).Language("js").Runtime("goja")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("oversized result should route to error port, not Go error: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
	msg, _ := out.Data["error"].(string)
	if msg == "" || !contains(msg, "exceeds limit") {
		t.Fatalf("expected size-limit error message, got %q", msg)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
