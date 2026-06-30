package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

func TestScript_Factory(t *testing.T) {
	b := node.Script(`({result: $input.x * 2})`).Runtime("qjs").Credentials("aes_key", "api_token")
	if b.NodeType() != "xflow.script" {
		t.Fatalf("type = %s", b.NodeType())
	}
	p := b.RawParams().(map[string]any)
	if p["runtime"] != "qjs" {
		t.Fatalf("runtime = %v", p["runtime"])
	}
	creds := p["credentials"].([]string)
	if len(creds) != 2 || creds[0] != "aes_key" {
		t.Fatalf("credentials = %v", creds)
	}
}

func TestScript_Defaults(t *testing.T) {
	p := node.Script(`({})`).RawParams().(map[string]any)
	if _, ok := p["runtime"]; ok {
		t.Fatal("default runtime should be omitted from RawParams")
	}
	if _, ok := p["language"]; ok {
		t.Fatal("default language should be omitted from RawParams")
	}
}

func TestScript_ExecJSDefault(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`({doubled: $input.x * 2})`)
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
	b := node.Script(`throw new Error('boom')`)
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
	input := &node.Input{Params: map[string]any{"code": `({})`, "runtime": "v8"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for unknown runtime")
	}
}

func TestScript_CredentialInjected(t *testing.T) {
	h, _ := node.Lookup("xflow.script")
	b := node.Script(`({token: $credential.token})`).Credentials("api_token")
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
	b := node.Script(`({seen: typeof $credentials.api_token})`)
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
