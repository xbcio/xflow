package script_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
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
	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`({doubled: $input.x * 2})`).Language("js").Runtime("goja")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"x": 21.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Execute's single-record success path explicitly sets Port: "main"
	// (script.go); the previous "main" or "" check let that literal be
	// silently dropped to "" without ever turning the suite red.
	if out.Port != "main" {
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
	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`throw new Error('boom')`).Language("js").Runtime("goja")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
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
	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{Params: map[string]any{}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing code")
	}
}

func TestScript_UnknownRuntime_ConfigError(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{Params: map[string]any{"code": `({})`, "language": "js", "runtime": "v8"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for unknown runtime")
	}
}

// TestScript_MissingLanguage_ConfigError covers the "no implicit default"
// contract on the runtime path: an Input that supplies code but no language
// must surface a config error rather than silently picking js.
func TestScript_MissingLanguage_ConfigError(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{Params: map[string]any{"code": `({})`, "runtime": "goja"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing language")
	}
}

// TestScript_MissingRuntime_ConfigError is the runtime counterpart: language
// supplied but runtime omitted must also be a config error, not "guess goja
// because language is js".
func TestScript_MissingRuntime_ConfigError(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{Params: map[string]any{"code": `({})`, "language": "js"}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error for missing runtime")
	}
}

// TestScript_ConfigErrorsArePermanent verifies the A3 code/script classifier
// migration (2026-07-18 remediation §6.4 step 5): script config errors that
// cannot self-heal on retry are NewPermanentError, not bare fmt.Errorf.
func TestScript_ConfigErrorsArePermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")

	cases := []struct {
		name   string
		params map[string]any
		code   string
	}{
		{"missing_code", map[string]any{}, "script.code_required"},
		{"missing_language", map[string]any{"code": `({})`, "runtime": "goja"}, "script.language_required"},
		{"missing_runtime", map[string]any{"code": `({})`, "language": "js"}, "script.runtime_required"},
		{"unknown_engine", map[string]any{"code": `({})`, "language": "js", "runtime": "v8"}, "script.unknown_engine"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			input := &types.Input{Params: c.params}
			_, err := h.Execute(context.Background(), input)
			if err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
			if !types.IsPermanent(err) {
				t.Fatalf("%s must be permanent (not retried); got err=%v", c.name, err)
			}
			var ce *types.ClassifiedError
			if !errors.As(err, &ce) || ce.Code != c.code {
				t.Fatalf("expected ClassifiedError code=%q, got %T %v", c.code, err, err)
			}
		})
	}
}

// TestScript_CancelledContextIsTransient verifies that when the per-execution
// context expires (deadline/cancellation), the script node classifies the
// failure as transient with code script.timeout regardless of how the runtime
// surfaces it (goja raises an Interrupt error, qjs/wazero wrap ctx.Err()).
// A pre-cancelled context makes the test deterministic without depending on
// wall-clock timeout races.
func TestScript_CancelledContextIsTransient(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`({})`).Language("js").Runtime("goja")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &types.Input{Params: b.RawParams().(map[string]any), Timeout: 10 * time.Second}
	_, err := h.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected transient error for cancelled execution context")
	}
	if types.IsPermanent(err) {
		t.Fatalf("cancelled context must be transient (retryable); got permanent err=%v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "script.timeout" {
		t.Fatalf("expected ClassifiedError code=script.timeout, got %T %v", err, err)
	}
}

func TestScript_CredentialInjected(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`({token: $credential.token})`).Language("js").Runtime("goja").Credentials("api_token")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	input.SetNamespace(namespace.Default)
	input.SetCredentialResolver(func(namespace namespace.Namespace, name string) map[string]any {
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
	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`({seen: typeof $credentials.api_token})`).Language("js").Runtime("goja")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	input.SetNamespace(namespace.Default)
	input.SetCredentialResolver(func(namespace namespace.Namespace, name string) map[string]any {
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
	h, _ := registry.Lookup("xflow.script")
	// 'A'.repeat(1572864) produces a ~1.5 MiB result; the JSON encoding adds
	// quoting overhead but the raw payload alone already exceeds 1 MiB.
	b := node.Script(`({blob: 'A'.repeat(1572864)})`).Language("js").Runtime("goja")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
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

func TestScript_ExecuteNotifiesObserver(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	b := node.Script(`({doubled: $input.x * 2})`).Language("js").Runtime("goja")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"x": 21.0},
	}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rec.executes != 1 {
		t.Fatalf("execute notifications = %d, want 1", rec.executes)
	}
	if rec.lastOutcome != "main" {
		t.Fatalf("outcome = %q, want main", rec.lastOutcome)
	}
	if rec.lastLanguage != "js" || rec.lastRuntime != "goja" {
		t.Fatalf("language/runtime = %q/%q, want js/goja", rec.lastLanguage, rec.lastRuntime)
	}
	if rec.outputBytes <= 0 {
		t.Fatalf("output bytes = %d, want > 0", rec.outputBytes)
	}
}

func TestScript_ConfigErrorNotifiesObserver(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	input := &types.Input{Params: map[string]any{}}
	if _, err := h.Execute(context.Background(), input); err == nil {
		t.Fatal("expected Go config error")
	}

	if rec.executes != 1 || rec.lastOutcome != "config" {
		t.Fatalf("execute notifications = %d, outcome = %q; want 1/config", rec.executes, rec.lastOutcome)
	}
	if rec.outputBytes != 0 {
		t.Fatalf("output bytes = %d, want 0 for config error", rec.outputBytes)
	}
}

func TestScript_RuntimeErrorNotifiesObserver(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	b := node.Script(`throw new Error('boom')`).Language("js").Runtime("goja")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("runtime error should route to port: %v", err)
	}

	if rec.executes != 1 || rec.lastOutcome != "error" {
		t.Fatalf("execute notifications = %d, outcome = %q; want 1/error", rec.executes, rec.lastOutcome)
	}
	if rec.outputBytes != 0 {
		t.Fatalf("output bytes = %d, want 0 for runtime error", rec.outputBytes)
	}
}

type recordingScriptObserver struct {
	executes     int
	lastOutcome  string
	lastLanguage string
	lastRuntime  string
	outputBytes  int
}

func (r *recordingScriptObserver) OnScriptExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration) {
	r.executes++
	r.lastOutcome = outcome
	r.lastLanguage = language
	r.lastRuntime = runtime
}

func (r *recordingScriptObserver) OnScriptOutputBytes(ctx context.Context, language, runtime string, size int) {
	r.outputBytes = size
}
