package script

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/internal/utils/exprx"
	"github.com/xbcio/xflow/node/registry"

	_ "github.com/xbcio/xflow/node/internal/code/script/js"

	_ "github.com/xbcio/xflow/node/internal/code/script/wasm"
)

// ScriptNode implements xflow.script — runs a sandboxed dynamic script.
type ScriptNode struct {
	nodeinternal.BaseNode
	Code        string
	Lang        string
	RuntimeName string
	Creds       []string
}

// Script creates a script node. The caller MUST explicitly choose a language
// and runtime — there are no implicit defaults, so the choice (and its
// security/perf tradeoff) is always made consciously.
//
//	node.Script(code).Language("js").Runtime("goja")
//	node.Script(code).Language("js").Runtime("qjs")
//	node.Script(b64wasm).Language("wasm").Runtime("wazero")
//	node.Script(code).Language("js").Runtime("goja").Credentials("aes_key", "api_token")
//
// Runtime selection guide:
//   - js/goja: fastest cold start, pooled VMs, lowest per-call overhead.
//     CANNOT interrupt tight pure-computation loops (e.g. `while(true){}`).
//     Pick for short, well-bounded scripts.
//   - js/qjs: QuickJS via wasm. ~330ms first-load (cached process-wide),
//     ~3ms per call after. Genuine mid-execution termination. Pick when
//     scripts may be long-running or CPU-bound.
//   - wasm/wazero: any language compiled to wasip1. Strictest sandbox,
//     true ctx cancellation. Pick for untrusted code or non-JS guests.
func Script(code string) *ScriptNode {
	return &ScriptNode{Code: code}
}

func (n *ScriptNode) Language(lang string) *ScriptNode { n.Lang = lang; return n }
func (n *ScriptNode) Runtime(rt string) *ScriptNode    { n.RuntimeName = rt; return n }
func (n *ScriptNode) Credentials(names ...string) *ScriptNode {
	n.Creds = names
	return n
}

func (n *ScriptNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.script",
		DisplayName: "Script",
		Params: []types.ParamSpec{
			{Name: "language", DisplayName: "Language", Type: types.ParamString, Required: true, Description: "Language family: js | wasm (no default — choose explicitly)"},
			{Name: "runtime", DisplayName: "Runtime", Type: types.ParamString, Required: true, Description: "Engine: js->goja|qjs, wasm->wazero (no default — choose explicitly)"},
			{Name: "code", DisplayName: "Code", Type: types.ParamString, Required: true, Description: "JS source (js) or base64 wasm module (wasm)"},
			{Name: "credentials", DisplayName: "Credentials", Type: types.ParamArray, Required: false, Description: "Declared credential names injected as $credentials"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *ScriptNode) NodeType() string { return "xflow.script" }
func (n *ScriptNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *ScriptNode) RawParams() any {
	// Always emit language and runtime — there are no defaults. Empty values
	// flow through to Execute which surfaces them as config errors, so the
	// DSL output stays a faithful mirror of what was set on the builder.
	params := map[string]any{
		"code":     n.Code,
		"language": n.Lang,
		"runtime":  n.RuntimeName,
	}
	if len(n.Creds) > 0 {
		params["credentials"] = n.Creds
	}
	return params
}

func (n *ScriptNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	start := time.Now()
	language, _ := input.Params["language"].(string)
	runtime, _ := input.Params["runtime"].(string)

	code, _ := input.Params["code"].(string)
	if code == "" {
		observeExecute(language, runtime, "config", time.Since(start))
		return nil, fmt.Errorf("xflow.script: code parameter is required")
	}
	if language == "" {
		observeExecute(language, runtime, "config", time.Since(start))
		return nil, fmt.Errorf("xflow.script: language parameter is required (choose: js | wasm)")
	}
	if runtime == "" {
		observeExecute(language, runtime, "config", time.Since(start))
		return nil, fmt.Errorf("xflow.script: runtime parameter is required (js -> goja|qjs, wasm -> wazero)")
	}

	eng, ok := engine.Lookup(language, runtime)
	if !ok {
		observeExecute(language, runtime, "config", time.Since(start))
		return nil, fmt.Errorf("xflow.script: unknown engine (language=%q, runtime=%q)", language, runtime)
	}

	declared := readCredNames(input.Params["credentials"])
	// Input.Credential has no error return; a nil value means "not found", which
	// ResolveCredentials turns into a config error for a declared-but-absent name.
	creds, first, err := engine.ResolveCredentials(declared, func(name string) (map[string]any, error) {
		return input.Credential(name), nil
	})
	if err != nil {
		observeExecute(language, runtime, "config", time.Since(start))
		return nil, fmt.Errorf("xflow.script: %w", err)
	}

	globals := buildScriptGlobals(input, creds, first)

	// Enforce a per-execution timeout so runtimes that honour ctx cancellation
	// (js/qjs, wasm/wazero) terminate a runaway script at the deadline. Without
	// this, memory-backend dispatch (context.Background()) leaves the script
	// with no deadline, so `while(true){}` blocks the worker forever.
	timeout := readScriptTimeout(input.Params)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	result, err := eng.Execute(ctx, code, globals, engine.DefaultHelpers())
	if err != nil {
		observeExecute(language, runtime, "error", time.Since(start))
		return &types.Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}
	data := engine.MapResult(result)
	if sizeErr := checkResultSize(data); sizeErr != nil {
		observeExecute(language, runtime, "error", time.Since(start))
		return &types.Output{Data: map[string]any{"error": sizeErr.Error()}, Port: "error"}, nil
	}

	b, _ := json.Marshal(data)
	observeOutputBytes(language, runtime, len(b))
	observeExecute(language, runtime, "main", time.Since(start))
	return &types.Output{Data: data, Port: "main"}, nil
}

// checkResultSize enforces DefaultMaxOutputBytes on the JSON-encoded result.
// Done at the node layer so every engine inherits the same cap without
// having to thread limits through each runtime.
func checkResultSize(data map[string]any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("xflow.script: encode result: %w", err)
	}
	if len(b) > engine.DefaultMaxOutputBytes {
		return &engine.OutputSizeError{Size: len(b), Limit: engine.DefaultMaxOutputBytes}
	}
	return nil
}

// readCredNames accepts both []string (Go DSL) and []any (decoded YAML/JSON).
func readCredNames(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		names := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				names = append(names, s)
			}
		}
		return names
	default:
		return nil
	}
}

// readScriptTimeout resolves the per-execution timeout from node parameters.
// Accepts a numeric value (seconds) or a duration string ("5s", "90s"). Falls
// back to engine.DefaultScriptTimeout when absent or invalid so a script never
// runs without a deadline.
func readScriptTimeout(params map[string]any) time.Duration {
	switch v := params["timeout"].(type) {
	case float64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case int:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case int64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case string:
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return engine.DefaultScriptTimeout
}

func buildScriptGlobals(input *types.Input, creds map[string]any, first any) map[string]any {
	return exprx.BuildExprEnv(input, map[string]any{
		"$credentials": creds,
		"$credential":  first,
	})
}

func init() { registry.Register(&ScriptNode{}) }
