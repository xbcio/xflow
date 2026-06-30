package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/nodes/node/script"
	_ "github.com/xbcio/xflow/nodes/node/script/js"
	_ "github.com/xbcio/xflow/nodes/node/script/wasm"
)

// ScriptNode implements xflow.script — runs a sandboxed dynamic script.
type ScriptNode struct {
	BaseNode
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

func (n *ScriptNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.script",
		DisplayName: "Script",
		Params: []ParamSpec{
			{Name: "language", DisplayName: "Language", Type: ParamString, Required: true, Description: "Language family: js | wasm (no default — choose explicitly)"},
			{Name: "runtime", DisplayName: "Runtime", Type: ParamString, Required: true, Description: "Engine: js->goja|qjs, wasm->wazero (no default — choose explicitly)"},
			{Name: "code", DisplayName: "Code", Type: ParamString, Required: true, Description: "JS source (js) or base64 wasm module (wasm)"},
			{Name: "credentials", DisplayName: "Credentials", Type: ParamArray, Required: false, Description: "Declared credential names injected as $credentials"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *ScriptNode) NodeType() string { return "xflow.script" }
func (n *ScriptNode) OnError(s OnError) Builder {
	n.onError = s
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

func (n *ScriptNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	// TODO(metrics): emit end-to-end counters and timers when the project
	// metrics middleware lands:
	//   - xflow_script_execute_total{language,runtime,outcome=main|error|config}
	//   - xflow_script_execute_duration_seconds{language,runtime}
	//   - xflow_script_output_bytes (histogram of result size before the cap)
	code, _ := input.Params["code"].(string)
	if code == "" {
		return nil, fmt.Errorf("xflow.script: code parameter is required")
	}

	language, _ := input.Params["language"].(string)
	if language == "" {
		return nil, fmt.Errorf("xflow.script: language parameter is required (choose: js | wasm)")
	}
	runtime, _ := input.Params["runtime"].(string)
	if runtime == "" {
		return nil, fmt.Errorf("xflow.script: runtime parameter is required (js -> goja|qjs, wasm -> wazero)")
	}

	engine, ok := script.Lookup(language, runtime)
	if !ok {
		return nil, fmt.Errorf("xflow.script: unknown engine (language=%q, runtime=%q)", language, runtime)
	}

	declared := readCredNames(input.Params["credentials"])
	// Input.Credential has no error return; a nil value means "not found", which
	// ResolveCredentials turns into a config error for a declared-but-absent name.
	creds, first, err := script.ResolveCredentials(declared, func(name string) (map[string]any, error) {
		return input.Credential(name), nil
	})
	if err != nil {
		return nil, fmt.Errorf("xflow.script: %w", err)
	}

	globals := buildScriptGlobals(input, creds, first)

	result, err := engine.Execute(ctx, code, globals, script.DefaultHelpers())
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}
	data := script.MapResult(result)
	if sizeErr := checkResultSize(data); sizeErr != nil {
		return &Output{Data: map[string]any{"error": sizeErr.Error()}, Port: "error"}, nil
	}
	return &Output{Data: data, Port: "main"}, nil
}

// checkResultSize enforces DefaultMaxOutputBytes on the JSON-encoded result.
// Done at the node layer so every engine inherits the same cap without
// having to thread limits through each runtime.
func checkResultSize(data map[string]any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("xflow.script: encode result: %w", err)
	}
	if len(b) > script.DefaultMaxOutputBytes {
		return &script.OutputSizeError{Size: len(b), Limit: script.DefaultMaxOutputBytes}
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

func buildScriptGlobals(input *Input, creds map[string]any, first any) map[string]any {
	return map[string]any{
		"$input":       input.Data,
		"$inputs":      input.Inputs,
		"$vars":        input.Vars,
		"$config":      input.Config,
		"$params":      input.Params,
		"$runtime":     runtimeEnv(input),
		"$credentials": creds,
		"$credential":  first,
	}
}

func init() { Register(&ScriptNode{}) }
