package node

import (
	"context"
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

// Script creates a script node. Defaults: language=js, runtime=goja.
//
//	node.Script(`({result: $input.x * 2})`)
//	node.Script(code).Runtime("qjs")
//	node.Script(b64wasm).Language("wasm")
//	node.Script(code).Credentials("aes_key", "api_token")
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
			{Name: "language", DisplayName: "Language", Type: ParamString, Required: false, Default: "js", Description: "Language family: js | wasm"},
			{Name: "runtime", DisplayName: "Runtime", Type: ParamString, Required: false, Default: "goja", Description: "Engine: js->goja|qjs, wasm->wazero"},
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
	params := map[string]any{"code": n.Code}
	if n.Lang != "" && n.Lang != "js" {
		params["language"] = n.Lang
	}
	if n.RuntimeName != "" && n.RuntimeName != "goja" {
		params["runtime"] = n.RuntimeName
	}
	if len(n.Creds) > 0 {
		params["credentials"] = n.Creds
	}
	return params
}

func (n *ScriptNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	code, _ := input.Params["code"].(string)
	if code == "" {
		return nil, fmt.Errorf("xflow.script: code parameter is required")
	}

	language, _ := input.Params["language"].(string)
	if language == "" {
		language = "js"
	}
	runtime, _ := input.Params["runtime"].(string)
	if runtime == "" {
		if language == "wasm" {
			runtime = "wazero"
		} else {
			runtime = "goja"
		}
	}

	engine, ok := script.Lookup(language, runtime)
	if !ok {
		return nil, fmt.Errorf("xflow.script: unknown engine (language=%q, runtime=%q)", language, runtime)
	}

	declared := readCredNames(input.Params["credentials"])
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
	return &Output{Data: script.MapResult(result), Port: "main"}, nil
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
