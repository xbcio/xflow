package script

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestScriptNode_BatchInputProducesResults(t *testing.T) {
	n := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  "goja",
			"code":     `({hit: $input.path === "/api/a"})`,
		},
		Data: map[string]any{
			"messages": []any{
				map[string]any{"value": `{"path":"/api/a"}`},
				map[string]any{"value": `{"path":"/other"}`},
			},
			"count": 2,
		},
	}
	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want main", out.Port)
	}
	if out.Data["count"] != 2 {
		t.Fatalf("count = %v, want 2", out.Data["count"])
	}
	results, ok := out.Data["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("results = %#v, want 2 entries", out.Data["results"])
	}
}

// Non-batch input must go through the original path — shape unchanged.
func TestScriptNode_SingleInputUnchanged(t *testing.T) {
	n := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"language": "js", "runtime": "goja", "code": `({ok: true})`,
		},
		Data: map[string]any{"path": "/api/a"},
	}
	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Data["ok"] != true {
		t.Fatalf("single-input output shape changed: %#v", out.Data)
	}
	if _, has := out.Data["results"]; has {
		t.Fatal("single input must not produce a batch-shaped result")
	}
}
