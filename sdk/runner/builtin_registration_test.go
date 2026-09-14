package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestStandaloneRunnerLinksGenericPipelineBuiltins protects the import chain a
// standalone host gets from runner -> sdk/xflow. Do not import node or any
// individual builtin package here: doing so would register the handlers in the
// test itself and hide a missing production link.
func TestStandaloneRunnerLinksGenericPipelineBuiltins(t *testing.T) {
	kafka, ok := registry.LookupTrigger("xflow.trigger.kafka")
	if !ok {
		t.Fatal("Kafka trigger is not registered in the standalone runner")
	}
	if got := kafka.Descriptor().Type; got != "xflow.trigger.kafka" {
		t.Fatalf("Kafka trigger type = %q, want xflow.trigger.kafka", got)
	}

	reg := execution.NewRegistry()
	mapHandler, err := reg.Get("", "map", "xflow.map", 1)
	if err != nil {
		t.Fatalf("resolve xflow.map: %v", err)
	}
	mapOutput, err := mapHandler.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"items":      "records",
			"expression": "$item * 2",
		},
		Data: map[string]any{"records": []any{2, 4}},
	})
	if err != nil {
		t.Fatalf("execute xflow.map: %v", err)
	}
	if mapOutput == nil {
		t.Fatal("xflow.map returned no output")
	}
	if got := mapOutput.Data["count"]; got != 2 {
		t.Fatalf("xflow.map count = %v, want 2", got)
	}
	results, ok := mapOutput.Data["results"].([]any)
	if !ok {
		t.Fatalf("xflow.map results = %#v, want []any{4, 8}", mapOutput.Data["results"])
	}
	for i, want := range []any{4, 8} {
		if i >= len(results) || results[i] != want {
			t.Fatalf("xflow.map results = %#v, want []any{4, 8}", results)
		}
	}

	scriptHandler, err := reg.Get("", "script", "xflow.script", 1)
	if err != nil {
		t.Fatalf("resolve xflow.script: %v", err)
	}
	jsOutput, err := scriptHandler.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  "goja",
			"code":     "({ready: true})",
		},
	})
	if err != nil {
		t.Fatalf("execute xflow.script with goja: %v", err)
	}
	if jsOutput == nil || jsOutput.Port != "main" || jsOutput.Data["ready"] != true {
		t.Fatalf("xflow.script goja output = %#v, want main output with ready=true", jsOutput)
	}

	// ScriptNode resolves the engine before decoding its code. Invalid base64
	// therefore distinguishes a linked wazero engine (an error-port output from
	// its decoder) from a missing engine (script.unknown_engine returned as an
	// error).
	wasmOutput, err := scriptHandler.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"language": "wasm",
			"runtime":  "wazero",
			"code":     "not-base64",
		},
	})
	if err != nil {
		t.Fatalf("execute xflow.script with wazero: %v", err)
	}
	if wasmOutput == nil || wasmOutput.Port != "error" {
		t.Fatalf("xflow.script wazero output = %#v, want error-port output", wasmOutput)
	}
	errorText, ok := wasmOutput.Data["error"].(string)
	if !ok || !strings.Contains(strings.ToLower(errorText), "base64") {
		t.Fatalf("xflow.script wazero error = %#v, want base64 decode failure", wasmOutput.Data["error"])
	}
}
