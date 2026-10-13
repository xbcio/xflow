package flow_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestSubWorkflow_Factory(t *testing.T) {
	b := node.SubWorkflow("payment-processing")
	if b.NodeType() != types.SubWorkflowNodeType {
		t.Fatalf("expected %s, got %s", types.SubWorkflowNodeType, b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["workflow"] != "payment-processing" {
		t.Fatalf("expected workflow param, got %v", params)
	}
	for _, optional := range []string{"version", "entry", "input", "output"} {
		if _, ok := params[optional]; ok {
			t.Fatalf("optional param %q emitted when unset: %v", optional, params)
		}
	}
}

func TestSubWorkflow_ChainedSetters(t *testing.T) {
	b := node.SubWorkflow("payment-processing").
		Version("v2").
		Entry("start_node").
		Input(map[string]any{"order_id": "${{ $params.order_id }}"}).
		Output(map[string]any{"receipt": "${{ $child['charge'].receipt_id }}"})

	params := b.RawParams().(map[string]any)
	if params["version"] != "v2" {
		t.Fatalf("version = %v, want v2", params["version"])
	}
	if params["entry"] != "start_node" {
		t.Fatalf("entry = %v, want start_node", params["entry"])
	}
	if in, _ := params["input"].(map[string]any); in["order_id"] != "${{ $params.order_id }}" {
		t.Fatalf("input = %v", params["input"])
	}
	if out, _ := params["output"].(map[string]any); out["receipt"] != "${{ $child['charge'].receipt_id }}" {
		t.Fatalf("output = %v", params["output"])
	}
}

func TestSubWorkflow_OnError(t *testing.T) {
	b := node.SubWorkflow("payment-processing").OnError(types.OnErrorContinue)
	if b.OnErrorStrategy() != types.OnErrorContinue {
		t.Fatalf("expected OnErrorContinue, got %v", b.OnErrorStrategy())
	}
}

func TestSubWorkflow_Descriptor(t *testing.T) {
	d := node.SubWorkflow("payment-processing").Descriptor()
	if d.Type != types.SubWorkflowNodeType {
		t.Fatalf("descriptor type = %q", d.Type)
	}
	byName := map[string]types.ParamSpec{}
	for _, p := range d.Params {
		byName[p.Name] = p
	}
	if p, ok := byName["workflow"]; !ok || !p.Required {
		t.Fatalf("workflow param must be required: %+v", byName["workflow"])
	}
	if p, ok := byName["input"]; !ok || p.Type != types.ParamObject {
		t.Fatalf("input param must be an object: %+v", byName["input"])
	}
	if p, ok := byName["output"]; !ok || p.Type != types.ParamObject {
		t.Fatalf("output param must be an object: %+v", byName["output"])
	}
	if len(d.Inputs) != 1 || d.Inputs[0].Name != "main" {
		t.Fatalf("inputs = %+v", d.Inputs)
	}
	ports := map[string]bool{}
	for _, p := range d.Outputs {
		ports[p.Name] = true
	}
	if !ports["main"] || !ports["error"] {
		t.Fatalf("outputs = %+v", d.Outputs)
	}
}

// TestSubWorkflow_RegisteredHandlerFailsClosed pins the stub contract: the type
// must be registered (the registration precheck and the runner-side registry
// both require a handler for every declared type), and that handler must refuse
// execution rather than fabricate an empty child result. A task carrying this
// type reaching a handler means the engine's routing is broken.
func TestSubWorkflow_RegisteredHandlerFailsClosed(t *testing.T) {
	h, ok := registry.Lookup(types.SubWorkflowNodeType)
	if !ok {
		t.Fatalf("%s is not registered", types.SubWorkflowNodeType)
	}
	if _, err := h.Execute(context.Background(), &types.Input{}); err == nil {
		t.Fatal("stub handler must fail closed, got nil error")
	}
}
