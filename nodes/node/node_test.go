package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

func TestDefineNewReturnsPortableBuilderWithHandler(t *testing.T) {
	called := false
	def := node.Define("test.node.define", func(_ context.Context, input *node.Input) (*node.Output, error) {
		called = true
		return &node.Output{Data: map[string]any{"severity": input.Params["severity"]}}, nil
	})

	b := def.New(map[string]any{"severity": "critical"})

	if got := b.NodeType(); got != "test.node.define" {
		t.Fatalf("NodeType() = %q, want %q", got, "test.node.define")
	}
	if got := b.RawParams().(map[string]any)["severity"]; got != "critical" {
		t.Fatalf("RawParams()[severity] = %v, want critical", got)
	}

	carrier, ok := b.(node.HandlerCarrier)
	if !ok {
		t.Fatal("Definition.New() builder does not expose HandlerCarrier")
	}
	if got := carrier.Handler().Descriptor().Type; got != "test.node.define" {
		t.Fatalf("handler type = %q, want test.node.define", got)
	}

	out, err := carrier.Handler().Execute(context.Background(), &node.Input{
		Params: map[string]any{"severity": "critical"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !called {
		t.Fatal("custom execute function was not called")
	}
	if got := out.Data["severity"]; got != "critical" {
		t.Fatalf("output severity = %v, want critical", got)
	}
}

func TestDefinePanicsOnNilExecute(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when passing nil execute to node.Define")
		}
	}()
	node.Define("test.node.define.nil", nil)
}

func TestDefinePanicsOnEmptyType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when defining empty node type")
		}
	}()
	node.Define("", func(_ context.Context, _ *node.Input) (*node.Output, error) {
		return &node.Output{}, nil
	})
}

func TestDefinitionDescriptorMetadata(t *testing.T) {
	def := node.Define("test.node.define.metadata",
		func(_ context.Context, input *node.Input) (*node.Output, error) {
			return &node.Output{Data: input.Params}, nil
		},
	).DisplayName("Defined Node").
		Param(node.ParamSpec{Name: "severity", Type: node.ParamString, Required: true}).
		Output("main")

	got := def.Descriptor()
	if got.DisplayName != "Defined Node" {
		t.Fatalf("DisplayName = %q, want Defined Node", got.DisplayName)
	}
	if len(got.Params) != 1 || got.Params[0].Name != "severity" {
		t.Fatalf("Params = %#v, want severity param", got.Params)
	}
	if len(got.Outputs) != 1 || got.Outputs[0].Name != "main" {
		t.Fatalf("Outputs = %#v, want main output", got.Outputs)
	}
}
