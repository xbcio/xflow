package flow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestEndNodeDescriptor(t *testing.T) {
	n := End()

	if n.NodeType() != "xflow.end" {
		t.Fatalf("NodeType() = %q, want xflow.end", n.NodeType())
	}
	desc := n.Descriptor()
	if desc.Type != "xflow.end" {
		t.Fatalf("Descriptor().Type = %q, want xflow.end", desc.Type)
	}
	if len(desc.Inputs) != 1 || desc.Inputs[0].Name != "main" {
		t.Fatalf("inputs = %+v, want [main]", desc.Inputs)
	}
	if len(desc.Outputs) != 0 {
		t.Fatalf("outputs = %+v, want none", desc.Outputs)
	}
}

func TestEndNodeReturnsInputData(t *testing.T) {
	n := End()
	input := &types.Input{Data: map[string]any{"ticket_id": "vuln-1"}}

	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Data["ticket_id"] != "vuln-1" {
		t.Fatalf("output = %+v, want input data", out)
	}
	if out.Port != "" {
		t.Fatalf("Port = %q, want empty", out.Port)
	}
}

func TestEndNodeReturnsEmptyDataForNilInput(t *testing.T) {
	n := End()

	out, err := n.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || len(out.Data) != 0 {
		t.Fatalf("output = %+v, want empty data", out)
	}
}
