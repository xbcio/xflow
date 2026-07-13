package flow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestStartNodeDescriptor(t *testing.T) {
	n := Start()

	if n.NodeType() != "xflow.start" {
		t.Fatalf("NodeType() = %q, want xflow.start", n.NodeType())
	}
	desc := n.Descriptor()
	if desc.Type != "xflow.start" {
		t.Fatalf("Descriptor().Type = %q, want xflow.start", desc.Type)
	}
	if len(desc.Outputs) != 1 || desc.Outputs[0].Name != "main" {
		t.Fatalf("outputs = %+v, want [main]", desc.Outputs)
	}
}

func TestStartNodeReturnsInputData(t *testing.T) {
	n := Start()
	input := &types.Input{Data: map[string]any{"ticket_id": "vuln-1"}}

	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Data["ticket_id"] != "vuln-1" {
		t.Fatalf("output = %+v, want input data", out)
	}
}
