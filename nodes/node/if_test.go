package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

func TestIF_Factory(t *testing.T) {
	b := node.IF("age > 18")
	if b.NodeType() != "xflow.if" {
		t.Fatalf("expected xflow.if, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["condition"] != "age > 18" {
		t.Fatalf("expected condition, got %v", params)
	}
}

func TestIF_OnError(t *testing.T) {
	b := node.IF("true").OnError(node.OnErrorContinue)
	if b.OnErrorStrategy() != node.OnErrorContinue {
		t.Fatalf("expected OnErrorContinue, got %v", b.OnErrorStrategy())
	}
}

func TestIF_TrueCondition(t *testing.T) {
	h, _ := node.Lookup("xflow.if")
	b := node.IF("age > 18")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"age": 20},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "true" {
		t.Fatalf("expected port \"true\", got %q", out.Port)
	}
}

func TestIF_FalseCondition(t *testing.T) {
	h, _ := node.Lookup("xflow.if")
	b := node.IF("age > 18")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"age": 10},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "false" {
		t.Fatalf("expected port \"false\", got %q", out.Port)
	}
}

func TestIF_EmptyCondition(t *testing.T) {
	h, _ := node.Lookup("xflow.if")
	input := &node.Input{
		Params: map[string]any{},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for empty condition")
	}
}

func TestIF_InvalidExpression(t *testing.T) {
	h, _ := node.Lookup("xflow.if")
	input := &node.Input{
		Params: map[string]any{"condition": "??? invalid"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

func TestIF_PassesDataThrough(t *testing.T) {
	h, _ := node.Lookup("xflow.if")
	b := node.IF("true")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"key": "value"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["key"] != "value" {
		t.Fatalf("expected data passthrough, got %v", out.Data)
	}
}
