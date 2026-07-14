package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestSwitch_Factory_Rules(t *testing.T) {
	b := node.Switch([]node.SwitchRule{
		{Condition: "x > 0", Output: "positive"},
		{Condition: "x < 0", Output: "negative"},
	}, "zero")
	if b.NodeType() != "xflow.switch" {
		t.Fatalf("expected xflow.switch, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["default_output"] != "zero" {
		t.Fatalf("expected default_output=zero, got %v", params["default_output"])
	}
}

func TestSwitch_Factory_Expr(t *testing.T) {
	b := node.SwitchExpr("category", "other")
	params := b.RawParams().(map[string]any)
	if params["mode"] != "expression" {
		t.Fatalf("expected mode=expression, got %v", params["mode"])
	}
	if params["expression"] != "category" {
		t.Fatalf("expected expression=category, got %v", params["expression"])
	}
}

func TestSwitch_RulesMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.Switch([]node.SwitchRule{
		{Condition: "status == \"active\"", Output: "active"},
		{Condition: "status == \"inactive\"", Output: "inactive"},
	}, "unknown")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"status": "active"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "active" {
		t.Fatalf("expected port \"active\", got %q", out.Port)
	}
}

func TestSwitch_RulesMode_Default(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.Switch([]node.SwitchRule{
		{Condition: "status == \"active\"", Output: "active"},
	}, "fallback")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"status": "pending"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "fallback" {
		t.Fatalf("expected port \"fallback\", got %q", out.Port)
	}
}

func TestSwitch_ExpressionMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	b := node.SwitchExpr("category", "other")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"category": "electronics"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "electronics" {
		t.Fatalf("expected port \"electronics\", got %q", out.Port)
	}
}

func TestSwitch_ExpressionMode_MissingExpr(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	input := &types.Input{
		Params: map[string]any{"mode": "expression"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing expression")
	}
}

func TestSwitch_UnknownMode(t *testing.T) {
	h, _ := registry.Lookup("xflow.switch")
	input := &types.Input{
		Params: map[string]any{"mode": "invalid"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
