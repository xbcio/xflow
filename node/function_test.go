package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestFunction_Factory(t *testing.T) {
	b := node.Function("calculate_tax")
	if b.NodeType() != "xflow.function" {
		t.Fatalf("expected xflow.function, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["function_name"] != "calculate_tax" {
		t.Fatalf("expected function_name, got %v", params)
	}
}

func TestExpr_Factory(t *testing.T) {
	b := node.Expr("price * 1.1")
	params := b.RawParams().(map[string]any)
	if params["code"] != "price * 1.1" {
		t.Fatalf("expected code, got %v", params)
	}
}

func TestFunction_InlineExpr(t *testing.T) {
	h, _ := node.Lookup("xflow.function")
	b := node.Expr("a + b")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"a": 3.0, "b": 4.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 7.0 {
		t.Fatalf("expected result=7, got %v", out.Data["result"])
	}
}

func TestFunction_InlineExpr_ReturnsMap(t *testing.T) {
	h, _ := node.Lookup("xflow.function")
	b := node.Expr(`{"sum": a + b, "product": a * b}`)
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"a": 2.0, "b": 3.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["sum"] != 5.0 {
		t.Fatalf("expected sum=5, got %v", out.Data["sum"])
	}
	if out.Data["product"] != 6.0 {
		t.Fatalf("expected product=6, got %v", out.Data["product"])
	}
}

func TestFunction_NamedFunction(t *testing.T) {
	node.RegisterFunc("test_double", func(_ context.Context, input *node.Input) (*node.Output, error) {
		val, _ := input.Data["value"].(float64)
		return &node.Output{Data: map[string]any{"result": val * 2}}, nil
	})

	h, _ := node.Lookup("xflow.function")
	b := node.Function("test_double")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"value": 5.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 10.0 {
		t.Fatalf("expected result=10, got %v", out.Data["result"])
	}
}

func TestFunction_UnregisteredFunction(t *testing.T) {
	h, _ := node.Lookup("xflow.function")
	b := node.Function("nonexistent_fn")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unregistered function")
	}
}

func TestFunction_NeitherCodeNorName(t *testing.T) {
	h, _ := node.Lookup("xflow.function")
	input := &node.Input{
		Params: map[string]any{},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error when neither code nor function_name provided")
	}
}

func TestFunction_ExtraParams(t *testing.T) {
	h, _ := node.Lookup("xflow.function")
	input := &node.Input{
		Params: map[string]any{
			"code":   "x + multiplier",
			"params": map[string]any{"multiplier": 10.0},
		},
		Data: map[string]any{"x": 5.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["result"] != 15.0 {
		t.Fatalf("expected result=15, got %v", out.Data["result"])
	}
}
