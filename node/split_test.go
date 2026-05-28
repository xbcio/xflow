package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestSplit_Factory(t *testing.T) {
	b := node.Split("items")
	if b.NodeType() != "xflow.split" {
		t.Fatalf("expected xflow.split, got %s", b.NodeType())
	}
}

func TestSplit_BasicSplit(t *testing.T) {
	h, _ := node.Lookup("xflow.split")
	b := node.Split("items")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{"a", "b", "c"}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["total"] != 3 {
		t.Fatalf("expected total=3, got %v", out.Data["total"])
	}
	if out.Data["batch_count"] != 3 {
		t.Fatalf("expected batch_count=3, got %v", out.Data["batch_count"])
	}
}

func TestSplit_WithBatchSize(t *testing.T) {
	h, _ := node.Lookup("xflow.split")
	input := &node.Input{
		Params: map[string]any{"items": "items", "batch_size": 2},
		Data:   map[string]any{"items": []any{1, 2, 3, 4}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["batch_count"] != 2 {
		t.Fatalf("expected batch_count=2, got %v", out.Data["batch_count"])
	}
}

func TestSplit_MissingItems(t *testing.T) {
	h, _ := node.Lookup("xflow.split")
	input := &node.Input{
		Params: map[string]any{},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing items")
	}
}
