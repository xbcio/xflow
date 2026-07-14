package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestLoop_Factory(t *testing.T) {
	b := node.Loop("orders", 5)
	if b.NodeType() != "xflow.loop" {
		t.Fatalf("expected xflow.loop, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["batch_size"] != 5 {
		t.Fatalf("expected batch_size=5, got %v", params["batch_size"])
	}
}

func TestLoop_BasicIteration(t *testing.T) {
	h, _ := registry.Lookup("xflow.loop")
	b := node.Loop("items", 2)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{1, 2, 3, 4, 5}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["total"] != 5 {
		t.Fatalf("expected total=5, got %v", out.Data["total"])
	}
	if out.Data["batch_count"] != 3 {
		t.Fatalf("expected batch_count=3, got %v", out.Data["batch_count"])
	}
}

func TestLoop_SingleBatch(t *testing.T) {
	h, _ := registry.Lookup("xflow.loop")
	b := node.Loop("items", 1)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{"a", "b", "c"}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["batch_count"] != 3 {
		t.Fatalf("expected batch_count=3, got %v", out.Data["batch_count"])
	}
}

func TestLoop_MissingItems(t *testing.T) {
	h, _ := registry.Lookup("xflow.loop")
	input := &types.Input{
		Params: map[string]any{"batch_size": 1},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing items")
	}
}

func TestLoop_ItemsNotArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.loop")
	b := node.Loop("items", 1)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": "not_an_array"},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for non-array items")
	}
}

func TestLoop_EmptyArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.loop")
	b := node.Loop("items", 1)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["total"] != 0 {
		t.Fatalf("expected total=0, got %v", out.Data["total"])
	}
}
