package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestMap_Factory(t *testing.T) {
	b := node.Map("orders", 5)
	if b.NodeType() != "xflow.map" {
		t.Fatalf("expected xflow.map, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["batch_size"] != 5 {
		t.Fatalf("expected batch_size=5, got %v", params["batch_size"])
	}
}

func TestMap_BasicIteration(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 2)
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

func TestMap_SingleBatch(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
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

func TestMap_MissingItems(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	input := &types.Input{
		Params: map[string]any{"batch_size": 1},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing items")
	}
}

func TestMap_ItemsNotArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": "not_an_array"},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for non-array items")
	}
}

func TestMap_EmptyArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
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
