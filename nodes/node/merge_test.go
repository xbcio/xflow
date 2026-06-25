package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/nodes/node"
)

func TestMerge_Factory(t *testing.T) {
	b := node.Merge(node.MergeWaitAll)
	if b.NodeType() != "xflow.merge" {
		t.Fatalf("expected xflow.merge, got %s", b.NodeType())
	}
}

func TestMerge_WaitAll(t *testing.T) {
	h, _ := node.Lookup("xflow.merge")
	b := node.Merge(node.MergeWaitAll)
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"from_main": "hello"},
		Inputs: map[string]any{"branch_a": "data_a", "branch_b": "data_b"},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["branch_a"] != "data_a" {
		t.Fatalf("expected branch_a data, got %v", out.Data)
	}
	if out.Data["branch_b"] != "data_b" {
		t.Fatalf("expected branch_b data, got %v", out.Data)
	}
	if out.Data["from_main"] != "hello" {
		t.Fatalf("expected from_main data, got %v", out.Data)
	}
}

func TestMerge_WaitAny(t *testing.T) {
	h, _ := node.Lookup("xflow.merge")
	b := node.Merge(node.MergeWaitAny)
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"winner": true},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["winner"] != true {
		t.Fatalf("expected winner=true, got %v", out.Data)
	}
}

func TestMerge_UnknownMode(t *testing.T) {
	h, _ := node.Lookup("xflow.merge")
	input := &node.Input{
		Params: map[string]any{"mode": "invalid"},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
