package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

// newHandler is a minimal DescriptorProvider+TaskHandler used only in New() tests.
// Its Type is distinct from goodHandler to avoid re-registration panics.
type newHandler struct{}

func (h *newHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.node.new"}
}
func (h *newHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	return &node.Output{}, nil
}

// TestNew_ReturnsCorrectNodeType verifies that New() captures the handler's
// Descriptor().Type and exposes it via Builder.NodeType().
func TestNew_ReturnsCorrectNodeType(t *testing.T) {
	b := node.New(&newHandler{}, nil)
	if got := b.NodeType(); got != "test.node.new" {
		t.Fatalf("NodeType() = %q, want %q", got, "test.node.new")
	}
}

// TestNew_PanicsOnNilHandler verifies that New(nil, ...) panics immediately.
func TestNew_PanicsOnNilHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when passing nil handler to node.New")
		}
	}()
	node.New(nil, nil)
}
