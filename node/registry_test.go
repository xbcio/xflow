package node_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// goodHandler satisfies both TaskHandler and DescriptorProvider.
type goodHandler struct{}

func (h *goodHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "test.registry.good"}
}
func (h *goodHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	return &node.Output{}, nil
}

// noDescriptorHandler implements TaskHandler only (no Descriptor method).
type noDescriptorHandler struct{}

func (h *noDescriptorHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	return nil, nil
}

// emptyTypeHandler has Descriptor().Type == "".
type emptyTypeHandler struct{}

func (h *emptyTypeHandler) Descriptor() node.Descriptor { return node.Descriptor{Type: ""} }
func (h *emptyTypeHandler) Execute(_ context.Context, _ *node.Input) (*node.Output, error) {
	return nil, nil
}

func init() {
	node.Register(&goodHandler{})
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestRegistry_RegisterAndLookup verifies the happy-path contract:
// a handler registered via Register is retrievable via Lookup.
func TestRegistry_RegisterAndLookup(t *testing.T) {
	h, found := node.Lookup("test.registry.good")
	if !found {
		t.Fatal("Lookup: expected handler to be found after Register")
	}
	if h == nil {
		t.Fatal("Lookup: returned nil handler")
	}
}

// TestRegistry_PanicOnNoDescriptor verifies that registering a handler that
// does not implement DescriptorProvider causes a panic.
func TestRegistry_PanicOnNoDescriptor(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when registering handler without DescriptorProvider")
		}
	}()
	node.Register(&noDescriptorHandler{})
}

// TestRegistry_PanicOnEmptyType verifies that registering a handler whose
// Descriptor().Type is the empty string causes a panic.
func TestRegistry_PanicOnEmptyType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when registering handler with empty Type")
		}
	}()
	node.Register(&emptyTypeHandler{})
}
