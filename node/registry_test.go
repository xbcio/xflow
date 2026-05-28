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

// TestRegistry_LookupVersion verifies version-based lookup.
func TestRegistry_LookupVersion(t *testing.T) {
	h, found := node.LookupVersion("test.registry.good", 1)
	if !found {
		t.Fatal("LookupVersion: expected v1 handler to be found")
	}
	if h == nil {
		t.Fatal("LookupVersion: returned nil handler")
	}
}

// TestRegistry_LookupVersion_NotFound verifies missing version returns false.
func TestRegistry_LookupVersion_NotFound(t *testing.T) {
	_, found := node.LookupVersion("test.registry.good", 99)
	if found {
		t.Fatal("LookupVersion: expected v99 to not be found")
	}
}

// TestRegistry_Versions verifies listing all versions for a type.
func TestRegistry_Versions(t *testing.T) {
	versions := node.Versions("test.registry.good")
	if len(versions) == 0 {
		t.Fatal("Versions: expected at least one version")
	}
	found := false
	for _, v := range versions {
		if v == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("Versions: expected v1 in list")
	}
}

// TestRegistry_BuiltinNodesHaveVersion verifies all builtin nodes have version via BaseNode.
func TestRegistry_BuiltinNodesHaveVersion(t *testing.T) {
	types := []string{"xflow.if", "xflow.switch", "xflow.http", "xflow.loop", "xflow.split", "xflow.merge", "xflow.function", "xflow.grpc", "xflow.database", "xflow.wait", "xflow.approval"}
	for _, typ := range types {
		h, ok := node.LookupVersion(typ, 1)
		if !ok {
			t.Errorf("LookupVersion(%q, 1): not found", typ)
			continue
		}
		if h == nil {
			t.Errorf("LookupVersion(%q, 1): nil handler", typ)
		}
	}
}
