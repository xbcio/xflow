package node_test

import (
	"context"
	"github.com/xbcio/xflow/internal/noderuntime"
	"testing"

	"github.com/xbcio/xflow/types"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// goodHandler satisfies ActionHandler.
type goodHandler struct{}

func (h *goodHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.registry.good"}
}
func (h *goodHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

// emptyTypeHandler has Descriptor().Type == "".
type emptyTypeHandler struct{}

func (h *emptyTypeHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: ""} }
func (h *emptyTypeHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, nil
}

type triggerHandler struct{}

func (h *triggerHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.registry.trigger", Kind: types.NodeKindTrigger}
}
func (h *triggerHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}
func (h *triggerHandler) Activate(_ context.Context, _ *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

type versionedTriggerHandler struct {
	nodeType string
	version  int
}

func (h *versionedTriggerHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindTrigger}
}

func (h *versionedTriggerHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"version": h.version}}, nil
}

func (h *versionedTriggerHandler) Activate(_ context.Context, _ *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

func (h *versionedTriggerHandler) NodeVersion() int { return h.version }

func init() {
	noderuntime.Register(&goodHandler{})
	noderuntime.Register(&triggerHandler{})
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestRegistry_RegisterAndLookup verifies the happy-path contract:
// a handler registered via Register is retrievable via Lookup.
func TestRegistry_RegisterAndLookup(t *testing.T) {
	h, found := noderuntime.Lookup("test.registry.good")
	if !found {
		t.Fatal("Lookup: expected handler to be found after Register")
	}
	if h == nil {
		t.Fatal("Lookup: returned nil handler")
	}
}

// TestRegistry_PanicOnEmptyType verifies that registering a handler whose
// Descriptor().Type is the empty string causes a panic.
func TestRegistry_PanicOnEmptyType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when registering handler with empty Type")
		}
	}()
	noderuntime.Register(&emptyTypeHandler{})
}

// TestRegistry_LookupVersion verifies version-based lookup.
func TestRegistry_LookupVersion(t *testing.T) {
	h, found := noderuntime.LookupVersion("test.registry.good", 1)
	if !found {
		t.Fatal("LookupVersion: expected v1 handler to be found")
	}
	if h == nil {
		t.Fatal("LookupVersion: returned nil handler")
	}
}

// TestRegistry_LookupVersion_NotFound verifies missing version returns false.
func TestRegistry_LookupVersion_NotFound(t *testing.T) {
	_, found := noderuntime.LookupVersion("test.registry.good", 99)
	if found {
		t.Fatal("LookupVersion: expected v99 to not be found")
	}
}

func TestRegistry_LookupTrigger(t *testing.T) {
	h, found := noderuntime.LookupTrigger("test.registry.trigger")
	if !found {
		t.Fatal("LookupTrigger: expected trigger handler to be found")
	}
	if h == nil {
		t.Fatal("LookupTrigger: returned nil handler")
	}
}

func TestRegisterTriggerPreservesLatestActionDefault(t *testing.T) {
	const nodeType = "test.registry.trigger.versioned"

	newer := &versionedTriggerHandler{nodeType: nodeType, version: 2}
	older := &versionedTriggerHandler{nodeType: nodeType, version: 1}

	noderuntime.RegisterTrigger(newer)
	noderuntime.RegisterTrigger(older)

	h, found := noderuntime.Lookup(nodeType)
	if !found {
		t.Fatal("Lookup: expected action handler to be found after RegisterTrigger")
	}
	got, ok := h.(*versionedTriggerHandler)
	if !ok {
		t.Fatalf("Lookup: handler type = %T, want *versionedTriggerHandler", h)
	}
	if got.NodeVersion() != 2 {
		t.Fatalf("Lookup: default version = %d, want 2", got.NodeVersion())
	}

	v1, found := noderuntime.LookupVersion(nodeType, 1)
	if !found {
		t.Fatal("LookupVersion: expected v1 handler to be registered")
	}
	gotV1, ok := v1.(*versionedTriggerHandler)
	if !ok {
		t.Fatalf("LookupVersion: handler type = %T, want *versionedTriggerHandler", v1)
	}
	if gotV1.NodeVersion() != 1 {
		t.Fatalf("LookupVersion: version = %d, want 1", gotV1.NodeVersion())
	}
}

// TestRegistry_Versions verifies listing all versions for a type.
func TestRegistry_Versions(t *testing.T) {
	versions := noderuntime.Versions("test.registry.good")
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
		h, ok := noderuntime.LookupVersion(typ, 1)
		if !ok {
			t.Errorf("LookupVersion(%q, 1): not found", typ)
			continue
		}
		if h == nil {
			t.Errorf("LookupVersion(%q, 1): nil handler", typ)
		}
	}
}
