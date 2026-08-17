package wasm

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// TestABI_MinimalGuestWorks holds the host to the ABI contract in §4.1: a guest
// implementing only the REQUIRED exports must be fully usable. reactormin omits
// `out_len` (recommended) and `teardown` (optional), so this fails if the host
// ever starts assuming either is present.
func TestABI_MinimalGuestWorks(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), engine.Code(b64(reactorMinWasm)), map[string]any{
		"$config": map[string]any{},
		"x":       7.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("minimal guest execute: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result not an object: %#v", out)
	}
	echo, ok := m["echo"].(map[string]any)
	if !ok {
		t.Fatalf("no echo in result: %#v", out)
	}
	if echo["x"] != 7.0 {
		t.Fatalf("echo x = %#v, want 7", echo["x"])
	}
}

// TestABI_OptionalExportsAreNil documents the host-side representation: the
// optional handles resolve to nil for a minimal guest and the instance still
// tears down cleanly (teardown must skip the missing hook, not panic).
func TestABI_OptionalExportsAreNil(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	eng, err := h.engineFor(ctx, reactorMinWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	inst, err := eng.newInstance(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("newInstance: %v", err)
	}
	if inst.outLen != nil {
		t.Error("out_len should be nil for a guest that does not export it")
	}
	if inst.td != nil {
		t.Error("teardown should be nil for a guest that does not export it")
	}
	inst.teardown(ctx) // must not panic despite the nil hook
}

// TestABI_FullGuestHasOptionalExports is the positive counterpart: the reference
// guest exports both, and the host must resolve and cache the handles (the P2
// change that moved them off the per-call lookup path).
func TestABI_FullGuestHasOptionalExports(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	eng, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	inst, err := eng.newInstance(ctx, []byte(`{"rules":[]}`))
	if err != nil {
		t.Fatalf("newInstance: %v", err)
	}
	defer inst.teardown(ctx)
	if inst.outLen == nil {
		t.Error("reference guest exports out_len but the host did not cache it")
	}
	if inst.td == nil {
		t.Error("reference guest exports teardown but the host did not cache it")
	}
}

// TestABI_VersionMismatchRefused: the negotiation in §4.1 must reject a guest
// speaking a different ABI rather than calling into it and misreading memory.
func TestABI_VersionMismatchRefused(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	eng, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	// Bump the host's expectation so the (unchanged) guest looks incompatible.
	// Restored via t.Cleanup so later tests see the real version.
	orig := reactorABIVersion
	reactorABIVersion = orig + 1
	t.Cleanup(func() { reactorABIVersion = orig })

	if _, err := eng.newInstance(ctx, []byte(`{"rules":[]}`)); err == nil {
		t.Fatal("newInstance accepted a guest with a mismatched ABI version")
	}
}
