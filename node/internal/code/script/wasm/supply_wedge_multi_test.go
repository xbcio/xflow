package wasm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// pointerContent is the shape an artifact-pointer supply carries: a map of
// node name → {digest, version}. It is deliberately NOT a pool config — no
// rules/pre_analysis/post_decode key — which is what makes it ineligible to be
// installed as a guest's rule set.
func pointerContent() []byte {
	return []byte(`{"decode":{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","version":"v1"}}`)
}

// TestMultiSupplyInstallsOnlyTheConfigSupply covers the production shape that
// made the naive "install the single cached supply" fix unsafe: a wasm node
// declares MORE THAN ONE supply, and only one of them is its rule set.
//
// SAS's collection node is exactly this — it consumes the artifact-pointer
// supply (so the module is pre-compiled when the pointer flips) and the
// clean-rules supply — and its own comments record the stake: a config whose
// keys a guest does not recognise decodes into an empty rule set that configures
// successfully and then discards nothing, so every record passes through. The
// pointer supply's content is a digest map, which is precisely such a config.
//
// The registration index must therefore not hand the pointer supply's content to
// the pool. Both arrival orders are exercised because a last-writer-wins
// resolution would pick differently for each.
func TestMultiSupplyInstallsOnlyTheConfigSupply(t *testing.T) {
	ctx := context.Background()
	const (
		owner = "node:collect/decode"
		rules = "rules-for-multi-supply"
		ptr   = "wasm-artifacts-for-multi-supply"
	)
	rulesSnap := supply.Snapshot{
		Name: rules, Content: []byte(`{"rules":[{"name":"from-rules","expr":"true"}]}`),
		Hash: "rules-h1", Revision: 7, FetchedAt: time.Now(),
	}
	ptrSnap := supply.Snapshot{
		Name: ptr, Content: pointerContent(),
		Hash: "ptr-h1", Revision: 9, FetchedAt: time.Now(),
	}

	for _, order := range []struct {
		name    string
		first   supply.Snapshot
		second  supply.Snapshot
		wantRev uint64
	}{
		{"rules first", rulesSnap, ptrSnap, 7},
		{"pointer first", ptrSnap, rulesSnap, 7},
	} {
		t.Run(order.name, func(t *testing.T) {
			h := newTestReactorHost(t)
			reg := supply.NewRegistry()
			code := testReactorCode(t)
			digest := "sha256:" + mustModuleKey(t, code)

			if err := registerSupplyConsumerByDigestOn(h, digest, rules, owner, reg); err != nil {
				t.Fatalf("register rules supply: %v", err)
			}
			if err := registerSupplyConsumerByDigestOn(h, digest, ptr, owner, reg); err != nil {
				t.Fatalf("register pointer supply: %v", err)
			}
			if err := reg.Apply(ctx, order.first); err != nil {
				t.Fatalf("apply first: %v", err)
			}
			if err := reg.Apply(ctx, order.second); err != nil {
				t.Fatalf("apply second: %v", err)
			}

			e, err := h.engineForCode(ctx, code)
			if err != nil {
				t.Fatalf("engineForCode: %v", err)
			}
			if got := e.availability(); got != AvailFresh {
				t.Fatalf("availability = %v, want AvailFresh (the rules supply has arrived)", got)
			}
			if got := e.Generation(); got != order.wantRev {
				t.Fatalf("Generation() = %d, want the RULES supply's revision %d; the "+
					"artifact-pointer supply's content must never be installed as the guest "+
					"configuration", got, order.wantRev)
			}
		})
	}
}

// TestMultipleConfigSuppliesFailClosed: two registered supplies that both parse
// as a pool config leave the platform with no way to know which one drives the
// guest. swapConfig replaces the whole pool, so choosing would serve the wrong
// rule set silently. Nothing is installed and the reason names both supplies.
func TestMultipleConfigSuppliesFailClosed(t *testing.T) {
	ctx := context.Background()
	const owner = "node:collect/decode"
	h := newTestReactorHost(t)
	reg := supply.NewRegistry()
	code := testReactorCode(t)
	digest := "sha256:" + mustModuleKey(t, code)

	if err := registerSupplyConsumerByDigestOn(h, digest, "rules-a", owner, reg); err != nil {
		t.Fatalf("register rules-a: %v", err)
	}
	if err := registerSupplyConsumerByDigestOn(h, digest, "rules-b", owner, reg); err != nil {
		t.Fatalf("register rules-b: %v", err)
	}
	for _, name := range []string{"rules-a", "rules-b"} {
		if err := reg.Apply(ctx, supply.Snapshot{
			Name: name, Content: []byte(`{"rules":[{"name":"` + name + `"}]}`),
			Hash: name + "-h", Revision: 1, FetchedAt: time.Now(),
		}); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}

	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.availability(); got != AvailUnavailable {
		t.Fatalf("availability = %v, want AvailUnavailable — two config supplies are "+
			"ambiguous and neither may be installed", got)
	}
	if got := e.Generation(); got != 0 {
		t.Fatalf("Generation() = %d, want 0 (nothing installed)", got)
	}

	installed, reason := h.installModuleConfigFromRegistry(ctx, e, mustModuleKey(t, code))
	if installed {
		t.Fatal("an ambiguous multi-config-supply module reported installed")
	}
	for _, name := range []string{"rules-a", "rules-b"} {
		if !strings.Contains(reason, name) {
			t.Fatalf("reason %q does not name %q; an operator cannot tell which declaration "+
				"to fix", reason, name)
		}
	}
}

// TestSupplyRefsPrunedOnLastOwnerRelease: the registration index is a record of
// a live dependency, so it must be released with the last owner. Otherwise an
// engine created after the module stopped being hosted here would install
// content for a supply nothing consumes any more.
func TestSupplyRefsPrunedOnLastOwnerRelease(t *testing.T) {
	ctx := context.Background()
	const (
		supplyNode = "rules-ref-prune"
		ownerA     = "node:wf/a"
		ownerB     = "node:wf/b"
	)
	h := newTestReactorHost(t)
	reg := supply.NewRegistry()
	code := testReactorCode(t)
	digest := "sha256:" + mustModuleKey(t, code)
	key := mustModuleKey(t, code)

	if err := registerSupplyConsumerByDigestOn(h, digest, supplyNode, ownerA, reg); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := registerSupplyConsumerByDigestOn(h, digest, supplyNode, ownerB, reg); err != nil {
		t.Fatalf("register B: %v", err)
	}
	if got := len(h.supplyRefsFor(key)); got != 1 {
		t.Fatalf("refs = %d, want 1 — the index is a SET of slots, not a per-owner count", got)
	}
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: supplyNode, Content: []byte(`{"rules":[]}`), Hash: "h", Revision: 3, FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	unregisterSupplyConsumerByDigestOn(h, digest, supplyNode, ownerA, reg)
	if got := len(h.supplyRefsFor(key)); got != 1 {
		t.Fatalf("refs = %d after releasing one of two owners, want 1 (the slot is still held)", got)
	}

	unregisterSupplyConsumerByDigestOn(h, digest, supplyNode, ownerB, reg)
	if got := len(h.supplyRefsFor(key)); got != 0 {
		t.Fatalf("refs = %d after the last owner released, want 0", got)
	}
	if _, ok := reg.Get(supplyNode); !ok {
		t.Fatal("precondition: UnregisterConsumer only drops the consumer, never the " +
			"applied Snapshot; the ref must be pruned without the content vanishing")
	}

	// A fresh engine must not resolve config from the released supply.
	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.Generation(); got != 0 {
		t.Fatalf("a module whose last owner released still installed supply content "+
			"(generation %d); the dependency is gone here and must not be resolved", got)
	}
}
