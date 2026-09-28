package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// TestReclaimedEngineLosesSupplyConfigWedge covers the second, deterministic
// route to the wedge: a source-driven module whose engine is reclaimed while
// idle.
//
// reclaimIdleEngines deliberately keeps INTENT (the sourceDriven marker and the
// consumer registration) and drops the COMPILED ARTIFACT, on the stated premise
// that "the rebuild path (script.ensureWasmSupplyConsumers) already restores
// everything else on the next message". It does not, on its own:
//
//   - the engine is recreated by CompileModule with no pool;
//   - RegisterSupplyConsumerByDigest short-circuits on alreadyOwner, so the
//     registry never re-notifies for the unchanged content;
//   - SupplyConfiguredByDigest therefore stayed false and the guard refused the
//     message forever.
//
// What must restore it is engine creation resolving the config from the supplies
// the module is registered against. This matters in production because
// engineIdleTTL defaults to 15 minutes and a supply-driven node commonly goes
// quiet for longer than that between bursts.
//
// The test runs against a private host so the reclaim is ours: the registry is
// private too, and the registration is made through the host-scoped entry point
// so create-time resolution reads the same host that reclamation emptied.
func TestReclaimedEngineLosesSupplyConfigWedge(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	digest := "sha256:" + mustModuleKey(t, code)
	const (
		supplyNode = "wasm-artifacts"
		owner      = "node:collect/decode"
	)

	// A host of our own so the reclaim is ours. newTestReactorHost disables the
	// async sweep, so nothing is reclaimed behind the test's back.
	h := newTestReactorHost(t)
	reg := supply.NewRegistry()

	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if err := registerSupplyConsumerByDigestOn(h, digest, supplyNode, owner, reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Apply(ctx, supply.Snapshot{
		Name:      supplyNode,
		Content:   []byte(`{"rules":[{"name":"from-pointer","expr":"true"}]}`),
		Hash:      "pointer-h1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("precondition: registration must have marked the engine source-driven")
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("precondition: engine revision = %d, want the applied supply revision 1", got)
	}

	// Idle the engine past a one-minute TTL. touchLocked was stamped by the
	// notification above, so rewind explicitly rather than relying on clock skew.
	h.mu.Lock()
	h.engines[mustModuleKey(t, code)].lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	h.mu.Unlock()
	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 1 {
		t.Fatalf("reclaimed %d engines, want exactly the one this test configured", n)
	}

	// The next message: engineForCode recreates the engine, and creation must
	// resolve the module's config from the registry without any notification.
	rebuilt, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode after reclaim: %v", err)
	}
	if got := rebuilt.availability(); got != AvailFresh {
		t.Fatalf("WEDGE: the recreated engine's availability = %v, want AvailFresh — the "+
			"registration survived the reclaim but nothing handed the recreated engine the "+
			"supply content the registry still holds, so every message fails closed until "+
			"the process restarts", got)
	}
	if got := rebuilt.Generation(); got != 1 {
		t.Fatalf("recreated engine generation = %d, want the supply revision 1 (the config "+
			"must come from the registry, not the legacy globals path)", got)
	}
}
