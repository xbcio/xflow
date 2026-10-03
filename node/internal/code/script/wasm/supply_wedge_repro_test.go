package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// TestSupplyAppliedBeforeEngineExistsWedge reproduces the production wedge
// reported on 2026-09-28: a collection runner whose wasm node refused every
// batch forever with "is declared source-driven for [X] but holds no
// supply-borne configuration", freezing the Kafka commit frontier until a
// process restart.
//
// The sequence is ordinary, not exotic: content reaches the registry before
// anything has compiled the module (ApplyHints runs on its own goroutine and
// does not wait for activation), and the registration that would install it
// arrives afterwards from a DIFFERENT owner than any that already registered.
//
// The two halves of the guard then disagree permanently:
//
//   - seedSourceDrivenByKey marks the module source-driven, so
//     SupplyConfiguredByDigest's revision==0 test is what refuses traffic;
//   - the notify handler returned nil while the engine did not exist, so no
//     swapConfig ever ran and active.revision stayed 0.
//
// Nothing re-delivers: RegisterConsumer only notifies when r.snapshots already
// has the name (it does here), so the arriving owner's callback runs — and finds
// the engine that by then EXISTS and swaps it. The wedge is therefore specific
// to the ordering below, and the assertion is that it must not persist.
func TestSupplyAppliedBeforeEngineExistsWedge(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	digest := "sha256:" + mustModuleKey(t, code)

	// A dedicated registry: the wedge is about this module's own registration,
	// and supply.Default carries state other tests depend on.
	reg := supply.NewRegistry()

	// The supply node name is the module's own artifact supply. Its content is
	// the artifact pointer. It is applied BEFORE the module is compiled —
	// ApplyHints is asynchronous and does not wait for activation.
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "wasm-artifacts",
		// The real pointer content is opaque to this layer; what matters is
		// that a consumer of "wasm-artifacts" is meant to configure its pool
		// from it.
		Content:   []byte(`{"rules":[{"name":"from-pointer","expr":"true"}]}`),
		Hash:      "pointer-h1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply pointer content: %v", err)
	}

	// Registration arrives first, from the warm-up owner — the module is not
	// compiled yet, so the notify handler records the content as accepted
	// without configuring anything.
	if err := RegisterSupplyConsumerByDigest(digest, "wasm-artifacts", "node:collect/decode", reg); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if SupplyConfiguredByDigest(digest) {
		t.Fatal("precondition: a module with no engine cannot report configured")
	}

	// Execution-time guard: compile, then register under ITS owner. This is
	// exactly script.ensureWasmSupplyConsumers' order.
	if err := CompileModule(ctx, code); err != nil {
		t.Fatalf("CompileModule: %v", err)
	}

	// The guard now asks the predicate before doing anything.
	if !SupplyConfiguredByDigest(digest) {
		t.Fatal("WEDGE REPRODUCED: module was created AFTER the supply content was " +
			"already applied and accepted, so no notification will ever reach it, " +
			"and SupplyConfiguredByDigest stays false forever; " +
			"every message fails closed with 'holds no supply-borne configuration'")
	}
}
