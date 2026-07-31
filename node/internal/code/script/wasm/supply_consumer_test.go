package wasm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// Registering a consumer for a module that has NOT been compiled yet must still
// mark it source-driven: at activation time the module is usually uncompiled, and
// losing this fact sends production traffic down the globals path where there are
// no rules at all.
func TestRegisterSupplyConsumerBeforeCompileMarksSourceDriven(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	if !sharedReactorHost.isSourceDriven(code) {
		t.Fatal("code must be recorded source-driven before its engine exists")
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("engine created after registration must be source-driven")
	}
}

// Applying content must build a pool whose revision is the SERVER revision, so
// config_generation is comparable across runners.
func TestOnSupplyChangedCarriesServerRevision(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "h1", Revision: 42,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.Generation(); got != 42 {
		t.Fatalf("Generation() = %d, want the server revision 42", got)
	}
}

// The double-check in engineForCode: a registration racing engine creation must
// not leave the module on the globals path.
func TestConcurrentRegisterAndEngineCreate(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = RegisterSupplyConsumer(code, "rules", reg)
	}()
	go func() {
		defer wg.Done()
		_, _ = sharedReactorHost.engineForCode(context.Background(), code)
	}()
	wg.Wait()

	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("a registration racing engine creation must still mark the engine source-driven")
	}
}

// A rejected supply content change must not tear down the module's last-good
// pool: OnSupplyChanged returns the error, but Execute keeps serving.
func TestOnSupplyChangedRejectsBadContentKeepsLastGood(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[{"name":"ok","expr":"x > 0"}]}`),
		Hash: "good", Revision: 1, FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(good): %v", err)
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.availability(); got != AvailFresh {
		t.Fatalf("availability after good content = %v, want Fresh", got)
	}

	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[{"bad":true}]}`),
		Hash: "bad", Revision: 2, FetchedAt: time.Now(),
	}); err == nil {
		t.Fatal("bad content must be rejected by the consumer")
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after rejected content, want the last-good revision 1", got)
	}
}

// The registry notifies with content a module may already be serving:
// RegisterConsumer notifies immediately on every workflow re-activation, and
// Apply re-notifies whenever any consumer's verdict for the current content is
// unresolved. Rebuilding the pool for identical bytes recompiles every instance
// and drains the old pool for nothing, so the consumer must short-circuit.
func TestSupplyConsumerIsNoOpForIdenticalContent(t *testing.T) {
	ctx := context.Background()
	code := b64(reactorWasm)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	t.Cleanup(func() { UnregisterSupplyConsumer(code, "rules", reg) })

	content := []byte(`{"rules":[{"name":"r1","expr":"true","tag":"t1"}]}`)
	if err := reg.Apply(ctx, supply.Snapshot{Name: "rules", Content: content, Hash: "h1", Revision: 1}); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	first := e.active.Load()
	if first == nil {
		t.Fatal("first Apply did not build a pool")
	}

	// Re-registering is what a workflow re-activation does; it notifies
	// immediately with the already-cached content.
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if got := e.active.Load(); got != first {
		t.Fatal("re-registration with identical content rebuilt the pool; it must be a no-op")
	}

	// A revision bump with identical bytes must not swap either.
	if err := reg.Apply(ctx, supply.Snapshot{Name: "rules", Content: content, Hash: "h1", Revision: 2}); err != nil {
		t.Fatalf("same-content Apply: %v", err)
	}
	if got := e.active.Load(); got != first {
		t.Fatal("a revision bump with identical bytes rebuilt the pool; it must be a no-op")
	}

	// Genuinely different content still swaps.
	next := []byte(`{"rules":[{"name":"r2","expr":"true","tag":"t2"}]}`)
	if err := reg.Apply(ctx, supply.Snapshot{Name: "rules", Content: next, Hash: "h2", Revision: 3}); err != nil {
		t.Fatalf("changed-content Apply: %v", err)
	}
	if got := e.active.Load(); got == first {
		t.Fatal("changed content must swap the pool")
	}
}
