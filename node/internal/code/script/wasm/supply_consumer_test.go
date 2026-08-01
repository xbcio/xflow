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

// Registration and engine creation must resolve source-driven config no matter
// which order they interleave. The two sides are a Dekker-style crossing —
// each publishes its own state then looks for the other's — so the guarantee is
// that they resolve under one h.mu hold, not that a double-check narrows the gap.
func TestRegistrationAndEngineCreationResolveInEitherOrder(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)

	// Warmup order: engine first, registration after. This is the production
	// ordering — activation registers consumers long after warmup compiled the
	// module.
	h := newReactorHost()
	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if e.configFromSource.Load() {
		t.Fatal("a module with no loader and no supply consumer must start on the globals path")
	}
	h.markConfigFromSourceOrSeed(code)
	if !e.configFromSource.Load() {
		t.Fatal("registration after engine creation must flip the existing engine to source-driven")
	}

	// Activation order: registration first, engine created after. The engine
	// must resolve the flag at birth, from the seeded intent.
	h2 := newReactorHost()
	h2.markConfigFromSourceOrSeed(code)
	e2, err := h2.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode (post-registration host): %v", err)
	}
	if !e2.configFromSource.Load() {
		t.Fatal("an engine created after registration must resolve source-driven at creation")
	}
}

// Note on what is NOT tested here: the nanosecond crossing where a registration
// lands between engineForCode's intent read and its codeCache.Add. Two attempts
// failed to establish it — a 200-iteration concurrent version, and a version
// that parked the registration on h.mu — and both passed identically against
// the defective split-lock code. The window is unreachable by any test, which is
// exactly why the fix is structural: engineForCode resolves the flag and
// publishes the engine under ONE h.mu hold (host.go:127-146), so the crossing
// cannot occur rather than being caught after the fact. A test asserting it
// would assert nothing.
func TestConcurrentRegisterAndEngineCreateIsRaceFree(t *testing.T) {
	// Three iterations, not twenty. Each one builds a fresh host and recompiles
	// the module, so this test cost 27s of the package's 48s — more than half,
	// and 11x the next slowest test. That price only buys value if more
	// iterations raise the odds of catching something, and they demonstrably do
	// not: the window this exercises is unreachable by scheduling (see the note
	// above), so iteration 20 tells us exactly what iteration 3 does. What the
	// repetition does still buy is more chances for the race detector to observe
	// the two goroutines interleaving, which is why it is not reduced to one.
	const iterations = 3
	for i := 0; i < iterations; i++ {
		h := newReactorHost()
		code := testReactorCode(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var raced *reactorEngine
		var raceErr error
		go func() {
			defer wg.Done()
			<-start
			h.markConfigFromSourceOrSeed(code)
		}()
		go func() {
			defer wg.Done()
			<-start
			raced, raceErr = h.engineForCode(context.Background(), code)
		}()
		close(start)
		wg.Wait()

		if raceErr != nil {
			t.Fatalf("iteration %d: engineForCode: %v", i, raceErr)
		}
		// Assert on the engine the RACING goroutine got: a fresh lookup would hit
		// codeCache and return without re-resolving the flag.
		if raced != nil && !raced.configFromSource.Load() {
			t.Fatalf("iteration %d: concurrent registration left the module on the globals path", i)
		}
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
	// testReactorCode, not b64(reactorWasm): RegisterSupplyConsumer flips this
	// code's engine to source-driven permanently (configFromSource is one-way by
	// design — a production module never reverts to the legacy $config path), and
	// the flag lives on the process-wide sharedReactorHost keyed by module
	// identity. Sharing reactorWasm's bytes therefore leaks this test's
	// source-driven engine — still holding the r2 pool built below — into every
	// other test using the same fixture. That surfaced as
	// TestReactor_EmptyConfigValid seeing "r2" under -count=2: its $config was
	// stripped on the source-driven branch and the stale pool answered instead.
	code := testReactorCode(t)
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
