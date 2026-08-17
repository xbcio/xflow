package wasm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
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
	h.seedSourceDrivenByKey(mustModuleKey(t, code))
	if !e.configFromSource.Load() {
		t.Fatal("registration after engine creation must flip the existing engine to source-driven")
	}

	// Activation order: registration first, engine created after. The engine
	// must resolve the flag at birth, from the seeded intent.
	h2 := newReactorHost()
	h2.seedSourceDrivenByKey(mustModuleKey(t, code))
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
		// Hashed before the goroutines start, both because t.Fatalf must not be
		// called from a non-test goroutine and because hashing inside the racing
		// goroutine would add ~3 ms of work before it reaches the window this test
		// is trying to hit.
		key := mustModuleKey(t, code)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var raced *reactorEngine
		var raceErr error
		go func() {
			defer wg.Done()
			<-start
			h.seedSourceDrivenByKey(key)
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

// An empty ruleset is a VALID config, not an error: it means "pass everything
// through, tag nothing", which is a real business setting. It must build a pool
// and serve, which is what distinguishes it from "content has not arrived yet"
// (§6.5). The two were conflated once and the engine would then have had no way
// to express "the source deliberately sent zero rules".
func TestSupplyConsumerAcceptsEmptyRuleset(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	t.Cleanup(func() { UnregisterSupplyConsumer(code, "rules", reg) })

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: emptyContent(), Hash: "h-empty", Revision: 1,
	}); err != nil {
		t.Fatalf("an empty ruleset must be accepted: %v", err)
	}

	f := &reactorFacade{host: sharedReactorHost}
	out, err := f.Execute(ctx, engine.Code(code), map[string]any{"x": 8.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute against an empty ruleset: %v", err)
	}
	if got := matched(t, out); len(got) != 0 {
		t.Fatalf("an empty ruleset matched %v; it must match nothing", got)
	}
}

// A module marked source-driven whose content never arrived must FAIL the call,
// not evaluate against zero rules. Passing every record through untagged and
// uncleansed is a silent data-quality incident, so the error is explicit — and it
// must be transient/retryable so the caller backs off and retries rather than
// treating the message as permanently bad and dead-lettering it.
//
// Reachable only under require_ready:false; the activation gate keeps traffic away
// otherwise.
func TestSourceDrivenWithNoContentFailsRetryably(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	t.Cleanup(func() { UnregisterSupplyConsumer(code, "rules", reg) })

	// No Apply: the module is source-driven with nothing ever installed.
	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if e.active.Load() != nil {
		t.Fatal("precondition failed: a pool exists, so this does not exercise the unconfigured path")
	}

	f := &reactorFacade{host: sharedReactorHost}
	_, execErr := f.Execute(ctx, engine.Code(code), map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if execErr == nil {
		t.Fatal("Execute with no content succeeded; it must fail rather than evaluate " +
			"against zero rules and pass every record through untagged")
	}
	var ce *types.ClassifiedError
	if !errors.As(execErr, &ce) {
		t.Fatalf("error must be classified so the caller can decide to retry, got %T: %v", execErr, execErr)
	}
	if !ce.Retryable || ce.Kind != types.ErrorKindTransient {
		t.Fatalf("error must be transient and retryable, got kind=%v retryable=%v",
			ce.Kind, ce.Retryable)
	}
}

// Content rejected at boot must not be terminal. The engine has no last-good pool
// to fall back on, so what matters is that a LATER good Apply still lands: nothing
// about the first rejection may leave the module permanently unable to configure.
func TestSupplyConsumerRecoversAfterFirstContentRejected(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	t.Cleanup(func() { UnregisterSupplyConsumer(code, "rules", reg) })

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: badContent(), Hash: "h-bad", Revision: 1,
	}); err == nil {
		t.Fatal("bad first content must be rejected")
	}
	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if e.active.Load() != nil {
		t.Fatal("rejected content must not install a pool")
	}
	if got := e.availability(); got != AvailUnavailable {
		t.Fatalf("availability = %v after the only content was rejected, want Unavailable", got)
	}

	// The source is fixed and re-published. This must configure the module.
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: contentWithRule("small", "x < 3"), Hash: "h-good", Revision: 2,
	}); err != nil {
		t.Fatalf("good content after a rejection: %v", err)
	}

	f := &reactorFacade{host: sharedReactorHost}
	out, err := f.Execute(ctx, engine.Code(code), map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute after recovery: %v", err)
	}
	if !matched(t, out)["small"] {
		t.Fatalf("recovered content is not in effect, matched %v", matched(t, out))
	}
	if got := e.Generation(); got != 2 {
		t.Fatalf("Generation() = %d, want the recovered revision 2", got)
	}
}
