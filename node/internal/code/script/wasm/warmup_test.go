package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// TestWarmup_OpensRuntimeWithoutModules asserts the warmer is useful even with
// nothing pre-registered: it opens the wazero runtime (resolving the on-disk
// compilation cache and instantiating WASI) so the first request does not.
func TestWarmup_OpensRuntimeWithoutModules(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	if err := f.warmup(context.Background()); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	if h.rt == nil {
		t.Fatal("warmup did not open the runtime; first request would pay for it")
	}
}

// warmup must tolerate a nil context — engine.Warmer callers are hosts, and the
// contract in engine/warmup.go substitutes Background rather than panicking.
func TestWarmup_NilContext(t *testing.T) {
	f := &reactorFacade{host: newReactorHost()}
	var nilCtx context.Context
	if err := f.warmup(nilCtx); err != nil {
		t.Fatalf("warmup(nil): %v", err)
	}
}

// TestPrewarm_BuildsPoolBeforeFirstRequest is the P2 acceptance test: after
// warm-up the module is compiled AND its pool is configured, so the first
// Execute borrows a ready instance instead of building the pool under the
// request's deadline.
func TestPrewarm_BuildsPoolBeforeFirstRequest(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := ruleConfig([2]string{"big", "x > 5"})

	h.addPrewarm(code, cfg)
	if err := f.warmup(context.Background()); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	e, err := h.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	p := e.active.Load()
	if p == nil {
		t.Fatal("warmup left no active pool; the first request would build it")
	}
	if got := len(p.free); got != p.size {
		t.Fatalf("pool has %d/%d instances ready", got, p.size)
	}

	// A subsequent Execute with the same config must reuse that pool, not swap.
	gen := p.gen
	out, err := f.Execute(context.Background(), code, map[string]any{
		"$config": cfg,
		"x":       9.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatalf("x=9 did not match rule big: %#v", out)
	}
	if now := e.active.Load(); now.gen != gen {
		t.Fatalf("config unchanged but pool was swapped: gen %d → %d", gen, now.gen)
	}
}

// TestPrewarm_ReregisterReplacesConfig guards against the prewarm set growing an
// entry per config revision, which would make warm-up build stale pools.
func TestPrewarm_ReregisterReplacesConfig(t *testing.T) {
	h := newReactorHost()
	code := b64(reactorWasm)
	h.addPrewarm(code, ruleConfig([2]string{"v1", "x > 1"}))
	h.addPrewarm(code, ruleConfig([2]string{"v2", "x > 2"}))

	mods := h.prewarmModules()
	if len(mods) != 1 {
		t.Fatalf("prewarm has %d entries, want 1 (re-register must replace)", len(mods))
	}
	rules := mods[0].cfg.(map[string]any)["rules"].([]any)
	if name := rules[0].(map[string]any)["name"]; name != "v2" {
		t.Fatalf("kept config %v, want the latest (v2)", name)
	}
}

// TestPrewarm_BadModuleSurfacesError: warm-up must report a failure rather than
// silently leaving a broken module to blow up on the first request.
func TestPrewarm_BadModuleSurfacesError(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	h.addPrewarm("!!!not base64!!!", nil)
	if err := f.warmup(context.Background()); err == nil {
		t.Fatal("warmup should fail on an undecodable module")
	}
}

// TestWarmup_ViaEngineRegistry exercises the real wiring the runner uses:
// engine.Warmup fans out to every registered warmer, including the reactor's.
func TestWarmup_ViaEngineRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := engine.Warmup(ctx); err != nil {
		t.Fatalf("engine.Warmup: %v", err)
	}
	if sharedReactorHost.rt == nil {
		t.Fatal("engine.Warmup did not reach the reactor warmer")
	}
}
