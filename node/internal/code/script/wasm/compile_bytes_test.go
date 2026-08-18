package wasm

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/supply"
)

// The bytes entry point must resolve the source-driven flag exactly as the code
// entry point does. This is the whole reason CompileModuleBytes cannot simply
// call engineForKey: registration usually runs before the module is compiled, so
// the intent is sitting in h.sourceDriven waiting for whoever creates the
// engine to pick it up. An entry point that skips that step leaves the engine on
// the legacy globals path, evaluating every record against no rules at all, with
// nothing anywhere to repair it or report it.
func TestCompileModuleBytesResolvesSourceDriven(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	raw := decodeForTest(t, code)

	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}

	// The engine does not exist yet -- this is the activation-time ordering.
	if err := CompileModuleBytes(ctx, raw); err != nil {
		t.Fatalf("CompileModuleBytes: %v", err)
	}

	e, err := sharedReactorHost.engineForKey(ctx, moduleKeyOf(raw), raw)
	if err != nil {
		t.Fatalf("engineForKey: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("engine compiled via CompileModuleBytes must be source-driven; " +
			"skipping that resolution drops the module to the globals path with no rules")
	}
}

// The engine the bytes path PRODUCES must be the one the code path resolves.
// Two engines for one module means two instance pools, and a supply change
// would reconfigure only one of them -- the other keeps serving traffic under
// the old rules.
//
// The comparison takes engineForBytes' own return value, not a fresh lookup by
// key. An earlier version of this test re-fetched through engineForKey and
// compared that against engineForCode, which reduces to h.engines[key] ==
// h.engines[key]: it asserts engineForKey's sha256 dedup, which was never in
// question, and stays green even when engineForBytes returns an engine it
// never published (verified: that injection left the old form passing).
func TestCompileModuleBytesSharesEngineWithCodePath(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	raw := decodeForTest(t, code)

	viaBytes, err := sharedReactorHost.engineForBytes(ctx, raw)
	if err != nil {
		t.Fatalf("engineForBytes: %v", err)
	}
	viaCode, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if viaBytes != viaCode {
		t.Fatal("bytes and code entry points resolved to different engines for one module; " +
			"a supply change would reconfigure only one pool and the other would keep " +
			"serving traffic under the old rules")
	}
}

// A module the runtime rejects must surface an error rather than caching a
// broken engine. The bytes path skips base64 decoding, so compilation failure is
// the only failure mode left -- and it is the one an activation needs reported,
// because fail-closed is what keeps an unusable module from going live.
func TestCompileModuleBytesRejectsInvalidModule(t *testing.T) {
	if err := CompileModuleBytes(context.Background(), []byte("not a wasm module")); err == nil {
		t.Fatal("CompileModuleBytes accepted a non-wasm payload")
	}
}

// CompileModuleBytes deliberately does not populate codeCache, so the engine it
// creates is reachable ONLY through h.engines. That is fine precisely because
// the production hot path looks modules up by digest -- but "fine" here rests on
// moduleKeyOf(bytes) and moduleKeyFromDigest("sha256:"+hex) producing the same
// key. If those two ever diverge, activation-time compilation would silently
// stop priming the engine the first message resolves, putting a multi-second
// compile back under a request deadline with no diagnostic.
func TestCompileModuleBytesIsReachableByDigest(t *testing.T) {
	ctx := context.Background()
	raw := decodeForTest(t, testReactorCode(t))

	if err := CompileModuleBytes(ctx, raw); err != nil {
		t.Fatalf("CompileModuleBytes: %v", err)
	}

	digest := "sha256:" + moduleKeyOf(raw)
	key, ok := moduleKeyFromDigest(digest)
	if !ok {
		t.Fatalf("moduleKeyFromDigest rejected %q", digest)
	}
	sharedReactorHost.mu.Lock()
	_, hit := sharedReactorHost.engines[key]
	sharedReactorHost.mu.Unlock()
	if !hit {
		t.Fatal("module compiled from bytes is not reachable by its artifact digest; " +
			"the digest hot path would recompile it under a request deadline")
	}
}
