// mem_limit_crash_iter_test.go — regression test for the guest heap exhaustion
// bug described in pool.go:maxEvalsPerInstance.
//
// # Background
//
// Go wasm guests (GOOS=wasip1) compiled with -buildmode=c-shared accumulate
// GC-internal span/mcache metadata with every heap allocation. The live heap is
// reclaimed by the GC, so the working set stays bounded, but the bookkeeping
// grows monotonically. Under the production memory cap (256 pages, 16 MiB) a
// guest that json.Unmarshal's a ~7 KB input exhausts the cap after ~18,856 evals
// with a hard `unreachable` (SIGABRT) trap from runtime.mcache.refill.
//
// The fix in pool.go proactively recycles instances at maxEvalsPerInstance
// (currently 8,000) — well before the crash threshold. This test verifies that
// the production configuration survives a run through the recycle threshold
// without returning a doom error to the caller.
//
// # What this test asserts
//
//  1. Under production settings (WithCloseOnContextDone=true, 256 pages / 16 MiB),
//     10,000 consecutive evals return no error.
//  2. EXACTLY one OnInstanceRecycled("max_evals") event is observed. Pool width
//     is 1 and the threshold is 8,000, so 1 is the only correct answer: 0 means
//     the recycle never fired, more than 1 means instances are being retired far
//     more often than the memory evidence justifies.
//
// # Discriminating power (verified 2026-08-20, do not assume — re-run if changed)
//
//	maxEvalsPerInstance = 20,000  → FAIL, 0 events. Confirms the assertion sees
//	                                a recycle path that never runs.
//	maxEvalsPerInstance = 1       → 10,001 events, runtime 21 s → 63.6 s. Under
//	                                the original `>= 1` bound this PASSED, which
//	                                is why the bound is now exact.
//
// If maxEvalsPerInstance is ever raised above 10,000 this test must be updated
// to run at least maxEvalsPerInstance+1 iterations. If the underlying wazero or
// Go WASM runtime issue is ever fixed, the maxEvalsPerInstance constant and this
// test can both be removed.
package wasm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// TestMemLimitCrashIter verifies that the production configuration does not
// crash after maxEvalsPerInstance evals, and that exactly one "max_evals"
// recycle event is observed.
//
// The test runs 10,000 iterations — just above maxEvalsPerInstance (8,000) —
// to confirm the planned-recycle path fires and the pool recovers rather than
// crashing. Without the fix, this would crash at ~8,000 evals (well before the
// raw threshold of ~18,856 at p50 input) and fail with a doom error.
func TestMemLimitCrashIter(t *testing.T) {
	// Production observer, recording recycle events.
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()

	h := newReactorHost()
	rtCfg := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(engine.DefaultWasmMemoryPages). // 256 pages = 16 MiB
		WithCompilationCache(wazero.NewCompilationCache())
	h.rt = wazero.NewRuntimeWithConfig(ctx, rtCfg)
	wasi_snapshot_preview1.MustInstantiate(ctx, h.rt)
	h.rtOnce.Do(func() {})

	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	cfgBytes, err := json.Marshal(ruleConfig(benchRules(liveRuleCount)...))
	if err != nil {
		t.Fatalf("marshal cfg: %v", err)
	}
	if err := e.swapConfig(ctx, cfgBytes, 1, 1); err != nil {
		t.Fatalf("swapConfig: %v", err)
	}

	input, err := json.Marshal(realisticRecord(6845)) // mean record size
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	facade := &reactorFacade{host: h}

	// Warmup — must not fail.
	if _, err := facade.evalFromPool(ctx, e, input); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// 10,000 iterations at the mean record size: the fix must carry us through
	// the maxEvalsPerInstance=8,000 boundary without a crash.
	//
	// NOTE: if maxEvalsPerInstance is ever raised above 10,000 this limit must
	// be increased accordingly so the test still exercises the recycle path.
	const iterations = 10_000
	for i := 1; i <= iterations; i++ {
		if _, err := facade.evalFromPool(ctx, e, input); err != nil {
			t.Fatalf("iter %d: unexpected doom/error: %v\n"+
				"This means the planned-recycle fix is not preventing the "+
				"guest heap exhaustion crash. Check maxEvalsPerInstance in pool.go.",
				i, err)
		}
	}

	// EXACTLY one, not "at least one". Pool width is 1, the run is 10 000 evals
	// and the threshold is 8 000, so the correct answer is 1 and nothing else.
	//
	// A >= 1 bound was measured to be toothless: with maxEvalsPerInstance
	// temporarily set to 1, this test recycled 10 001 times and still PASSED,
	// while runtime went from 21 s to 63.6 s. That regression — a 62 ms cold
	// start charged to every single message — is precisely the failure this
	// file exists to catch, and the loose bound could not see it.
	causes := rec.recycledCauses()
	var maxEvalsCount int
	for _, c := range causes {
		if c == "max_evals" {
			maxEvalsCount++
		}
	}
	if maxEvalsCount != 1 {
		t.Fatalf("got %d OnInstanceRecycled(%q) events over %d evals, want exactly 1.\n"+
			"all causes: %v\n"+
			"  0 means the planned-recycle path never fired: either maxEvalsPerInstance\n"+
			"    was raised at or above the iteration count, or evalCount stopped\n"+
			"    incrementing.\n"+
			"  >1 means instances are being retired far too often. Each recycle costs a\n"+
			"    62 ms rebuild, so this is a throughput regression even though every\n"+
			"    eval still returns a correct result.",
			maxEvalsCount, "max_evals", iterations, causes)
	}
}
