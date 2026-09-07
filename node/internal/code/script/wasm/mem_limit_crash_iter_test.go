// mem_limit_crash_iter_test.go — regression test for the guest heap exhaustion
// bug described in pool.go:maxEvalsPerInstance.
//
// # Background
//
// Go wasm guests (GOOS=wasip1) compiled with -buildmode=c-shared can exhaust
// the production memory cap (256 pages, 16 MiB) after a long run of
// json.Unmarshal evals, trapping with `unreachable` from runtime.mcache.refill.
// pool.go proactively recycles instances at maxEvalsPerInstance (currently
// 8,000) so the rebuild is planned rather than an unplanned crash.
//
// WHAT THIS TEST DOES NOT SHOW: that a crash would occur without the recycle.
// At 7 066 B — the size the original analysis reported crashing at 18 856 evals
// — a re-measurement on 2026-08-20 with a freshly built runtime survived 30 000.
// See maxEvalsPerInstance in pool.go for the full reproduction. This test
// therefore guards the recycle MECHANISM (it fires, exactly once, at the
// threshold), not a demonstrated crash boundary. Do not cite it as evidence
// that 8 000 is the right number.
//
// # What this test asserts
//
//  1. A real reactor guest is constrained to the production 256-page / 16 MiB
//     maximum, and wazero refuses an attempted growth beyond it.
//  2. The production recycle threshold remains 8,000 successful evals.
//  3. A real eval at that boundary succeeds, closes the old instance, emits
//     exactly one OnInstanceRecycled("max_evals"), and leaves a fresh instance
//     able to serve the next eval.
//
// The old version drove the host-side counter by executing 10,000 full guest
// JSON/expr evals. Under -race that can exceed the package's five-minute budget,
// while still not establishing an actual guest crash boundary (see above). This
// version preloads only the test-visible host counter to threshold-1, then drives
// the boundary and recovery with real guest evals. It therefore preserves the
// production memory-limit/recycle contract without making a counter transition
// depend on thousands of redundant, machine-speed-sensitive calls.
//
// If the underlying wazero or Go WASM runtime issue is ever fixed, the
// maxEvalsPerInstance constant and this test can both be removed.
package wasm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// TestMemLimitCrashIter verifies that a successful eval at the production
// threshold is returned before the memory-limited instance is recycled, and
// that its replacement can continue serving traffic.
func TestMemLimitCrashIter(t *testing.T) {
	// Production observer, recording recycle events.
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	cache := wazero.NewCompilationCache()
	t.Cleanup(func() {
		if err := cache.Close(ctx); err != nil {
			t.Errorf("close compilation cache: %v", err)
		}
	})

	h := newReactorHost()
	rtCfg := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(engine.DefaultWasmMemoryPages). // 256 pages = 16 MiB
		WithCompilationCache(cache)
	h.rt = wazero.NewRuntimeWithConfig(ctx, rtCfg)
	t.Cleanup(func() {
		if err := h.rt.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
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

	// Pin the operational safety policy independently of the mechanics below.
	// Adapting the preload to an arbitrary new threshold would make an accidental
	// increase invisible and could move recycling beyond the memory-safe bound.
	const wantMaxEvalsPerInstance = 8_000
	if maxEvalsPerInstance != wantMaxEvalsPerInstance {
		t.Fatalf("maxEvalsPerInstance = %d, want %d; re-derive the memory-safe bound before changing it",
			maxEvalsPerInstance, wantMaxEvalsPerInstance)
	}

	inst, pool, err := e.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow instance: %v", err)
	}
	maxPages, maxEncoded := inst.mem.Definition().Max()
	// The reactor guest itself leaves max unencoded; wazero still clamps the
	// effective maximum to RuntimeConfig.WithMemoryLimitPages. The second return
	// reports the guest encoding, not whether the runtime-enforced value applies.
	if maxPages != engine.DefaultWasmMemoryPages {
		e.giveBack(ctx, pool, inst)
		t.Fatalf("reactor memory maximum = %d pages (encoded=%t), want %d",
			maxPages, maxEncoded, engine.DefaultWasmMemoryPages)
	}
	if _, ok := inst.mem.Grow(engine.DefaultWasmMemoryPages); ok {
		e.giveBack(ctx, pool, inst)
		t.Fatalf("reactor memory grew beyond the %d-page production limit", maxPages)
	}

	// evalCount is host bookkeeping, not guest state. Preloading it avoids 7,999
	// redundant guest calls; the boundary call below is still a real eval against
	// the production guest and memory cap.
	inst.evalCount = maxEvalsPerInstance - 1
	e.giveBack(ctx, pool, inst)

	if _, err := facade.evalFromPool(ctx, e, input); err != nil {
		t.Fatalf("eval at recycle boundary: %v", err)
	}
	if !inst.mod.IsClosed() {
		t.Fatal("instance remained open after reaching maxEvalsPerInstance")
	}

	// The rebuild is asynchronous. Bound the borrow so a failed replacement is a
	// useful test failure instead of another package-level timeout.
	rebuildCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	replacement, replacementPool, err := e.borrow(rebuildCtx)
	if err != nil {
		t.Fatalf("borrow replacement after planned recycle: %v; causes: %v",
			err, rec.recycledCauses())
	}
	if replacement == inst {
		t.Error("planned recycle returned the closed instance instead of a replacement")
	}
	if replacement.evalCount != 0 {
		t.Errorf("replacement evalCount = %d, want 0", replacement.evalCount)
	}
	e.giveBack(ctx, replacementPool, replacement)

	if _, err := facade.evalFromPool(rebuildCtx, e, input); err != nil {
		t.Fatalf("eval on replacement: %v", err)
	}

	// EXACTLY one, not "at least one": the boundary eval retires the old instance,
	// while the first eval on its zero-count replacement must not retire it again.
	causes := rec.recycledCauses()
	var maxEvalsCount int
	for _, c := range causes {
		if c == "max_evals" {
			maxEvalsCount++
		}
	}
	if maxEvalsCount != 1 {
		t.Fatalf("got %d OnInstanceRecycled(%q) events, want exactly 1; all causes: %v",
			maxEvalsCount, "max_evals", causes)
	}
}
