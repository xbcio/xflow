// pool_slot_leak_probe_test.go — records why WithCloseOnContextDone must stay on.
//
// That flag costs 44x (see context_done_bench_test.go), which makes turning it
// off the single largest performance lever in the wasm path. This test is the
// reason it cannot be pulled: without the flag, a guest that does not return
// holds its pool slot forever, and the pool is the whole width of wasm
// concurrency.
//
// wazero has no second interruption mechanism. Searching v1.9.0 for fuel or
// epoch interruption returns nothing — those are wasmtime APIs, not wazero's.
// WithCloseOnContextDone is the only checkpoint that exists, so "turn the flag
// off and bound the guest some other way" is not a design that is currently
// available.
//
// WHY THIS TEST IS SKIPPED BY DEFAULT
//
// Proving the leak requires leaking: the goroutine stuck in the spin guest
// keeps a core pegged for as long as the test binary lives, and nothing can
// reclaim it — that is the very claim under test. Measured cost of letting it
// run in the shared binary: TestWasm_ModuleCacheHit -count=5 goes from 1.39s
// to over 120s, because each probe run permanently consumes one core. So the
// probe is opt-in, and CI keeps its timings.
//
// Run it deliberately, in its own binary, when re-testing the claim against a
// new wazero version:
//
//	XFLOW_WASM_LEAK_PROBE=1 go test -run TestPoolSlotLeak_WithoutContextDone \
//	    -count=1 ./node/internal/code/script/wasm/
//
// Last measured 2026-08-20 against wazero v1.9.0: SLOT_PERMANENTLY_LEAKED.
//
// CONTRAST: TestWasm_InFlightTimeout (wasm_test.go:149) runs the same spin
// guest with the flag ON and the guest IS interrupted. That test passing while
// this one reports a leak is the paired evidence — the difference between them
// is the flag and nothing else.
package wasm

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// hostWithoutContextDone builds a reactorHost whose wazero runtime has
// WithCloseOnContextDone(false) — the configuration under test. It mirrors
// hostWithContextDone from context_done_bench_test.go.
func hostWithoutContextDone(t *testing.T) *reactorHost {
	t.Helper()
	ctx := context.Background()
	h := newReactorHost()
	rtCfg := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(false).
		WithMemoryLimitPages(engine.DefaultWasmMemoryPages).
		WithCompilationCache(wazero.NewCompilationCache())
	h.rt = wazero.NewRuntimeWithConfig(ctx, rtCfg)
	wasi_snapshot_preview1.MustInstantiate(ctx, h.rt)
	h.rtOnce.Do(func() {}) // prevent h.runtime() from re-initialising
	return h
}

// TestPoolSlotLeak_WithoutContextDone demonstrates that with the flag off, one
// non-returning guest permanently costs one pool slot.
//
// The probe evals a forever-looping guest under a 200 ms deadline, then tries
// to borrow. Passing the deadline is not itself the finding — the finding is
// that evalFromPool never returns afterwards, so doom() never runs, so the
// slot is never rebuilt, so the borrow starves too.
//
// A verdict of SLOT_PERMANENTLY_LEAKED is the EXPECTED result and passes: this
// test documents wazero's behaviour, it does not ask for it to change. What
// fails it is the opposite — a slot that comes back — because that would mean
// the 44x flag is buying something this codebase no longer needs, and the
// decision to keep it should be revisited rather than silently inherited.
func TestPoolSlotLeak_WithoutContextDone(t *testing.T) {
	if os.Getenv("XFLOW_WASM_LEAK_PROBE") != "1" {
		t.Skip("leaks a CPU-pegged goroutine by design; set XFLOW_WASM_LEAK_PROBE=1 to run")
	}

	const outerGuard = 3 * time.Second
	const evalTimeout = 200 * time.Millisecond
	const borrowTimeout = 500 * time.Millisecond

	verdict := make(chan string, 1)

	go func() {
		ctx := context.Background()
		h := hostWithoutContextDone(t)

		e, err := h.engineFor(ctx, reactorSpinWasm)
		if err != nil {
			verdict <- "SETUP_FAIL: engineFor: " + err.Error()
			return
		}
		// Pool size 1 makes the leak immediately visible. defaultPoolSize() is
		// GOMAXPROCS, which would mask a single-slot leak behind the remaining
		// slots — and masking is exactly the production failure mode: the pool
		// degrades one slot per bad message until nothing is left.
		const poolSize = 1
		if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), poolSize, 0); err != nil {
			verdict <- "SETUP_FAIL: swapConfig: " + err.Error()
			return
		}
		facade := &reactorFacade{host: h}

		evalCtx, evalCancel := context.WithTimeout(ctx, evalTimeout)
		defer evalCancel()

		evalDone := make(chan error, 1)
		go func() {
			_, err := facade.evalFromPool(evalCtx, e, []byte(`{}`))
			evalDone <- err
		}()

		select {
		case evalErr := <-evalDone:
			if evalErr == nil {
				verdict <- "UNEXPECTED_SUCCESS: eval returned nil error from an infinite-loop guest"
				return
			}
			// Eval returned. Whether the slot came back is the real question:
			// doom() rebuilds asynchronously, so a successful borrow here means
			// recovery actually happened.
			borrowCtx, borrowCancel := context.WithTimeout(ctx, borrowTimeout)
			defer borrowCancel()
			if _, _, borrowErr := e.borrow(borrowCtx); borrowErr != nil {
				verdict <- "SLOT_LOST: eval returned with error but borrow timed out: " + borrowErr.Error()
			} else {
				verdict <- "SLOT_RECOVERED: eval returned with error, borrow succeeded"
			}
			return

		case <-time.After(evalTimeout + 100*time.Millisecond):
			// The deadline passed and evalFromPool still has not returned. Confirm
			// the consequence rather than asserting it: borrow must starve too.
			borrowCtx, borrowCancel := context.WithTimeout(ctx, borrowTimeout)
			defer borrowCancel()
			if _, _, borrowErr := e.borrow(borrowCtx); borrowErr != nil {
				verdict <- "SLOT_PERMANENTLY_LEAKED: eval goroutine still blocked after ctx expiry; " +
					"borrow also timed out, confirming the pool is exhausted"
			} else {
				verdict <- "UNEXPECTED_BORROW_SUCCESS: eval goroutine still blocked but borrow succeeded"
			}
			return
		}
	}()

	select {
	case v := <-verdict:
		t.Logf("probe verdict: %s", v)
		switch {
		case strings.HasPrefix(v, "SETUP_FAIL: "):
			t.Fatalf("probe could not run: %s", v)
		case strings.HasPrefix(v, "SLOT_PERMANENTLY_LEAKED:"), strings.HasPrefix(v, "SLOT_LOST:"):
			// Expected. The flag stays on.
		default:
			t.Fatalf("the slot came back without WithCloseOnContextDone.\n\n"+
				"This test exists to justify paying 44x for that flag. If wazero can now\n"+
				"recover the slot without it, that justification is gone and the flag\n"+
				"should be re-evaluated rather than kept out of habit.\n\nVerdict: %s", v)
		}
	case <-time.After(outerGuard):
		// Even the borrow attempt never came back: a stronger form of the same
		// finding, not a different one.
		t.Logf("probe verdict: hard hang — probe goroutine did not return within %v; "+
			"both eval and borrow are blocked", outerGuard)
	}
}
