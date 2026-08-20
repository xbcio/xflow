// context_done_bench_test.go — quantifies the overhead of WithCloseOnContextDone.
//
// This is the single largest cost in the wasm path, and it is not the guest's:
// measured in one invocation at 2 rules and a 6845 B record, flag=true costs
// 1.564 cpu_ms/eval against 0.243 with it off, while the same three steps run
// natively cost 0.0299. So the 52× wasm/native ratio is 8.1× sandbox and 44×
// this one flag.
//
// Mechanism, read from wazero v1.9.0 rather than inferred: the flag makes the
// frontend emit a checkModuleExitCode trampoline at every wasm LOOP BACK-EDGE
// (internal/engine/wazevo/frontend/lower.go:1356). That trampoline is not a
// cheap memory read — it exits fully to Go (call_engine.go:419, the
// ExitCodeCheckModuleExitCode branch calling m.FailIfClosed) and re-enters via
// afterGoFunctionCallEntrypoint. wazero's own comment explains why it must:
// native code cannot be preempted.
//
// So the cost is PER LOOP ITERATION, not per call. An isolated 3-instruction
// wat loop measures 14.4 ns per iteration; the per-func.Call fixed cost is
// about 1 µs, and one eval makes three Calls — 0.2% of the 1.32 ms gap. That
// rules out both "make fewer Calls" and "pass a deadline-free ctx to the short
// ABI calls": ensureTermination is a RUNTIME-level compile-time property
// (call_engine.go:208 reads p.parent.ensureTermination), not a per-call one.
//
// Both arms run inside a SINGLE go test -bench invocation so cross-session
// machine drift (>25% on this host) cannot explain the gap. The two arms differ
// ONLY in that flag.
//
// Each arm uses its own fresh in-memory CompilationCache so wazero emits
// independently compiled machine code for each.  If they shared a cache the
// first arm to compile would determine the machine code for both, regardless of
// the flag — the benchmark would measure nothing.
package wasm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// hostWithContextDone builds a reactorHost whose wazero runtime is configured
// with CloseOnContextDone set to the given value.  It uses a fresh in-memory
// CompilationCache so the compiled machine code is independent of any other
// runtime in this process (including the other benchmark arm).
func hostWithContextDone(ctx context.Context, b *testing.B, closeOnDone bool) *reactorHost {
	b.Helper()
	h := newReactorHost()
	rtCfg := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(closeOnDone).
		WithMemoryLimitPages(engine.DefaultWasmMemoryPages).
		WithCompilationCache(wazero.NewCompilationCache()) // fresh, not shared
	h.rt = wazero.NewRuntimeWithConfig(ctx, rtCfg)
	wasi_snapshot_preview1.MustInstantiate(ctx, h.rt)
	// Mark rtOnce done so that h.runtime(ctx) below returns h.rt without
	// trying to re-initialise it.
	h.rtOnce.Do(func() {})
	return h
}

// BenchmarkCloseOnContextDone_Paired is the paired comparison.
//
// Both arms are inside one Benchmark function so they run in the same
// go test -bench invocation.  The only difference between them is the
// WithCloseOnContextDone flag; everything else — pool width, rule count,
// record size, eval path — is identical.
//
// Pair this with BenchmarkNativeBaseline_FullPipeline/mean=6845B in the SAME
// invocation to get the three-arm reading (flag on / flag off / native). The
// native arm is what turns "wasm is slow" into "the flag is slow" — without it
// the two wasm numbers say nothing about whether the sandbox is the problem.
//
// The ctx deliberately carries a deadline, matching script.go:241, which wraps
// every execution in context.WithTimeout. It changes nothing for the loop
// trampoline (that is compiled in per runtime, not per call) but it does keep
// the per-Call monitor goroutine live — a background ctx has a nil Done()
// channel, so the flag would look about 1 µs/call cheaper than it is.
//
// Metric: cpu_ms/eval (per-process CPU-seconds, via getrusage, divided by
// iteration count).  Machine noise cannot inflate this number; only work done
// by this process counts.
//
// Keep -benchtime below about 18000x: the guest OOMs near iteration 18856 at
// the production 256-page memory limit, and it does so in BOTH arms (measured
// identical to the iteration), so this is a property of the guest's heap, not
// of the flag.
func BenchmarkCloseOnContextDone_Paired(b *testing.B) {
	cases := []struct {
		name        string
		closeOnDone bool
	}{
		{"closeOnDone=true", true},
		{"closeOnDone=false", false},
	}

	cfgBytes, err := json.Marshal(ruleConfig(benchRules(liveRuleCount)...))
	if err != nil {
		b.Fatalf("marshal cfg: %v", err)
	}
	input, err := json.Marshal(realisticRecord(6845))
	if err != nil {
		b.Fatalf("marshal input: %v", err)
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), engine.DefaultScriptTimeout)
			defer cancel()

			h := hostWithContextDone(ctx, b, c.closeOnDone)
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			// Pool width 1: single-goroutine serial eval, same as
			// BenchmarkEvalRuleShape.  We are measuring per-eval CPU cost, not
			// pool contention.
			if err := e.swapConfig(ctx, cfgBytes, 1, 1); err != nil {
				b.Fatalf("swapConfig: %v", err)
			}
			facade := &reactorFacade{host: h}

			// Warmup: one eval to settle lazy initialisation inside the guest.
			// Log what it matched: the guest swallows a rule error with
			// `continue`, so a silently-erroring rule set would otherwise
			// benchmark as though its rules ran.
			out, err := facade.evalFromPool(ctx, e, input)
			if err != nil {
				b.Fatalf("warmup: %v", err)
			}
			b.Logf("guest output: %v", out)

			b.ResetTimer()
			cpuStart := cpuSeconds()
			for b.Loop() {
				if _, err := facade.evalFromPool(ctx, e, input); err != nil {
					b.Fatalf("eval: %v", err)
				}
			}
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpu/float64(b.N)*1000, "cpu_ms/eval")
		})
	}
}
