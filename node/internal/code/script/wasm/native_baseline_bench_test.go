// native_baseline_bench_test.go: native Go baseline for the wasm reactor logic.
//
// The wasm guest (testdata/reactor/main.go) does exactly three things per eval:
//
//  1. json.Unmarshal the input bytes into map[string]any
//  2. expr.Run each compiled program against that env
//  3. json.Marshal the result {"matched":[...]}
//
// This file runs the same three steps in the host process, with no sandbox.
// Comparing these numbers against BenchmarkEvalCPUByRecordSize (cpu_ms/eval)
// gives the wasm/native ratio N — the core deliverable.
//
// All benchmarks share the same five input sizes as the wasm counterpart
// (64B / 512B / 2594B / 6845B / 9753B) and the same live rule count (2),
// so the numbers are directly comparable.
//
// Constraint: all sizes run inside the SAME go test -bench invocation so the
// numbers can be compared without the >25% cross-session drift documented for
// this machine.
package wasm

import (
	"encoding/json"
	"testing"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// nativeProgs holds the two compiled programs that mirror the live SAS
// configuration.  Rules are compiled once at benchmark setup time, matching the
// reactor guest behaviour (configure() compiles, eval() only runs).
type nativeProgs struct {
	progs []struct {
		name string
		prog *vm.Program
	}
}

// compileNativeProgs compiles liveRuleCount rules using the same options the
// wasm guest uses (expr.Env with empty map, AllowUndefinedVariables, AsBool).
func compileNativeProgs(b *testing.B) *nativeProgs {
	b.Helper()
	rules := benchRules(liveRuleCount) // from cpu_saturation_bench_test.go
	np := &nativeProgs{}
	for _, r := range rules {
		p, err := expr.Compile(r[1],
			expr.Env(map[string]any{}),
			expr.AllowUndefinedVariables(),
			expr.AsBool(),
		)
		if err != nil {
			b.Fatalf("compile rule %s: %v", r[0], err)
		}
		np.progs = append(np.progs, struct {
			name string
			prog *vm.Program
		}{r[0], p})
	}
	return np
}

// runNativeEval mirrors testdata/reactor/main.go's eval() function body
// exactly, running in the host process with no wasm sandbox.
func (np *nativeProgs) runNativeEval(input []byte) ([]byte, error) {
	// Step 1: Unmarshal
	var env map[string]any
	if err := json.Unmarshal(input, &env); err != nil {
		return nil, err
	}

	// Step 2: expr.Run each program
	matched := make([]string, 0, len(np.progs))
	for _, r := range np.progs {
		out, err := expr.Run(r.prog, env)
		if err != nil {
			continue
		}
		if b, ok := out.(bool); ok && b {
			matched = append(matched, r.name)
		}
	}

	// Step 3: Marshal result
	return json.Marshal(map[string]any{"matched": matched})
}

// ─── Full pipeline: Unmarshal + expr.Run + Marshal ───────────────────────────

// BenchmarkNativeBaseline_FullPipeline sweeps the five input sizes used by
// BenchmarkEvalCPUByRecordSize. The cpu_ms/eval metric here is directly
// comparable to the wasm counterpart — divide wasm/native to get N.
func BenchmarkNativeBaseline_FullPipeline(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"tiny=64B", 64},
		{"small=512B", 512},
		{"p50=2594B", 2594},
		{"mean=6845B", 6845},
		{"p90=9753B", 9753},
	}

	np := compileNativeProgs(b)

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			// Pre-serialise a realistic record of this size.
			rec := realisticRecord(s.size)
			input, err := json.Marshal(rec)
			if err != nil {
				b.Fatalf("marshal input: %v", err)
			}

			// Warmup: one run to pre-populate any allocator slabs.
			if _, err := np.runNativeEval(input); err != nil {
				b.Fatalf("warmup: %v", err)
			}

			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			cpuStart := cpuSeconds() // from cpu_saturation_bench_test.go
			for b.Loop() {
				if _, err := np.runNativeEval(input); err != nil {
					b.Fatalf("eval: %v", err)
				}
			}
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpu/float64(b.N)*1000, "cpu_ms/eval")
		})
	}
}

// ─── Cost breakdown: Unmarshal / expr.Run / Marshal individually ─────────────
//
// These three benchmarks run at the two representative sizes (p50 and p90) so
// we can see which step dominates and how the shares change with payload size.
// All three use the same pre-serialised input to make the numbers additive.

// BenchmarkNativeBreakdown_Unmarshal measures only json.Unmarshal.
func BenchmarkNativeBreakdown_Unmarshal(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"p50=2594B", 2594},
		{"p90=9753B", 9753},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rec := realisticRecord(tc.size)
			input, err := json.Marshal(rec)
			if err != nil {
				b.Fatalf("marshal: %v", err)
			}
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			cpuStart := cpuSeconds()
			for b.Loop() {
				var env map[string]any
				if err := json.Unmarshal(input, &env); err != nil {
					b.Fatalf("unmarshal: %v", err)
				}
			}
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpu/float64(b.N)*1000, "cpu_ms/op")
		})
	}
}

// BenchmarkNativeBreakdown_ExprRun measures only expr.Run (liveRuleCount rules
// against a pre-decoded env; excludes JSON parse and serialise).
func BenchmarkNativeBreakdown_ExprRun(b *testing.B) {
	np := compileNativeProgs(b)

	for _, tc := range []struct {
		name string
		size int
	}{
		{"p50=2594B", 2594},
		{"p90=9753B", 9753},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rec := realisticRecord(tc.size)
			input, err := json.Marshal(rec)
			if err != nil {
				b.Fatalf("marshal: %v", err)
			}
			// Decode once; we hold the env across iterations so the loop
			// is pure rule execution.
			var env map[string]any
			if err := json.Unmarshal(input, &env); err != nil {
				b.Fatalf("unmarshal: %v", err)
			}

			b.ResetTimer()
			cpuStart := cpuSeconds()
			for b.Loop() {
				for _, r := range np.progs {
					out, err := expr.Run(r.prog, env)
					if err != nil {
						continue
					}
					_ = out
				}
			}
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpu/float64(b.N)*1000, "cpu_ms/op")
		})
	}
}

// BenchmarkNativeBreakdown_Marshal measures only json.Marshal of the result.
func BenchmarkNativeBreakdown_Marshal(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"p50=2594B", 2594},
		{"p90=9753B", 9753},
	} {
		b.Run(tc.name, func(b *testing.B) {
			// The result is tiny regardless of input size — {"matched":[...]} —
			// but we still tie it to a size label for comparability.
			matched := []string{"r0"} // one match, same as any single-rule hit
			result := map[string]any{"matched": matched}

			b.ResetTimer()
			cpuStart := cpuSeconds()
			for b.Loop() {
				if _, err := json.Marshal(result); err != nil {
					b.Fatalf("marshal: %v", err)
				}
			}
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpu/float64(b.N)*1000, "cpu_ms/op")
		})
	}
}
