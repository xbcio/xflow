package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// cpuSeconds returns the process's total CPU time (user + system) so far.
//
// This is the measurement no external monitor can be trusted to give here:
// the machine runs other work, and a system-wide utilisation figure attributes
// none of it. getrusage is per-process, so it answers "how much CPU did THIS
// benchmark burn" even under contention.
func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	sec := func(tv syscall.Timeval) float64 {
		return float64(tv.Sec) + float64(tv.Usec)/1e6
	}
	return sec(ru.Utime) + sec(ru.Stime)
}

// liveRuleCount is the rule count the live SAS pipeline actually ran:
// xflow_wasm_config_rule_count reported 2 in both measured windows. Benchmarks
// here default to it rather than to a round number, because rule count sets how
// much work the guest does per eval and picking it arbitrarily decides the
// answer to the question these benchmarks exist to ask. A 60-rule
// configuration measures a pipeline nobody is running.
const liveRuleCount = 2

// benchRules builds n distinct rules over fields the realistic record carries.
func benchRules(n int) [][2]string {
	rules := make([][2]string, 0, n)
	for i := range n {
		rules = append(rules, [2]string{
			fmt.Sprintf("r%d", i),
			fmt.Sprintf(`request.uri startsWith "/api/v%d/"`, i%8),
		})
	}
	return rules
}

// BenchmarkEvalCPUSaturation answers whether wasm eval throughput is bounded by
// CPU, and if so what one message actually costs in CPU-seconds.
//
// The pipeline's target is 5500 msg/s on 8 cores. That is a budget of
// 8/5500 = 1.45 ms of CPU per message, for EVERYTHING — two wasm evals, Kafka
// consumption, Redis state, engine scheduling, storage. A live run measured
// 408 msg/s with an eval mean of 26.9 ms, but a mean duration cannot say
// whether that time was spent computing or waiting, and the two have opposite
// fixes: waiting is a concurrency problem, computing is a cost problem.
//
// Two independent readings settle it, and they must AGREE:
//
//  1. cpu_ms_per_eval — CPU-seconds consumed divided by evals completed.
//     Compare against the 0.7 ms per-eval share of the budget. This is an
//     absolute number and does not care what else runs on the machine.
//
//  2. The scaling curve across GOMAXPROCS. Work that is CPU-bound scales with
//     cores and then stops; work that is blocked on something else does not
//     scale at all. Reading 1 alone could be explained away as measurement
//     overhead — reading 2 cannot.
//
// The parallelism figure (cpu_seconds / wall_seconds) is the cross-check: it
// should approach GOMAXPROCS when saturated. If it sits far below while
// throughput is flat, eval is NOT CPU-bound and the whole cost analysis is
// pointed at the wrong thing.
//
// Machine noise: other processes cannot inflate reading 1 (it is per-process),
// but they DO steal cores and so depress both throughput and parallelism. Run
// this on a quiet machine and note the load average alongside the result.
func BenchmarkEvalCPUSaturation(b *testing.B) {
	original := runtime.GOMAXPROCS(0)
	b.Cleanup(func() { runtime.GOMAXPROCS(original) })

	for _, procs := range []int{1, 2, 4, 8} {
		if procs > original {
			continue
		}
		b.Run(fmt.Sprintf("gomaxprocs=%d", procs), func(b *testing.B) {
			runtime.GOMAXPROCS(procs)
			// Restore before the subtest returns: the testing package warns
			// about a benchmark that leaves GOMAXPROCS changed, and a leaked
			// value would silently cap every benchmark that runs after this
			// one in the same binary.
			b.Cleanup(func() { runtime.GOMAXPROCS(original) })
			ctx := context.Background()
			h := newReactorHost()
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			cfgBytes, err := json.Marshal(ruleConfig(benchRules(liveRuleCount)...))
			if err != nil {
				b.Fatalf("marshal cfg: %v", err)
			}
			// Pool width tracks the core count: the question is what the cores
			// can do, not what a fixed-width pool permits.
			if err := e.swapConfig(ctx, cfgBytes, uint64(procs), 1); err != nil {
				b.Fatalf("swapConfig: %v", err)
			}
			facade := &reactorFacade{}
			input, err := json.Marshal(realisticRecord(6845))
			if err != nil {
				b.Fatalf("marshal input: %v", err)
			}

			// Offered concurrency exceeds the core count so the cores are the
			// only thing that can limit the result.
			offered := procs * 4
			var done atomic.Int64
			var budget atomic.Int64
			budget.Store(int64(b.N))

			// Warm every pool instance before timing: first eval on an instance
			// pays lazy initialisation that steady-state evals do not.
			for range procs {
				if _, err := facade.evalFromPool(ctx, e, input); err != nil {
					b.Fatalf("warmup eval: %v", err)
				}
			}

			b.ResetTimer()
			cpuStart := cpuSeconds()
			wallStart := time.Now()

			var wg sync.WaitGroup
			for range offered {
				wg.Go(func() {
					for budget.Add(-1) >= 0 {
						if _, err := facade.evalFromPool(ctx, e, input); err != nil {
							b.Errorf("eval: %v", err)
							return
						}
						done.Add(1)
					}
				})
			}
			wg.Wait()

			wall := time.Since(wallStart).Seconds()
			cpu := cpuSeconds() - cpuStart
			b.StopTimer()

			n := float64(done.Load())
			if n == 0 || wall == 0 {
				b.Fatal("no evals completed")
			}
			b.ReportMetric(n/wall, "evals/s")
			b.ReportMetric(cpu/n*1000, "cpu_ms/eval")
			b.ReportMetric(cpu/wall, "parallelism")
			// Two wasm evals per message (decode + clean) is the live topology,
			// so the message rate this eval cost permits is half the eval rate.
			b.ReportMetric(n/wall/2, "msg/s_at_2evals")
		})
	}
}

// BenchmarkEvalCPUByRecordSize measures how eval CPU cost scales with input
// size, across the live topic's measured distribution.
//
// It separates two candidate cost models that the mean alone cannot:
//
//   - fixed-dominated: cost is roughly flat across sizes, so the expense is
//     instance borrow plus the cross-sandbox round trip. Batching amortises it
//     and is the highest-leverage fix available.
//   - size-dominated: cost tracks bytes, so the expense is parsing the record.
//     Batching moves the same bytes and saves almost nothing; only doing less
//     work per record helps.
//
// Single-goroutine on purpose: this asks what one eval costs, not what the
// machine can sustain. BenchmarkEvalCPUSaturation asks the second question.
func BenchmarkEvalCPUByRecordSize(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"p50=2594B", 2594},
		{"mean=6845B", 6845},
		{"p90=9753B", 9753},
	}
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			ctx := context.Background()
			h := newReactorHost()
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			rules := benchRules(liveRuleCount)
			cfgBytes, err := json.Marshal(ruleConfig(rules...))
			if err != nil {
				b.Fatalf("marshal cfg: %v", err)
			}
			if err := e.swapConfig(ctx, cfgBytes, 1, 1); err != nil {
				b.Fatalf("swapConfig: %v", err)
			}
			facade := &reactorFacade{}
			input, err := json.Marshal(realisticRecord(s.size))
			if err != nil {
				b.Fatalf("marshal input: %v", err)
			}
			if _, err := facade.evalFromPool(ctx, e, input); err != nil {
				b.Fatalf("warmup: %v", err)
			}

			b.SetBytes(int64(len(input)))
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

// BenchmarkEvalCPUByRuleCount separates the per-eval fixed cost from the
// per-rule cost by sweeping the rule count with the input held constant.
//
// This is the number that decides between the three candidate remedies, and
// no metric collected so far distinguishes them:
//
//   - If cost is nearly flat from 0 to 64 rules, almost all of it is fixed
//     overhead — borrow, the cross-sandbox round trip, and stdin encode/decode.
//     Then merging the two guests into one halves it, and evaluating a whole
//     batch in one call amortises it by the batch size. Both are worth doing
//     and neither requires giving up wasm.
//   - If cost rises steeply with rule count, the expense is rule evaluation
//     itself. Batching moves the same work and saves nothing; only cheaper
//     rule evaluation (or leaving wasm) helps.
//
// Rule count 0 is the load-bearing data point: it is the fixed cost with the
// business logic removed, measured through the exact production borrow path
// rather than estimated. The live pipeline runs liveRuleCount (2), so the
// distance between 0 and 2 is the share of today's cost that any amount of
// rule optimisation could ever recover.
func BenchmarkEvalCPUByRuleCount(b *testing.B) {
	for _, count := range []int{0, 1, liveRuleCount, 8, 32, 64} {
		b.Run(fmt.Sprintf("rules=%d", count), func(b *testing.B) {
			ctx := context.Background()
			h := newReactorHost()
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			cfgBytes, err := json.Marshal(ruleConfig(benchRules(count)...))
			if err != nil {
				b.Fatalf("marshal cfg: %v", err)
			}
			if err := e.swapConfig(ctx, cfgBytes, 1, 1); err != nil {
				b.Fatalf("swapConfig: %v", err)
			}
			facade := &reactorFacade{}
			input, err := json.Marshal(realisticRecord(6845))
			if err != nil {
				b.Fatalf("marshal input: %v", err)
			}
			if _, err := facade.evalFromPool(ctx, e, input); err != nil {
				b.Fatalf("warmup: %v", err)
			}

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
