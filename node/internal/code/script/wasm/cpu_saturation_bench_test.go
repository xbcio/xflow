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
//
// The field is `value`, not `request.uri`, and that correction is load-bearing.
// realisticRecord nests the request inside `value` as a JSON *string*, so
// `request` is not an env key at all: an earlier version of these rules read
// `request.uri` and every expr.Run returned "cannot fetch uri from <nil>". The
// guest's eval() swallows a rule error with `continue`, so the benchmarks ran
// green while measuring rules that aborted at their first field fetch.
//
// Measured cost of the two shapes at the live record size (one expr.Run, host
// process, same invocation): erroring 942 ns, matching `len(value) > 100`
// 2621 ns, trivial `topic startsWith` 51 ns. So the old shape was not free —
// error construction is most of that 942 ns — but against a ~30 µs decode it
// was never going to move the totals. See BenchmarkEvalRuleShape for the
// in-sandbox version of that comparison.
func benchRules(n int) [][2]string {
	rules := make([][2]string, 0, n)
	for i := range n {
		rules = append(rules, [2]string{
			fmt.Sprintf("r%d", i),
			fmt.Sprintf(`value contains "/api/v%d/"`, i%8),
		})
	}
	return rules
}

// benchRulesErroring reproduces the pre-correction rule shape: it reads a root
// the record does not publish, so every rule aborts at its first field fetch.
// Kept only so BenchmarkEvalRuleShape can measure what that mistake cost.
func benchRulesErroring(n int) [][2]string {
	rules := make([][2]string, 0, n)
	for i := range n {
		rules = append(rules, [2]string{
			fmt.Sprintf("r%d", i),
			fmt.Sprintf(`request.uri startsWith "/api/v%d/"`, i%8),
		})
	}
	return rules
}

// BenchmarkEvalRuleShape measures how much the erroring-rule mistake distorted
// every other benchmark in this file.
//
// Both arms run in ONE invocation, at the live rule count and the live mean
// record size, differing only in whether the rules reach a field that exists.
// If the two arms are within noise, the numbers reported from the erroring
// shape stand as measured and only the per-rule attribution was wrong. If they
// separate, every rule-sensitive reading here has to be re-taken.
func BenchmarkEvalRuleShape(b *testing.B) {
	shapes := []struct {
		name  string
		rules [][2]string
	}{
		{"erroring", benchRulesErroring(liveRuleCount)},
		{"matching", benchRules(liveRuleCount)},
	}
	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			ctx := context.Background()
			h := newReactorHost()
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			cfgBytes, err := json.Marshal(ruleConfig(s.rules...))
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
			// Report what the guest actually matched, so a silently-erroring
			// rule set cannot pass itself off as a working one again.
			out, err := facade.evalFromPool(ctx, e, input)
			if err != nil {
				b.Fatalf("warmup: %v", err)
			}
			b.Logf("guest output: %s", out)

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
		// The small sizes are not realistic traffic — they exist to locate the
		// intercept. Extrapolating a straight line from three points clustered
		// between 2.5 KB and 10 KB puts the y-intercept far outside the measured
		// range, which is exactly where a linear fit is least trustworthy.
		{"tiny=64B", 64},
		{"small=512B", 512},
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

// BenchmarkEvalBatchAmortisation measures what batching actually buys, instead
// of assuming it buys the batch factor.
//
// The premise behind "one eval per batch of 20" is that a per-call fixed cost
// gets amortised. That premise holds only if the fixed cost is large relative
// to the per-record cost. BenchmarkEvalCPUByRecordSize says it is not: cost
// tracks bytes almost linearly, with an intercept near 0.19 ms against 1.6 ms
// at the live mean record size.
//
// So this benchmark reports CPU per RECORD both ways. If batching worked as
// hoped, batch=20 would cost a fraction of batch=1 per record. If cost is
// byte-dominated, the two are nearly equal — the guest parses the same total
// bytes either way, and the only saving is the intercept.
//
// This is the arithmetic that decides whether batching is worth building. It is
// measured rather than reasoned because the same reasoning already went wrong
// once here in the opposite direction: stripItems documents a case where
// bigger batches made per-item cost WORSE (80/62/38 msg/s at batch 10/20/40)
// because the payload grew with the batch.
func BenchmarkEvalBatchAmortisation(b *testing.B) {
	const recordSize = 6845 // the live topic's measured mean
	for _, batch := range []int{1, 5, 20, 50} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
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
			if err := e.swapConfig(ctx, cfgBytes, 1, 1); err != nil {
				b.Fatalf("swapConfig: %v", err)
			}
			facade := &reactorFacade{}

			// One payload carrying `batch` records. The key is deliberately not
			// $items: stripItems removes that root before the payload is
			// encoded, which would silently measure an empty batch.
			records := make([]any, 0, batch)
			for range batch {
				records = append(records, realisticRecord(recordSize))
			}
			payload := realisticRecord(recordSize)
			payload["batch"] = records
			input, err := json.Marshal(payload)
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
			// Per RECORD, not per call: a per-call figure would rise with batch
			// size and say nothing about whether batching helps.
			b.ReportMetric(cpu/float64(b.N)/float64(batch)*1000, "cpu_ms/record")
		})
	}
}

// BenchmarkEvalCPUByRuleCount separates the per-eval fixed cost from the
// per-rule cost by sweeping the rule count with the input held constant.
//
// What the rule-count axis does NOT measure: rule compilation. The reactor
// guest compiles rules once in configure() and keeps the programs in package
// state (testdata/reactor/main.go's progs); eval() only calls expr.Run against
// them. So a rise across this axis is genuine per-rule execution — n programs
// run against one decoded env — not repeated parsing.
//
// That makes the intercept here easy to misread, and it was misread once: the
// cost remaining at rules=0 is NOT "per-call fixed overhead that batching
// amortises". It is dominated by decoding this record's bytes, which is fixed
// with respect to rule count and linear with respect to input size. See
// BenchmarkEvalCPUByRecordSize for the axis that actually separates those, and
// BenchmarkEvalBatchAmortisation for what the genuine per-call intercept is
// worth once measured (about 0.19 ms, so: not much).
//
// Measured (2 rules is the live configuration):
//
//	rules:   0      1      2      8      32     64
//	cpu_ms:  2.07   2.21   2.59   2.72   3.80   5.06
//
// So at the live rule count roughly four fifths of the cost survives removing
// every rule. Read alone that invites "the business logic is nearly free, so
// attack the overhead" — and the overhead it points at is the wrong one. The
// record-size sweep shows where that residue actually goes.
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
