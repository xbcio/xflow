package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkPoolWidth answers whether widening the instance pool raises aggregate
// eval throughput, holding the offered concurrency fixed.
//
// The question matters because the pool's default width is GOMAXPROCS
// (host.go's defaultPoolSize) while admission allows far more concurrent
// borrowers than that, and a live run measured borrow wait at 83% of script
// execute duration. Two readings fit that number and they imply opposite fixes:
//
//   - the pool is an artificial cap: work is waiting on a channel while cores
//     sit idle, and widening converts waiting into throughput
//   - the pool is a queue in front of a saturated resource: eval is CPU-bound,
//     the cores are already busy, and widening moves the same queue from
//     p.free into the Go scheduler without adding any capacity
//
// Only a measurement separates them. If throughput is flat from width=GOMAXPROCS
// upward, the second reading holds and widening is not the fix — it only spends
// ~5.5 MiB of resident linear memory per instance to relabel the wait.
//
// Measured: flat from width 8 to 64 on an 8-core machine. The second reading
// holds — eval is CPU-bound and the pool is a queue in front of a saturated
// resource, not the cap itself. Keep this benchmark: "borrow_wait dominates
// execute duration" reads like a narrow pool every time it is seen, and this is
// the measurement that says otherwise.
func BenchmarkPoolWidth(b *testing.B) {
	// Offered concurrency stays fixed across widths: the variable under test is
	// the pool, not the load.
	const offered = 64

	for _, width := range []uint64{4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			ctx := context.Background()
			h := newReactorHost()
			e, err := h.engineFor(ctx, reactorWasm)
			if err != nil {
				b.Fatalf("engineFor: %v", err)
			}
			// Production-sized work, not a toy input. A 3-rule config on {"x":8.0}
			// evals in ~9us, where the live pipeline measures ~4.7ms per eval
			// (stdin_redundancy_bench_test's no-supplies case at the measured mean
			// record size). At 9us per eval the pool is never the constraint no
			// matter how narrow, so a toy input answers a different question than
			// the one asked.
			rules := make([][2]string, 0, 60)
			for i := range 60 {
				rules = append(rules, [2]string{
					fmt.Sprintf("r%d", i),
					fmt.Sprintf(`request.uri startsWith "/api/v%d/"`, i%8),
				})
			}
			cfgBytes, err := json.Marshal(ruleConfig(rules...))
			if err != nil {
				b.Fatalf("marshal cfg: %v", err)
			}
			if err := e.swapConfig(ctx, cfgBytes, width, 1); err != nil {
				b.Fatalf("swapConfig(width=%d): %v", width, err)
			}
			// Borrow through the same path production uses, so the measurement
			// includes borrow/giveBack bookkeeping rather than only guest time.
			facade := &reactorFacade{}
			// 6845 bytes is the measured mean of the live test-env topic.
			input, err := json.Marshal(realisticRecord(6845))
			if err != nil {
				b.Fatalf("marshal input: %v", err)
			}

			var done atomic.Int64
			var wg sync.WaitGroup
			b.ResetTimer()
			start := time.Now()

			// Every goroutine loops until the shared budget of b.N evals is spent.
			// Splitting b.N per goroutine instead would let fast goroutines idle
			// while slow ones finish, understating throughput at every width alike
			// but adding variance for no gain.
			var budget atomic.Int64
			budget.Store(int64(b.N))
			for range offered {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for budget.Add(-1) >= 0 {
						if _, err := facade.evalFromPool(ctx, e, input); err != nil {
							b.Errorf("eval: %v", err)
							return
						}
						done.Add(1)
					}
				}()
			}
			wg.Wait()
			b.StopTimer()

			elapsed := time.Since(start)
			if n := done.Load(); n > 0 && elapsed > 0 {
				b.ReportMetric(float64(n)/elapsed.Seconds(), "eval/s")
			}
		})
	}
}
