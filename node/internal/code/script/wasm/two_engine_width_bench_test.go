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

// BenchmarkTwoEnginePoolWidth answers whether defaultPoolSize being per-ENGINE
// rather than per-process costs throughput when a workflow uses more than one
// wasm module.
//
// The default is GOMAXPROCS (host.go's defaultPoolSize), and reactorHost keys
// engines by module sha256 (host.go's engines map), so a pipeline with two
// script nodes backed by two different modules gets 2*GOMAXPROCS resident
// instances. A live 5-minute run against real traffic showed exactly this
// shape: stack dumps had 14-20 goroutines inside wazero on an 8-core machine
// with 60-110 more parked in borrow, and the ready-instance gauge read 8
// because SupplyMetrics.OnInstanceCount Sets a single unlabeled series that the
// second engine's pool overwrites.
//
// Whether 2x oversubscription actually costs anything is a separate question
// from whether it exists. Eval is CPU-bound inside the wazero interpreter, so
// two competing readings fit:
//
//   - oversubscription is free: the Go scheduler timeslices 16 runnable
//     goroutines across 8 cores about as efficiently as it runs 8, and the
//     extra instances only cost resident memory
//   - oversubscription costs throughput: more runnable goroutines than cores
//     adds context switching and cache pressure to a workload whose working set
//     is a 5.5 MiB linear memory per instance
//
// The benchmark holds offered concurrency fixed and varies only the per-engine
// width, with both engines driven in the same serial order the pipeline uses
// (decode then clean per record).
func BenchmarkTwoEnginePoolWidth(b *testing.B) {
	// Fixed across widths: the variable under test is the pool, not the load.
	const offered = 64

	// Two hosts rather than two module blobs: reactorHost keys engines by module
	// hash, so separate hosts model two distinct modules exactly as far as pool
	// and runtime resources are concerned, without needing a second .wasm file.
	for _, width := range []uint64{2, 4, 8} {
		b.Run(fmt.Sprintf("per_engine_width=%d(total=%d)", width, width*2), func(b *testing.B) {
			ctx := context.Background()

			engines := make([]*reactorEngine, 2)
			for i := range engines {
				h := newReactorHost()
				e, err := h.engineFor(ctx, reactorWasm)
				if err != nil {
					b.Fatalf("engineFor[%d]: %v", i, err)
				}
				engines[i] = e
			}

			// Same production-sized config the single-engine benchmark uses: at a
			// toy input's ~9us per eval the pool is never the constraint, so a toy
			// input answers a different question than the one asked.
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
			for i, e := range engines {
				if err := e.swapConfig(ctx, cfgBytes, width, 1); err != nil {
					b.Fatalf("swapConfig[%d](width=%d): %v", i, width, err)
				}
			}

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

			// Budget counts RECORDS, and each record costs one eval on each
			// engine — the same serial decode-then-clean the workflow performs.
			// Reporting record/s rather than eval/s keeps the number comparable
			// to the pipeline's per-record cost.
			var budget atomic.Int64
			budget.Store(int64(b.N))
			for range offered {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for budget.Add(-1) >= 0 {
						for _, e := range engines {
							if _, err := facade.evalFromPool(ctx, e, input); err != nil {
								b.Errorf("eval: %v", err)
								return
							}
						}
						done.Add(1)
					}
				}()
			}
			wg.Wait()
			b.StopTimer()

			elapsed := time.Since(start)
			if n := done.Load(); n > 0 && elapsed > 0 {
				b.ReportMetric(float64(n)/elapsed.Seconds(), "record/s")
			}
		})
	}
}
