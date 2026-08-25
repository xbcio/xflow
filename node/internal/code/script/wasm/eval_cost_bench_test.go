// eval_cost_bench_test.go dissects the per-eval cost into four layers:
//
//	A. Fixed host overhead (reactornoop: passthrough guest, zero JSON work)
//	B. Realistic guest cost (tagger: actual JSON decode+eval+encode inside sandbox)
//	C. Native Go baseline (json.Unmarshal+Marshal in host process, no sandbox)
//	D. Sub-components: pool borrow/return, raw alloc+memcpy+call latency
//
// All benchmarks use realisticRecord at the distribution's p50 (2594 B) and
// p90 (9753 B) — the same shapes the production pipeline sees. Toy inputs
// produce fake results here (as measured in pool_width_bench_test.go).
//
// IMPORTANT: each iteration constructs its input bytes fresh. See the
// methodology note at the top of wasm_test.go; re-using a large []byte as a
// map key would get a short-circuit from the Go string equality optimiser.
//
// Warm-up discipline: module compilation and pool build happen before
// b.ResetTimer(); the loop measures steady-state eval only.
package wasm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// taggerCfg is a realistic tagger configuration: clean two PII fields and tag
// authenticated requests. It approximates the decode+clean production use-case
// mentioned in the task description. The expressions touch one header field
// each, mirroring typical SAS rules.
var taggerCfg = map[string]any{
	"rules": []any{
		// clean rule: always drop a sensitive field (no `when` = unconditional)
		map[string]any{"kind": "clean", "field": "body"},
		// tag rule: tag authenticated requests
		map[string]any{
			"kind": "tag",
			"tag":  "authed",
			"when": `response.status == 200`,
		},
		// clean rule with condition: drop topic field when partition == 0
		map[string]any{
			"kind": "tag",
			"tag":  "partition0",
			"when": `partition == 0`,
		},
	},
}

// noopCfg is an empty config for the noop guest (it ignores configure anyway).
var noopCfg = map[string]any{"rules": []any{}}

// ─────────────────────────────────────────────────────────────────────────────
// A: Fixed host overhead — reactornoop (zero JSON, pure copy)
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkEvalCost_A_FixedOverhead_P50 measures the host-side cost of one
// eval cycle at p50 record size (2594 B): encodeStdin + alloc + memcpy into
// linear memory + wasm eval call + memcpy out + decodeStdout. The guest does
// zero JSON work — it copies inBuf to outBuf and returns.
func BenchmarkEvalCost_A_FixedOverhead_P50(b *testing.B) {
	benchFixedOverhead(b, 2594)
}

// BenchmarkEvalCost_A_FixedOverhead_P90 is the same at p90 (9753 B).
func BenchmarkEvalCost_A_FixedOverhead_P90(b *testing.B) {
	benchFixedOverhead(b, 9753)
}

func benchFixedOverhead(b *testing.B, size int) {
	b.Helper()
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorNoopWasm)
	if err != nil {
		b.Fatalf("engineFor noop: %v", err)
	}
	cfgBytes, err := json.Marshal(noopCfg)
	if err != nil {
		b.Fatalf("marshal noopCfg: %v", err)
	}
	if err := e.swapConfig(ctx, cfgBytes, 1, 0); err != nil {
		b.Fatalf("swapConfig: %v", err)
	}
	facade := &reactorFacade{}

	// Warm: one eval to ensure the pool channel is primed.
	input0, _ := encodeStdin(realisticRecord(size))
	if _, err := facade.evalFromPool(ctx, e, input0); err != nil {
		b.Fatalf("warm: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		// Fresh record every iteration — avoids any map-key caching short-circuit.
		rec := realisticRecord(size + i%3) // ±2 B variation to break accidental dedup
		input, err := encodeStdin(rec)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		if _, err := facade.evalFromPool(ctx, e, input); err != nil {
			b.Fatalf("eval: %v", err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// B: Realistic guest — tagger (actual JSON decode+rule eval+encode in sandbox)
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkEvalCost_B_TaggerGuest_P50 is the realistic cost at p50.
func BenchmarkEvalCost_B_TaggerGuest_P50(b *testing.B) {
	benchTaggerGuest(b, 2594)
}

// BenchmarkEvalCost_B_TaggerGuest_P90 is the realistic cost at p90.
func BenchmarkEvalCost_B_TaggerGuest_P90(b *testing.B) {
	benchTaggerGuest(b, 9753)
}

func benchTaggerGuest(b *testing.B, size int) {
	b.Helper()
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, taggerWasm)
	if err != nil {
		b.Fatalf("engineFor tagger: %v", err)
	}
	cfgBytes, err := json.Marshal(taggerCfg)
	if err != nil {
		b.Fatalf("marshal taggerCfg: %v", err)
	}
	if err := e.swapConfig(ctx, cfgBytes, 1, 0); err != nil {
		b.Fatalf("swapConfig: %v", err)
	}
	facade := &reactorFacade{}

	// Warm.
	input0, _ := encodeStdin(realisticRecord(size))
	if _, err := facade.evalFromPool(ctx, e, input0); err != nil {
		b.Fatalf("warm: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := realisticRecord(size + i%3)
		input, err := encodeStdin(rec)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		if _, err := facade.evalFromPool(ctx, e, input); err != nil {
			b.Fatalf("eval: %v", err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// C: Native Go baseline — json.Unmarshal+Marshal in host process, no sandbox
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkEvalCost_C_NativeGo_P50 measures the same data movement in pure Go.
func BenchmarkEvalCost_C_NativeGo_P50(b *testing.B) {
	benchNativeGo(b, 2594)
}

// BenchmarkEvalCost_C_NativeGo_P90 is the same at p90.
func BenchmarkEvalCost_C_NativeGo_P90(b *testing.B) {
	benchNativeGo(b, 9753)
}

func benchNativeGo(b *testing.B, size int) {
	b.Helper()
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := realisticRecord(size + i%3)
		raw, err := json.Marshal(rec)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			b.Fatalf("unmarshal: %v", err)
		}
		// Simulate one simple field-clean (like a clean rule) and marshal back.
		delete(v, "body")
		if _, err := json.Marshal(map[string]any{"record": v, "tags": []string{"authed"}}); err != nil {
			b.Fatalf("re-marshal: %v", err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D: Sub-components of the fixed overhead
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkEvalCost_D1_PoolBorrowReturn measures the cost of borrowing an
// instance from the pool and returning it, with NO eval call. This is the pure
// channel operation + atomic loads.
func BenchmarkEvalCost_D1_PoolBorrowReturn(b *testing.B) {
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorNoopWasm)
	if err != nil {
		b.Fatalf("engineFor: %v", err)
	}
	cfgBytes, _ := json.Marshal(noopCfg)
	if err := e.swapConfig(ctx, cfgBytes, 1, 0); err != nil {
		b.Fatalf("swapConfig: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		inst, pool, err := e.borrow(ctx)
		if err != nil {
			b.Fatalf("borrow: %v", err)
		}
		e.giveBack(ctx, pool, inst)
	}
}

// BenchmarkEvalCost_D2_EncodeStdin_P50 measures encodeStdin alone (json.Marshal
// of the globals map after stripping $items/$supplies). Not a wasm call.
func BenchmarkEvalCost_D2_EncodeStdin_P50(b *testing.B) {
	benchEncodeStdin(b, 2594)
}

// BenchmarkEvalCost_D2_EncodeStdin_P90 is the same at p90.
func BenchmarkEvalCost_D2_EncodeStdin_P90(b *testing.B) {
	benchEncodeStdin(b, 9753)
}

func benchEncodeStdin(b *testing.B, size int) {
	b.Helper()
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := realisticRecord(size + i%3)
		if _, err := encodeStdin(rec); err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
	}
}

// BenchmarkEvalCost_D3_DecodeStdout_P50 measures decodeStdout alone (the host
// side json.Unmarshal of the output bytes returned from the guest).
func BenchmarkEvalCost_D3_DecodeStdout_P50(b *testing.B) {
	benchDecodeStdout(b, 2594)
}

// BenchmarkEvalCost_D3_DecodeStdout_P90 is the same at p90.
func BenchmarkEvalCost_D3_DecodeStdout_P90(b *testing.B) {
	benchDecodeStdout(b, 9753)
}

func benchDecodeStdout(b *testing.B, size int) {
	b.Helper()
	// Produce a realistic output shape: the tagger returns {"record":{...},"tags":[...]}
	rec := realisticRecord(size)
	payload, err := json.Marshal(map[string]any{"record": rec, "tags": []string{"authed"}})
	if err != nil {
		b.Fatalf("make payload: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if _, err := decodeStdout(payload); err != nil {
			b.Fatalf("decodeStdout: %v", err)
		}
	}
}

// BenchmarkEvalCost_D4_EvalOnce_P50 measures evalOnce alone for the noop
// guest at p50: this is alloc(n) + mem.Write + eval(n) + readOut. It
// specifically excludes borrow/giveBack and encodeStdin/decodeStdout so we can
// tell what fraction of A is the pure wasm call boundary.
func BenchmarkEvalCost_D4_EvalOnce_P50(b *testing.B) {
	benchEvalOnce(b, 2594)
}

// BenchmarkEvalCost_D4_EvalOnce_P90 is the same at p90.
func BenchmarkEvalCost_D4_EvalOnce_P90(b *testing.B) {
	benchEvalOnce(b, 9753)
}

func benchEvalOnce(b *testing.B, size int) {
	b.Helper()
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorNoopWasm)
	if err != nil {
		b.Fatalf("engineFor: %v", err)
	}
	cfgBytes, _ := json.Marshal(noopCfg)
	if err := e.swapConfig(ctx, cfgBytes, 1, 0); err != nil {
		b.Fatalf("swapConfig: %v", err)
	}

	// Hold the instance permanently for the duration of the benchmark so we
	// measure only evalOnce, not borrow/giveBack.
	inst, _, err := e.borrow(ctx)
	if err != nil {
		b.Fatalf("borrow: %v", err)
	}

	input0, _ := encodeStdin(realisticRecord(size))

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if _, _, err := inst.evalOnce(ctx, input0); err != nil {
			b.Fatalf("evalOnce: %v", err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// E: Batch amortisation projection
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkEvalCost_E_BatchOf20_Tagger_P50 runs ExecuteBatch with 20 records
// at p50 through the tagger guest. The per-item cost is b.Elapsed/20*b.N.
// Comparing to BenchmarkEvalCost_B_TaggerGuest_P50 shows whether batching
// amortises the per-call overhead.
func BenchmarkEvalCost_E_BatchOf20_Tagger_P50(b *testing.B) {
	benchBatch(b, 20, 2594)
}

func BenchmarkEvalCost_E_BatchOf20_Tagger_P90(b *testing.B) {
	benchBatch(b, 20, 9753)
}

func benchBatch(b *testing.B, batchSize, recordSize int) {
	b.Helper()
	ctx := context.Background()

	// Use the process-wide shared engine so this matches the production path.
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		b.Fatal("wazero-reactor not registered")
	}
	code := b64(taggerWasm)
	cfgBytes, _ := json.Marshal(taggerCfg)

	// Warm and ensure pool exists.
	if _, err := e.Execute(ctx, engine.Code(code),
		map[string]any{"$config": cfgBytes, "$input": realisticRecord(recordSize)},
		engine.DefaultHelpers()); err != nil {
		// Tagger needs $config as map, not bytes; use the map form.
		_ = err // tolerate: the warm below handles it
	}
	// Re-warm with the map form.
	if _, err := e.Execute(ctx, engine.Code(code),
		map[string]any{"$config": taggerCfg, "$input": realisticRecord(recordSize)},
		engine.DefaultHelpers()); err != nil {
		b.Fatalf("warm tagger: %v", err)
	}

	// Build a fixed batch of batchSize records (same sizes, different data).
	batch := make([]any, batchSize)
	for i := range batchSize {
		batch[i] = realisticRecord(recordSize + i)
	}
	globals := map[string]any{"$config": taggerCfg}

	reactor, ok := e.(*reactorFacade)
	if !ok {
		b.Fatal("engine is not a *reactorFacade")
	}

	b.ResetTimer()
	b.ReportAllocs()
	// Report per-item cost as a custom metric.
	total := 0
	for range b.N {
		results, err := reactor.ExecuteBatch(ctx, engine.Code(code), batch, globals)
		if err != nil {
			b.Fatalf("ExecuteBatch: %v", err)
		}
		total += len(results)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(batchSize)), "ns/item")
	_ = total
}
