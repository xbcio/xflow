package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// BenchmarkStdinSlope measures eval cost as a function of stdin size alone, so
// a measured eval time can be turned back into the payload size that would
// produce it.
//
// It exists to settle a specific question from the scenario-A run: how much of
// a live eval's cost is the payload. Once xflow_wasm_eval_duration_seconds and
// xflow_wasm_eval_stdin_bytes were both wired, that run measured mean eval
// 44.4ms at mean stdin 27081 bytes — so the size term is checkable directly
// rather than inferred. (An earlier 83ms figure quoted here was DERIVED from
// throughput and instance count, not timed, and the direct measurement
// supersedes it; the derivation is kept out of the code so a stale number
// cannot be cited as data.)
//
// If eval cost is driven by stdin bytes — and stripItems' three-stage ABI
// breakdown says 99% of it is the guest rebuilding objects inside the sandbox,
// where the same bytes decode ~32x slower than natively — then a measured eval
// time names a payload size, and that size is checkable against what the
// pipeline actually sends.
//
// The sizes span the live topic's measured distribution (p50 2594, mean 6845,
// p90 9753, p99 119111) plus one point past p99, because the tail is what a
// mean hides: a p99 record is 17x the mean, so a batch that happens to hold one
// pays for it at 32x sandbox parse cost while every host-side metric still
// reports a healthy mean.
//
// Read ns/op against the size label. A linear fit through those points is the
// conversion this benchmark exists to provide; a superlinear one is a finding
// in its own right, since it would mean large records are disproportionately
// expensive rather than merely proportionally so.
func BenchmarkStdinSlope(b *testing.B) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		b.Fatal("wazero-reactor engine not registered")
	}
	code := b64(reactorWasm)
	cfg := ruleConfig([2]string{"r1", "x > 5"}, [2]string{"r2", "x < 100"})

	// Supplies are registered but stripped from the payload by encodeStdin, the
	// same as production: the guest holds its rules from configure. Registering
	// them anyway keeps the env shape identical to the live path, so a future
	// change that stops stripping them shows up here as a size jump.
	reg := supply.NewRegistry()
	content, err := json.Marshal(suppliesOfSize(60))
	if err != nil {
		b.Fatal(err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "sas-clean-rules", Content: content, Revision: 42,
	}); err != nil {
		b.Fatal(err)
	}
	supplies := reg.Decoded()

	for _, size := range []int{2594, 6845, 9753, 40000, 119111, 250000} {
		rec := realisticRecord(size)
		in := &types.Input{Data: map[string]any{"$item": rec, "x": float64(10)}}
		env := exprx.BuildExprEnv(in, nil)
		env["$config"] = cfg
		env["$supplies"] = supplies

		// The payload the guest actually receives, after the same exclusions the
		// production path applies. Reported so ns/op can be read against real
		// bytes rather than against the nominal record size, which differs: the
		// record is flattened to the top level AND published as $input, so the
		// record appears twice.
		encoded, err := encodeStdin(env)
		if err != nil {
			b.Fatal(err)
		}
		stdinBytes := len(encoded)

		b.Run(fmt.Sprintf("record=%d/stdin=%d", size, stdinBytes), func(b *testing.B) {
			if _, err := e.Execute(context.Background(), engine.Code(code), env, engine.DefaultHelpers()); err != nil {
				b.Fatalf("warm: %v", err)
			}
			b.SetBytes(int64(stdinBytes))
			b.ResetTimer()
			for range b.N {
				if _, err := e.Execute(context.Background(), engine.Code(code), env, engine.DefaultHelpers()); err != nil {
					b.Fatalf("eval: %v", err)
				}
			}
		})
	}
}
