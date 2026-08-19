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

// realisticRecord builds one apisix traffic record of approximately `size`
// bytes, matching the distribution measured against the live test-env topic
// (mean 6845, p50 2594, p90 9753, p99 119111 bytes).
func realisticRecord(size int) map[string]any {
	body := make([]byte, 0, size)
	for len(body) < size {
		body = append(body, "abcdefghijklmnopqrstuvwxyz0123456789"...)
	}
	return map[string]any{
		"topic":     "apisix-traffic",
		"partition": 0,
		"offset":    1,
		"value": fmt.Sprintf(`{"response":{"status":200,"headers":`+
			`{"content-type":"application/json"}},"request":{"method":"GET",`+
			`"uri":"/api/v1/resource"},"body":%q}`, body[:size]),
	}
}

// suppliesOfSize builds a rule-supply payload of roughly `rules` entries, the
// shape SAS publishes (pre_analysis + post_decode rule lists).
func suppliesOfSize(rules int) map[string]any {
	mk := func() []any {
		out := make([]any, 0, rules)
		for i := range rules {
			out = append(out, map[string]any{
				"id":     i,
				"field":  fmt.Sprintf("request.headers.x-custom-%d", i),
				"when":   fmt.Sprintf(`request.uri startsWith "/api/v%d/"`, i%8),
				"action": "clean",
			})
		}
		return out
	}
	return map[string]any{
		"revision":     int64(42),
		"pre_analysis": mk(),
		"post_decode":  mk(),
	}
}

// BenchmarkStdinRedundancy separates the three components of a wasm guest's
// per-eval stdin cost so a cut can be chosen on measurement rather than on
// inference about which one dominates.
//
// The three cases are cumulative subtractions from what the guest receives
// today:
//
//	full            what encodeStdin produces now
//	no-supplies     minus $supplies (the guest already got its rules via configure)
//	no-supplies-input  also minus the $input duplicate of the record
//
// Sizes bracket the measured live distribution. Read the bytes/op column: it is
// the payload the guest must re-parse inside the sandbox, and the sandbox parse
// is ~32x the native one (see stripItems), so the byte count is the term that
// actually predicts the eval cost.
func BenchmarkStdinRedundancy(b *testing.B) {
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

	for _, size := range []int{2594, 6845, 9753} {
		rec := realisticRecord(size)
		in := &types.Input{Data: map[string]any{"$item": rec, "$index": 0}}
		base := exprx.BuildExprEnv(in, nil)
		base["$supplies"] = supplies

		variants := []struct {
			name  string
			build func() map[string]any
		}{
			{"full", func() map[string]any { return base }},
			{"no-supplies", func() map[string]any {
				out := make(map[string]any, len(base))
				for k, v := range base {
					if k == "$supplies" {
						continue
					}
					out[k] = v
				}
				return out
			}},
			{"no-supplies-input", func() map[string]any {
				out := make(map[string]any, len(base))
				for k, v := range base {
					if k == "$supplies" || k == "$input" {
						continue
					}
					out[k] = v
				}
				return out
			}},
		}
		for _, v := range variants {
			g := v.build()
			b.Run(fmt.Sprintf("size=%d/%s", size, v.name), func(b *testing.B) {
				payload, err := encodeStdin(g)
				if err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for range b.N {
					if _, err := encodeStdin(g); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(payload)), "stdin_bytes")
			})
		}
	}
}

// BenchmarkStdinRedundancyEndToEnd measures the same three variants through a
// real guest, because the Go-side marshal is historically ~1% of the cost and
// the sandbox re-parse is the rest (stripItems documents the three-stage
// split). Only this benchmark can say what a cut is worth.
func BenchmarkStdinRedundancyEndToEnd(b *testing.B) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		b.Fatal("wazero-reactor engine not registered")
	}
	code := b64(reactorWasm)
	cfg := ruleConfig([2]string{"r1", "x > 5"}, [2]string{"r2", "x < 100"})

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

	for _, size := range []int{2594, 6845, 9753} {
		rec := realisticRecord(size)
		in := &types.Input{Data: map[string]any{"$item": rec, "x": float64(10)}}

		build := func(dropSupplies, dropInput bool) map[string]any {
			env := exprx.BuildExprEnv(in, nil)
			env["$config"] = cfg
			if dropSupplies {
				delete(env, "$supplies")
			} else {
				env["$supplies"] = supplies
			}
			if dropInput {
				delete(env, "$input")
			}
			return env
		}
		cases := []struct {
			name           string
			noSup, noInput bool
		}{
			{"full", false, false},
			{"no-supplies", true, false},
			{"no-supplies-input", true, true},
		}
		for _, c := range cases {
			g := build(c.noSup, c.noInput)
			b.Run(fmt.Sprintf("size=%d/%s", size, c.name), func(b *testing.B) {
				if _, err := e.Execute(context.Background(), engine.Code(code), g, engine.DefaultHelpers()); err != nil {
					b.Fatalf("warm: %v", err)
				}
				b.ResetTimer()
				for range b.N {
					if _, err := e.Execute(context.Background(), engine.Code(code), g, engine.DefaultHelpers()); err != nil {
						b.Fatalf("eval: %v", err)
					}
				}
			})
		}
	}
}
