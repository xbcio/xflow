package wasm

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// BenchmarkReactorEval_AllItemsScope measures the FULL per-item cost a map body
// pays, not just the Go-side encode.
//
// The body's scope carries $items -- the map node's ENTIRE items array
// (execution/subgraph/map_body.go bodyItemScope) -- and every item's eval
// re-encodes it (encodeStdin json.Marshals the whole globals map), copies the
// bytes into the guest's linear memory, and makes the guest parse them again.
// A batch of n items with two guests therefore pays 2n times for the whole
// array: the per-item cost grows with the batch the item belongs to.
//
// BenchmarkEncodeStdin_AllItems isolates the Go-side marshal alone and shows it
// is only ~33 us at n=40 -- far too small to explain the pipeline numbers. This
// benchmark exists because that made the marshal a tempting but wrong answer:
// the question is what the host->guest crossing costs END TO END as $items
// grows, which is where the copy and the guest-side parse land.
//
// Measured against the live SAS pipeline (local Kafka, 4 partitions, 1000
// messages), where end-to-end per-item cost ROSE with batch size instead of
// amortising -- the signature of a per-item cost that scales with n:
//
//	batch=10 -> 80 msg/s (12.5 ms/item)
//	batch=20 -> 62 msg/s (16.1 ms/item)
//	batch=40 -> 38 msg/s (26.3 ms/item)
//
// Dropping $items from bodyItemScope entirely (a throwaway local edit, to
// measure the ceiling) turned that into 220 msg/s at batch=20 and 233 at
// batch=40 -- ~3.5x, with the superlinearity gone: bigger batches got FASTER
// again, which is what amortisation is supposed to look like.
//
// Read this benchmark as: how much of that is the $items crossing? Run the
// items=0 case beside the others -- it is the same eval with $items dropped, so
// the difference is the whole cost of carrying the array.
//
// Note the array crosses TWICE per item, not once: exprx.BuildExprEnv flattens
// Input.Data's keys to the env top level AND publishes Input.Data itself as
// $input, so encodeStdin marshals $items once as a root and once inside $input.
func BenchmarkReactorEval_AllItemsScope(b *testing.B) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		b.Fatal("wazero-reactor engine not registered")
	}
	code := b64(reactorWasm)
	cfg := ruleConfig(
		[2]string{"r1", "x > 5"}, [2]string{"r2", "x < 100"}, [2]string{"r3", "x > 0"},
	)

	// items=0 is the control: identical eval, no $items in scope. Everything the
	// other cases cost above this is attributable to carrying the array.
	for _, n := range []int{0, 10, 20, 40, 80} {
		items := make([]any, 0, n)
		for i := range n {
			items = append(items, map[string]any{
				"topic":     "apisix-traffic",
				"partition": i % 4,
				"offset":    i,
				"key":       fmt.Sprintf("k%d", i),
				// The apisix JSON arrives as a string inside the envelope's
				// "value" -- see node/trigger/kafka/kafka.go messageData.
				"value": fmt.Sprintf(`{"seq":%d,"response":{"status":200,`+
					`"headers":{"content-type":"application/json"}},`+
					`"request":{"method":"GET","uri":"/api/v1/resource/%d"},`+
					`"body":{"code":0,"data":{"id":%d}}}`, i, i, i),
			})
		}

		b.Run(fmt.Sprintf("items=%d", n), func(b *testing.B) {
			globals := func(x int) map[string]any {
				g := map[string]any{"$config": cfg, "x": float64(x % 20)}
				if n > 0 {
					g["$item"] = items[x%n]
					g["$index"] = x % n
					g["$items"] = items
				}
				return g
			}
			// Warm: compile the module and fill the instance pool so neither is
			// charged to the measured loop.
			if _, err := e.Execute(context.Background(), code, globals(0), engine.DefaultHelpers()); err != nil {
				b.Fatalf("warm: %v", err)
			}
			b.ResetTimer()
			for i := range b.N {
				if _, err := e.Execute(context.Background(), code, globals(i), engine.DefaultHelpers()); err != nil {
					b.Fatalf("eval: %v", err)
				}
			}
		})
	}
}

// BenchmarkEncodeStdin_AllItems isolates the Go-side half of the same crossing:
// json.Marshal of the globals map, with no guest involved. Kept beside the
// full-eval benchmark above because the two together separate "Go re-encodes the
// array" from "the guest re-parses it", and only the pair identifies which half
// is worth fixing.
func BenchmarkEncodeStdin_AllItems(b *testing.B) {
	for _, n := range []int{10, 20, 40, 80} {
		items := make([]any, 0, n)
		for i := range n {
			items = append(items, map[string]any{
				"topic":     "apisix-traffic",
				"partition": i % 4,
				"offset":    i,
				"key":       fmt.Sprintf("k%d", i),
				"value": fmt.Sprintf(`{"seq":%d,"response":{"status":200,`+
					`"headers":{"content-type":"application/json"}},`+
					`"request":{"method":"GET","uri":"/api/v1/resource/%d"},`+
					`"body":{"code":0,"data":{"id":%d}}}`, i, i, i),
			})
		}
		b.Run(fmt.Sprintf("items=%d", n), func(b *testing.B) {
			globals := map[string]any{"$item": items[0], "$index": 0, "$items": items}
			b.ResetTimer()
			for range b.N {
				if _, err := encodeStdin(globals); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestGuestPayloadCarriesItemsTwice pins the duplication the benchmarks above
// describe, so the claim is a checked assertion rather than a comment.
//
// exprx.BuildExprEnv flattens Input.Data's keys to the env top level AND
// publishes Input.Data itself as $input. A map body's $items therefore reaches
// encodeStdin under two paths and is serialised into the guest payload twice --
// per item, per guest. Same shape as the second copy that let cleansed
// credentials reach storage through $input/$supplies: a key appearing once at
// the root does not mean the value crosses once.
//
// If a future change makes $items cross only once, this test fails loudly and
// the benchmark's cost model above needs revisiting -- that is the point.
func TestGuestPayloadCarriesItemsTwice(t *testing.T) {
	const marker = "UNIQUE_ITEMS_MARKER_9f3a"
	in := &types.Input{Data: map[string]any{"$items": []any{marker}}}

	payload, err := encodeStdin(exprx.BuildExprEnv(in, nil))
	if err != nil {
		t.Fatalf("encode guest payload: %v", err)
	}
	got := strings.Count(string(payload), marker)
	if got != 2 {
		t.Fatalf("items array serialised %d time(s) into the guest payload, want 2\n"+
			"payload: %s", got, payload)
	}
}
