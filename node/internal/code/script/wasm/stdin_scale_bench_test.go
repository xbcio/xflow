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

// BenchmarkReactorEval_AllItemsScope guards the property that a wasm guest's
// per-item cost does NOT scale with the size of the map node's items array.
//
// It measures the FULL per-item cost, not just the Go-side encode. Run every
// case together and compare them: items=0 is the control (no $items in scope at
// all), and the 10/20/40/80 cases must stay FLAT relative to each other. A curve
// that climbs with n means something started carrying the whole array across the
// sandbox boundary again -- which is the defect stripItems (wasm.go) exists to
// prevent, and which TestGuestPayloadDropsItems pins at the byte level.
//
// What the numbers looked like when the array DID cross, per item, twice
// (exprx.BuildExprEnv publishes Input.Data both flattened and as $input):
//
//	items:      0        10       20       40        80
//	before:  0.058ms   0.94ms   1.76ms   3.53ms    6.87ms   <- 61x control at n=40
//	after:   0.053ms   0.147ms  0.149ms  0.154ms   0.150ms  <- flat
//
// End-to-end against a live Kafka pipeline (4 partitions, 1000 messages), the
// same defect made bigger batches SLOWER per item, which is backwards -- a batch
// is supposed to amortise:
//
//	batch=10 -> 80 msg/s (12.5 ms/item)
//	batch=20 -> 62 msg/s (16.1 ms/item)
//	batch=40 -> 38 msg/s (26.3 ms/item)
//
// After the fix that became ~220 msg/s at batch=20 and 233 at batch=40: bigger
// batches got faster again.
//
// BenchmarkEncodeStdin_AllItems below isolates the Go-side marshal and shows it
// was only ~33us at n=40 -- 1% of the 3531us above, and the reason this
// full-eval benchmark exists: the marshal was a tempting but wrong answer. The
// remaining 99% was the guest rebuilding objects from those bytes inside the
// sandbox, which no transport change can avoid (see stripItems for the
// three-stage measurement).
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

// BenchmarkEncodeStdin_AllItems is the historical record of the Go-side half of
// the crossing: json.Marshal of the globals map, no guest involved.
//
// It now measures encodeStdin WITH stripItems in place, so the array never
// reaches json.Marshal and the numbers are flat. That is the point: kept beside
// the full-eval benchmark above because the pair is what separated "Go
// re-encodes the array" (~33us at n=40, 1%) from "the guest re-parses it"
// (the other 99%), and only that split ruled out fixing this with a cheaper
// encoding instead of by not sending the value.
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

// TestGuestPayloadDropsItems pins that $items reaches a wasm guest ZERO times,
// counting VALUES rather than keys.
//
// The array reached the payload by two independent routes: exprx.BuildExprEnv
// flattens Input.Data's keys to the env top level AND publishes Input.Data itself
// as $input, so a map body's $items was serialised twice per item. Asserting the
// "$items" KEY is absent would pass while the second copy still crossed — the
// same shape as the cleansed credentials that reached storage through their
// $input copy. Hence the marker: it is scanned for in the encoded bytes, so any
// route that still carries the value fails this test regardless of the key it
// arrives under.
//
// $items stays a live DSL root for js and expression bodies; see stripItems for
// why only wasm drops it, and BenchmarkReactorEval_AllItemsScope for the cost.
func TestGuestPayloadDropsItems(t *testing.T) {
	const marker = "UNIQUE_ITEMS_MARKER_9f3a"
	in := &types.Input{Data: map[string]any{"$items": []any{marker}, "keep": "kept"}}

	env := exprx.BuildExprEnv(in, nil)
	payload, err := encodeStdin(env)
	if err != nil {
		t.Fatalf("encode guest payload: %v", err)
	}
	if got := strings.Count(string(payload), marker); got != 0 {
		t.Fatalf("items array serialised %d time(s) into the guest payload, want 0\n"+
			"payload: %s", got, payload)
	}
	// Dropping $items must not drop anything else: the rest of Data still has to
	// reach the guest, both flattened and under $input.
	if got := strings.Count(string(payload), "kept"); got != 2 {
		t.Fatalf("sibling key crossed %d time(s), want 2 (root + $input)\npayload: %s", got, payload)
	}

	// The engine's activation record must survive: deleting in place would leave
	// the node unable to run a second time, and would strip $items from the js
	// and expression bodies that still promise it.
	if _, ok := env["$items"]; !ok {
		t.Fatal("encodeStdin mutated the caller's globals map")
	}
	if _, ok := in.Data["$items"]; !ok {
		t.Fatal("encodeStdin mutated Input.Data")
	}
}
