package subgraph

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// payloadItemsN builds n items the size of one real apisix access log message.
//
// itemsN's plain integers are the wrong shape for a scaling measurement: with
// $items carrying the whole array into every item's scope, the per-item cost of
// that array is proportional to the SIZE of an item, and 500 ints is 4 KB where
// 500 log lines is 1 MB. The live pipeline's items are the latter.
func payloadItemsN(n int) []any {
	// ~2 KB, the order of magnitude of an apisix access log line with request
	// and response bodies. The content is filler; only its length matters here.
	payload := strings.Repeat("x", 2048)
	items := make([]any, 0, n)
	for i := range n {
		items = append(items, map[string]any{
			"index": i,
			"body":  payload,
		})
	}
	return items
}

// TestExecuteBatchBody_PerItemCostIsFlatInBatchSize pins per-item cost as
// independent of how many items the batch carries.
//
// It exists because a live pipeline measured the opposite: with a serial body
// and an idle sink, per-item wall clock was 4.6ms at batch=20, 5.2ms at
// batch=100, and 30ms at batch=500. Superlinear per-item cost means something in
// the batch path scales with the batch's LENGTH rather than with the item being
// run, and at batch=500 it was the difference between a pipeline that drains and
// one that does not.
//
// The body member here does no work at all (hold=0), so everything measured is
// the per-item machinery: package resolve, collector, backend construction,
// engine.New, Bind/stop, submit, WaitDone. A body that slept would put the same
// constant into every batch size and hide a scaling term underneath it.
//
// The assertion is a RATIO between two batch sizes, not an absolute budget: an
// absolute per-item number is a property of the machine, but "500 items cost 25x
// per item what 20 items cost" is a property of the code (see the
// coldstart-budget-test-is-io-fragile lesson). The 4x tolerance is wide on
// purpose -- it must not fail on a loaded machine, only on a genuine scaling
// term, and the live numbers exceeded it by 6x.
func TestExecuteBatchBody_PerItemCostIsFlatInBatchSize(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; runs the same body 20 + 500 times")
	}

	ex, _ := newOverlapExecutor(t, 0)
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	perItem := func(n int) time.Duration {
		t.Helper()
		items := payloadItemsN(n)
		start := time.Now()
		results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
			Body:      pkg,
			BodyHash:  hash,
			Items:     items,
			BatchSize: n,
			// AllItems is what becomes $items in every item's scope. The live
			// pipeline always sets it (the engine passes the map node's whole
			// items array), so leaving it nil here would test a shape production
			// never runs -- and $items is the one input whose SIZE grows with the
			// batch, which is exactly the scaling term under investigation.
			AllItems: items,
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("execute batch body (n=%d): %v", n, err)
		}
		if len(results) != n {
			t.Fatalf("results = %d, want %d", len(results), n)
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("item %d failed: %v", i, r.Err)
			}
		}
		return elapsed / time.Duration(n)
	}

	// The small batch runs first and its cost is discarded: it warms the package
	// cache and the wasm-free handler path, so the comparison below is not the
	// first-call cost of one size against the steady-state cost of the other.
	perItem(20)

	small := perItem(20)
	large := perItem(500)
	t.Logf("per-item: batch=20 %v, batch=500 %v (ratio %.1fx)",
		small.Round(time.Microsecond), large.Round(time.Microsecond),
		float64(large)/float64(small))

	if small <= 0 {
		t.Fatalf("batch=20 per-item cost measured as %v; the clock resolution is too "+
			"coarse for this comparison to mean anything", small)
	}
	if ratio := float64(large) / float64(small); ratio > 4 {
		t.Fatalf("per-item cost at batch=500 is %.1fx the cost at batch=20 "+
			"(%v vs %v). Per-item work scales with the batch's LENGTH, so a bigger "+
			"batch is superlinearly slower -- the live pipeline measured 30ms/item "+
			"at batch=500 against 4.6ms at batch=20 and stopped draining",
			ratio, large.Round(time.Microsecond), small.Round(time.Microsecond))
	}
}

// TestExecuteBatchBody_AllItemsRunAtEveryBatchSize is the completeness half of
// the scaling check: a batch path that dropped items as the batch grew would
// make the per-item ratio above look fine while doing less work.
func TestExecuteBatchBody_AllItemsRunAtEveryBatchSize(t *testing.T) {
	for _, n := range []int{20, 100, 500} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ex, h := newOverlapExecutor(t, 0)
			pkg, hash := buildOverlapBodyPackage(t)
			x := NewMapBodyExecutor(ex, false, time.Time{})

			results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
				Body:      pkg,
				BodyHash:  hash,
				Items:     payloadItemsN(n),
				BatchSize: n,
				AllItems:  payloadItemsN(n),
			})
			if err != nil {
				t.Fatalf("execute batch body: %v", err)
			}
			if len(results) != n {
				t.Fatalf("results = %d, want %d", len(results), n)
			}
			if _, invoked := h.stats(); invoked != n {
				t.Fatalf("body invoked %d time(s), want %d", invoked, n)
			}
		})
	}
}
