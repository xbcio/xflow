package subgraph

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// TestExecuteBatchBody_ItemResultExcludesExecutionRoots pins that a body's
// per-item result carries only the body's product, not the execution-scope
// expression roots.
//
// It exists because it did carry them, and the consequence was quadratic. The
// engine's applyExecutionScope merges $item/$index/$items into EVERY node's
// Input.Data so any body member can read them; the collector node is a member,
// and it echoed its Data straight out as the exit result. $items is the map
// node's WHOLE items array, so each of n items' results held a copy of all n
// items.
//
// Measured on this exact shape before the fix: one batch's collected results
// totalled 1.2 MB at 20 items, 27.7 MB at 100, and 681 MB at 500. The 500 case
// is one Redis string, and Redis is single-threaded — writing it stalls every
// other command on the instance, which is how a live pipeline went from 4.2
// ms/item at batch=20 to 30 ms/item at batch=500 and stopped draining.
func TestExecuteBatchBody_ItemResultExcludesExecutionRoots(t *testing.T) {
	ex, _ := newOverlapExecutor(t, 0)
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	items := payloadItemsN(8)
	results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:     pkg,
		BodyHash: hash,
		Items:    items,
		// AllItems is what becomes $items. Passing it is what production does and
		// what makes the leak observable: with it nil there is no array to copy
		// and the test would pass against the unfixed code.
		AllItems:  items,
		BatchSize: 8,
	})
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}
	if len(results) != len(items) {
		t.Fatalf("results = %d, want %d", len(results), len(items))
	}

	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("item %d failed: %v", i, r.Err)
		}
		for k := range r.Data {
			if strings.HasPrefix(k, "$") {
				t.Errorf("item %d result carries execution root %q. The roots are "+
					"the engine's per-node expression scope, not the body's product, "+
					"and $items holds every item -- one copy per item is quadratic in "+
					"the batch size", i, k)
			}
		}
		// The body's own output must survive the strip. A filter that removed
		// everything would satisfy the assertion above while destroying the map
		// node's result.
		if _, ok := r.Data["item"]; !ok {
			t.Errorf("item %d result = %#v, want the body member's \"item\" key", i, r.Data)
		}
	}
}

// TestExecuteBatchBody_ItemResultSizeIsFlatInBatchSize is the quantitative half:
// the assertion above would still pass if some FUTURE root arrived under a name
// without the "$" prefix, and the cost -- not the key -- is what took the
// pipeline down.
//
// The assertion is on one item's serialized size across two batch sizes, which
// is a property of the code: an item's result cannot depend on how many siblings
// it has. An absolute byte budget would be a property of the fixture instead.
func TestExecuteBatchBody_ItemResultSizeIsFlatInBatchSize(t *testing.T) {
	ex, _ := newOverlapExecutor(t, 0)
	pkg, hash := buildOverlapBodyPackage(t)
	x := NewMapBodyExecutor(ex, false, time.Time{})

	firstItemBytes := func(n int) int {
		t.Helper()
		items := payloadItemsN(n)
		results, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
			Body: pkg, BodyHash: hash, Items: items, AllItems: items, BatchSize: n,
		})
		if err != nil {
			t.Fatalf("execute batch body (n=%d): %v", n, err)
		}
		encoded, err := json.Marshal(results[0].Data)
		if err != nil {
			t.Fatalf("marshal item result (n=%d): %v", n, err)
		}
		return len(encoded)
	}

	small, large := firstItemBytes(10), firstItemBytes(200)
	t.Logf("first item result: n=10 %d B, n=200 %d B", small, large)
	if large > small*2 {
		t.Fatalf("one item's result is %d B in a 200-item batch against %d B in a "+
			"10-item batch. Per-item output scales with the batch's LENGTH, so the "+
			"batch's total output is quadratic -- at 500 items of real apisix log "+
			"size that was 681 MB in a single Redis value", large, small)
	}
}
