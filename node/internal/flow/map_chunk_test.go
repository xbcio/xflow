package flow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestMap_BatchesAreContiguousEqualSlicesOfItems pins the one property the engine
// relies on when it stores a map node's items array once and gives each batch a
// [start, end) window of it: the batches the map node emits, laid end to end, are
// exactly its items array, every batch but the last is batch_size long, and the
// last is between one item and batch_size long. engine.expandLoopSplit also
// checks that the batch lengths add up to the item count and keeps the batches
// inline when they do not, but this is what makes that check pass for every map
// node this package can produce.
func TestMap_BatchesAreContiguousEqualSlicesOfItems(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	cases := []struct{ total, size int }{
		{6, 2},  // divides evenly
		{7, 3},  // short last batch
		{5, 1},  // one item per batch
		{3, 10}, // a single batch shorter than batch_size
		{4, 4},  // exactly one full batch
		{1, 2},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d items in batches of %d", tc.total, tc.size), func(t *testing.T) {
			items := make([]any, tc.total)
			for i := range items {
				items[i] = fmt.Sprintf("item-%d", i)
			}
			out, err := h.Execute(context.Background(), &types.Input{
				Params: mapParams(node.Map("items", tc.size)),
				Data:   map[string]any{"items": items},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			batches, ok := out.Data["batches"].([][]any)
			if !ok {
				t.Fatalf("batches has type %T, want [][]any", out.Data["batches"])
			}

			at := 0
			for i, batch := range batches {
				last := i == len(batches)-1
				switch {
				case len(batch) == 0:
					t.Fatalf("batch %d is empty", i)
				case !last && len(batch) != tc.size:
					t.Errorf("batch %d has %d items, want %d (only the last batch may be short)", i, len(batch), tc.size)
				case last && len(batch) > tc.size:
					t.Errorf("last batch has %d items, want at most %d", len(batch), tc.size)
				}
				for j, item := range batch {
					if at+j >= len(items) || item != items[at+j] {
						t.Fatalf("batch %d item %d = %v, want items[%d] = %v", i, j, item, at+j, items[min(at+j, len(items)-1)])
					}
				}
				at += len(batch)
			}
			if at != len(items) {
				t.Errorf("batches cover %d items, want %d", at, len(items))
			}
			if got := out.Data["batch_count"]; got != len(batches) {
				t.Errorf("batch_count = %v, want %d", got, len(batches))
			}
		})
	}
}
