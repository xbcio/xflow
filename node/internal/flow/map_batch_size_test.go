package flow_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestMap_OutputCarriesTheEffectiveBatchSize pins the "batch_size" key in a
// batch-form map node's own output literal.
//
// engine/expand.go's expansionBatchTask reads this exact key off the map
// node's output (batchPayloadInt(data, "batch_size")) and hands it to every
// batch task so the body can turn a within-batch item position into a global
// index into $items. TestMap_EmitsTheBatchesThemselves already pins
// batches/items/total/batch_count off this same literal but never reads
// batch_size itself, so swapping it for another in-scope int (e.g. the total
// item count) would ship a wrong value to every batch task's index
// arithmetic with nothing in this package noticing.
func TestMap_OutputCarriesTheEffectiveBatchSize(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 2)
	out, err := h.Execute(context.Background(), &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": []any{"a", "b", "c", "d", "e"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["batch_size"] != 2 {
		t.Fatalf("batch_size = %v, want 2", out.Data["batch_size"])
	}
}
