package transform_test

import (
	"context"
	"math"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestAggregate_AverageOfEmptyItemsIsZeroNotNaN covers the len(items) == 0
// guard at aggregate.go:94. No existing aggregate fixture ever passes an
// empty items array, so nothing exercises this branch: without the guard,
// sumField returns 0 and it gets divided by float64(len(items)) == 0,
// writing 0/0 = NaN into the output map. That NaN then blows up far from
// this line -- typically when the workflow engine JSON-encodes the node's
// output for the next hop, since encoding/json refuses to marshal NaN --
// so the failure surfaces at serialization time with no obvious link back
// to an aggregate over an empty batch.
func TestAggregate_AverageOfEmptyItemsIsZeroNotNaN(t *testing.T) {
	b := node.Aggregate("items").Average("amount", "average_amount")

	h, ok := registry.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	avg, ok := out.Data["average_amount"].(float64)
	if !ok {
		t.Fatalf("average_amount = %#v (%T), want a float64", out.Data["average_amount"], out.Data["average_amount"])
	}
	if math.IsNaN(avg) {
		t.Fatalf("average_amount = NaN, want 0: averaging zero items must short-circuit " +
			"instead of dividing by zero")
	}
	if avg != 0 {
		t.Fatalf("average_amount = %v, want 0 for an empty items array", avg)
	}
}
