package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestFilter_KeepsMatchingItems(t *testing.T) {
	b := node.Filter("items", "item.price >= 100")

	h, ok := registry.Lookup("xflow.transform.filter")
	if !ok {
		t.Fatal("filter handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"sku": "a", "price": 90},
			map[string]any{"sku": "b", "price": 120},
			map[string]any{"sku": "c", "price": 200},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["sku"] != "b" || items[1].(map[string]any)["sku"] != "c" {
		t.Fatalf("items = %#v, want b and c", items)
	}
	if out.Data["total"] != 2 {
		t.Fatalf("total = %v, want 2", out.Data["total"])
	}
}

// TestFilter_ItemFieldNamedIndexDoesNotShadowLoopIndex covers the assignment
// order in evalItemCondition (filter.go:81-87). The function expands the
// item's own map fields into env and only afterwards sets env["index"] to the
// loop position, specifically so that an item field literally named "index"
// cannot overwrite the loop-control variable a condition relies on. No
// existing filter fixture ever gives an item a field named "index" or
// "item", so nothing distinguishes that order from writing the loop keys
// first and letting the item's fields clobber them afterwards -- both orders
// produce identical results everywhere else in this package.
//
// Both items below share the same "index" field value on purpose: if the
// item's field were allowed to shadow the loop variable, "index == 1" would
// evaluate against that shared field value for every item (never true) and
// filter would return nothing. With the loop index kept authoritative, only
// the item actually at position 1 matches.
func TestFilter_ItemFieldNamedIndexDoesNotShadowLoopIndex(t *testing.T) {
	b := node.Filter("items", "index == 1")

	h, ok := registry.Lookup("xflow.transform.filter")
	if !ok {
		t.Fatal("filter handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"index": 999, "name": "a"},
			map[string]any{"index": 999, "name": "b"},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["name"] != "b" {
		t.Fatalf("items = %#v, want only the item at loop position 1 (name %q): a "+
			"condition referencing \"index\" must see the loop position, not an "+
			"item field that happens to share the name", items, "b")
	}
}
