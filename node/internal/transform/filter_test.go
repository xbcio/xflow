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
