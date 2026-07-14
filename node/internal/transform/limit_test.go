package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestLimit_SlicesToMax(t *testing.T) {
	b := node.Limit("items", 2)

	h, ok := registry.Lookup("xflow.transform.limit")
	if !ok {
		t.Fatal("limit handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"name": "b", "score": 5},
			map[string]any{"name": "c", "score": 3},
			map[string]any{"name": "a", "score": 2},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["name"] != "b" || items[1].(map[string]any)["name"] != "c" {
		t.Fatalf("limited items = %#v, want top two", items)
	}
}
