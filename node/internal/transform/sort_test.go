package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestSort_OrdersByScoreDesc(t *testing.T) {
	b := node.Sort("items", node.SortDesc("score"))

	h, ok := registry.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"name": "a", "score": 2},
			map[string]any{"name": "b", "score": 5},
			map[string]any{"name": "c", "score": 3},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if items[0].(map[string]any)["name"] != "b" || items[2].(map[string]any)["name"] != "a" {
		t.Fatalf("sorted items = %#v, want score desc", items)
	}
}
