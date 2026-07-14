package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestRemoveDuplicates_KeepsFirstByFields(t *testing.T) {
	b := node.RemoveDuplicates("items", "email")

	h, ok := registry.Lookup("xflow.transform.remove_duplicates")
	if !ok {
		t.Fatal("remove duplicates handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"email": "a@example.com", "name": "first"},
			map[string]any{"email": "b@example.com", "name": "second"},
			map[string]any{"email": "a@example.com", "name": "duplicate"},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["name"] != "first" || items[1].(map[string]any)["name"] != "second" {
		t.Fatalf("items = %#v, want first unique records", items)
	}
}
