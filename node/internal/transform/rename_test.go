package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestRename_RenamesMappedFields(t *testing.T) {
	b := node.Rename(map[string]string{"first_name": "firstName", "last_name": "lastName"})

	h, ok := registry.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"first_name": "Ada", "last_name": "Lovelace", "age": 36},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["firstName"] != "Ada" || out.Data["lastName"] != "Lovelace" || out.Data["age"] != 36 {
		t.Fatalf("data = %#v, want renamed names and preserved age", out.Data)
	}
	if _, ok := out.Data["first_name"]; ok {
		t.Fatalf("first_name should have been removed: %#v", out.Data)
	}
}
