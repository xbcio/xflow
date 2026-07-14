package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestSet_AssignsLiteralsAndExpressions(t *testing.T) {
	b := node.Set(map[string]any{"status": "approved"}).
		SetExpr(map[string]string{"total": "price * quantity"})

	if got := b.NodeType(); got != "xflow.transform.set" {
		t.Fatalf("NodeType() = %q, want xflow.transform.set", got)
	}

	h, ok := registry.Lookup("xflow.transform.set")
	if !ok {
		t.Fatal("set handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"price": 12, "quantity": 3, "keep": true},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["status"] != "approved" {
		t.Fatalf("status = %v, want approved", out.Data["status"])
	}
	if out.Data["total"] != 36 {
		t.Fatalf("total = %v, want 36", out.Data["total"])
	}
	if out.Data["keep"] != true {
		t.Fatalf("keep = %v, want true", out.Data["keep"])
	}
}
