package group_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestNotification_Factory(t *testing.T) {
	b := node.Notification("email", "ops@example.com").
		Subject("Order blocked").
		Message("inventory unavailable").
		SetData(map[string]any{"order_id": "ord-1"})

	if b.NodeType() != "xflow.notification" {
		t.Fatalf("expected xflow.notification, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["channel"] != "email" {
		t.Fatalf("expected channel=email, got %v", params["channel"])
	}
	if params["to"] != "ops@example.com" {
		t.Fatalf("expected to=ops@example.com, got %v", params["to"])
	}
	if params["subject"] != "Order blocked" {
		t.Fatalf("expected subject, got %v", params["subject"])
	}
	data := params["data"].(map[string]any)
	if data["order_id"] != "ord-1" {
		t.Fatalf("expected order_id=ord-1, got %v", data["order_id"])
	}
}

// xflow.notification has no delivery backend. Returning success would claim a
// notification that was never sent, so every invocation must fail — even one
// with otherwise valid parameters.
func TestNotification_ExecuteAlwaysFails(t *testing.T) {
	h, found := registry.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	for _, tc := range []struct {
		name  string
		input *types.Input
	}{
		{name: "nil input", input: nil},
		{name: "missing params", input: &types.Input{}},
		{
			name: "valid params",
			input: &types.Input{
				Params: map[string]any{
					"channel": "email",
					"to":      "ops@example.com",
					"subject": "Order blocked",
					"message": "inventory unavailable",
					"data":    map[string]any{"order_id": "ord-1"},
				},
				Data: map[string]any{"status": "out_of_stock"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := h.Execute(context.Background(), tc.input)
			if err == nil {
				t.Fatalf("out = %#v, want error: a successful return claims a notification that was never sent", out)
			}
			if out != nil {
				t.Fatalf("out = %#v, want nil on failure", out)
			}
			for _, want := range []string{"not implemented", "xflow.http", "webhook"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want it to mention %q", err, want)
				}
			}
		})
	}
}
