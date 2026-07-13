package group_test

import (
	"context"
	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
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

func TestNotification_ExecuteReturnsDeterministicPayload(t *testing.T) {
	h, found := noderuntime.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	b := node.Notification("email", "ops@example.com").
		Subject("Order blocked").
		Message("inventory unavailable").
		SetData(map[string]any{"order_id": "ord-1"})
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"status": "out_of_stock"},
	}

	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port main, got %q", out.Port)
	}
	if out.Data["channel"] != "email" {
		t.Fatalf("expected channel=email, got %v", out.Data["channel"])
	}
	if out.Data["to"] != "ops@example.com" {
		t.Fatalf("expected to=ops@example.com, got %v", out.Data["to"])
	}
	if out.Data["subject"] != "Order blocked" {
		t.Fatalf("expected subject, got %v", out.Data["subject"])
	}
	if out.Data["message"] != "inventory unavailable" {
		t.Fatalf("expected message, got %v", out.Data["message"])
	}
	if out.Data["status"] != "out_of_stock" {
		t.Fatalf("expected upstream status to be preserved, got %v", out.Data["status"])
	}
	if out.Data["sent"] != true {
		t.Fatalf("expected sent=true, got %v", out.Data["sent"])
	}
}

func TestNotification_RequiresChannelAndRecipient(t *testing.T) {
	h, found := noderuntime.Lookup("xflow.notification")
	if !found {
		t.Fatal("expected xflow.notification to be registered")
	}

	_, err := h.Execute(context.Background(), &types.Input{Params: map[string]any{"to": "ops@example.com"}})
	if err == nil {
		t.Fatal("expected error for missing channel")
	}

	_, err = h.Execute(context.Background(), &types.Input{Params: map[string]any{"channel": "email"}})
	if err == nil {
		t.Fatal("expected error for missing recipient")
	}
}
