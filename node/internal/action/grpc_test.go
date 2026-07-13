package action_test

import (
	"context"
	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestGRPC_Factory(t *testing.T) {
	b := node.GRPC("inventory.Service", "GetStock", "localhost:50051").
		SetRequest(map[string]any{"sku": "ABC"}).
		SetMetadata(map[string]any{"x-trace": "123"}).
		TLS(true).
		Timeout("5s")

	if b.NodeType() != "xflow.grpc" {
		t.Fatalf("expected xflow.grpc, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["service"] != "inventory.Service" {
		t.Fatalf("expected service, got %v", params["service"])
	}
	opts := params["options"].(map[string]any)
	if opts["tls"] != true {
		t.Fatalf("expected tls=true, got %v", opts["tls"])
	}
}

func TestGRPC_MissingHost(t *testing.T) {
	h, _ := noderuntime.Lookup("xflow.grpc")
	input := &types.Input{
		Params: map[string]any{
			"service": "test.Service",
			"method":  "Call",
		},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestGRPC_MissingService(t *testing.T) {
	h, _ := noderuntime.Lookup("xflow.grpc")
	input := &types.Input{
		Params: map[string]any{
			"host":   "localhost:50051",
			"method": "Call",
		},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing service")
	}
}

func TestGRPC_MissingMethod(t *testing.T) {
	h, _ := noderuntime.Lookup("xflow.grpc")
	input := &types.Input{
		Params: map[string]any{
			"host":    "localhost:50051",
			"service": "test.Service",
		},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing method")
	}
}

func TestGRPC_ConnectionError(t *testing.T) {
	h, _ := noderuntime.Lookup("xflow.grpc")
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	b := node.GRPC("test.Service", "Call", "localhost:1").
		SetRequest(map[string]any{"key": "value"}).
		Timeout("100ms")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	out, err := h.Execute(ctx, input)
	if err != nil {
		return
	}
	if out.Port != "error" {
		t.Fatalf("expected error port for connection failure, got %q", out.Port)
	}
}
