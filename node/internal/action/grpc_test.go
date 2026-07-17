package action_test

import (
	"context"
	"errors"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/types"
	"strings"
	"testing"
	"time"

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
	h, _ := registry.Lookup("xflow.grpc")
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
	if !types.IsPermanent(err) {
		t.Fatalf("missing host must be permanent; got %v", err)
	}
}

func TestGRPC_MissingService(t *testing.T) {
	h, _ := registry.Lookup("xflow.grpc")
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
	if !types.IsPermanent(err) {
		t.Fatalf("missing service must be permanent; got %v", err)
	}
}

func TestGRPC_MissingMethod(t *testing.T) {
	h, _ := registry.Lookup("xflow.grpc")
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
	if !types.IsPermanent(err) {
		t.Fatalf("missing method must be permanent; got %v", err)
	}
}

// TestGRPC_NoPoolIsPermanent pins the contract: when no ResourcePool is
// attached to the context, GRPCNode.Execute must return a permanent classified
// error (not route to the error port). No resource pool means a deployment
// configuration error that retry will never fix.
func TestGRPC_NoPoolIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.grpc")
	b := node.GRPC("test.Service", "Call", "localhost:1").
		SetRequest(map[string]any{"key": "value"})
	input := &types.Input{Params: b.RawParams().(map[string]any)}

	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error when no resource pool is configured, got nil")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("no-pool error must be permanent (not retried); got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "grpc.no_pool" {
		t.Fatalf("expected ClassifiedError code=grpc.no_pool, got %T %v", err, err)
	}
}

// TestGRPC_ConnectionErrorIsTransient pins the contract that a real dial
// failure returns a transient classified error so the engine retries it.
// A real DefaultResourcePool is injected so the call reaches acquireGRPC →
// pool.GRPC → conn.Invoke, which then fails on the dead host localhost:1.
func TestGRPC_ConnectionErrorIsTransient(t *testing.T) {
	h, _ := registry.Lookup("xflow.grpc")
	pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Close(ctx)
	}()
	ctx := types.WithResourcePool(context.Background(), pool)

	b := node.GRPC("test.Service", "Call", "localhost:1").
		SetRequest(map[string]any{"key": "value"}).
		Timeout("100ms")
	input := &types.Input{Params: b.RawParams().(map[string]any)}

	_, err := h.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected transient error for connection failure, got nil")
	}
	if types.IsPermanent(err) {
		t.Fatalf("connection failure must be transient (retryable); got permanent err=%v", err)
	}
	errMsg := err.Error()
	if strings.Contains(errMsg, "no resource pool configured") {
		t.Fatalf("error = %q: hit the no-pool path instead of real connection failure", errMsg)
	}
}
