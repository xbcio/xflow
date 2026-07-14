package action_test

import (
	"context"
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
}

// TestGRPC_NoPoolErrors pins the Task 2 contract: when no ResourcePool is
// attached to the context, GRPCNode.Execute must route to the error port with
// "no resource pool configured" in the error data — NOT fall back to a
// per-call dial. The input passes host/service/method validation so the only
// failure point is the pool lookup at acquireGRPC.
func TestGRPC_NoPoolErrors(t *testing.T) {
	h, _ := registry.Lookup("xflow.grpc")
	// No pool is attached, so acquireGRPC fails before any timeout is used;
	// the Timeout call is intentionally omitted as it is irrelevant here.
	b := node.GRPC("test.Service", "Call", "localhost:1").
		SetRequest(map[string]any{"key": "value"})
	input := &types.Input{Params: b.RawParams().(map[string]any)}

	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute returned err = %v, want nil err + error-port output", err)
	}
	if out == nil || out.Port != "error" {
		t.Fatalf("output = %+v, want error port", out)
	}
	errMsg, _ := out.Data["error"].(string)
	if !strings.Contains(errMsg, "no resource pool configured") {
		t.Fatalf("error data = %q, want substring %q", errMsg, "no resource pool configured")
	}
}

// TestGRPC_ConnectionError pins the contract that a real dial failure (not the
// no-pool path) routes to the error port. Pre-Task-2 this test dialed
// localhost:1 directly via a now-removed fallback; after Task 2 the fallback
// was gone and the test silently became a tautology that passed for the wrong
// reason (it hit the no-pool path and only asserted Port == "error").
//
// This version injects a real default ResourcePool so the call reaches
// acquireGRPC -> grpc.NewClient -> conn.Invoke, then fails on the dead host
// localhost:1. The assertion checks the error port AND that the error message
// describes a connection/dial failure, explicitly NOT the no-pool message.
func TestGRPC_ConnectionError(t *testing.T) {
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

	out, err := h.Execute(ctx, input)
	if err != nil {
		// Execute returns nil err and routes failures to the error port; a
		// non-nil err here indicates a regression in the handler's structure.
		t.Fatalf("Execute returned err = %v, want nil err + error-port output", err)
	}
	if out == nil || out.Port != "error" {
		t.Fatalf("output = %+v, want error port for connection failure", out)
	}
	errMsg, _ := out.Data["error"].(string)
	if errMsg == "" {
		t.Fatalf("output error data is empty, want a connection failure message")
	}
	if strings.Contains(errMsg, "no resource pool configured") {
		t.Fatalf("error = %q, want a real connection/dial failure (NOT the no-pool path); "+
			"this means the injected pool did not reach acquireGRPC", errMsg)
	}
	// Positive assertion: the error must describe a real dial/connection
	// failure (e.g. "dial", "connection", "code = Unavailable", or
	// "context deadline"), so that a non-connection failure routed to the
	// error port (e.g. a future marshal error) would NOT pass this test.
	// Observed message shape from grpc: `rpc error: code = Unavailable
	// desc = connection error: desc = "transport: Error while dialing:
	// dial tcp [::1]:1: connect: connection refused"`.
	if !strings.Contains(errMsg, "dial") &&
		!strings.Contains(errMsg, "connection") &&
		!strings.Contains(errMsg, "context deadline") &&
		!strings.Contains(errMsg, "code = Unavailable") {
		t.Fatalf("expected a dial/connection failure error, got: %q", errMsg)
	}
}
