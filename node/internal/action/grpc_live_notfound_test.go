package action_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// startGRPCStatusServer starts a real, listening gRPC server whose only
// behavior is to answer every unary call with the given status. It uses
// grpc.UnknownServiceHandler because GRPCNode.Execute calls conn.Invoke with a
// fully-qualified method name that has no compiled proto descriptor behind
// it (xflow.grpc is meant to call arbitrary third-party services), so there
// is nothing to register a normal typed service against.
func startGRPCStatusServer(t *testing.T, code codes.Code, msg string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		return grpcstatus.Error(code, msg)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// Before this file, no test in the package connected GRPCNode to a live gRPC
// server at all: TestGRPC_ConnectionErrorIsTransient (grpc_test.go) dials a
// dead host and never reaches a real status response, so the classification
// switch at grpc.go:173-181 -- which decides permanent vs. transient by
// status code -- was entirely unexercised. That switch's code list
// (grpc.go:174-176) could have codes.NotFound deleted from it and the whole
// suite would stay green while a "no such resource" RPC response got
// classified as transient and retried forever instead of failing the
// workflow once.
func TestGRPC_NotFoundStatusIsPermanent(t *testing.T) {
	addr := startGRPCStatusServer(t, codes.NotFound, "no such widget")

	pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Close(ctx)
	})
	ctx := types.WithResourcePool(context.Background(), pool)

	h, ok := registry.Lookup("xflow.grpc")
	if !ok {
		t.Fatal("xflow.grpc is not registered")
	}
	b := node.GRPC("probe.Service", "GetWidget", addr).
		SetRequest(map[string]any{"id": "123"}).
		Timeout("5s")
	input := &types.Input{Params: b.RawParams().(map[string]any)}

	_, err := h.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected an error for a NotFound RPC status, got nil")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("a NotFound RPC status must be permanent (retrying will never find the "+
			"resource); got a transient/unclassified err=%v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "grpc.NotFound" {
		t.Fatalf("expected ClassifiedError code=grpc.NotFound, got %T %v", err, err)
	}
}

// TestGRPC_UnavailableStatusIsTransient is the contrast case: a status code
// that is NOT in the permanent list must still come back transient. Without
// this, a mutation that made every gRPC status classify as permanent would
// pass TestGRPC_NotFoundStatusIsPermanent above for the wrong reason.
func TestGRPC_UnavailableStatusIsTransient(t *testing.T) {
	addr := startGRPCStatusServer(t, codes.Unavailable, "try again later")

	pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Close(ctx)
	})
	ctx := types.WithResourcePool(context.Background(), pool)

	h, ok := registry.Lookup("xflow.grpc")
	if !ok {
		t.Fatal("xflow.grpc is not registered")
	}
	b := node.GRPC("probe.Service", "GetWidget", addr).
		SetRequest(map[string]any{"id": "123"}).
		Timeout("5s")
	input := &types.Input{Params: b.RawParams().(map[string]any)}

	_, err := h.Execute(ctx, input)
	if err == nil {
		t.Fatal("expected an error for an Unavailable RPC status, got nil")
	}
	if types.IsPermanent(err) {
		t.Fatalf("an Unavailable RPC status must be transient (retryable); got permanent err=%v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "grpc.Unavailable" {
		t.Fatalf("expected ClassifiedError code=grpc.Unavailable, got %T %v", err, err)
	}
}
