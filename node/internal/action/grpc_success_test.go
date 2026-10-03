package action_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/types"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// startStructEchoServer starts a real, listening gRPC server that decodes a
// google.protobuf.Struct request, hands its fields to requests, and answers
// with a fixed Struct. UnknownServiceHandler, like startGRPCStatusServer: the
// node invokes a fully-qualified method name with no compiled proto behind it,
// so there is nothing to register a typed service against.
func startStructEchoServer(t *testing.T, requests chan<- map[string]any) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	resp, err := structpb.NewStruct(map[string]any{"status": "ok", "count": 3})
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		in := &structpb.Struct{}
		if err := stream.RecvMsg(in); err != nil {
			return err
		}
		requests <- in.AsMap()
		return stream.SendMsg(resp)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestGRPC_SuccessPathReturnsTheStructAsData pins the node's success path: a
// Struct request goes out, and the returned Struct comes back as the node's
// output data.
//
// Until 2026-10-04 the node decoded the response into a zero-value
// *dynamicpb.Message, which carries no message descriptor: the first successful
// response dereferenced the nil descriptor inside conn.Invoke and panicked,
// taking down the runner process (the execution goroutine has no recover). No
// test caught it because every other grpc test drives the error-classification
// path; a regression to a descriptor-less decode target panics this test, which
// is the correct signal.
func TestGRPC_SuccessPathReturnsTheStructAsData(t *testing.T) {
	requests := make(chan map[string]any, 1)
	addr := startStructEchoServer(t, requests)

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
	b := node.GRPC("probe.StructEcho", "Get", addr).
		SetRequest(map[string]any{"id": "123"}).
		Timeout("5s")

	out, err := h.Execute(ctx, &types.Input{Params: b.RawParams().(map[string]any)})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := out.Data["status"]; got != "ok" {
		t.Fatalf("out.Data[status] = %#v, want %q", got, "ok")
	}
	// JSON types, not proto scalar types: numbers arrive as float64.
	if got, ok := out.Data["count"].(float64); !ok || got != 3 {
		t.Fatalf("out.Data[count] = %#v (%T), want float64(3)", out.Data["count"], out.Data["count"])
	}

	select {
	case got := <-requests:
		if got["id"] != "123" {
			t.Fatalf("server received %#v, want the request Struct to carry id=123", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never received the request")
	}
}
