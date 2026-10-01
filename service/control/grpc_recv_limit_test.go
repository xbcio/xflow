package control

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// startGRPCTestServerWithServerOptions is startGRPCTestServer with the
// grpc.Server built from serverOpts, so a test can compare the runner-protocol
// options against grpc-go's defaults.
func startGRPCTestServerWithServerOptions(t *testing.T, runners RunnerDirectory, serverOpts []grpc.ServerOption) *protocol.GRPCClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(serverOpts...)
	runnerpb.RegisterRunnerProtocolServer(srv, NewGRPCServer(&fakeControlEngine{}, runners))
	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return protocol.NewGRPCClient(conn)
}

// registerWithInventoryOfSize builds a Register whose protobuf encoding is at
// least minBytes, growing the activation inventory — the one unbounded field —
// the same way the HTTP LargeActivationInventoryAccepted test does.
func registerWithInventoryOfSize(t *testing.T, minBytes int) protocol.RegisterRunnerRequest {
	t.Helper()
	item := func(i int) protocol.ActivationInventoryItem {
		return protocol.ActivationInventoryItem{
			WorkflowID:      fmt.Sprintf("wf-%s-%06d", strings.Repeat("o", 24), i),
			WorkflowVersion: strings.Repeat("a", 64),
			EntryUnitID:     fmt.Sprintf("entry-%s-%02d", strings.Repeat("e", 16), i%8),
			ReplicaIndex:    3,
			Generation:      1234567890,
		}
	}
	req := protocol.RegisterRunnerRequest{
		InstanceUID:  "test-instance",
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}
	base := proto.Size(protocol.RegisterRequestToProto(req))
	req.Activations = []protocol.ActivationInventoryItem{item(0)}
	perItem := proto.Size(protocol.RegisterRequestToProto(req)) - base
	n := (minBytes-base)/perItem + 1
	req.Activations = make([]protocol.ActivationInventoryItem, n)
	for i := range req.Activations {
		req.Activations[i] = item(i)
	}
	if got := proto.Size(protocol.RegisterRequestToProto(req)); got < minBytes {
		t.Fatalf("register encodes to %d bytes, want at least %d", got, minBytes)
	}
	return req
}

func TestRunnerGRPCServerOptionsReceiveLimitMatchesHTTPRegisterCap(t *testing.T) {
	// 5.5 MiB: past grpc-go's 4 MiB default, inside the 8 MiB HTTP cap.
	const largeRegisterBytes = 11 << 19
	if largeRegisterBytes <= 4<<20 || largeRegisterBytes >= MaxRegisterRunnerBodyBytes {
		t.Fatalf("fixture size %d must sit between 4 MiB and %d", largeRegisterBytes, MaxRegisterRunnerBodyBytes)
	}
	large := registerWithInventoryOfSize(t, largeRegisterBytes)

	t.Run("DefaultServerRejectsLargeRegister", func(t *testing.T) {
		client := startGRPCTestServerWithServerOptions(t, NewMemoryRunnerDirectory(), nil)
		_, err := client.Register(context.Background(), large)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("default server Register() code = %v (err %v), want ResourceExhausted", got, err)
		}
	})

	t.Run("RunnerOptionsAcceptLargeRegister", func(t *testing.T) {
		dir := NewMemoryRunnerDirectory()
		client := startGRPCTestServerWithServerOptions(t, dir, RunnerGRPCServerOptions())
		resp, err := client.Register(context.Background(), large)
		if err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		if resp.SessionID == "" {
			t.Fatal("Register() session id is empty")
		}
		if _, ok := dir.Runner(context.Background(), "runner-1"); !ok {
			t.Fatal("runner not registered")
		}
	})

	t.Run("RunnerOptionsRejectRegisterAboveCap", func(t *testing.T) {
		dir := NewMemoryRunnerDirectory()
		client := startGRPCTestServerWithServerOptions(t, dir, RunnerGRPCServerOptions())
		_, err := client.Register(context.Background(), registerWithInventoryOfSize(t, MaxRegisterRunnerBodyBytes+1))
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("Register() code = %v (err %v), want ResourceExhausted", got, err)
		}
		if _, ok := dir.Runner(context.Background(), "runner-1"); ok {
			t.Fatal("over-cap runner was registered")
		}
	})
}
