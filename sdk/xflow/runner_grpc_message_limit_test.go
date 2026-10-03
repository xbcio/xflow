package xflow

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/types"
)

// largePollServer answers every PollTask with one fixed, pre-encoded response.
type largePollServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	resp *runnerpb.PollTaskResponse
}

func (s *largePollServer) PollTask(context.Context, *runnerpb.PollTaskRequest) (*runnerpb.PollTaskResponse, error) {
	return s.resp, nil
}

// The gRPC runner client must accept what a server built with
// control.RunnerGRPCServerOptions may send. A 5.5 MiB lease is past grpc-go's
// 4 MiB default receive limit and inside the 8 MiB protocol cap.
func TestRunnerGRPCClientReceivesLeaseAboveDefaultLimit(t *testing.T) {
	const largeLeaseBytes = 11 << 19
	resp, err := protocol.PollTaskResponseToProto(protocol.PollTaskResponse{Lease: &engine.TaskLease{
		LeaseID:  "lease-big",
		Task:     engine.Task{ExecutionID: "exec-big", NodeName: "big"},
		Input:    &types.Input{Data: map[string]any{"blob": strings.Repeat("x", largeLeaseBytes)}},
		NodeType: "xflow.function",
	}})
	if err != nil {
		t.Fatalf("PollTaskResponseToProto: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(control.RunnerGRPCServerOptions()...)
	runnerpb.RegisterRunnerProtocolServer(srv, &largePollServer{resp: resp})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	target := "passthrough:///" + lis.Addr().String()

	t.Run("RunnerClientReceives", func(t *testing.T) {
		cfg := RunnerConfig{Transport: RunnerTransportGRPC, GRPCTarget: target}
		reloader, err := newCredentialReloader(cfg)
		if err != nil {
			t.Fatalf("newCredentialReloader: %v", err)
		}
		client, closeFn, err := newRunnerProtocolClient(cfg, runnerOptionsFrom(nil), reloader)
		if err != nil {
			t.Fatalf("newRunnerProtocolClient(gRPC): %v", err)
		}
		defer closeFn()
		got, err := client.Poll(context.Background(), protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: "s-1"})
		if err != nil {
			t.Fatalf("Poll() error = %v", err)
		}
		if got.Lease == nil || got.Lease.LeaseID != "lease-big" {
			t.Fatalf("Poll() lease = %+v, want lease-big", got.Lease)
		}
	})

	// Guards the fixture: without the runner dial options the same response
	// must be rejected, or the subtest above proves nothing.
	t.Run("DefaultClientRejects", func(t *testing.T) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		_, err = protocol.NewGRPCClient(conn).Poll(context.Background(), protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: "s-1"})
		if code := status.Code(err); code != codes.ResourceExhausted {
			t.Fatalf("default client Poll() code = %v (err %v), want ResourceExhausted", code, err)
		}
	})
}
