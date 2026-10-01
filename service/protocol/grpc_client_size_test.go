package protocol

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/types"
)

// sizeLimitServer counts the unary RPCs that reached it, answers PollTask with
// pollResp and ReportResult with reportErr when set.
type sizeLimitServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	pollResp  *runnerpb.PollTaskResponse
	reportErr error
	calls     atomic.Int32
}

func (s *sizeLimitServer) PollTask(context.Context, *runnerpb.PollTaskRequest) (*runnerpb.PollTaskResponse, error) {
	s.calls.Add(1)
	return s.pollResp, nil
}

func (s *sizeLimitServer) ReportResult(context.Context, *runnerpb.ReportResultRequest) (*runnerpb.ReportResultResponse, error) {
	s.calls.Add(1)
	if s.reportErr != nil {
		return nil, s.reportErr
	}
	return &runnerpb.ReportResultResponse{Accepted: true}, nil
}

// startSizeLimitServer serves srv over bufconn and returns a real gRPC client
// dialed to it.
func startSizeLimitServer(t *testing.T, srv *sizeLimitServer, serverOpts []grpc.ServerOption, callOpts ...grpc.CallOption) *GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(serverOpts...)
	runnerpb.RegisterRunnerProtocolServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(callOpts...),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewGRPCClient(conn)
}

func reportWithOutput(padBytes int) ReportResultRequest {
	return ReportResultRequest{
		RunnerID:  "runner-1",
		SessionID: "s",
		Lease:     &engine.TaskLease{LeaseID: "lease-1", Task: engine.Task{ExecutionID: "exec-1", NodeName: "n"}},
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"pad": strings.Repeat("x", padBytes)}}},
	}
}

// A request over either side's message limit can never be accepted as sent,
// so it must surface as ErrRunnerRequestTooLarge like the HTTP Client's
// oversize body. Every other ResourceExhausted keeps its own meaning.
func TestGRPCClientMapsRequestSizeErrors(t *testing.T) {
	const limit = 4 << 10
	t.Run("ClientSendLimit", func(t *testing.T) {
		srv := &sizeLimitServer{}
		client := startSizeLimitServer(t, srv, nil, grpc.MaxCallSendMsgSize(limit))
		_, err := client.ReportResult(context.Background(), reportWithOutput(2*limit))
		if !errors.Is(err, ErrRunnerRequestTooLarge) {
			t.Fatalf("ReportResult() over the client send limit error = %v, want ErrRunnerRequestTooLarge", err)
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("ReportResult() error code = %v, want the ResourceExhausted status kept", status.Code(err))
		}
		if n := srv.calls.Load(); n != 0 {
			t.Fatalf("server saw %d calls, want 0", n)
		}
	})
	t.Run("ServerRecvLimit", func(t *testing.T) {
		srv := &sizeLimitServer{}
		client := startSizeLimitServer(t, srv, []grpc.ServerOption{grpc.MaxRecvMsgSize(limit)})
		_, err := client.ReportResult(context.Background(), reportWithOutput(2*limit))
		if !errors.Is(err, ErrRunnerRequestTooLarge) {
			t.Fatalf("ReportResult() over the server receive limit error = %v, want ErrRunnerRequestTooLarge", err)
		}
		if n := srv.calls.Load(); n != 0 {
			t.Fatalf("server handler saw %d calls, want 0", n)
		}
	})
	t.Run("WithinLimits", func(t *testing.T) {
		srv := &sizeLimitServer{}
		client := startSizeLimitServer(t, srv, []grpc.ServerOption{grpc.MaxRecvMsgSize(limit)}, grpc.MaxCallSendMsgSize(limit))
		resp, err := client.ReportResult(context.Background(), reportWithOutput(limit/4))
		if err != nil || !resp.Accepted {
			t.Fatalf("ReportResult() within limits = %+v, %v, want accepted", resp, err)
		}
	})
	t.Run("ResponseOverClientRecvLimitIsNotMapped", func(t *testing.T) {
		pollResp, err := PollTaskResponseToProto(PollTaskResponse{Lease: &engine.TaskLease{
			LeaseID: "lease-big",
			Task:    engine.Task{ExecutionID: "exec-1", NodeName: "n"},
			Input:   &types.Input{Data: map[string]any{"blob": strings.Repeat("x", 2*limit)}},
		}})
		if err != nil {
			t.Fatalf("PollTaskResponseToProto: %v", err)
		}
		client := startSizeLimitServer(t, &sizeLimitServer{pollResp: pollResp}, nil, grpc.MaxCallRecvMsgSize(limit))
		_, err = client.Poll(context.Background(), PollTaskRequest{RunnerID: "runner-1", SessionID: "s"})
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("Poll() with an oversize response code = %v (err %v), want ResourceExhausted", status.Code(err), err)
		}
		if errors.Is(err, ErrRunnerRequestTooLarge) {
			t.Fatalf("Poll() with an oversize response error = %v, must not claim the request was too large", err)
		}
	})
	t.Run("ServerResourceExhaustedIsNotMapped", func(t *testing.T) {
		srv := &sizeLimitServer{reportErr: status.Error(codes.ResourceExhausted, "rate limited")}
		client := startSizeLimitServer(t, srv, nil)
		_, err := client.ReportResult(context.Background(), reportWithOutput(16))
		if status.Code(err) != codes.ResourceExhausted || errors.Is(err, ErrRunnerRequestTooLarge) {
			t.Fatalf("ReportResult() against a rate-limiting server error = %v, want plain ResourceExhausted", err)
		}
	})
}
