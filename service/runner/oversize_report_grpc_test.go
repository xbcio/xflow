package runner

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/types"
)

// bigOutputHandler returns an output of size bytes and counts executions.
type bigOutputHandler struct {
	size int
	runs *atomic.Int32
}

func (bigOutputHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.runner.big_output"}
}

func (h bigOutputHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.runs.Add(1)
	return &types.Output{Data: map[string]any{"blob": strings.Repeat("x", h.size)}}, nil
}

// replayingGRPCServer is the gRPC counterpart of replayingReportServer: it
// hands the lease out on every poll until a report for it is accepted, and
// counts every report that reaches the handler.
type replayingGRPCServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	pollResp *runnerpb.PollTaskResponse
	cancel   context.CancelFunc

	mu       sync.Mutex
	accepted *protocol.ReportResultRequest
	reports  int
}

func (*replayingGRPCServer) Register(context.Context, *runnerpb.RegisterRequest) (*runnerpb.RegisterResponse, error) {
	return &runnerpb.RegisterResponse{RunnerId: "runner-1", SessionId: "session-1"}, nil
}

func (*replayingGRPCServer) Heartbeat(context.Context, *runnerpb.HeartbeatRequest) (*runnerpb.HeartbeatResponse, error) {
	return &runnerpb.HeartbeatResponse{}, nil
}

func (s *replayingGRPCServer) PollTask(context.Context, *runnerpb.PollTaskRequest) (*runnerpb.PollTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accepted != nil {
		return protocol.PollTaskResponseToProto(protocol.PollTaskResponse{Wait: time.Hour})
	}
	return s.pollResp, nil
}

func (s *replayingGRPCServer) ReportResult(_ context.Context, in *runnerpb.ReportResultRequest) (*runnerpb.ReportResultResponse, error) {
	req, err := protocol.ReportResultRequestFromProto(in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.reports++
	s.accepted = &req
	s.mu.Unlock()
	s.cancel()
	return &runnerpb.ReportResultResponse{Accepted: true}, nil
}

// The gRPC transport rejects an oversize report with ResourceExhausted, not
// HTTP 413. The runner must still land it as one permanent failure rather than
// leave the lease to be replayed and re-executed on every poll.
func TestRunnerReportsOversizeGRPCResultAsPermanentFailure(t *testing.T) {
	const outputBytes = 9 << 20
	tests := []struct {
		name     string
		dialOpts []grpc.DialOption
	}{
		// RunnerGRPCDialOptions caps sends at 8 MiB, so the client refuses
		// the report before sending it.
		{"client send limit", control.RunnerGRPCDialOptions()},
		// A client allowed to send more is refused by the server's 8 MiB
		// receive limit instead.
		{"server receive limit", []grpc.DialOption{grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(control.MaxRegisterRunnerBodyBytes),
			grpc.MaxCallSendMsgSize(4*control.MaxRegisterRunnerBodyBytes),
		)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pollResp, err := protocol.PollTaskResponseToProto(protocol.PollTaskResponse{Lease: &engine.TaskLease{
				LeaseID:  "lease-big",
				Attempt:  1,
				Task:     engine.Task{ExecutionID: "exec-big", NodeName: "big"},
				Input:    &types.Input{Data: map[string]any{"k": "v"}},
				NodeType: "test.runner.big_output",
			}})
			if err != nil {
				t.Fatalf("PollTaskResponseToProto: %v", err)
			}
			srv := &replayingGRPCServer{pollResp: pollResp, cancel: cancel}

			lis := bufconn.Listen(1 << 20)
			g := grpc.NewServer(control.RunnerGRPCServerOptions()...)
			runnerpb.RegisterRunnerProtocolServer(g, srv)
			go func() { _ = g.Serve(lis) }()
			t.Cleanup(g.Stop)
			dialOpts := append([]grpc.DialOption{
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			}, tt.dialOpts...)
			conn, err := grpc.NewClient("passthrough:///bufconn", dialOpts...)
			if err != nil {
				t.Fatalf("dial bufconn: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			var runs atomic.Int32
			registry := execution.NewRegistry()
			registry.RegisterGlobal("test.runner.big_output", bigOutputHandler{size: outputBytes, runs: &runs})
			r := New(protocol.NewGRPCClient(conn), registry, Config{
				RunnerID:          "runner-1",
				Concurrency:       1,
				Capabilities:      []protocol.Capability{{NodeType: "test.runner.big_output"}},
				HeartbeatInterval: time.Hour,
				PollWait:          time.Millisecond,
			})
			if err := r.Run(ctx); err != nil {
				t.Fatalf("Run() error = %v after %d handler runs", err, runs.Load())
			}

			srv.mu.Lock()
			defer srv.mu.Unlock()
			if srv.accepted == nil {
				t.Fatalf("no report accepted after %d handler runs, want one permanent failure", runs.Load())
			}
			if got := runs.Load(); got != 1 {
				t.Fatalf("handler ran %d times, want 1", got)
			}
			if srv.reports != 1 {
				t.Fatalf("server committed %d reports, want 1", srv.reports)
			}
			if got := srv.accepted.Result.Error; !types.IsPermanent(got) {
				t.Fatalf("reported error = %v, want permanent", got)
			}
			if out := srv.accepted.Result.Output; out != nil {
				t.Fatalf("reported output = %d keys, want none", len(out.Data))
			}
			if echo := srv.accepted.Lease; echo == nil || echo.LeaseID != "lease-big" {
				t.Fatalf("echoed lease = %+v, want lease-big", echo)
			}
		})
	}
}
