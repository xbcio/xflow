package protocol

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

func TestGRPCClientRegisterPreservesSessionID(t *testing.T) {
	fake := &fakeRunnerProtocolClient{
		registerResp: &runnerpb.RegisterResponse{
			RunnerId:  "runner-1",
			SessionId: "session-1",
		},
	}
	client := &GRPCClient{grpc: fake}

	resp, err := client.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if fake.registerReq == nil {
		t.Fatal("Register() did not call gRPC client")
	}
	if resp.RunnerID != "runner-1" {
		t.Fatalf("RunnerID = %q, want runner-1", resp.RunnerID)
	}
	if resp.SessionID != "session-1" {
		t.Fatalf("SessionID = %q, want session-1", resp.SessionID)
	}
}

type fakeRunnerProtocolClient struct {
	registerReq  *runnerpb.RegisterRequest
	registerResp *runnerpb.RegisterResponse
}

func (*fakeRunnerProtocolClient) Connect(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerpb.RunnerFrame, runnerpb.ServerFrame], error) {
	return nil, status.Errorf(codes.Unimplemented, "Connect not implemented in fake")
}

func (f *fakeRunnerProtocolClient) Register(_ context.Context, in *runnerpb.RegisterRequest, _ ...grpc.CallOption) (*runnerpb.RegisterResponse, error) {
	f.registerReq = in
	return f.registerResp, nil
}

func (*fakeRunnerProtocolClient) Heartbeat(context.Context, *runnerpb.HeartbeatRequest, ...grpc.CallOption) (*runnerpb.HeartbeatResponse, error) {
	panic("unexpected Heartbeat call")
}

func (*fakeRunnerProtocolClient) PollTask(context.Context, *runnerpb.PollTaskRequest, ...grpc.CallOption) (*runnerpb.PollTaskResponse, error) {
	panic("unexpected PollTask call")
}

func (*fakeRunnerProtocolClient) ReportResult(context.Context, *runnerpb.ReportResultRequest, ...grpc.CallOption) (*runnerpb.ReportResultResponse, error) {
	panic("unexpected ReportResult call")
}
