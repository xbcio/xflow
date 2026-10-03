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

// TestGRPCClientActivationAckSendsRequest verifies the client marshals every
// ActivationAck field onto the wire request (mirroring Client.ActivationAck's
// HTTP POST body) and reports success as a nil error, matching the HTTP
// transport's "200, no response payload" contract.
func TestGRPCClientActivationAckSendsRequest(t *testing.T) {
	fake := &fakeRunnerProtocolClient{}
	client := &GRPCClient{grpc: fake}

	ack := ActivationAck{
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		GroupID:         "entry-1",
		ReplicaIndex:    2,
		Generation:      7,
		Status:          ActivationStatusFailed,
		Error:           "supply not available",
	}
	if err := client.ActivationAck(context.Background(), ack); err != nil {
		t.Fatalf("ActivationAck() error = %v", err)
	}
	if fake.ackActivationReq == nil {
		t.Fatal("ActivationAck() did not call gRPC client")
	}
	got := fake.ackActivationReq
	if got.GetRunnerId() != ack.RunnerID || got.GetSessionId() != ack.SessionID {
		t.Fatalf("identity = %+v, want runner_id=%q session_id=%q", got, ack.RunnerID, ack.SessionID)
	}
	if got.GetWorkflowId() != ack.WorkflowID || got.GetWorkflowVersion() != ack.WorkflowVersion {
		t.Fatalf("workflow = %+v, want workflow_id=%q workflow_version=%q", got, ack.WorkflowID, ack.WorkflowVersion)
	}
	if got.GetGroupId() != ack.GroupID || got.GetReplicaIndex() != ack.ReplicaIndex {
		t.Fatalf("entry unit = %+v, want group_id=%q replica_index=%d", got, ack.GroupID, ack.ReplicaIndex)
	}
	if got.GetGeneration() != ack.Generation {
		t.Fatalf("generation = %d, want %d", got.GetGeneration(), ack.Generation)
	}
	if got.GetStatus() != string(ack.Status) || got.GetError() != ack.Error {
		t.Fatalf("status/error = %+v, want status=%q error=%q", got, ack.Status, ack.Error)
	}
}

// TestGRPCClientActivationAckPropagatesError verifies a server-side rejection
// surfaces to the caller as an error rather than being swallowed, mirroring
// the HTTP client's non-2xx-to-error behavior.
func TestGRPCClientActivationAckPropagatesError(t *testing.T) {
	fake := &fakeRunnerProtocolClient{ackActivationErr: status.Error(codes.InvalidArgument, "workflow_version is required")}
	client := &GRPCClient{grpc: fake}

	err := client.ActivationAck(context.Background(), ActivationAck{RunnerID: "runner-1", SessionID: "sess-1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status code = %v, want InvalidArgument", status.Code(err))
	}
}

type fakeRunnerProtocolClient struct {
	registerReq  *runnerpb.RegisterRequest
	registerResp *runnerpb.RegisterResponse

	ackActivationReq *runnerpb.ActivationAckRequest
	ackActivationErr error
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

func (f *fakeRunnerProtocolClient) AckActivation(_ context.Context, in *runnerpb.ActivationAckRequest, _ ...grpc.CallOption) (*runnerpb.ActivationAckResponse, error) {
	f.ackActivationReq = in
	if f.ackActivationErr != nil {
		return nil, f.ackActivationErr
	}
	return &runnerpb.ActivationAckResponse{}, nil
}
