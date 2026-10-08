package protocol

import (
	"context"
	"testing"
	"time"

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

// TestGRPCClientRenewLeaseSendsRequest verifies the client marshals every
// RenewLeaseRequest field onto the wire request and decodes a successful
// renewal's deadline back off the wire (UnixNano round-trip), mirroring the
// HTTP transport's TestClientRenewLease.
func TestGRPCClientRenewLeaseSendsRequest(t *testing.T) {
	deadline := time.Now().UTC().Add(90 * time.Second).Truncate(time.Second)
	fake := &fakeRunnerProtocolClient{renewLeaseResp: &runnerpb.RenewLeaseResponse{
		Renewed:          true,
		DeadlineUnixNano: deadline.UnixNano(),
	}}
	client := &GRPCClient{grpc: fake}

	resp, err := client.RenewLease(context.Background(), RenewLeaseRequest{
		RunnerID:   "runner-1",
		SessionID:  "sess-1",
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Extend:     90_000,
	})
	if err != nil {
		t.Fatalf("RenewLease() error = %v", err)
	}
	if !resp.Renewed || !resp.Deadline.Equal(deadline) {
		t.Fatalf("RenewLease() = %+v, want renewed with deadline %v", resp, deadline)
	}
	if fake.renewLeaseReq == nil {
		t.Fatal("RenewLease() did not call gRPC client")
	}
	got := fake.renewLeaseReq
	if got.GetRunnerId() != "runner-1" || got.GetSessionId() != "sess-1" ||
		got.GetLeaseId() != "lease-1" || got.GetLeaseToken() != "token-1" || got.GetExtendMs() != 90_000 {
		t.Fatalf("request = %+v, want the fence token, session, and extend to reach the server verbatim", got)
	}
}

// TestGRPCClientRenewLeaseRefusalIsNotAnError mirrors the HTTP transport's
// TestClientRenewLeaseRefusalIsNotAnError: a lease the server refuses to
// renew is a normal response (Renewed=false), not a transport error. The
// runner's renewal loop (service/runner/lease_renew.go) depends on this
// distinction to tell "cancel the handler" from "retry the network call".
func TestGRPCClientRenewLeaseRefusalIsNotAnError(t *testing.T) {
	fake := &fakeRunnerProtocolClient{renewLeaseResp: &runnerpb.RenewLeaseResponse{
		Renewed: false,
		Error:   "lease not found",
	}}
	client := &GRPCClient{grpc: fake}

	resp, err := client.RenewLease(context.Background(), RenewLeaseRequest{
		RunnerID: "runner-1", SessionID: "sess-1", LeaseID: "l", LeaseToken: "t",
	})
	if err != nil {
		t.Fatalf("RenewLease() error = %v, want a Renewed=false response", err)
	}
	if resp.Renewed {
		t.Fatal("RenewLease() reported renewed for a refusal")
	}
	if resp.Error != "lease not found" {
		t.Errorf("resp.Error = %q, want the server's reason", resp.Error)
	}
}

// TestGRPCClientRenewLeasePropagatesTransportError verifies a gRPC-level
// failure (not a Renewed=false response) surfaces to the caller as an error,
// mirroring the HTTP client's non-2xx-to-error behavior.
func TestGRPCClientRenewLeasePropagatesTransportError(t *testing.T) {
	fake := &fakeRunnerProtocolClient{renewLeaseErr: status.Error(codes.Unauthenticated, "stale session")}
	client := &GRPCClient{grpc: fake}

	_, err := client.RenewLease(context.Background(), RenewLeaseRequest{
		RunnerID: "runner-1", SessionID: "sess-1", LeaseID: "l", LeaseToken: "t",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated", status.Code(err))
	}
}

type fakeRunnerProtocolClient struct {
	registerReq  *runnerpb.RegisterRequest
	registerResp *runnerpb.RegisterResponse

	ackActivationReq *runnerpb.ActivationAckRequest
	ackActivationErr error

	renewLeaseReq  *runnerpb.RenewLeaseRequest
	renewLeaseResp *runnerpb.RenewLeaseResponse
	renewLeaseErr  error
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

func (f *fakeRunnerProtocolClient) RenewLease(_ context.Context, in *runnerpb.RenewLeaseRequest, _ ...grpc.CallOption) (*runnerpb.RenewLeaseResponse, error) {
	f.renewLeaseReq = in
	if f.renewLeaseErr != nil {
		return nil, f.renewLeaseErr
	}
	if f.renewLeaseResp != nil {
		return f.renewLeaseResp, nil
	}
	return &runnerpb.RenewLeaseResponse{}, nil
}
