package control

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/types"
)

func startGRPCTestServer(t *testing.T, eng EngineFacade, runners *RunnerPool) *protocol.GRPCClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(srv, NewGRPCServer(eng, runners))
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

func TestGRPCRegisterPollAndResultRoundTrip(t *testing.T) {
	eng := &fakeControlEngine{}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)
	ctx := context.Background()

	if _, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if _, err := client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: "runner-1",
		Capacity: 2,
		InFlight: 0,
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	// Empty poll returns no lease plus a wait hint.
	empty, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", Capacity: 2})
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if empty.Lease != nil {
		t.Fatalf("expected no lease, got %+v", empty.Lease)
	}
	if empty.Wait <= 0 {
		t.Fatalf("expected positive wait hint, got %v", empty.Wait)
	}

	// Assign a rich lease and confirm it survives the JSON-bytes boundary intact.
	want := engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-1"),
		LeaseToken: engine.LeaseToken("token-1"),
		Attempt:    1,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-1"),
			NodeName:    "start",
			NodeIdx:     0,
		},
		Input:    &types.Input{Params: map[string]any{"k": "v"}},
		NodeType: "xflow.function",
	}
	if err := runners.Assign(want); err != nil {
		t.Fatalf("Assign() = %v, want nil", err)
	}

	got, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", Capacity: 2})
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if got.Lease == nil {
		t.Fatal("expected a lease, got nil")
	}
	if got.Lease.LeaseID != want.LeaseID || got.Lease.LeaseToken != want.LeaseToken ||
		got.Lease.NodeType != want.NodeType || got.Lease.Task.NodeName != want.Task.NodeName {
		t.Fatalf("lease mismatch: got %+v want %+v", got.Lease, want)
	}
	if got.Lease.Input == nil || got.Lease.Input.Params["k"] != "v" {
		t.Fatalf("lease input not preserved: %+v", got.Lease.Input)
	}

	resp, err := client.ReportResult(ctx, protocol.ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    got.Lease,
		Result:   engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	})
	if err != nil {
		t.Fatalf("ReportResult() error = %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("result not accepted: %+v", resp)
	}
	if eng.committedLease == nil || eng.committedLease.LeaseToken != "token-1" {
		t.Fatalf("committed lease = %+v, want token-1", eng.committedLease)
	}
	if eng.committedResult.Output == nil || eng.committedResult.Output.Data["ok"] != true {
		t.Fatalf("committed result output not preserved: %+v", eng.committedResult.Output)
	}
}

func TestGRPCReportResultRejectsStaleLeaseToken(t *testing.T) {
	eng := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	resp, err := client.ReportResult(context.Background(), protocol.ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    &engine.TaskLease{LeaseID: engine.LeaseID("lease-1"), LeaseToken: engine.LeaseToken("stale")},
		Result:   engine.TaskResult{},
	})
	if err != nil {
		t.Fatalf("ReportResult() transport error = %v", err)
	}
	if resp.Accepted {
		t.Fatal("expected rejection, got accepted")
	}
	if resp.Error == "" {
		t.Fatal("expected rejection reason, got empty")
	}
}

func TestGRPCHeartbeatUnknownRunnerReturnsNotFound(t *testing.T) {
	eng := &fakeControlEngine{}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	_, err := client.Heartbeat(context.Background(), protocol.HeartbeatRequest{
		RunnerID:  "ghost",
		Timestamp: time.Now().Unix(),
	})
	if err == nil {
		t.Fatal("expected error for unknown runner")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("status code = %v, want NotFound", got)
	}
}

func TestGRPCRegisterRejectsMissingFields(t *testing.T) {
	eng := &fakeControlEngine{}
	client := startGRPCTestServer(t, eng, NewRunnerPool())

	_, err := client.Register(context.Background(), protocol.RegisterRunnerRequest{Concurrency: 0})
	if err == nil {
		t.Fatal("expected error for missing fields")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("status code = %v, want InvalidArgument", got)
	}
}
