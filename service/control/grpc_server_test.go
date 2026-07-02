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

func startGRPCTestServer(t *testing.T, eng EngineFacade, runners RunnerDirectory, opts ...GRPCServerOption) *protocol.GRPCClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(srv, NewGRPCServer(eng, runners, opts...))
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
	runners := NewMemoryRunnerDirectory()
	client := startGRPCTestServer(t, eng, runners)
	ctx := context.Background()

	registerResp, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if registerResp.SessionID == "" {
		t.Fatal("Register() session id is empty")
	}

	if _, err := client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID:  "runner-1",
		SessionID: registerResp.SessionID,
		Capacity:  2,
		InFlight:  0,
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	// Empty poll returns no lease plus a wait hint.
	empty, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: registerResp.SessionID, Capacity: 2})
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
	eng.buildLease = &want
	enqueued, err := runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(&want.Task),
		Task:         want.Task,
		Routing:      engine.TaskRouting{NodeType: want.NodeType, NodeVersion: want.NodeVersion},
	})
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("EnqueueAssignment() enqueued=false, want true")
	}

	got, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: registerResp.SessionID, Capacity: 2})
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
		RunnerID:  "runner-1",
		SessionID: registerResp.SessionID,
		Lease:     got.Lease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
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
	enqueued, err = runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(&want.Task),
		Task:         want.Task,
		Routing:      engine.TaskRouting{NodeType: want.NodeType, NodeVersion: want.NodeVersion},
	})
	if err != nil {
		t.Fatalf("requeue after report EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatal("requeue after report EnqueueAssignment() enqueued=false, want true")
	}
}

func TestGRPCReportResultRejectsStaleLeaseToken(t *testing.T) {
	eng := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	runners := NewMemoryRunnerDirectory()
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
	runners := NewMemoryRunnerDirectory()
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
	client := startGRPCTestServer(t, eng, NewMemoryRunnerDirectory())

	_, err := client.Register(context.Background(), protocol.RegisterRunnerRequest{Concurrency: 0})
	if err == nil {
		t.Fatal("expected error for missing fields")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("status code = %v, want InvalidArgument", got)
	}
}

func TestGRPCRegisterRejectedWithoutTokenReturnsUnauthenticated(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:             "functions",
			IDPrefix:         "runner-",
			Token:            "secret",
			AllowedNodeTypes: []string{"xflow.function"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	client := startGRPCTestServer(t, &fakeControlEngine{}, NewMemoryRunnerDirectory(), WithGRPCAuthenticator(store))

	_, err = client.Register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err == nil {
		t.Fatal("expected unauthenticated error")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated", got)
	}
}
