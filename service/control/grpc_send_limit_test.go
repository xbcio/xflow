package control

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

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

// startRunnerGRPCTestServer serves eng and runners on a grpc.Server built from
// serverOpts and dials it with dialOpts, so a test can compare the
// runner-protocol options against grpc-go's defaults on either side.
func startRunnerGRPCTestServer(t *testing.T, eng EngineFacade, runners RunnerDirectory, serverOpts []grpc.ServerOption, dialOpts []grpc.DialOption, opts ...GRPCServerOption) *protocol.GRPCClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(serverOpts...)
	runnerpb.RegisterRunnerProtocolServer(srv, NewGRPCServer(eng, runners, opts...))
	go func() {
		_ = srv.Serve(lis)
	}()

	dial := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, dialOpts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", dial...)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return protocol.NewGRPCClient(conn)
}

// enqueueLeaseOfSize registers runner-1 and queues one assignment whose built
// lease carries an input string of inputBytes, returning the session id.
func enqueueLeaseOfSize(t *testing.T, client *protocol.GRPCClient, eng *fakeControlEngine, runners RunnerDirectory, inputBytes int) string {
	t.Helper()
	ctx := context.Background()
	reg, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		InstanceUID:  "test-instance",
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lease := engine.TaskLease{
		LeaseID:    "lease-big",
		LeaseToken: "token-big",
		Attempt:    1,
		Task:       engine.Task{ExecutionID: "exec-big", NodeName: "big", NodeIdx: 0},
		Input:      &types.Input{Data: map[string]any{"blob": strings.Repeat("x", inputBytes)}},
		NodeType:   "xflow.function",
	}
	eng.buildLease = &lease
	if _, err := runners.EnqueueAssignment(ctx, Assignment{
		AssignmentID: BuildAssignmentID(&lease.Task),
		Task:         lease.Task,
		Routing:      engine.TaskRouting{NodeType: lease.NodeType},
	}); err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	return reg.SessionID
}

func TestGRPCPollFailsLeaseAboveRunnerMessageLimit(t *testing.T) {
	ctx := context.Background()
	eng := &fakeControlEngine{}
	runners := NewMemoryRunnerDirectory()
	logger := &recordingLogger{}
	// A client that would accept the frame, so a delivered lease would show.
	client := startRunnerGRPCTestServer(t, eng, runners, RunnerGRPCServerOptions(),
		[]grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4 * MaxRegisterRunnerBodyBytes))},
		WithGRPCLogger(logger))
	session := enqueueLeaseOfSize(t, client, eng, runners, MaxRegisterRunnerBodyBytes+1)

	got, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: session, Capacity: 1})
	if err != nil {
		t.Fatalf("Poll() error = %v, want a no-task answer that keeps the session alive", err)
	}
	if got.Lease != nil {
		t.Fatalf("Poll() delivered lease %q above the message limit", got.Lease.LeaseID)
	}
	if got.Wait <= 0 {
		t.Fatalf("Poll() wait = %v, want a positive wait hint", got.Wait)
	}

	if eng.committedLease == nil || eng.committedLease.LeaseID != "lease-big" {
		t.Fatalf("committed lease = %+v, want lease-big failed", eng.committedLease)
	}
	var classified *types.ClassifiedError
	if !errors.As(eng.committedResult.Error, &classified) || !types.IsPermanent(classified) || classified.Code != LeaseTooLargeErrorCode {
		t.Fatalf("committed error = %v, want permanent %s", eng.committedResult.Error, LeaseTooLargeErrorCode)
	}

	entries := logger.withMsg("task lease exceeds runner gRPC message limit; failing task")
	if len(entries) != 1 {
		t.Fatalf("oversize log entries = %d, want 1 (all: %+v)", len(entries), logger.entries)
	}
	for key, want := range map[string]any{"exec": "exec-big", "node": "big", "lease": "lease-big", "attempt": 1} {
		if got := entries[0].field(key); got != want {
			t.Errorf("log field %q = %v, want %v", key, got, want)
		}
	}

	// The memory directory replays finalized leases to a recovery-only poll;
	// a released lease must not come back.
	replay, err := client.Poll(ctx, protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: session, Capacity: 1, RecoveryOnly: true})
	if err != nil {
		t.Fatalf("recovery Poll() error = %v", err)
	}
	if replay.Lease != nil {
		t.Fatalf("recovery Poll() replayed failed lease %q", replay.Lease.LeaseID)
	}
}

func TestRunnerGRPCOptionsDeliverLeaseAboveDefaultLimit(t *testing.T) {
	// 5.5 MiB: past grpc-go's 4 MiB default receive limit, inside the cap.
	const largeLeaseBytes = 11 << 19
	if largeLeaseBytes <= 4<<20 || largeLeaseBytes >= MaxRegisterRunnerBodyBytes {
		t.Fatalf("fixture size %d must sit between 4 MiB and %d", largeLeaseBytes, MaxRegisterRunnerBodyBytes)
	}
	tests := []struct {
		name       string
		serverOpts []grpc.ServerOption
		dialOpts   []grpc.DialOption
		wantCode   codes.Code
	}{
		{"BothRunnerOptionSetsDeliver", RunnerGRPCServerOptions(), RunnerGRPCDialOptions(), codes.OK},
		{"DefaultsReject", nil, nil, codes.ResourceExhausted},
		{"DefaultDialRejects", RunnerGRPCServerOptions(), nil, codes.ResourceExhausted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := &fakeControlEngine{}
			runners := NewMemoryRunnerDirectory()
			client := startRunnerGRPCTestServer(t, eng, runners, tt.serverOpts, tt.dialOpts)
			session := enqueueLeaseOfSize(t, client, eng, runners, largeLeaseBytes)

			got, err := client.Poll(context.Background(), protocol.PollTaskRequest{RunnerID: "runner-1", SessionID: session, Capacity: 1})
			if code := status.Code(err); code != tt.wantCode {
				t.Fatalf("Poll() code = %v (err %v), want %v", code, err, tt.wantCode)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if got.Lease == nil || got.Lease.LeaseID != "lease-big" {
				t.Fatalf("Poll() lease = %+v, want lease-big", got.Lease)
			}
			if blob, _ := got.Lease.Input.Data["blob"].(string); len(blob) != largeLeaseBytes {
				t.Fatalf("delivered input blob length = %d, want %d", len(blob), largeLeaseBytes)
			}
			if eng.committedLease != nil {
				t.Fatalf("deliverable lease was failed: %+v", eng.committedResult)
			}
		})
	}
}
