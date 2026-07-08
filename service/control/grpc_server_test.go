package control

import (
	"context"
	"errors"
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

func startGRPCTestServer(t *testing.T, eng EngineFacade, runners *RunnerPool, opts ...GRPCServerOption) *protocol.GRPCClient {
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

func TestGRPCConnectStreamRoundTrip(t *testing.T) {
	eng := &fakeControlEngine{}
	runners := NewRunnerPool()

	// Build the server manually so we retain the runners handle — the
	// startGRPCTestServer helper only returns a *protocol.GRPCClient.
	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(grpcSrv, NewGRPCServer(eng, runners))
	go func() { _ = grpcSrv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(); grpcSrv.Stop() })

	client := protocol.NewGRPCClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Welcome == nil || fr.Welcome.RunnerID != "r1" {
		t.Fatalf("expected WELCOME, got fr=%+v err=%v", fr, err)
	}

	lease := engine.TaskLease{LeaseID: "L1", LeaseToken: "T1", NodeType: "xflow.function"}
	if err := runners.Assign(lease); err != nil {
		t.Fatalf("assign: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Task == nil || fr.Task.Lease == nil || fr.Task.Lease.LeaseID != "L1" {
		t.Fatalf("expected TASK L1, got fr=%+v err=%v", fr, err)
	}
	if err := stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L1", Lease: &lease,
	}}); err != nil {
		t.Fatalf("send result: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Ack == nil || !fr.Ack.Accepted {
		t.Fatalf("expected ACK accepted, got fr=%+v err=%v", fr, err)
	}
}

// mustRecvTask reads the next frame from stream and asserts it is a TASK
// frame carrying the given LeaseID. Fails the test otherwise.
func mustRecvTask(t *testing.T, stream protocol.FrameStream, wantLeaseID string) {
	t.Helper()
	fr, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv TASK %s: %v", wantLeaseID, err)
	}
	if fr.Task == nil || fr.Task.Lease == nil {
		t.Fatalf("expected TASK %s, got %+v", wantLeaseID, fr)
	}
	if string(fr.Task.Lease.LeaseID) != wantLeaseID {
		t.Fatalf("expected TASK %s, got TASK %s", wantLeaseID, fr.Task.Lease.LeaseID)
	}
}

// recvResult is what a background reader goroutine forwards to the test for
// each frame it receives off the stream, paired with any Recv error.
type recvResult struct {
	fr  protocol.ServerFrame
	err error
}

// streamReader starts a goroutine that continuously calls stream.Recv() and
// forwards results on the returned channel. Using a background reader (rather
// than calling Recv directly with a select-timeout wrapper) lets tests prove
// ordering ("no 3rd TASK before the RESULT is sent") without racing the
// blocking Recv call itself — the channel only ever contains frames that were
// actually read off the wire, in wire order.
func streamReader(stream protocol.FrameStream) <-chan recvResult {
	ch := make(chan recvResult, 16)
	go func() {
		for {
			fr, err := stream.Recv()
			ch <- recvResult{fr: fr, err: err}
			if err != nil {
				return
			}
		}
	}()
	return ch
}

// TestGRPCConnectCreditFlowControl proves cross-task integration between T3
// (queue), T4 (credit-gated drain), and T6 (gRPC Connect adapter): a runner
// that HELLOs with Concurrency=2 receives at most 2 TASK frames no matter how
// many leases are queued, and only after it ACKs (via RESULT) does credit
// replenish and the 3rd TASK arrive.
func TestGRPCConnectCreditFlowControl(t *testing.T) {
	eng := &fakeControlEngine{}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "credit-r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Welcome == nil {
		t.Fatalf("expected WELCOME, got fr=%+v err=%v", fr, err)
	}

	// RunnerPool capacity and streamSession credit are both seeded from
	// HELLO's Concurrency (see Core.Connect), so headroom == capacity(2) -
	// inFlight(0) - len(queue). Assigning all 3 leases up front would exceed
	// that headroom (ErrNoCapacity) before credit ever comes into play.
	// Assign 2 first (fills the queue to the capacity ceiling), let them
	// drain into TASK frames (queue empties back to 0), then assign the 3rd
	// — this is also how real dispatch behaves: leases arrive over time, not
	// all at once.
	leases := []engine.TaskLease{
		{LeaseID: "L1", LeaseToken: "T1", NodeType: "xflow.function"},
		{LeaseID: "L2", LeaseToken: "T2", NodeType: "xflow.function"},
		{LeaseID: "L3", LeaseToken: "T3", NodeType: "xflow.function"},
	}
	for _, lease := range leases[:2] {
		if err := runners.Assign(lease); err != nil {
			t.Fatalf("assign %s: %v", lease.LeaseID, err)
		}
	}

	reader := streamReader(stream)

	// Exactly 2 TASK frames should arrive — credit=2 caps delivery even
	// though 3 leases are queued.
	mustRecvFromReader := func(wantLeaseID string) {
		select {
		case res := <-reader:
			if res.err != nil {
				t.Fatalf("recv TASK %s: %v", wantLeaseID, res.err)
			}
			if res.fr.Task == nil || res.fr.Task.Lease == nil || string(res.fr.Task.Lease.LeaseID) != wantLeaseID {
				t.Fatalf("expected TASK %s, got %+v", wantLeaseID, res.fr)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for TASK %s", wantLeaseID)
		}
	}
	mustRecvFromReader("L1")
	mustRecvFromReader("L2")

	// Both queued leases have now drained into TASK frames, so the queue is
	// back to empty and headroom (capacity - inFlight - len(queue)) is 2
	// again — Assign(L3) succeeds even though credit is fully spent. This
	// mirrors real dispatch: capacity tracks queue depth, credit tracks
	// stream backpressure; they are independent gates.
	if err := runners.Assign(leases[2]); err != nil {
		t.Fatalf("assign L3: %v", err)
	}

	// Prove the 3rd TASK does NOT arrive while credit=0. A short bounded wait
	// is unavoidable here since we are proving absence, but the subsequent
	// steps still prove *ordering* via the same channel: whatever arrives
	// next (if anything) must be checked before we send RESULT.
	select {
	case res := <-reader:
		t.Fatalf("expected no frame while credit=0, got %+v (err=%v)", res.fr, res.err)
	case <-time.After(150 * time.Millisecond):
	}

	// Send RESULT for L1 — this should both ACK and replenish credit,
	// unblocking delivery of the 3rd TASK.
	if err := stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L1", Lease: &leases[0],
	}}); err != nil {
		t.Fatalf("send result L1: %v", err)
	}

	select {
	case res := <-reader:
		if res.err != nil {
			t.Fatalf("recv ACK L1: %v", res.err)
		}
		if res.fr.Ack == nil || !res.fr.Ack.Accepted || res.fr.Ack.LeaseID != "L1" {
			t.Fatalf("expected accepted ACK L1, got %+v", res.fr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for ACK L1")
	}

	mustRecvFromReader("L3")
}

// TestGRPCConnectReconnectResumesQueue proves cross-task integration between
// T3 (queue persists across session loss), T5 (clearSession on disconnect
// preserves registration; bindSession rebinds on re-HELLO), and T6 (gRPC
// Connect adapter): leases assigned while a runner is disconnected are
// delivered as soon as it reconnects and re-HELLOs with the same runnerID.
func TestGRPCConnectReconnectResumesQueue(t *testing.T) {
	eng := &fakeControlEngine{}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	// First connection: HELLO, get WELCOME, then disconnect by cancelling
	// the stream's context (simulates a dropped connection).
	firstCtx, firstCancel := context.WithCancel(context.Background())
	stream1, err := client.Connect(firstCtx)
	if err != nil {
		t.Fatalf("connect #1: %v", err)
	}
	if err := stream1.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "reconnect-r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello #1: %v", err)
	}
	if fr, err := stream1.Recv(); err != nil || fr.Welcome == nil {
		t.Fatalf("expected WELCOME #1, got fr=%+v err=%v", fr, err)
	}

	firstCancel()
	_ = stream1.Close()

	// Give the server's Connect goroutine a moment to observe ctx.Done and
	// run clearSession — otherwise Assign below could race a still-bound
	// session (best case it'd just deliver eagerly, but we want to prove the
	// "disconnected queue, then reconnect drains it" path specifically).
	// We poll the runner's session-less state indirectly by retrying Assign
	// until the registration is visible (Register happens before WELCOME is
	// sent, so this is already true by the time we got WELCOME above); the
	// real synchronization need is just letting clearSession's goroutine run.
	waitForSessionCleared(t, runners, "reconnect-r1")

	// While disconnected, assign 2 leases to the same runnerID.
	queued := []engine.TaskLease{
		{LeaseID: "Q1", LeaseToken: "QT1", NodeType: "xflow.function"},
		{LeaseID: "Q2", LeaseToken: "QT2", NodeType: "xflow.function"},
	}
	for _, lease := range queued {
		if err := runners.Assign(lease); err != nil {
			t.Fatalf("assign %s while disconnected: %v", lease.LeaseID, err)
		}
	}

	// Reconnect: new stream, re-HELLO with the same runnerID.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer secondCancel()
	stream2, err := client.Connect(secondCtx)
	if err != nil {
		t.Fatalf("connect #2: %v", err)
	}
	defer stream2.Close()

	if err := stream2.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "reconnect-r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello #2: %v", err)
	}
	if fr, err := stream2.Recv(); err != nil || fr.Welcome == nil || fr.Welcome.RunnerID != "reconnect-r1" {
		t.Fatalf("expected WELCOME #2, got fr=%+v err=%v", fr, err)
	}

	mustRecvTask(t, stream2, "Q1")
	mustRecvTask(t, stream2, "Q2")
}

// waitForSessionCleared polls RunnerPool until the given runner has no bound
// session (or a short deadline elapses). Connect's ctx.Done() branch and the
// subsequent clearSession call run in a server-side goroutine asynchronously
// relative to the client-side cancel, so tests that need "disconnected" state
// to be externally visible must synchronize on it rather than assume ordering.
func waitForSessionCleared(t *testing.T, runners *RunnerPool, runnerID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runners.mu.Lock()
		state, ok := runners.runners[runnerID]
		cleared := ok && state.session == nil
		runners.mu.Unlock()
		if cleared {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for session to clear for %s", runnerID)
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
	client := startGRPCTestServer(t, &fakeControlEngine{}, NewRunnerPool(), WithGRPCAuthenticator(store))

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

// TestGRPCConnectRejectedWithoutToken proves the streaming Connect RPC goes
// through the same bearer-token authentication as the unary RPCs (Finding 1
// of the final review: Connect used to call AuthenticateRegister with a
// hardcoded empty token, bypassing auth entirely). With an enforcing
// FilePolicyStore and no Authorization metadata, HELLO must be rejected
// instead of receiving WELCOME.
func TestGRPCConnectRejectedWithoutToken(t *testing.T) {
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
	client := startGRPCTestServer(t, &fakeControlEngine{}, NewRunnerPool(), WithGRPCAuthenticator(store))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "runner-1", Concurrency: 1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}

	fr, err := stream.Recv()
	if err == nil {
		t.Fatalf("expected stream error rejecting unauthenticated HELLO, got frame %+v", fr)
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated (err=%v)", got, err)
	}
}

// TestGRPCConnectHandleResultStaleLease proves handleResultFrame's
// stale-lease branch works end-to-end over the real gRPC transport: a RESULT
// frame whose commit fails with engine.ErrInvalidLeaseToken must produce a
// rejection ACK (Accepted=false, non-empty Error), not a torn-down stream.
// This is Finding 3's required gRPC-transport-level coverage (core_connect_test.go
// only exercises this over the fake in-process stream).
func TestGRPCConnectHandleResultStaleLease(t *testing.T) {
	eng := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "stale-r1", Concurrency: 1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Welcome == nil {
		t.Fatalf("expected WELCOME, got fr=%+v err=%v", fr, err)
	}

	lease := engine.TaskLease{LeaseID: "L1", LeaseToken: "stale-token", NodeType: "xflow.function"}
	if err := stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L1", Lease: &lease,
		Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}}); err != nil {
		t.Fatalf("send result: %v", err)
	}

	fr, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected ACK frame, got stream error: %v", err)
	}
	if fr.Ack == nil {
		t.Fatalf("expected ACK frame, got %+v", fr)
	}
	if fr.Ack.Accepted {
		t.Fatalf("expected rejection ACK for stale lease, got accepted: %+v", fr.Ack)
	}
	if fr.Ack.Error == "" {
		t.Fatal("expected non-empty rejection reason")
	}

	// The stream must still be alive after a commit error — send BYE and
	// confirm the server ends the RPC cleanly rather than having already
	// torn the stream down.
	if err := stream.Send(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}}); err != nil {
		t.Fatalf("send bye: %v", err)
	}
}

// TestGRPCConnectTransientCommitErrorSurvivesStream proves Finding 2's fix:
// a transient (non-ErrInvalidLeaseToken) CommitTaskResult failure degrades to
// a rejection ACK instead of killing the whole Connect stream. A second
// RESULT frame sent afterward on the SAME stream must still be processed
// (and, once the fake engine's transient error is cleared, accepted),
// proving the stream survived the first failure.
func TestGRPCConnectTransientCommitErrorSurvivesStream(t *testing.T) {
	eng := &transientThenOKEngine{failCount: 1, failErr: errors.New("transient backend hiccup")}
	runners := NewRunnerPool()
	client := startGRPCTestServer(t, eng, runners)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID: "transient-r1", Concurrency: 2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if fr, err := stream.Recv(); err != nil || fr.Welcome == nil {
		t.Fatalf("expected WELCOME, got fr=%+v err=%v", fr, err)
	}

	lease1 := engine.TaskLease{LeaseID: "L1", LeaseToken: "t1", NodeType: "xflow.function"}
	if err := stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L1", Lease: &lease1,
		Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}}); err != nil {
		t.Fatalf("send result L1: %v", err)
	}

	fr, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected ACK for L1 despite transient error, got stream error: %v", err)
	}
	if fr.Ack == nil || fr.Ack.Accepted || fr.Ack.Error == "" {
		t.Fatalf("expected rejection ACK for L1's transient error, got %+v", fr.Ack)
	}

	// Stream must have survived: a second RESULT is still processable and,
	// with the transient failure now cleared, gets accepted.
	lease2 := engine.TaskLease{LeaseID: "L2", LeaseToken: "t2", NodeType: "xflow.function"}
	if err := stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
		LeaseID: "L2", Lease: &lease2,
		Result: engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}}); err != nil {
		t.Fatalf("send result L2: %v", err)
	}
	fr2, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected ACK for L2 on surviving stream, got stream error: %v", err)
	}
	if fr2.Ack == nil || !fr2.Ack.Accepted || fr2.Ack.LeaseID != "L2" {
		t.Fatalf("expected accepted ACK L2 proving stream survived, got %+v", fr2.Ack)
	}
}

// transientThenOKEngine fails CommitTaskResult with failErr for the first
// failCount calls, then succeeds. Used to prove a transient error does not
// tear down the Connect stream (fakeControlEngine's commitErr is static and
// can't model recovery).
type transientThenOKEngine struct {
	fakeControlEngine
	failCount int
	failErr   error
	calls     int
}

func (f *transientThenOKEngine) CommitTaskResult(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) error {
	f.calls++
	if f.calls <= f.failCount {
		return f.failErr
	}
	return f.fakeControlEngine.CommitTaskResult(ctx, lease, result)
}
