package control

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// A report for a group-exec lease with no GroupResult must be rejected
// explicitly, releasing the runner's leased capacity immediately (aligned
// with the pre-022bcfc behavior of releasing on an accidental stale-token
// classification) rather than falling through to the node commit path, which
// CommitTaskResultWithOutcome now refuses outright (ErrGroupLeaseNotSupported)
// without releasing capacity — leaving it to the sweeper instead.
func TestReportResultRejectsGroupLeaseWithNilGroupResult(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	// registerRedisDirectoryRunner only registers the xflow.function capability,
	// which a group-exec assignment's routing (engine.GroupNodeType) would never
	// claim -- register directly with the matching capability/policy instead.
	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-group-nil-result",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: engine.GroupNodeType}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{engine.GroupNodeType}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	assignment := Assignment{
		AssignmentID: "exec-group-nil-result/group/activation-1",
		Task: engine.Task{
			ExecutionID: "exec-group-nil-result",
			NodeName:    "group",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing:   engine.TaskRouting{NodeType: engine.GroupNodeType},
		Namespace: "default",
	}
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-group-nil-result", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	before := server.HGet(directory.keys.runnerLeaseCount, session.RunnerID)
	if before != "1" {
		t.Fatalf("runnerLeaseCount before report = %q, want %q", before, "1")
	}

	fake := &fakeControlEngine{}
	core := &Core{
		engine:   fake,
		runners:  directory,
		pollWait: time.Second,
	}

	_, reportErr := core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		// GroupResult deliberately left nil: this is the exact gap being closed.
	}, TransportInfo{})

	if !errors.Is(reportErr, ErrGroupResultMissing) {
		t.Fatalf("reportResult() error = %v, want ErrGroupResultMissing", reportErr)
	}
	if fake.committedLease != nil {
		t.Fatal("engine was invoked, want the report rejected before reaching it")
	}

	after := server.HGet(directory.keys.runnerLeaseCount, session.RunnerID)
	if after != "0" {
		t.Fatalf("runnerLeaseCount after report = %q, want %q (capacity released immediately)", after, "0")
	}
}

// The tests below pin the transport-layer half of the group-lease rejection:
// core.reportResult hands back ErrGroupResultMissing with Accepted=false plus a
// reason in the response DTO, and every transport must carry that in-band —
// HTTP 409, gRPC Accepted=false, Connect Ack frames — exactly like
// engine.ErrInvalidLeaseToken. Without the mapping the sentinel falls into
// writeRunnerError / runnerStatus' default arm and the runner sees
// 500 / codes.Internal instead: a control-plane fault, not the fence rejection
// it actually is. Each test asserts the in-band shape, because only that
// distinguishes "mapped" from "still on the default arm".
//
// The Connect-stream mapping has a third shape to pin and it is asserted too:
// the rejection must NOT drop the stream. connect() returns nil at the clean
// EOF only if the result branch kept the stream alive after Acking.

// TestHTTPReportResultMapsGroupResultMissingToConflict drives the real HTTP
// handler stack (httptest + NewServer) for a group lease reported with no
// GroupResult and pins four facts at once: status 409, Accepted=false with the
// sentinel text in the body, the engine never invoked, and the runner's leased
// capacity released immediately — the same contract the HTTP transport already
// had for engine.ErrInvalidLeaseToken.
func TestHTTPReportResultMapsGroupResultMissingToConflict(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	lease, session := registerGroupRunnerForNilResultReport(t, ctx, directory,
		"runner-http-group-nil-result", "exec-http-group-nil-result", "lease-http-group-nil-result")

	fake := &fakeControlEngine{}
	srv := httptest.NewServer(NewServer(fake, directory).Handler())
	defer srv.Close()

	var resp protocol.ReportResultResponse
	postJSON(t, srv.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		// GroupResult deliberately nil: the shape under test.
	}, http.StatusConflict, &resp)

	if resp.Accepted {
		t.Fatal("group lease report with no GroupResult was accepted, want rejection")
	}
	if resp.Error != ErrGroupResultMissing.Error() {
		t.Fatalf("rejection reason = %q, want %q (the writeRunnerError default arm would answer the generic internal-server text and a 500 instead of this 409)",
			resp.Error, ErrGroupResultMissing.Error())
	}
	if fake.committedLease != nil {
		t.Fatal("engine was invoked, want the report rejected before reaching it")
	}
	if after := mr.HGet(directory.keys.runnerLeaseCount, session.RunnerID); after != "0" {
		t.Fatalf("runnerLeaseCount after report = %q, want %q (capacity must be released on the 409, same as a stale token)", after, "0")
	}
}

// TestGRPCReportResultCarriesGroupResultMissingInBand calls the gRPC unary
// handler directly. The decisive assertion is the nil error: an unmapped
// sentinel comes back as codes.Internal from runnerStatus, so a runner-side
// caller (protocol.GRPCClient.ReportResult) would see a transport failure
// instead of Accepted=false with the reason.
func TestGRPCReportResultCarriesGroupResultMissingInBand(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	lease, session := registerGroupRunnerForNilResultReport(t, ctx, directory,
		"runner-grpc-group-nil-result", "exec-grpc-group-nil-result", "lease-grpc-group-nil-result")

	fake := &fakeControlEngine{}
	g := NewGRPCServer(fake, directory)

	in, err := protocol.ReportResultRequestToProto(protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
	})
	if err != nil {
		t.Fatalf("ReportResultRequestToProto() error = %v", err)
	}
	resp, err := g.ReportResult(ctx, in)
	if err != nil {
		t.Fatalf("ReportResult() transport error = %v; want the rejection carried in-band (Accepted=false), not codes.Internal from runnerStatus' default arm", err)
	}
	if resp.Accepted {
		t.Fatal("group lease report with no GroupResult was accepted, want rejection")
	}
	if resp.Error != ErrGroupResultMissing.Error() {
		t.Fatalf("rejection reason = %q, want %q", resp.Error, ErrGroupResultMissing.Error())
	}
	if fake.committedLease != nil {
		t.Fatal("engine was invoked, want the report rejected before reaching it")
	}
}

// scriptedConnectStream replays a scripted Recv sequence into
// GRPCServer.connect and records what the server sent, so the Connect result
// branch is testable without a network stack. Each step runs just before its
// frame is returned, which is what lets the group lease be finalized between
// the HELLO handshake and the RESULT frame (the lease must bind to the session
// the stream's own register created). Steps run on the receive goroutine, so
// they report failures as returned errors, never t.Fatal.
type scriptedConnectStream struct {
	ctx   context.Context
	steps []func() (protocol.RunnerFrame, error)

	mu   sync.Mutex
	next int
	sent []protocol.ServerFrame
}

func (s *scriptedConnectStream) Context() context.Context { return s.ctx }

func (s *scriptedConnectStream) Recv() (protocol.RunnerFrame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= len(s.steps) {
		return protocol.RunnerFrame{}, io.EOF
	}
	step := s.steps[s.next]
	s.next++
	return step()
}

func (s *scriptedConnectStream) Send(frame protocol.ServerFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, frame)
	return nil
}

func (s *scriptedConnectStream) ackFrames() []*protocol.AckFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	var acks []*protocol.AckFrame
	for _, frame := range s.sent {
		if frame.Ack != nil {
			acks = append(acks, frame.Ack)
		}
	}
	return acks
}

// TestGRPCConnectAcksGroupResultMissingWithoutDroppingStream pins the Connect
// transport's share of the mapping: on this stream a stale-token-equivalent
// rejection rides an Ack frame with Accepted=false and the stream stays open
// (connect returns nil at the clean EOF). An unmapped sentinel takes the
// `return runnerStatus(err)` arm instead — codes.Internal and a torn-down
// stream.
func TestGRPCConnectAcksGroupResultMissingWithoutDroppingStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	fake := &fakeControlEngine{}
	g := NewGRPCServer(fake, directory)

	const (
		runnerID    = "runner-connect-group-nil-result"
		executionID = "exec-connect-group-nil-result"
		leaseID     = "lease-connect-group-nil-result"
	)
	assignment := Assignment{
		AssignmentID: AssignmentID(executionID + "/group/activation-1"),
		Task: engine.Task{
			ExecutionID: executionID,
			NodeName:    "group",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing:   engine.TaskRouting{NodeType: engine.GroupNodeType},
		Namespace: "default",
	}
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, leaseID, time.Minute)

	stream := &scriptedConnectStream{ctx: ctx, steps: []func() (protocol.RunnerFrame, error){
		func() (protocol.RunnerFrame, error) {
			return protocol.RunnerFrame{Hello: &protocol.HelloFrame{
				InstanceUID:  "test-instance",
				RunnerID:     runnerID,
				Concurrency:  1,
				Capabilities: []protocol.Capability{{NodeType: engine.GroupNodeType}},
			}}, nil
		},
		func() (protocol.RunnerFrame, error) {
			// The session the lease must bind to is the one the stream's own
			// HELLO just created.
			snapshot, found := directory.Runner(ctx, runnerID)
			if !found {
				return protocol.RunnerFrame{}, errors.New("HELLO handshake did not register the runner")
			}
			session := RunnerSession{RunnerID: snapshot.RunnerID, SessionID: snapshot.SessionID}
			enqueued, err := directory.EnqueueAssignment(ctx, assignment)
			if err != nil {
				return protocol.RunnerFrame{}, err
			}
			if !enqueued {
				return protocol.RunnerFrame{}, errors.New("EnqueueAssignment() enqueued=false")
			}
			// Claim with the real clock: the HELLO registered under the real
			// clock too, and the shared lease-meta helpers pin claim times to
			// the same fixed epochs the process-level test uses.
			claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
				RunnerID:  session.RunnerID,
				SessionID: session.SessionID,
				Now:       time.Now().UTC(),
			})
			if err != nil {
				return protocol.RunnerFrame{}, err
			}
			if !ok {
				return protocol.RunnerFrame{}, errors.New("ClaimForRunner() ok=false")
			}
			if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
				return protocol.RunnerFrame{}, err
			}
			return protocol.RunnerFrame{Result: &protocol.ResultFrame{
				LeaseID: string(lease.LeaseID),
				Lease:   lease,
				Result:  engine.TaskResult{},
			}}, nil
		},
	}}

	if err := g.connect(stream); err != nil {
		t.Fatalf("connect() = %v; want nil — the group rejection must ride the Ack frame and keep the stream open (a non-nil error here is the runnerStatus default arm)", err)
	}
	acks := stream.ackFrames()
	if len(acks) != 1 {
		t.Fatalf("Ack frames sent = %d, want exactly 1 for the rejected report", len(acks))
	}
	if acks[0].Accepted {
		t.Fatal("Ack.Accepted = true, want false")
	}
	if acks[0].Error != ErrGroupResultMissing.Error() {
		t.Fatalf("Ack.Error = %q, want %q", acks[0].Error, ErrGroupResultMissing.Error())
	}
	if fake.committedLease != nil {
		t.Fatal("engine was invoked, want the report rejected before reaching it")
	}
}

// registerGroupRunnerForNilResultReport registers a group-capable runner
// against directory and finalizes one group-exec lease for it — the fixture
// the handler-level tests report against with no GroupResult.
func registerGroupRunnerForNilResultReport(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, runnerID, executionID, leaseID string) (*engine.TaskLease, RunnerSession) {
	t.Helper()

	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: engine.GroupNodeType}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{engine.GroupNodeType}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	assignment := Assignment{
		AssignmentID: AssignmentID(executionID + "/group/activation-1"),
		Task: engine.Task{
			ExecutionID: types.ExecutionID(executionID),
			NodeName:    "group",
			Type:        engine.TaskTypeGroupExec,
		},
		Routing:   engine.TaskRouting{NodeType: engine.GroupNodeType},
		Namespace: "default",
	}
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, leaseID, time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)
	return lease, session
}
