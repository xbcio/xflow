package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestRunStopsWhenTheServerRejectsAResult pins runner.go:487-489:
//
//	if !reportResp.Accepted {
//		signalError(fmt.Errorf("task result rejected: %s", reportResp.Error))
//	}
//
// Every fake client in this package returns Accepted: true — eight of them, at
// runner_test.go:133 and :216, runner_tracing_test.go:52 and :271,
// active_leases_test.go:46, runner_activation_ack_wiring_test.go:125,
// supply_key_report_test.go:132 and register_supply_key_wiring_test.go:41.
// Grepping for Accepted in this package's tests returns those eight lines and
// nothing else, so no test has ever driven the false arm and the whole block is
// deletable with the package green.
//
// What Accepted: false means. The server produces it in exactly one situation
// (service/control/core.go:632, :651, :660): engine.ErrInvalidLeaseToken. The
// lease this runner is holding is no longer this runner's — it expired and was
// reclaimed, or the runner was fenced. Every result reported under it is
// refused. Losing the check means the runner treats that refusal as success:
// it discards the result, keeps polling on a session the server has moved past,
// and each subsequent report is refused the same way. Nothing errors, the
// heartbeats stay healthy, and the runner burns CPU executing tasks whose
// results are thrown away — indefinitely, because the only thing that would
// have torn the session down and forced a re-register is this branch.
//
// Range, stated honestly: this branch is reachable on the gRPC transport only.
// grpc_server.go:139-142 deliberately carries the rejection in-band —
// `return &ReportResultResponse{Accepted: false, ...}, nil` — with the comment
// "mirroring the HTTP 409 contract". It is not in fact mirrored at the client:
// on plain HTTP the same rejection is a 409, and protocol/client.go:152-155
// turns every non-2xx into an error, so the HTTP runner exits one branch
// earlier at runner.go:483. The HTTP streaming server (protocol/http_stream.go:
// 89, :92) does emit an AckFrame with Accepted: false, but no client in this
// repo maps an AckFrame back into a ReportResultResponse, so that path does not
// reach here today either. gRPC is not this project's target transport, which
// is why this is worth a unit test and not worth more than one.
//
// The complementary direction — a runner that treats an accepted result as a
// failure — is already pinned by TestRunnerSendsLabelsOnRegisterAndPoll
// (runner_test.go:169), which asserts Run returns nil against an
// Accepted: true client. This test does not duplicate it.
func TestRunStopsWhenTheServerRejectsAResult(t *testing.T) {
	// A distinctive reason, so the assertion below distinguishes "the runner
	// noticed the rejection" from "the runner produced some error of its own".
	const serverReason = "lease token fenced by runner-2"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease := &engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		Input:    &types.Input{Data: map[string]any{"x": 1}},
		NodeType: "test.function",
	}
	client := &rejectingClient{lease: lease, cancel: cancel, reason: serverReason}

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.function", functionHandler{})

	r := New(client, registry, Config{
		RunnerID:          "runner-rejected",
		Concurrency:       1,
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})

	err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run() returned nil after the server refused the result: the " +
			"runner has swallowed a fenced-lease rejection, so it keeps executing " +
			"tasks under a session the server has already replaced and every " +
			"result it produces from here on is discarded, with no error and no " +
			"reconnect")
	}
	if !strings.Contains(err.Error(), serverReason) {
		t.Fatalf("Run() error = %v, want it to carry the server's reason %q: the "+
			"operator has to be able to tell a fenced lease from a transport "+
			"failure, and the reason string is the only thing that distinguishes "+
			"them", err, serverReason)
	}
	// Setup guard: an assertion about the report is meaningless if no task was
	// ever executed and reported.
	if !client.reportedOnce {
		t.Fatal("the client was never asked to report a result, so the rejection " +
			"branch was not the thing under test")
	}
}

// rejectingClient hands out one lease and then refuses the result the way the
// gRPC server does: Accepted false, a reason, and no transport error.
type rejectingClient struct {
	lease  *engine.TaskLease
	cancel context.CancelFunc
	reason string

	reportedOnce bool
}

func (c *rejectingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-rejected", SessionID: "sess-1"}, nil
}

func (*rejectingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *rejectingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	if c.lease == nil {
		return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
	}
	lease := c.lease
	c.lease = nil
	return protocol.PollTaskResponse{Lease: lease}, nil
}

func (c *rejectingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.reportedOnce = true
	c.cancel()
	// nil error on purpose: this is the shape grpc_server.go:142 produces, and
	// it is the only shape that reaches runner.go:487 rather than :483.
	return protocol.ReportResultResponse{Accepted: false, Error: c.reason}, nil
}
