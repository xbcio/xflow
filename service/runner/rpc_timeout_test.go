package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// callSafetyWindow bounds how long a fake call may block before the test
// declares that no deadline was applied. It is far above any timeout the tests
// configure and far below go test's own timeout, so a regression fails with a
// clear message instead of hanging the package.
const callSafetyWindow = 5 * time.Second

// deadConnProtocolClient models a half-open connection: every RPC blocks until
// its context is done, which is exactly the state a caller cannot escape
// without a deadline.
type deadConnProtocolClient struct{}

func (c *deadConnProtocolClient) hang(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(callSafetyWindow):
		return errors.New(name + " returned only after the test safety window: no deadline was applied")
	}
}

func (c *deadConnProtocolClient) Register(ctx context.Context, _ protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{}, c.hang(ctx, "Register")
}

func (c *deadConnProtocolClient) Heartbeat(ctx context.Context, _ protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, c.hang(ctx, "Heartbeat")
}

func (c *deadConnProtocolClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{}, c.hang(ctx, "Poll")
}

func (c *deadConnProtocolClient) ReportResult(ctx context.Context, _ protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, c.hang(ctx, "ReportResult")
}

// requireWithin runs fn in its own goroutine and fails the test if it has not
// returned within limit. The goroutine is necessary: without it a regression
// that removed a deadline would block the test body itself.
func requireWithin(t *testing.T, limit time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("call did not return within %v; its RPC has no deadline", limit)
		return nil
	}
}

// A hung Register must hand control back to the reconnect loop at its
// deadline; otherwise the runner never registers at all.
func TestRunRegisterIsBoundedByItsDeadline(t *testing.T) {
	client := &deadConnProtocolClient{}
	r := New(client, execution.NewRegistry(), Config{RunnerID: "runner-1"})
	r.registerTimeout = 25 * time.Millisecond

	err := requireWithin(t, time.Second, func() error {
		return r.Run(context.Background())
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned %v, want context.DeadlineExceeded from the hung Register", err)
	}
}

// A hung long-poll must fail the claim loop instead of parking it forever.
func TestPollLoopIsBoundedByItsDeadline(t *testing.T) {
	client := &deadConnProtocolClient{}
	r := New(client, execution.NewRegistry(), Config{RunnerID: "runner-1"})
	r.pollTimeout = 25 * time.Millisecond

	err := requireWithin(t, time.Second, func() error {
		return r.pollLoop(context.Background(), "session-1",
			make(chan *engine.TaskLease), new(atomic.Int32), newActiveLeases())
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pollLoop returned %v, want context.DeadlineExceeded from the hung Poll", err)
	}
}

// A hung heartbeat must be observed as a failure so heartbeatGrace can end the
// session; if it blocked forever, the grace check would never run.
func TestHeartbeatIsBoundedByItsDeadline(t *testing.T) {
	client := &deadConnProtocolClient{}
	r := New(client, execution.NewRegistry(), Config{RunnerID: "runner-1"})
	r.heartbeatTimeout = 25 * time.Millisecond

	var err error
	_ = requireWithin(t, time.Second, func() error {
		_, err = r.heartbeat(context.Background(), "session-1", 0)
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("heartbeat returned %v, want context.DeadlineExceeded from the hung Heartbeat", err)
	}
}

// deadConnRenewClient is the lease-renewal half of the same half-open model.
type deadConnRenewClient struct{}

func (deadConnRenewClient) RenewLease(ctx context.Context, _ protocol.RenewLeaseRequest) (protocol.RenewLeaseResponse, error) {
	select {
	case <-ctx.Done():
		return protocol.RenewLeaseResponse{}, ctx.Err()
	case <-time.After(callSafetyWindow):
		return protocol.RenewLeaseResponse{}, errors.New(
			"RenewLease returned only after the test safety window: no deadline was applied")
	}
}

// A hung renewal must surface as a transport failure the loop can count; a call
// that never returns would stop the MaxRetries path from ever cancelling the
// handler.
func TestProtocolLeaseRenewerIsBoundedByItsDeadline(t *testing.T) {
	p := protocolLeaseRenewer{
		client:    deadConnRenewClient{},
		runnerID:  "runner-1",
		sessionID: "session-1",
		timeout:   25 * time.Millisecond,
	}

	start := time.Now()
	_, err := p.RenewLease(context.Background(), "lease-1", "token-1", time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("RenewLease returned after %v, want near its 25ms deadline", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RenewLease returned %v, want context.DeadlineExceeded from the hung call", err)
	}
}
