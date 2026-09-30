package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// deregisterRecordingClient is a minimal HTTP-shaped protocol client that
// records deregister calls. pollErr makes Poll fail as a transport error would.
type deregisterRecordingClient struct {
	mu           sync.Mutex
	cancel       context.CancelFunc
	pollErr      error
	polls        int
	deregistered []protocol.DeregisterRequest
}

func (c *deregisterRecordingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-d", SessionID: "session-1"}, nil
}

func (c *deregisterRecordingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *deregisterRecordingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.polls++
	if c.pollErr != nil {
		return protocol.PollTaskResponse{}, c.pollErr
	}
	if c.polls == 2 {
		c.cancel()
	}
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *deregisterRecordingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *deregisterRecordingClient) Deregister(_ context.Context, req protocol.DeregisterRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deregistered = append(c.deregistered, req)
	return nil
}

func newDeregisterTestRunner(client *deregisterRecordingClient) *Runner {
	return New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-d",
		Concurrency:       1,
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})
}

func TestRunnerDeregistersOnCleanStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &deregisterRecordingClient{cancel: cancel}
	_ = newDeregisterTestRunner(client).Run(ctx)

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.deregistered) != 1 {
		t.Fatalf("deregister calls = %d, want 1", len(client.deregistered))
	}
	if got := client.deregistered[0]; got.RunnerID != "runner-d" || got.SessionID != "session-1" {
		t.Fatalf("deregister request = %+v, want runner-d/session-1", got)
	}
}

// A transport failure is followed by a reconnect of this same instance, so
// the session must stay protected: no deregister.
func TestRunnerDoesNotDeregisterOnTransportFailure(t *testing.T) {
	client := &deregisterRecordingClient{cancel: func() {}, pollErr: errors.New("connection reset")}
	if err := newDeregisterTestRunner(client).Run(context.Background()); err == nil {
		t.Fatal("Run() = nil, want the poll error")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.deregistered) != 0 {
		t.Fatalf("deregister calls = %d, want 0 after a transport failure", len(client.deregistered))
	}
}
