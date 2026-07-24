package runner

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestRunnerReturnsNilWhenCanceledDuringPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pollStarted := make(chan struct{})
	r := New(&blockingPollClient{pollStarted: pollStarted}, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		HeartbeatInterval: time.Hour,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	select {
	case <-pollStarted:
	case <-time.After(time.Second):
		t.Fatal("runner did not begin polling")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil after cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop after cancellation")
	}
}

func TestRunnerRegistersPollsExecutesAndReportsResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease := &engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		Input:    &types.Input{Data: map[string]any{"claim_id": "c-1"}},
		NodeType: "test.function",
	}
	client := &fakeProtocolClient{lease: lease, cancel: cancel}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.function", functionHandler{})

	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "test.function"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})

	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !client.registered {
		t.Fatal("runner did not register")
	}
	if client.polls == 0 {
		t.Fatal("runner did not poll")
	}
	if client.reported.Lease == nil || client.reported.Lease.LeaseID != lease.LeaseID {
		t.Fatalf("reported lease = %+v, want %+v", client.reported.Lease, lease)
	}
	if client.reported.Result.Output == nil || client.reported.Result.Output.Data["claim_id"] != "c-1" {
		t.Fatalf("reported result = %+v, want claim_id output", client.reported.Result)
	}
}

type blockingPollClient struct {
	pollStarted chan<- struct{}
}

func (c *blockingPollClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{SessionID: "session-1"}, nil
}

func (*blockingPollClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *blockingPollClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	close(c.pollStarted)
	<-ctx.Done()
	return protocol.PollTaskResponse{}, ctx.Err()
}

func (*blockingPollClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, nil
}

type fakeProtocolClient struct {
	lease      *engine.TaskLease
	cancel     context.CancelFunc
	registered bool
	polls      int
	reported   protocol.ReportResultRequest
}

func (c *fakeProtocolClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.registered = true
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1"}, nil
}

func (c *fakeProtocolClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *fakeProtocolClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.polls++
	if c.lease == nil {
		return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
	}
	lease := c.lease
	c.lease = nil
	return protocol.PollTaskResponse{Lease: lease}, nil
}

func (c *fakeProtocolClient) ReportResult(_ context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.reported = req
	c.cancel()
	return protocol.ReportResultResponse{Accepted: true}, nil
}

type functionHandler struct{}

func (functionHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.function"}
}

func (functionHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}
