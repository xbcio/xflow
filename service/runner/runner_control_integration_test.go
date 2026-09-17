package runner

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

type drainingProtocolClient struct {
	mu         sync.Mutex
	polls      []protocol.PollTaskRequest
	heartbeats []protocol.HeartbeatRequest
	cancel     context.CancelFunc
}

func (c *drainingProtocolClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{
		RunnerID: "runner-a", SessionID: "session-a",
		Control: &protocol.RunnerControlDirective{DesiredState: "draining", Generation: 1, RecoveryOnly: true},
	}, nil
}

func (c *drainingProtocolClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.heartbeats = append(c.heartbeats, req)
	c.mu.Unlock()
	return protocol.HeartbeatResponse{}, nil
}

func (c *drainingProtocolClient) Poll(_ context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	c.polls = append(c.polls, req)
	c.mu.Unlock()
	c.cancel()
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (*drainingProtocolClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func TestRunnerDrainingDirectiveUsesRecoveryOnlyPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &drainingProtocolClient{cancel: cancel}
	runner := New(client, execution.NewRegistry(), Config{
		RunnerID: "runner-a", Concurrency: 1, HeartbeatInterval: time.Hour, PollWait: time.Millisecond,
	})
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.polls) != 1 || !client.polls[0].RecoveryOnly {
		t.Fatalf("polls = %#v, want one recovery-only poll", client.polls)
	}
}

func TestRunnerHeartbeatReportsLocalDrainObservation(t *testing.T) {
	client := &drainingProtocolClient{}
	tracker := NewActivationTracker(&mockActivationHandler{}, slog.Default())
	if err := tracker.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{Activate: []protocol.ActivateDirective{
		{WorkflowID: "workflow-a", WorkflowVersion: "v1", EntryUnitID: "entry-a", Generation: 1},
		{WorkflowID: "workflow-b", WorkflowVersion: "v1", EntryUnitID: "entry-b", Generation: 1},
	}}); err != nil {
		t.Fatalf("activate tracker subscriptions: %v", err)
	}
	runner := New(client, execution.NewRegistry(), Config{
		RunnerID: "runner-a", Concurrency: 2, ActivationTracker: tracker,
	})

	if _, err := runner.heartbeat(context.Background(), "session-a", 1); err != nil {
		t.Fatalf("active heartbeat: %v", err)
	}
	runner.applyRunnerControl(&protocol.RunnerControlDirective{DesiredState: "draining", Generation: 7, RecoveryOnly: true})
	if _, err := runner.heartbeat(context.Background(), "session-a", 0); err != nil {
		t.Fatalf("draining heartbeat: %v", err)
	}

	client.mu.Lock()
	heartbeats := append([]protocol.HeartbeatRequest(nil), client.heartbeats...)
	client.mu.Unlock()
	if len(heartbeats) != 2 {
		t.Fatalf("heartbeats = %#v, want two", heartbeats)
	}
	if heartbeats[0].DrainObservation != nil {
		t.Fatalf("active heartbeat drain observation = %#v, want nil", heartbeats[0].DrainObservation)
	}
	want := protocol.RunnerDrainObservation{Generation: 7, RecoveryOnly: true, ActiveActivations: 2}
	if heartbeats[1].DrainObservation == nil || *heartbeats[1].DrainObservation != want {
		t.Fatalf("draining heartbeat drain observation = %#v, want %#v", heartbeats[1].DrainObservation, want)
	}
}
