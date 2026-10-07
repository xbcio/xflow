package runner

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// heartbeatCapturingClient records every heartbeat request so tests can assert
// on the exact body the runner would send.
type heartbeatCapturingClient struct {
	mu       sync.Mutex
	requests []protocol.HeartbeatRequest
}

func (*heartbeatCapturingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{}, nil
}

func (c *heartbeatCapturingClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	return protocol.HeartbeatResponse{}, nil
}

func (*heartbeatCapturingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (*heartbeatCapturingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *heartbeatCapturingClient) last() protocol.HeartbeatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		return protocol.HeartbeatRequest{}
	}
	return c.requests[len(c.requests)-1]
}

func testRunnerLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

var _ ProtocolClient = (*heartbeatCapturingClient)(nil)

// TestRunnerHeartbeatReportsHostedActivations pins the runner half of the
// lost-directive detection: every heartbeat carries the tracker's current
// inventory, and an empty-but-present report means "reporting, hosting
// nothing".
func TestRunnerHeartbeatReportsHostedActivations(t *testing.T) {
	ctx := context.Background()
	client := &heartbeatCapturingClient{}
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, testRunnerLogger())
	r := New(client, execution.NewRegistry(), Config{RunnerID: "runner-1", ActivationTracker: tracker})

	// Before anything is activated: present and explicitly empty.
	if _, err := r.heartbeat(ctx, "sess-1", 0); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	first := client.last()
	if first.HostedActivations == nil {
		t.Fatal("heartbeat without activations must still carry a present, empty report")
	}
	if len(first.HostedActivations.Activations) != 0 {
		t.Fatalf("hosted activations = %+v, want none before any activation", first.HostedActivations.Activations)
	}

	// Host one activation; the next heartbeat reports it with its generation.
	if err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{Activate: []protocol.ActivateDirective{{
		Namespace: "default", WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "tg", Generation: 4,
	}}}); err != nil {
		t.Fatalf("ProcessDirectives: %v", err)
	}
	if _, err := r.heartbeat(ctx, "sess-1", 0); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	second := client.last()
	if second.HostedActivations == nil || len(second.HostedActivations.Activations) != 1 {
		t.Fatalf("hosted activations = %+v, want the one hosted activation", second.HostedActivations)
	}
	item := second.HostedActivations.Activations[0]
	if item.WorkflowID != "wf-1" || item.EntryUnitID != "tg" || item.WorkflowVersion != "v1" || item.Generation != 4 {
		t.Fatalf("hosted activation = %+v, want wf-1/tg@4", item)
	}

	// Deactivating it returns the report to present-and-empty, not to nil.
	if err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{Deactivate: []protocol.DeactivateDirective{{
		Namespace: "default", WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "tg", Generation: 4,
	}}}); err != nil {
		t.Fatalf("ProcessDirectives (deactivate): %v", err)
	}
	if _, err := r.heartbeat(ctx, "sess-1", 0); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	third := client.last()
	if third.HostedActivations == nil {
		t.Fatal("heartbeat after deactivation must keep reporting (present, empty)")
	}
	if len(third.HostedActivations.Activations) != 0 {
		t.Fatalf("hosted activations = %+v, want none after deactivation", third.HostedActivations.Activations)
	}
}

// TestRunnerHeartbeatOmitsHostedActivationsWithoutTracker pins the other half
// of the presence contract: with no tracker the runner cannot report at all,
// and the field stays nil rather than claiming "hosts nothing".
func TestRunnerHeartbeatOmitsHostedActivationsWithoutTracker(t *testing.T) {
	client := &heartbeatCapturingClient{}
	r := New(client, execution.NewRegistry(), Config{RunnerID: "runner-1"})

	if _, err := r.heartbeat(context.Background(), "sess-1", 0); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if got := client.last(); got.HostedActivations != nil {
		t.Fatalf("hosted activations = %+v, want nil without a tracker", got.HostedActivations)
	}
}
