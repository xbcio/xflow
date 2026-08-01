package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// End-to-end wiring proof: Runner.New must connect ActivationTracker's
// failure callback to activationAcker, using the session ID assigned by
// Register, and the same per-generation dedup as activationAcker's own unit
// tests. This exercises the real Run() heartbeat loop rather than calling
// ackFailed directly, so it also catches a broken SetOnActivateFailed wiring
// (e.g. wrong session ID, or wiring skipped when the client doesn't
// implement activationAckClient).
func TestRunnerAcksFailedActivationOncePerGeneration(t *testing.T) {
	directive := protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 7}
	handler := &mockActivationHandler{activateErr: errors.New("supply not ready: rules")}
	tracker := NewActivationTracker(handler, slog.New(slog.NewTextHandler(io.Discard, nil)))

	client := &ackCapturingClient{
		heartbeatResp: protocol.HeartbeatResponse{
			Activations: &protocol.HeartbeatActivations{Activate: []protocol.ActivateDirective{directive}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := New(client, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		HeartbeatInterval: 10 * time.Millisecond,
		PollWait:          time.Millisecond,
		ActivationTracker: tracker,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// The same directive is re-delivered (and re-fails, same generation) on
	// every heartbeat tick until the tracker sees a higher generation or a
	// deactivate. Waiting for a second heartbeat proves the retry actually
	// happened, so the assertion below on ack count is meaningful rather than
	// trivially true from a single attempt.
	waitForHeartbeats(t, client, 2)
	cancel()
	<-errCh

	// ackFailed dispatches at most one goroutine total for this (workflow,
	// entryUnit, generation) triple — every dedup decision happens
	// synchronously before any goroutine is spawned (see activationAcker).
	// So waiting for it to land is bounded and its result ("exactly 1") is
	// the true terminal state, not a snapshot that could still grow.
	waitForAckCount(t, client, 1)

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.acks) != 1 {
		t.Fatalf("acks = %d, want exactly 1 despite %d heartbeats re-delivering the same generation", len(client.acks), client.heartbeats)
	}
	ack := client.acks[0]
	if ack.Generation != 7 {
		t.Fatalf("ack.Generation = %d, want 7", ack.Generation)
	}
	if ack.Status != protocol.ActivationStatusFailed {
		t.Fatalf("ack.Status = %q, want %q", ack.Status, protocol.ActivationStatusFailed)
	}
	if ack.WorkflowID != "wf-1" || ack.GroupID != "grp-a" {
		t.Fatalf("ack identity = %+v, want workflow_id=wf-1 group_id=grp-a", ack)
	}
	if ack.RunnerID != "runner-1" {
		t.Fatalf("ack.RunnerID = %q, want runner-1", ack.RunnerID)
	}
	if ack.SessionID != "session-1" {
		t.Fatalf("ack.SessionID = %q, want session-1 (from Register response)", ack.SessionID)
	}
	if ack.Error == "" {
		t.Fatal("ack.Error must carry the activation failure reason")
	}
}

// ackCapturingClient is a ProtocolClient that also implements
// activationAckClient, so Runner.New wires an activationAcker over it. It
// replays the same heartbeatResp on every Heartbeat call (simulating a
// server that keeps redispatching an activation the runner cannot take) and
// never hands out a lease, keeping the worker pool idle.
type ackCapturingClient struct {
	heartbeatResp protocol.HeartbeatResponse

	mu         sync.Mutex
	heartbeats int
	acks       []protocol.ActivationAck
}

func (c *ackCapturingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{SessionID: "session-1"}, nil
}

func (c *ackCapturingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	c.heartbeats++
	c.mu.Unlock()
	return c.heartbeatResp, nil
}

func (c *ackCapturingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *ackCapturingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *ackCapturingClient) ActivationAck(_ context.Context, ack protocol.ActivationAck) error {
	c.mu.Lock()
	c.acks = append(c.acks, ack)
	c.mu.Unlock()
	return nil
}

func waitForHeartbeats(t *testing.T, c *ackCapturingClient, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := c.heartbeats
		c.mu.Unlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d heartbeats, got %d", want, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func waitForAckCount(t *testing.T, c *ackCapturingClient, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.acks)
		c.mu.Unlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d ack(s), got %d", want, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
