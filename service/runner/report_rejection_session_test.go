package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// siblingRejectClient hands one session two leases, refuses the report for
// "reject" and accepts the one for "accept". The "accept" report waits for
// the refusal first, so it lands after the session has seen the rejection.
// Reports are refused for any session but the current one, as the server's
// ValidateSession does.
type siblingRejectClient struct {
	rejected chan struct{}

	mu            sync.Mutex
	registrations int
	handedOut     int
	pollsAfterRej int
	accepted      []string
}

func (c *siblingRejectClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registrations++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: "session-1"}, nil
}

func (*siblingRejectClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *siblingRejectClient) Poll(_ context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.rejected:
		c.pollsAfterRej++
	default:
	}
	switch c.handedOut {
	case 0:
		c.handedOut++
		return protocol.PollTaskResponse{Lease: gatedLease("reject")}, nil
	case 1:
		c.handedOut++
		return protocol.PollTaskResponse{Lease: gatedLease("accept")}, nil
	}
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *siblingRejectClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	if string(req.Lease.LeaseID) == "reject" {
		close(c.rejected)
		return protocol.ReportResultResponse{Accepted: false, Error: "lease reclaimed by the sweeper"}, nil
	}
	select {
	case <-c.rejected:
	case <-ctx.Done():
		return protocol.ReportResultResponse{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.SessionID != "session-1" || c.registrations != 1 {
		return protocol.ReportResultResponse{Accepted: false, Error: "unknown session"}, nil
	}
	c.accepted = append(c.accepted, string(req.Lease.LeaseID))
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *siblingRejectClient) snapshot() (polls int, accepted []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pollsAfterRej, append([]string(nil), c.accepted...)
}

var _ ProtocolClient = (*siblingRejectClient)(nil)

// A refused report is about one lease, not the session: the server refuses a
// lease its deadline backstop or sweeper already settled while the session is
// healthy. Ending the session on it would make the re-register invalidate
// every sibling lease still running, so their results would be refused too.
func TestRunKeepsTheSessionWhenOneReportIsRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &gatedHandler{started: make(chan struct{}, 2), release: make(chan struct{})}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.gated", handler)
	client := &siblingRejectClient{rejected: make(chan struct{})}
	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       2,
		Capabilities:      []protocol.Capability{{NodeType: "test.gated"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})
	r.shutdownTimeout = 20 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-handler.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 2 workers started", i)
		}
	}
	close(handler.release)

	deadline := time.After(5 * time.Second)
	for {
		polls, accepted := client.snapshot()
		if len(accepted) == 1 && polls >= 5 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("Run returned %v after one lease's report was refused: the "+
				"session ended, so a re-register would refuse every sibling lease", err)
		case <-deadline:
			t.Fatalf("timed out: sibling accepted = %v, polls after the refusal = %d", accepted, polls)
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the session was healthy", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "lease reclaimed by the sweeper") {
			t.Fatalf("Run() = %v, want the refusal reason surfaced on stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.registrations != 1 {
		t.Fatalf("registrations = %d, want 1", client.registrations)
	}
}
