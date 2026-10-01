package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// gatedHandler blocks every execution until release is closed, announces each
// start on started, and tracks how many executions overlap.
type gatedHandler struct {
	started chan struct{}
	release chan struct{}

	running    atomic.Int32
	maxRunning atomic.Int32
}

func (*gatedHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.gated"}
}

func (h *gatedHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	n := h.running.Add(1)
	defer h.running.Add(-1)
	for {
		m := h.maxRunning.Load()
		if n <= m || h.maxRunning.CompareAndSwap(m, n) {
			break
		}
	}
	h.started <- struct{}{}
	// Deliberately ignores ctx: a busy worker outlives the session that
	// claimed it, which is the state a reconnect has to account for.
	<-h.release
	return &types.Output{Data: input.Data}, nil
}

// inFlightReconnectClient fails session-1's second poll once its first lease
// is executing, then hands session 2 two fresh leases, one per poll. It
// records session 2's polls and heartbeats.
type inFlightReconnectClient struct {
	handlerActive <-chan struct{}

	mu         sync.Mutex
	session    int
	s1Polls    int
	s2Polls    int
	s2Leases   int
	heartbeats []int
	reports    int
}

func (c *inFlightReconnectClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: fmt.Sprintf("session-%d", c.session)}, nil
}

func (c *inFlightReconnectClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	if req.SessionID == "session-2" {
		c.mu.Lock()
		c.heartbeats = append(c.heartbeats, req.InFlight)
		c.mu.Unlock()
	}
	return protocol.HeartbeatResponse{}, nil
}

func gatedLease(id string) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID:  engine.LeaseID(id),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-" + id), NodeName: "gated"},
		Input:    &types.Input{Data: map[string]any{"id": id}},
		NodeType: "test.gated",
	}
}

func (c *inFlightReconnectClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	if req.SessionID == "session-1" {
		c.mu.Lock()
		c.s1Polls++
		first := c.s1Polls == 1
		c.mu.Unlock()
		if first {
			return protocol.PollTaskResponse{Lease: gatedLease("s1-a")}, nil
		}
		select {
		case <-c.handlerActive:
		case <-ctx.Done():
			return protocol.PollTaskResponse{}, ctx.Err()
		}
		return protocol.PollTaskResponse{}, errReconnectTransport
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s2Polls++
	if c.s2Leases < 2 {
		c.s2Leases++
		return protocol.PollTaskResponse{Lease: gatedLease(fmt.Sprintf("s2-%d", c.s2Leases))}, nil
	}
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *inFlightReconnectClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports++
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *inFlightReconnectClient) snapshot() (s2Polls int, heartbeats []int, reports int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s2Polls, append([]int(nil), c.heartbeats...), c.reports
}

var _ ProtocolClient = (*inFlightReconnectClient)(nil)

// A transport-error reconnect re-enters Run while the previous session's
// worker is still executing. The new session must count that worker: it may
// fill only the remaining slot, must stop polling once Concurrency handlers
// are busy, and must heartbeat the true in-flight count.
func TestRunnerCountsPreviousSessionInFlightAfterReconnect(t *testing.T) {
	const concurrency = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &gatedHandler{started: make(chan struct{}, 8), release: make(chan struct{})}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.gated", handler)
	firstStarted := make(chan struct{})
	client := &inFlightReconnectClient{handlerActive: firstStarted}
	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       concurrency,
		Capabilities:      []protocol.Capability{{NodeType: "test.gated"}},
		HeartbeatInterval: time.Millisecond,
		PollWait:          time.Millisecond,
	})
	r.shutdownTimeout = 20 * time.Millisecond

	go func() {
		<-handler.started
		close(firstStarted)
	}()
	if err := r.Run(ctx); !errors.Is(err, errReconnectTransport) {
		t.Fatalf("first session Run = %v, want the transport fault", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// The second session's first lease fills the last slot.
	select {
	case <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("second session never started a lease")
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for !cond() {
			select {
			case <-deadline:
				t.Fatalf("timed out waiting for %s", what)
			case <-time.After(time.Millisecond):
			}
		}
	}
	// With every slot busy, give the poll loop many PollWait rounds (paced by
	// heartbeats) in which it must not poll.
	_, base, _ := client.snapshot()
	waitFor("ten more heartbeats", func() bool {
		_, hb, _ := client.snapshot()
		return len(hb) >= len(base)+10
	})
	polls, heartbeats, _ := client.snapshot()
	if polls != 1 {
		t.Fatalf("second session polled %d times while %d workers were busy, want 1 (the poll that filled the last slot)",
			polls, concurrency)
	}
	for i, n := range heartbeats[len(base):] {
		if n != concurrency {
			t.Fatalf("second-session heartbeat %d InFlight = %d while %d handlers run, want %d",
				len(base)+i, n, handler.running.Load(), concurrency)
		}
	}
	for i, n := range heartbeats {
		if n < 1 || n > concurrency {
			t.Fatalf("second-session heartbeat %d InFlight = %d, want 1..%d: the first session's worker is still busy",
				i, n, concurrency)
		}
	}
	if got := handler.running.Load(); got != concurrency {
		t.Fatalf("handlers running = %d, want %d", got, concurrency)
	}

	// Once the workers finish, the freed capacity is claimed again.
	close(handler.release)
	waitFor("the second session to poll again", func() bool {
		p, _, reports := client.snapshot()
		return p > 1 && reports >= 3
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second session did not stop")
	}
	if got := handler.maxRunning.Load(); got > concurrency {
		t.Fatalf("max concurrent handlers = %d, want at most %d", got, concurrency)
	}
}
