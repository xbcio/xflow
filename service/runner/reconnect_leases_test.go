package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

var errReconnectTransport = errors.New("transport fault")

// replayingReconnectClient models the Redis runner directory across one
// transport-error reconnect: Register rebinds the runner's leases to the new
// session, and every poll of that session replays a lease that is still
// unreported and absent from ActiveLeaseIDs. A report from a superseded
// session is refused, as ValidateSession refuses it on the server.
type replayingReconnectClient struct {
	lease         *engine.TaskLease
	handlerActive <-chan struct{}

	mu              sync.Mutex
	session         int
	delivered       bool
	reported        bool
	session2Polls   [][]string
	staleRejections int
	cancel          context.CancelFunc
}

func (c *replayingReconnectClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: fmt.Sprintf("session-%d", c.session)}, nil
}

func (c *replayingReconnectClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *replayingReconnectClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	if req.SessionID == "session-1" {
		c.mu.Lock()
		first := !c.delivered
		c.delivered = true
		c.mu.Unlock()
		if first {
			return protocol.PollTaskResponse{Lease: c.lease}, nil
		}
		// Fail the session only once the handler is mid-flight, which is the
		// window the reconnect hazard lives in.
		select {
		case <-c.handlerActive:
		case <-ctx.Done():
			return protocol.PollTaskResponse{}, ctx.Err()
		}
		return protocol.PollTaskResponse{}, errReconnectTransport
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.session2Polls = append(c.session2Polls, slices.Clone(req.ActiveLeaseIDs))
	if !c.reported && !slices.Contains(req.ActiveLeaseIDs, string(c.lease.LeaseID)) {
		return protocol.PollTaskResponse{Lease: c.lease}, nil
	}
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *replayingReconnectClient) ReportResult(_ context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.SessionID != fmt.Sprintf("session-%d", c.session) {
		c.staleRejections++
		return protocol.ReportResultResponse{Accepted: false, Error: "stale session"}, nil
	}
	c.reported = true
	c.cancel()
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *replayingReconnectClient) snapshot() (polls [][]string, staleRejections int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.session2Polls), c.staleRejections
}

var _ ProtocolClient = (*replayingReconnectClient)(nil)

// countingBlockingHandler blocks its first execution until released and tracks
// how many executions overlap.
type countingBlockingHandler struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once

	calls      atomic.Int32
	running    atomic.Int32
	maxRunning atomic.Int32
}

func (*countingBlockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.slow"}
}

func (h *countingBlockingHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.calls.Add(1)
	n := h.running.Add(1)
	defer h.running.Add(-1)
	for {
		m := h.maxRunning.Load()
		if n <= m || h.maxRunning.CompareAndSwap(m, n) {
			break
		}
	}
	h.once.Do(func() { close(h.started) })
	select {
	case <-h.release:
	case <-ctx.Done():
	}
	return &types.Output{Data: input.Data}, nil
}

// A transport-error reconnect re-enters Run in the same process while a
// worker of the previous session is still inside its handler. The server has
// rebound that lease to the new session and replays it on any poll that does
// not name it, so the new session's polls must report it or the node runs
// twice concurrently. Once the old worker finishes, its report is refused as
// stale and the replay that follows is a sequential redelivery.
func TestRunnerReportsPreviousSessionLeasesAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &countingBlockingHandler{started: make(chan struct{}), release: make(chan struct{})}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.slow", handler)

	client := &replayingReconnectClient{
		handlerActive: handler.started,
		cancel:        cancel,
		lease: &engine.TaskLease{
			LeaseID:    engine.LeaseID("lease-s1"),
			LeaseToken: engine.LeaseToken("token-s1"),
			Task:       engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "slow"},
			Input:      &types.Input{Data: map[string]any{"x": 1}},
			NodeType:   "test.slow",
		},
	}
	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       2,
		Capabilities:      []protocol.Capability{{NodeType: "test.slow"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})
	r.shutdownTimeout = 50 * time.Millisecond

	if err := r.Run(ctx); !errors.Is(err, errReconnectTransport) {
		t.Fatalf("first session Run = %v, want the transport fault", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		polls, _ := client.snapshot()
		if len(polls) >= 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second session never polled")
		case <-time.After(2 * time.Millisecond):
		}
	}
	polls, _ := client.snapshot()
	for i, ids := range polls {
		if !slices.Contains(ids, "lease-s1") {
			t.Fatalf("second-session poll %d reported ActiveLeaseIDs=%v while the first session's worker "+
				"still runs lease-s1; the server replays it and the node executes twice", i, ids)
		}
	}
	if got := handler.calls.Load(); got != 1 {
		t.Fatalf("handler executed %d times while the first copy was running, want 1", got)
	}

	close(handler.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second session did not finish after the redelivered lease was reported")
	}
	if _, stale := client.snapshot(); stale != 1 {
		t.Fatalf("stale-session report rejections = %d, want 1 (the first worker's late report)", stale)
	}
	if got := handler.calls.Load(); got != 2 {
		t.Fatalf("handler executions = %d, want 2 (original plus one sequential redelivery)", got)
	}
	if got := handler.maxRunning.Load(); got != 1 {
		t.Fatalf("max concurrent executions = %d, want 1", got)
	}
}

// failingPollClient registers and heartbeats fine, then fails every poll: a
// session that dies of a transport fault with every worker idle.
type failingPollClient struct{ registers atomic.Int32 }

func (c *failingPollClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	n := c.registers.Add(1)
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: fmt.Sprintf("session-%d", n)}, nil
}

func (c *failingPollClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *failingPollClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{}, errReconnectTransport
}

func (c *failingPollClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

// Each transport-error reconnect starts a fresh worker pool. The previous
// pool's idle workers must exit with their run rather than stay parked on its
// lease channel for the life of the parent context, and Run must not sit out
// the shutdown timeout waiting for workers that have nothing to do.
func TestRunnerDoesNotLeakIdleWorkersAcrossReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const concurrency = 4
	const reconnects = 10
	r := New(&failingPollClient{}, execution.NewRegistry(), Config{
		RunnerID:          "runner-1",
		Concurrency:       concurrency,
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})
	r.shutdownTimeout = time.Second

	settle := func(limit int) int {
		deadline := time.Now().Add(2 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= limit || time.Now().After(deadline) {
				return n
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	base := runtime.NumGoroutine()

	start := time.Now()
	for i := 0; i < reconnects; i++ {
		if err := r.Run(ctx); !errors.Is(err, errReconnectTransport) {
			t.Fatalf("Run %d = %v, want the transport fault", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed >= r.shutdownTimeout {
		t.Errorf("%d idle reconnects took %v; Run waited out the shutdown timeout for idle workers", reconnects, elapsed)
	}
	// A small allowance covers goroutines the runtime or test harness starts
	// on its own; a leak here is concurrency per reconnect.
	if n := settle(base + 2); n > base+2 {
		t.Fatalf("goroutines grew from %d to %d across %d reconnects (concurrency %d): idle workers leaked",
			base, n, reconnects, concurrency)
	}
}
