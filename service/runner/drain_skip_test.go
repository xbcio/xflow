package runner

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// drainClient hands session 1 one lease per poll until leases are out. Once failHeartbeats is closed it refuses session 1's heartbeats as an
// unknown session. It records what session 2 reports on its polls and
// heartbeats, refuses reports under a replaced session as the server does,
// and records deregister calls.
type drainClient struct {
	concurrency    int
	leases         int
	failHeartbeats <-chan struct{}

	mu           sync.Mutex
	session      int
	handedOut    int
	s2Active     [][]string
	s2InFlight   []int
	reports      int
	deregistered int
}

func (c *drainClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: fmt.Sprintf("session-%d", c.session)}, nil
}

func (c *drainClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	if req.SessionID == "session-1" {
		select {
		case <-c.failHeartbeats:
			return protocol.HeartbeatResponse{}, errHeartbeatSessionGone
		default:
		}
		return protocol.HeartbeatResponse{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s2InFlight = append(c.s2InFlight, req.InFlight)
	return protocol.HeartbeatResponse{}, nil
}

func (c *drainClient) Poll(_ context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.SessionID == "session-1" {
		if c.handedOut < c.leases {
			id := fmt.Sprintf("s1-%d", c.handedOut)
			c.handedOut++
			return protocol.PollTaskResponse{Lease: gatedLease(id)}, nil
		}
		return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
	}
	active := append([]string(nil), req.ActiveLeaseIDs...)
	slices.Sort(active)
	c.s2Active = append(c.s2Active, active)
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *drainClient) ReportResult(_ context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports++
	if req.SessionID != fmt.Sprintf("session-%d", c.session) {
		return protocol.ReportResultResponse{Accepted: false, Error: "unknown session"}, nil
	}
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *drainClient) Deregister(context.Context, protocol.DeregisterRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deregistered++
	return nil
}

var (
	_ ProtocolClient   = (*drainClient)(nil)
	_ deregisterClient = (*drainClient)(nil)
)

func newDrainTestRunner(client *drainClient, handler *gatedHandler) *Runner {
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.gated", handler)
	return New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       client.concurrency,
		Capabilities:      []protocol.Capability{{NodeType: "test.gated"}},
		HeartbeatInterval: time.Millisecond,
		PollWait:          time.Millisecond,
	})
}

func waitUntil(t *testing.T, what string, cond func() bool) {
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

// A session that ends on an error must not sit out the shutdown timeout for
// its busy workers: their leases are tracked runner-wide and carry into the
// next session, so waiting only keeps the runner unregistered while they run.
//
// One slot stays free so the next session polls and reports ActiveLeaseIDs.
func TestRunSkipsTheDrainWaitWhenASessionFails(t *testing.T) {
	const concurrency, busy = 3, 2
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &gatedHandler{started: make(chan struct{}, 8), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(handler.release) }) }
	defer release()
	failHeartbeats := make(chan struct{})
	client := &drainClient{concurrency: concurrency, leases: busy, failHeartbeats: failHeartbeats}
	r := newDrainTestRunner(client, handler)
	if r.shutdownTimeout != defaultRunnerShutdownTimeout {
		t.Fatalf("shutdownTimeout = %s, want the production default %s", r.shutdownTimeout, defaultRunnerShutdownTimeout)
	}

	first := make(chan error, 1)
	go func() { first <- r.Run(ctx) }()
	for i := 0; i < busy; i++ {
		select {
		case <-handler.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d workers started", i, busy)
		}
	}
	failedAt := time.Now()
	close(failHeartbeats)
	const promptly = 100 * time.Millisecond
	select {
	case <-first:
	case <-time.After(promptly):
		t.Fatalf("Run did not return within %s of the session failing with %d busy workers: "+
			"it is waiting out the %s drain", promptly, busy, r.shutdownTimeout)
	}
	t.Logf("Run returned %s after the session failed", time.Since(failedAt))
	if got := handler.running.Load(); got != busy {
		t.Fatalf("handlers running after the session ended = %d, want %d still busy", got, busy)
	}

	// The next session must name the still-running leases and count them.
	second := make(chan error, 1)
	go func() { second <- r.Run(ctx) }()
	want := []string{"s1-0", "s1-1"}
	waitUntil(t, "a second-session poll and heartbeat", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.s2Active) > 0 && len(client.s2InFlight) > 0
	})
	client.mu.Lock()
	active, inFlight := client.s2Active[0], client.s2InFlight[0]
	client.mu.Unlock()
	if !slices.Equal(active, want) {
		t.Fatalf("second-session poll ActiveLeaseIDs = %v, want %v", active, want)
	}
	if inFlight != busy {
		t.Fatalf("second-session heartbeat InFlight = %d, want %d", inFlight, busy)
	}

	// Once they finish, the first session's workers leave the shared
	// accounting and exit.
	release()
	waitUntil(t, "the first session's workers to finish", func() bool {
		return r.inFlight.Load() == 0 && len(r.active.snapshot()) == 0
	})
	cancel()
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("second session did not stop")
	}
	waitUntil(t, fmt.Sprintf("goroutines to return to the baseline of %d", baseline), func() bool {
		return runtime.NumGoroutine() <= baseline
	})
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reports != busy {
		t.Fatalf("reports = %d, want %d: one per lease from the first session's workers", client.reports, busy)
	}
}

// An operator stop keeps the drain contract: Run waits for the busy worker to
// report before returning, then gives the session up.
func TestRunStillDrainsOnOperatorStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &gatedHandler{started: make(chan struct{}, 1), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(handler.release) }) }
	defer release()
	client := &drainClient{concurrency: 1, leases: 1, failHeartbeats: make(chan struct{})}
	r := newDrainTestRunner(client, handler)

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned %v on stop while its worker was busy, want it to drain", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its worker finished")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reports != 1 || client.deregistered != 1 {
		t.Fatalf("at return: reports = %d, deregister calls = %d; want 1 and 1", client.reports, client.deregistered)
	}
}
