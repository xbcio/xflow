package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

var errHeartbeatSessionGone = errors.New("heartbeat: unknown session")

// heartbeatFailClient hands session 1 one lease per poll until Concurrency
// leases are out, then fails session 1's heartbeats once failHeartbeats is
// closed. Like the server, it refuses reports under a replaced session and
// replays to a later session every lease that was handed out, is not yet
// reported and is not named in ActiveLeaseIDs. It has no RenewLease and its
// leases carry no token, so lease renewal is not exercised.
type heartbeatFailClient struct {
	concurrency    int
	failHeartbeats <-chan struct{}

	mu         sync.Mutex
	session    int
	handedOut  []string
	reported   map[string]bool
	attempts   map[string]int // every report, refused or accepted
	executions map[string]int // accepted reports only
	replays    int
	s2Beats    []int
}

func (c *heartbeatFailClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: fmt.Sprintf("session-%d", c.session)}, nil
}

func (c *heartbeatFailClient) Heartbeat(_ context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	if req.SessionID == "session-1" {
		select {
		case <-c.failHeartbeats:
			return protocol.HeartbeatResponse{}, errHeartbeatSessionGone
		default:
		}
		return protocol.HeartbeatResponse{}, nil
	}
	c.mu.Lock()
	c.s2Beats = append(c.s2Beats, req.InFlight)
	c.mu.Unlock()
	return protocol.HeartbeatResponse{}, nil
}

func (c *heartbeatFailClient) Poll(_ context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.SessionID == "session-1" {
		if len(c.handedOut) < c.concurrency {
			id := fmt.Sprintf("s1-%d", len(c.handedOut))
			c.handedOut = append(c.handedOut, id)
			return protocol.PollTaskResponse{Lease: gatedLease(id)}, nil
		}
		return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
	}
	named := make(map[string]bool, len(req.ActiveLeaseIDs))
	for _, id := range req.ActiveLeaseIDs {
		named[id] = true
	}
	for _, id := range c.handedOut {
		if !c.reported[id] && !named[id] {
			c.replays++
			c.reported[id] = true // replay once, so a regression shows as a count, not a loop
			return protocol.PollTaskResponse{Lease: gatedLease(id)}, nil
		}
	}
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (c *heartbeatFailClient) ReportResult(_ context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := string(req.Lease.LeaseID)
	c.attempts[id]++
	// Like the server's ValidateSession: a report under a session the runner
	// has since replaced is refused and the lease stays unreported.
	if req.SessionID != fmt.Sprintf("session-%d", c.session) {
		return protocol.ReportResultResponse{Accepted: false, Error: "unknown session"}, nil
	}
	c.executions[id]++
	c.reported[id] = true
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *heartbeatFailClient) snapshot() (beats []int, executions map[string]int, replays int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	executions = make(map[string]int, len(c.executions))
	for k, v := range c.executions {
		executions[k] = v
	}
	return append([]int(nil), c.s2Beats...), executions, c.replays
}

func (c *heartbeatFailClient) reportAttempts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	attempts := make(map[string]int, len(c.attempts))
	for k, v := range c.attempts {
		attempts[k] = v
	}
	return attempts
}

var _ ProtocolClient = (*heartbeatFailClient)(nil)

// A heartbeat failure must end the session even while every worker is busy
// and the poll loop is idle, so the caller re-registers at once rather than
// when a slot frees. The busy workers keep running under the next session,
// which must count them and name their leases instead of letting the server
// replay them to a second worker while they run. Their results are refused as
// stale once they finish, so each lease is redelivered once, after the first
// run ends: sequential redelivery, never a concurrent double run.
func TestRunEndsSessionWhenHeartbeatFailsWithAllWorkersBusy(t *testing.T) {
	const concurrency = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &gatedHandler{started: make(chan struct{}, 8), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(handler.release) }) }
	defer release()

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.gated", handler)
	failHeartbeats := make(chan struct{})
	client := &heartbeatFailClient{
		concurrency:    concurrency,
		failHeartbeats: failHeartbeats,
		reported:       map[string]bool{},
		attempts:       map[string]int{},
		executions:     map[string]int{},
	}
	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       concurrency,
		Capabilities:      []protocol.Capability{{NodeType: "test.gated"}},
		HeartbeatInterval: time.Millisecond,
		PollWait:          time.Millisecond,
	})
	r.shutdownTimeout = 20 * time.Millisecond

	first := make(chan error, 1)
	go func() { first <- r.Run(ctx) }()
	for i := 0; i < concurrency; i++ {
		select {
		case <-handler.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d workers started", i, concurrency)
		}
	}
	close(failHeartbeats)

	select {
	case err := <-first:
		if !errors.Is(err, errHeartbeatSessionGone) {
			t.Fatalf("first session Run = %v, want the heartbeat failure %v", err, errHeartbeatSessionGone)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after heartbeats failed with every worker busy: " +
			"the dead session is only noticed once a slot frees")
	}
	if got := handler.running.Load(); got != concurrency {
		t.Fatalf("handlers running after the session ended = %d, want %d still busy", got, concurrency)
	}

	// Re-register as runWithReconnect would.
	second := make(chan error, 1)
	go func() { second <- r.Run(ctx) }()
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
	waitFor("five second-session heartbeats", func() bool {
		beats, _, _ := client.snapshot()
		return len(beats) >= 5
	})
	beats, _, replays := client.snapshot()
	for i, n := range beats {
		if n != concurrency {
			t.Fatalf("second-session heartbeat %d InFlight = %d, want %d: the first session's workers are still busy", i, n, concurrency)
		}
	}
	if replays != 0 {
		t.Fatalf("server replayed %d busy leases to the second session", replays)
	}

	release()
	waitFor("both leases to be accepted", func() bool {
		_, executions, _ := client.snapshot()
		return len(executions) == concurrency
	})
	cancel()
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("second session did not stop")
	}
	_, executions, replays := client.snapshot()
	for id, n := range executions {
		if n != 1 {
			t.Errorf("lease %s accepted %d times, want 1", id, n)
		}
	}
	for id, n := range client.reportAttempts() {
		if n != 2 {
			t.Errorf("lease %s reported %d times, want 2: refused under session-1, accepted after one replay", id, n)
		}
	}
	if replays != concurrency {
		t.Errorf("server replayed %d leases, want %d: one per stale-session refusal", replays, concurrency)
	}
	if got := handler.maxRunning.Load(); got > concurrency {
		t.Errorf("max concurrent handlers = %d, want at most %d: a lease ran twice at once", got, concurrency)
	}
}
