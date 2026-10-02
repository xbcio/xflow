package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

var errHeartbeatTransient = errors.New("heartbeat: connection reset")

// manualClock is a test clock that only moves when advanced.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// scriptedHeartbeatClient answers heartbeat n (1-based) with fail(n), and
// advances clock by step on every heartbeat so the tolerance window is driven
// by the number of beats, not by wall time. It never hands out a lease.
type scriptedHeartbeatClient struct {
	clock *manualClock
	step  time.Duration
	fail  func(n int) error

	mu            sync.Mutex
	registrations int
	beats         int
	failed        int
}

func (c *scriptedHeartbeatClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registrations++
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: "session-1"}, nil
}

func (c *scriptedHeartbeatClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beats++
	c.clock.advance(c.step)
	if err := c.fail(c.beats); err != nil {
		c.failed++
		return protocol.HeartbeatResponse{}, err
	}
	return protocol.HeartbeatResponse{}, nil
}

func (*scriptedHeartbeatClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
}

func (*scriptedHeartbeatClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *scriptedHeartbeatClient) counts() (registrations, beats, failed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.registrations, c.beats, c.failed
}

var _ ProtocolClient = (*scriptedHeartbeatClient)(nil)

func TestHeartbeatLoopTolerance(t *testing.T) {
	const grace = 5 * time.Second

	start := func(t *testing.T, fail func(n int) error) (*scriptedHeartbeatClient, <-chan error) {
		t.Helper()
		clock := &manualClock{now: time.Unix(1_000_000, 0)}
		client := &scriptedHeartbeatClient{clock: clock, step: time.Second, fail: fail}
		r := New(client, execution.NewRegistry(), Config{
			RunnerID:          "runner-1",
			HeartbeatInterval: time.Millisecond,
			PollWait:          time.Millisecond,
		})
		r.heartbeatGrace = grace
		r.now = clock.Now
		r.shutdownTimeout = 20 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		stopped := make(chan struct{})
		go func() { done <- r.Run(ctx); close(stopped) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Error("Run did not stop")
			}
		})
		return client, done
	}

	// One failed beat inside the window is a blip: the session survives it
	// and later beats succeed under the same registration.
	t.Run("transient failure keeps the session", func(t *testing.T) {
		client, done := start(t, func(n int) error {
			if n == 2 {
				return errHeartbeatTransient
			}
			return nil
		})
		deadline := time.After(5 * time.Second)
		for {
			if _, beats, _ := client.counts(); beats >= 10 {
				break
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned %v after one transient heartbeat failure, want the session kept", err)
			case <-deadline:
				t.Fatal("timed out waiting for 10 heartbeats")
			case <-time.After(time.Millisecond):
			}
		}
		if registrations, _, failed := client.counts(); registrations != 1 || failed != 1 {
			t.Fatalf("registrations = %d, failed beats = %d; want 1 and 1", registrations, failed)
		}
	})

	// Failures that outlast the window end the session, but only once the
	// window has passed since the last success: each beat moves the clock 1s,
	// so with a 5s grace the sixth consecutive failure is the first past it.
	t.Run("sustained failures end the session after the window", func(t *testing.T) {
		client, done := start(t, func(n int) error {
			if n > 1 {
				return errHeartbeatTransient
			}
			return nil
		})
		select {
		case err := <-done:
			if !errors.Is(err, errHeartbeatTransient) {
				t.Fatalf("Run() = %v, want it to wrap %v", err, errHeartbeatTransient)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return although heartbeats kept failing past the grace window")
		}
		if _, _, failed := client.counts(); failed != 6 {
			t.Fatalf("session ended after %d failed beats, want 6 (the first more than %s after the last success)", failed, grace)
		}
	})

	// A refusal that says the session is gone cannot be retried away, so it
	// ends the session on the spot, well inside the window.
	t.Run("invalid session ends the session at once", func(t *testing.T) {
		stale := status.Error(codes.FailedPrecondition, "runner session stale")
		client, done := start(t, func(n int) error {
			if n == 2 {
				return stale
			}
			return nil
		})
		select {
		case err := <-done:
			if !errors.Is(err, stale) {
				t.Fatalf("Run() = %v, want %v", err, stale)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after the server refused the session")
		}
		if _, beats, failed := client.counts(); beats != 2 || failed != 1 {
			t.Fatalf("beats = %d, failed = %d; want the session ended on the first refusal (2, 1)", beats, failed)
		}
	})
}

func TestSessionInvalid(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"grpc not found", status.Error(codes.NotFound, "runner not found"), true},
		{"grpc stale session", status.Error(codes.FailedPrecondition, "runner session stale"), true},
		{"grpc unauthenticated", status.Error(codes.Unauthenticated, "unauthenticated"), true},
		{"grpc unavailable", status.Error(codes.Unavailable, "connection refused"), false},
		{"grpc internal", status.Error(codes.Internal, "internal server error"), false},
		{"plain transport error", errHeartbeatTransient, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionInvalid(tt.err); got != tt.want {
				t.Errorf("sessionInvalid(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
