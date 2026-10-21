package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// ctxAwareHandler announces the context of each execution and then blocks until
// released. It deliberately does not stop on ctx cancellation: the tests below
// inspect that context after the run is cancelled, and a handler that returned
// early would hide whether the run cancellation reached it.
type ctxAwareHandler struct {
	started chan context.Context
	release chan struct{}

	mu     sync.Mutex
	ctxErr error
}

func (*ctxAwareHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.gated"}
}

func (h *ctxAwareHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	select {
	case h.started <- ctx:
	case <-time.After(5 * time.Second):
	}
	<-h.release
	h.mu.Lock()
	h.ctxErr = ctx.Err()
	h.mu.Unlock()
	return &types.Output{Data: input.Data}, nil
}

func (h *ctxAwareHandler) handlerContextErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ctxErr
}

func newShutdownTestRunner(client ProtocolClient, handler types.ActionHandler) *Runner {
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.gated", handler)
	return New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "test.gated"}},
		HeartbeatInterval: time.Millisecond,
		PollWait:          time.Millisecond,
	})
}

// An operator stop (SIGTERM) stops polling but must not cancel work that is
// already executing: the handler owns the lease until it finishes or the lease
// is lost, and a cancelled handler discards work the server will only replay
// after the lease ages out. Run must still be waiting during the grace window.
func TestShutdownDoesNotCancelAnInFlightHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &ctxAwareHandler{started: make(chan context.Context, 1), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(handler.release) }) }
	defer release()
	client := &drainClient{concurrency: 1, leases: 1, failHeartbeats: make(chan struct{})}
	r := newShutdownTestRunner(client, handler)

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	var handlerCtx context.Context
	select {
	case handlerCtx = <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	// The run context is cancelled now. Give the shutdown path a window to
	// (wrongly) propagate that cancellation into the handler.
	const graceWindow = 50 * time.Millisecond
	select {
	case <-handlerCtx.Done():
		t.Fatalf("handler context was cancelled %v after the stop: SIGTERM aborted work that was "+
			"already in flight instead of letting it finish", handlerCtx.Err())
	case <-time.After(graceWindow):
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the handler was still running, want it to drain", err)
	default:
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v after a clean stop, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the handler finished")
	}
	if err := handler.handlerContextErr(); err != nil {
		t.Fatalf("handler context error at completion = %v, want nil", err)
	}

	client.mu.Lock()
	reports, deregistered := client.reports, client.deregistered
	client.mu.Unlock()
	if reports != 1 || deregistered != 1 {
		t.Fatalf("at return: reports = %d, deregister calls = %d; want 1 and 1 "+
			"(the drained handler reports, then the session is given up)", reports, deregistered)
	}
}

// The shutdown timeout is the upper bound on a graceful stop. A handler that
// ignores its context must not pin the process past it, and the abandoned
// lease must keep the session protected (no deregister) so the server replays
// the work instead of counting it as finished.
func TestShutdownTimeoutBoundsAnUnresponsiveHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &ctxAwareHandler{started: make(chan context.Context, 1), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(handler.release) }) }
	defer release()
	client := &drainClient{concurrency: 1, leases: 1, failHeartbeats: make(chan struct{})}
	r := newShutdownTestRunner(client, handler)
	const timeout = 20 * time.Millisecond
	r.shutdownTimeout = timeout

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()
	startedShutdown := time.Now()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return within the test window after its %s shutdown timeout: "+
			"a handler that ignores cancellation pins the process forever", timeout)
	}
	if elapsed := time.Since(startedShutdown); elapsed < timeout {
		t.Fatalf("Run returned %v after the stop, before its %s shutdown timeout", elapsed, timeout)
	}

	client.mu.Lock()
	deregistered := client.deregistered
	client.mu.Unlock()
	if deregistered != 0 {
		t.Fatalf("deregister calls = %d after the drain timed out, want 0: the session must stay "+
			"protected while an abandoned worker still holds a lease", deregistered)
	}

	// The worker outlived Run. Release it and let it report so the test does
	// not leave an executing goroutine behind.
	release()
	waitUntil(t, "the abandoned worker to report its result", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.reports == 1
	})
	waitUntil(t, "the abandoned worker to leave the shared lease accounting", func() bool {
		return r.inFlight.Load() == 0 && len(r.active.snapshot()) == 0
	})
}

// errOnlyContext reports a cancelled Err without closing Done, so workerLoop's
// select cannot take its ctx.Done branch and deterministically reaches the
// lease branch. That is the interleaving the pre-check exists for: a poll send
// and a cancellation can both be ready, and Go's select may pick the send.
type errOnlyContext struct{ context.Context }

func (errOnlyContext) Err() error { return context.Canceled }

// A lease handed to a worker after the run began shutting down is work this
// process has not started. Executing it would extend the shutdown for a task
// the server can replay, so the worker must return the lease (undoing the
// active/in-flight accounting pollLoop applied) instead of calling the handler.
func TestWorkerLoopReleasesALeaseReceivedAfterShutdown(t *testing.T) {
	handler := &ctxAwareHandler{started: make(chan context.Context, 1), release: make(chan struct{})}
	close(handler.release) // only read if the pre-check fails and the handler runs
	client := &drainClient{concurrency: 1, leases: 0, failHeartbeats: make(chan struct{})}
	r := newShutdownTestRunner(client, handler)

	ctx := errOnlyContext{Context: context.Background()}

	leaseCh := make(chan *engine.TaskLease, 1)
	leaseCh <- gatedLease("queued-1")
	// pollLoop closes the queue before Run's drain wait; closing it here too
	// means a worker that (wrongly) executes the lease still returns, so the
	// regression shows up as a failed assertion instead of a parked goroutine.
	close(leaseCh)
	r.inFlight.Add(1)
	r.active.add("queued-1")

	r.workerLoop(ctx, "session-1", leaseCh, &r.inFlight, r.active, func(error) {})

	select {
	case <-handler.started:
		t.Fatal("a lease queued at shutdown was executed: the worker started work the shutdown " +
			"deadline may not wait for instead of returning it for replay")
	default:
	}
	if got := r.inFlight.Load(); got != 0 {
		t.Fatalf("in-flight leases after the pre-check = %d, want 0", got)
	}
	if got := r.active.snapshot(); len(got) != 0 {
		t.Fatalf("active leases after the pre-check = %v, want none: a released lease must not be "+
			"reported on the next session's polls", got)
	}
	if got := len(leaseCh); got != 0 {
		t.Fatalf("leases left on the channel = %d, want 0 (the worker consumes the one it rejects)", got)
	}
}

// Config.ShutdownTimeout is the operator-facing knob; zero and negative values
// must keep the production default rather than disabling the bound.
func TestNewAppliesTheConfiguredShutdownTimeout(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"unset keeps the default", 0, defaultRunnerShutdownTimeout},
		{"configured value wins", 3 * time.Second, 3 * time.Second},
		{"negative keeps the default", -time.Second, defaultRunnerShutdownTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &drainClient{concurrency: 1, failHeartbeats: make(chan struct{})}
			r := New(client, execution.NewRegistry(), Config{ShutdownTimeout: tt.configured})
			if r.shutdownTimeout != tt.want {
				t.Fatalf("shutdownTimeout = %s, want %s", r.shutdownTimeout, tt.want)
			}
		})
	}
}
