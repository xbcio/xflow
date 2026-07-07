package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// fakeStream — in-memory FrameStream for testing
// ---------------------------------------------------------------------------

type fakeStream struct {
	recvCh chan protocol.ServerFrame
	sendCh chan protocol.RunnerFrame
	closed atomic.Bool
}

func newFakeStream(buf int) *fakeStream {
	return &fakeStream{
		recvCh: make(chan protocol.ServerFrame, buf),
		sendCh: make(chan protocol.RunnerFrame, buf),
	}
}

func (f *fakeStream) Send(fr protocol.RunnerFrame) error { f.sendCh <- fr; return nil }
func (f *fakeStream) Recv() (protocol.ServerFrame, error) {
	fr, ok := <-f.recvCh
	if !ok {
		return protocol.ServerFrame{}, context.Canceled
	}
	return fr, nil
}
func (f *fakeStream) Close() error {
	if f.closed.CompareAndSwap(false, true) {
		close(f.recvCh)
	}
	return nil
}

// ---------------------------------------------------------------------------
// fakeClient — implements the new ProtocolClient (Connect only)
// ---------------------------------------------------------------------------

type fakeClient struct{ stream *fakeStream }

func (c *fakeClient) Connect(ctx context.Context) (protocol.FrameStream, error) {
	return c.stream, nil
}

// ---------------------------------------------------------------------------
// funcHandler — adapter that wraps a plain func into types.ActionHandler
// ---------------------------------------------------------------------------

// funcActionHandler wraps a func(ctx, *types.Input) -> (*types.Output, error)
// as a types.ActionHandler so it can be registered with execution.Registry.
type funcActionHandler struct {
	nodeType string
	fn       func(ctx context.Context, input *types.Input) (*types.Output, error)
}

func (h funcActionHandler) Descriptor() node_Descriptor { return node_Descriptor{Type: h.nodeType} }
func (h funcActionHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	return h.fn(ctx, input)
}

// node_Descriptor is an alias to avoid importing nodes/node in tests.
// types.Descriptor is the actual type; use it directly.
type node_Descriptor = types.Descriptor

// registerTestHandler registers a handler that sleeps and counts active
// workers. handlerFn receives ctx and the task input; its return maps to
// types.ActionHandler.Execute. Registered under nodeType via RegisterGlobal.
func registerTestHandler(
	t *testing.T,
	registry *execution.Registry,
	nodeType string,
	handlerFn func(ctx context.Context, input *types.Input) (*types.Output, error),
) {
	t.Helper()
	registry.RegisterGlobal(nodeType, funcActionHandler{nodeType: nodeType, fn: handlerFn})
}

// ---------------------------------------------------------------------------
// TestRunnerConcurrencyParallel — proves worker pool size = Concurrency
// ---------------------------------------------------------------------------

func TestRunnerConcurrencyParallel(t *testing.T) {
	stream := newFakeStream(16)
	// Pre-load WELCOME so recvLoop unblocks immediately.
	stream.recvCh <- protocol.ServerFrame{Welcome: &protocol.WelcomeFrame{RunnerID: "r1"}}

	var active, maxActive atomic.Int32
	var wg sync.WaitGroup
	wg.Add(3)

	registry := execution.NewRegistry()
	registerTestHandler(t, registry, "xflow.function", func(ctx context.Context, input *types.Input) (*types.Output, error) {
		cur := active.Add(1)
		// CAS loop to track the high-water mark.
		for {
			m := maxActive.Load()
			if cur <= m || maxActive.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		active.Add(-1)
		wg.Done()
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})

	r := New(
		&fakeClient{stream},
		registry,
		Config{
			RunnerID:     "r1",
			Concurrency:  3,
			Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Send 3 TASK frames with unique LeaseIDs.
	for i, id := range []string{"L1", "L2", "L3"} {
		_ = i
		stream.recvCh <- protocol.ServerFrame{Task: &protocol.TaskFrame{
			Lease: &engine.TaskLease{
				LeaseID:  engine.LeaseID(id),
				NodeType: "xflow.function",
			},
		}}
	}

	// Collect 3 RESULT frames.
	results := 0
	for results < 3 {
		select {
		case fr := <-stream.sendCh:
			if fr.Result != nil {
				results++
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for results; got %d/3", results)
		}
	}
	wg.Wait()

	if maxActive.Load() < 3 {
		t.Fatalf("max concurrent = %d, want >= 3 (concurrency bug not fixed)", maxActive.Load())
	}
	cancel()
}

// ---------------------------------------------------------------------------
// TestRunnerGracefulDrain — proves cross-task integration between T7's
// recv-loop/worker-pool rewrite (in-flight work drains via wg.Wait before
// Run exits) and T9's exit-code normalization (ctx.Canceled -> nil). A single
// slow handler is mid-flight when ctx is cancelled; Run must not abandon it —
// it waits for the RESULT to be sent before tearing down, and returns nil
// rather than a cancellation error.
// ---------------------------------------------------------------------------

func TestRunnerGracefulDrain(t *testing.T) {
	stream := newFakeStream(16)
	stream.recvCh <- protocol.ServerFrame{Welcome: &protocol.WelcomeFrame{RunnerID: "r1"}}

	const handlerDelay = 100 * time.Millisecond
	handlerStarted := make(chan struct{}, 1)
	handlerDone := make(chan struct{}, 1)

	registry := execution.NewRegistry()
	registerTestHandler(t, registry, "xflow.function", func(ctx context.Context, input *types.Input) (*types.Output, error) {
		handlerStarted <- struct{}{}
		time.Sleep(handlerDelay)
		handlerDone <- struct{}{}
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})

	r := New(
		&fakeClient{stream},
		registry,
		Config{
			RunnerID:     "r1",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	// Send exactly 1 TASK frame, then wait until the worker has actually
	// started executing it before cancelling. This proves the specific
	// invariant the brief calls out — cancel lands mid-execution, not before
	// pickup — without racing the ambiguous "did the runner even see the
	// task yet" window that a bare cancel() right after Send would leave
	// open (recvCh is buffered, so sending never synchronizes with recvLoop
	// actually consuming it).
	stream.recvCh <- protocol.ServerFrame{Task: &protocol.TaskFrame{
		Lease: &engine.TaskLease{LeaseID: engine.LeaseID("L1"), NodeType: "xflow.function"},
	}}
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()

	// (a) Exactly 1 RESULT frame for L1 must be sent — the worker completed
	// its in-flight task despite the ctx cancellation racing its start.
	// sendCh also carries the initial HELLO frame and (on drain) a trailing
	// BYE frame; skip past those and look specifically for RESULT.
	var resultFr protocol.RunnerFrame
	found := false
	for !found {
		select {
		case fr := <-stream.sendCh:
			if fr.Result != nil {
				resultFr = fr
				found = true
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for RESULT frame; worker was not drained gracefully")
		}
	}
	if resultFr.Result.LeaseID != "L1" {
		t.Fatalf("expected RESULT for L1, got %+v", resultFr)
	}

	// Confirm the handler itself actually ran to completion (not skipped).
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never completed")
	}

	// (b) Run must return nil — ctx.Canceled normalizes to nil per T9.
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil (ctx.Canceled should normalize to nil)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run() to return")
	}
}
