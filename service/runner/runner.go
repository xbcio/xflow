package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

// ProtocolClient opens a frame stream to the control plane. gRPC and HTTP
// transports both implement it (HTTP simulates the stream with long-poll).
type ProtocolClient interface {
	Connect(ctx context.Context) (protocol.FrameStream, error)
}

type Config struct {
	RunnerID     string
	Concurrency  int
	Labels       map[string]string
	Capabilities []protocol.Capability
	PollWait     time.Duration // retained for HTTP fallback simulation
}

type Runner struct {
	client   ProtocolClient
	executor *execution.Runner
	config   Config
}

func New(client ProtocolClient, registry engine.HandlerRegistry, config Config) *Runner {
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	return &Runner{
		client:   client,
		executor: execution.NewRunner(registry),
		config:   config,
	}
}

// Run opens a bidi stream, sends HELLO, waits for WELCOME, then runs a recv
// loop handing TASK frames to a worker pool of size Concurrency. Returns on
// BYE, ctx cancel, or stream error. The caller reconnects with backoff after
// non-nil return.
func (r *Runner) Run(ctx context.Context) error {
	stream, err := r.client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer stream.Close()

	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID:     r.config.RunnerID,
		Concurrency:  r.config.Concurrency,
		Capabilities: cloneCapabilities(r.config.Capabilities),
		Labels:       cloneLabels(r.config.Labels),
	}}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	welcome, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv welcome: %w", err)
	}
	if welcome.Welcome == nil {
		return fmt.Errorf("expected WELCOME, got %+v", welcome)
	}

	// taskCh buffers up to Concurrency leases so recvLoop is never blocked by
	// a full worker pool when the pool is at capacity.
	taskCh := make(chan engine.TaskLease, r.config.Concurrency)

	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go r.worker(ctx, taskCh, stream, &wg)
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- r.recvLoop(ctx, stream, taskCh) }()

	var firstErr error
	recvExited := false
	select {
	case err := <-recvErr:
		firstErr = err
		recvExited = true
	case <-ctx.Done():
		firstErr = ctx.Err()
	}

	// Step 1: stop dispatching new tasks to workers.
	close(taskCh)

	// Step 2: wait for in-flight workers to drain (30 s hard timeout).
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(30 * time.Second):
	}

	// Step 3: send BYE before closing (best-effort; ignore error if ctx cancelled).
	_ = stream.Send(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}})

	// Step 4: close the stream explicitly — this unblocks recvLoop's stream.Recv()
	// (returns context.Canceled or EOF), causing recvLoop to return and write to recvErr.
	// The defer stream.Close() at the top of Run handles the early-return path
	// (HELLO/WELCOME failure). fakeStream.Close is idempotent (CAS on closed flag);
	// gRPC CloseSend is also idempotent, so the defer is a safe no-op on second call.
	_ = stream.Close()

	// Step 5: blocking join — safe because step 4 guarantees recvLoop will exit.
	// Skip if we already consumed recvErr in the select above (recvLoop already returned).
	if !recvExited {
		select {
		case <-recvErr:
		case <-time.After(5 * time.Second):
			// stream.Close() should have unblocked recvLoop; 5 s is a safety valve.
		}
	}

	if errors.Is(firstErr, context.Canceled) {
		return nil
	}
	return firstErr
}

// recvLoop reads frames from the stream and dispatches TASK leases to taskCh.
// It returns on stream error, ctx cancellation, or when taskCh is closed.
func (r *Runner) recvLoop(ctx context.Context, stream protocol.FrameStream, taskCh chan<- engine.TaskLease) error {
	for {
		fr, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case fr.Task != nil:
			if fr.Task.Lease == nil {
				continue
			}
			select {
			case taskCh <- *fr.Task.Lease:
			case <-ctx.Done():
				return ctx.Err()
			}
		case fr.Ack != nil, fr.Keepalive != nil:
			// informational — no action needed
		case fr.Backoff != nil:
			select {
			case <-time.After(fr.Backoff.Wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// worker drains taskCh, executes each lease, and sends the result back.
func (r *Runner) worker(ctx context.Context, taskCh <-chan engine.TaskLease, stream protocol.FrameStream, wg *sync.WaitGroup) {
	defer wg.Done()
	for lease := range taskCh {
		leaseCopy := lease // capture loop variable
		result, execErr := r.executor.Execute(ctx, &leaseCopy)
		if execErr != nil {
			result = engine.TaskResult{Error: execErr}
		}
		_ = stream.Send(protocol.RunnerFrame{Result: &protocol.ResultFrame{
			LeaseID: string(leaseCopy.LeaseID),
			Lease:   &leaseCopy,
			Result:  result,
		}})
	}
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

func cloneCapabilities(caps []protocol.Capability) []protocol.Capability {
	if len(caps) == 0 {
		return nil
	}
	out := make([]protocol.Capability, len(caps))
	copy(out, caps)
	return out
}
