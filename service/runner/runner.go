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
		Capabilities: r.config.Capabilities,
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
	select {
	case err := <-recvErr:
		firstErr = err
	case <-ctx.Done():
		firstErr = ctx.Err()
	}

	close(taskCh)

	// Wait for in-flight workers to drain (30 s hard timeout).
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(30 * time.Second):
	}

	_ = stream.Send(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}})

	// Drain the recvLoop goroutine (it may still be blocked on Recv).
	// recvErr already has at most one value; a second send is harmless after
	// the channel is buffered size-1 — but recvLoop may still be running if we
	// got here via ctx.Done, so drain it.
	select {
	case <-recvErr:
	default:
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
