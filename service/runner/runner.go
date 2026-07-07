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

	// sendCh serializes every frame send (RESULT from workers, BYE from Run)
	// through a single send-loop goroutine below. grpc-go's ClientStream.Send
	// is not safe to call from multiple goroutines concurrently, and
	// CloseSend is explicitly not safe to call concurrently with Send — so
	// with Concurrency > 1, workers must never call stream.Send directly.
	// HELLO above is exempt: it happens before any worker or the send-loop
	// exists, so nothing can race it.
	sendCh := make(chan protocol.RunnerFrame, r.config.Concurrency*2)
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for fr := range sendCh {
			_ = stream.Send(fr)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go r.worker(ctx, taskCh, sendCh, &wg)
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- r.recvLoop(ctx, stream, taskCh) }()

	var firstErr error
	select {
	case err := <-recvErr:
		firstErr = err
	case <-ctx.Done():
		firstErr = ctx.Err()
		// recvLoop may be blocked in stream.Recv() (not itself ctx-aware —
		// e.g. fakeStream.Recv only unblocks via Close, and even gRPC's Recv
		// can outlast ctx by a beat) or about to race taskCh<- against the
		// very ctx.Done() we just observed. Close the stream first: that
		// unblocks a pending Recv() (returns an error) and, together with
		// ctx.Done() already firing, guarantees recvLoop's inner select
		// takes the ctx.Done() branch on any subsequent iteration. Only
		// after recvLoop is confirmed to have returned (drained from
		// recvErr) is it safe to close(taskCh) below without racing its
		// send — closing taskCh while recvLoop might still pick the
		// taskCh<- branch would panic with "send on closed channel".
		_ = stream.Close()
		<-recvErr
	}

	// Step 1: stop dispatching new tasks to workers. Safe now — recvLoop has
	// definitely returned (drained above), so nothing can still be sending.
	close(taskCh)

	// Step 2: wait for in-flight workers to drain (30 s hard timeout).
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
		// Normal case: wg.Wait() completed, so every worker has returned and
		// none can still be writing to sendCh. It is now provably safe to
		// send the final BYE and close(sendCh) — no writer can race the
		// close. Join the send-loop (sendDone) so the BYE (and anything
		// still in flight) is actually flushed via stream.Send before we
		// close the stream below, preserving the single-writer contract.
		sendCh <- protocol.RunnerFrame{Bye: &protocol.ByeFrame{}}
		close(sendCh)
		<-sendDone

	case <-time.After(30 * time.Second):
		// Abnormal case: a worker is still stuck/running past the timeout.
		// We do NOT know that sendCh has no more writers, so we must NOT
		// close(sendCh) here — if the stuck worker later finishes and
		// executes its ctx-aware `sendCh <- RunnerFrame{Result: ...}`, that
		// send would panic with "send on closed channel" if we'd closed it.
		// This mirrors the existing, already-accepted tolerance for the
		// taskCh/recvLoop timeout path above (see the T10 fix comment on
		// close(taskCh)): on a forced timeout we accept that an abandoned
		// goroutine may leak rather than risk a send-on-closed-channel
		// panic. It is not a new gap — it's the same trade-off already made
		// for taskCh, applied consistently to sendCh.
		//
		// Best-effort BYE: attempt a non-blocking send so a healthy send-loop
		// still gets a chance to relay BYE, but never block waiting for it
		// and never join sendDone here — the send-loop may itself be stuck
		// forwarding frames from the stuck worker, and blocking on it could
		// hang Run indefinitely.
		select {
		case sendCh <- protocol.RunnerFrame{Bye: &protocol.ByeFrame{}}:
		default:
		}
	}

	// Step 4: close the stream explicitly. recvLoop has already returned by
	// this point (drained above via recvErr, either directly or after
	// ctx.Done()). On the waitDone path the send-loop has also already
	// exited (joined above via sendDone), so this call no longer needs to
	// unblock anything there — it just releases the transport promptly
	// instead of waiting for the deferred stream.Close() at the top of Run.
	// On the timeout path the send-loop may still be running (possibly
	// stuck relaying a stuck worker's eventual frame); closing the stream
	// here causes its next stream.Send to error out so it can exit on its
	// own — or it leaks, which is the same accepted tolerance as above.
	// fakeStream.Close is idempotent (CAS on closed flag); gRPC CloseSend is
	// also idempotent, so the defer at the top of Run is a safe no-op on
	// second call.
	_ = stream.Close()

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

// worker drains taskCh, executes each lease, and writes the RESULT frame to
// sendCh — never directly to the stream. All frame sends are serialized
// through the single send-loop goroutine started in Run, since
// protocol.FrameStream.Send (backed by grpc-go's ClientStream.SendMsg) is not
// safe to call from multiple goroutines concurrently.
func (r *Runner) worker(ctx context.Context, taskCh <-chan engine.TaskLease, sendCh chan<- protocol.RunnerFrame, wg *sync.WaitGroup) {
	defer wg.Done()
	for lease := range taskCh {
		leaseCopy := lease // capture loop variable
		result, execErr := r.executor.Execute(ctx, &leaseCopy)
		if execErr != nil {
			result = engine.TaskResult{Error: execErr}
		}
		// Unconditional send: a worker that has already finished executing
		// must always report its result, even if ctx has since been
		// cancelled. sendCh is only ever closed on the waitDone path in Run,
		// strictly after wg.Wait() proves every worker (including this one)
		// has returned — so a running worker can never be blocked forever on
		// a send to a channel that gets closed out from under it. A ctx-aware
		// select here previously raced ctx.Done() against this send: when
		// both were ready, Go picks pseudo-randomly, so an already-computed
		// result could be silently dropped on ordinary cancellation instead
		// of being sent, breaking the graceful-drain guarantee. The
		// truly-stuck-forever scenario (a hung send-loop backing up sendCh)
		// is already covered by Run's outer 30s wg.Wait() timeout, which
		// does not close(sendCh) on that path — so blocking here is safe.
		sendCh <- protocol.RunnerFrame{Result: &protocol.ResultFrame{
			LeaseID: string(leaseCopy.LeaseID),
			Lease:   &leaseCopy,
			Result:  result,
		}}
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
