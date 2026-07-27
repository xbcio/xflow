package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// defaultRunnerShutdownTimeout bounds how long Run waits for in-flight workers
// to finish after the run context is cancelled. Workers observe the cancelled
// context and abort their current handler, so this is a safety upper bound.
const defaultRunnerShutdownTimeout = 10 * time.Second

// defaultReportTimeout bounds how long executeAndReport waits for the server
// to acknowledge a task result. The report deliberately uses a background
// context (so a cancelled run does not discard a computed result the server
// still needs), but a bare Background has no upper bound: a hung network could
// pin a worker forever. This cap releases the worker back to the pool.
const defaultReportTimeout = 15 * time.Second

type ProtocolClient interface {
	Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error)
	Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error)
	Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error)
	ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error)
}

type Config struct {
	RunnerID          string
	Concurrency       int
	Labels            map[string]string
	Capabilities      []protocol.Capability
	HeartbeatInterval time.Duration
	PollWait          time.Duration
	// Tracer, when set, enables the server→runner→server trace graph: the
	// runner extracts the remote parent from the lease's TraceCarrier, starts
	// an xflow.task.execute span, and injects a report carrier so the server's
	// commit span is properly parented. Nil means no-op tracing.
	Tracer tracing.Tracer
	// ResourcePool, when set, is installed on the per-call context so
	// resource-aware nodes (xflow.database, xflow.grpc) can pool connections.
	// nil preserves the existing no-pool behavior (resource-aware nodes error).
	ResourcePool types.ResourcePool
	// CredentialResolver, when set, is applied to each Input before the handler
	// runs so nodes can resolve named credentials via input.Credential(name).
	// nil means no resolver; existing behavior is unchanged.
	CredentialResolver func(namespace namespace.Namespace, name string) map[string]any
	// Namespaces lists the namespaces this runner is willing to serve. Empty or nil
	// means ["default"] for single-namespace compatibility.
	Namespaces []namespace.Namespace
}

type Runner struct {
	client   ProtocolClient
	executor *execution.Runner
	config   Config
	tracer   tracing.Tracer
}

func New(client ProtocolClient, registry engine.HandlerRegistry, config Config) *Runner {
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.PollWait <= 0 {
		config.PollWait = time.Second
	}
	tracer := config.Tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	return &Runner{
		client:   client,
		executor: execution.NewRunner(registry, execution.WithResourcePool(config.ResourcePool), execution.WithCredentialResolver(config.CredentialResolver)),
		config:   config,
		tracer:   tracer,
	}
}

// Run drives register → poll → execute → report with a pool of Concurrency
// workers and an independent heartbeat goroutine. Heartbeats are delivered on
// their own ticker so a long-running handler cannot starve them (which would
// otherwise let the server's lease sweeper reclaim and re-execute the task).
// On any transport error Run returns and the caller (cmd/runner) reconnects.
func (r *Runner) Run(ctx context.Context) error {
	registerResp, err := r.client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     r.config.RunnerID,
		Concurrency:  r.config.Concurrency,
		Capabilities: r.config.Capabilities,
		Labels:       r.config.Labels,
		Namespaces:   namespaceStrings(r.config.Namespaces),
	})
	if err != nil {
		return runContextError(ctx, err)
	}
	sessionID := registerResp.SessionID

	var inFlight atomic.Int32
	leaseCh := make(chan *engine.TaskLease, r.config.Concurrency)
	var errOnce sync.Once
	errCh := make(chan error, 1)
	signalError := func(err error) {
		errOnce.Do(func() {
			select {
			case errCh <- err:
			default:
			}
		})
	}

	// Independent heartbeat goroutine — survives while workers are blocked on
	// long handlers, reflecting the true in-flight count to the server.
	heartbeatCtx, hbCancel := context.WithCancel(ctx)
	go r.heartbeatLoop(heartbeatCtx, sessionID, &inFlight, signalError)

	// Worker pool of Concurrency goroutines executing leases in parallel.
	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.workerLoop(ctx, sessionID, leaseCh, &inFlight, signalError)
		}()
	}

	pollErr := r.pollLoop(ctx, sessionID, leaseCh, &inFlight)
	hbCancel()

	// Graceful shutdown: stop polling, then wait (bounded) for workers to
	// finish in-flight tasks. Workers see ctx cancellation and exit.
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(defaultRunnerShutdownTimeout):
	}

	if pollErr != nil {
		// A worker or heartbeat may have already surfaced a more specific
		// error; prefer it over the poll error when present so the caller
		// reports the root cause rather than a transport side-effect.
		select {
		case err := <-errCh:
			return err
		default:
		}
		return pollErr
	}
	select {
	case err := <-errCh:
		return err
	default:
	}
	return runContextError(ctx, ctx.Err())
}

// pollLoop claims leases at the rate the worker pool can absorb them. The
// Capacity advertised to the server is always the total Concurrency — the
// single source of truth for this runner's parallelism. The control-plane
// directory derives server-side headroom from its own claim/lease accounting
// (which already tracks every in-flight task), so advertising a client-side
// remainder here would double-count in-flight work and silently suppress the
// effective concurrency. The local in-flight gate below is a complementary
// safety valve that stops the runner from claiming more leases than its worker
// pool can execute in parallel; it does not change the advertised capacity.
func (r *Runner) pollLoop(ctx context.Context, sessionID string, leaseCh chan<- *engine.TaskLease, inFlight *atomic.Int32) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if r.config.Concurrency-int(inFlight.Load()) <= 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.config.PollWait):
			}
			continue
		}
		resp, err := r.client.Poll(ctx, protocol.PollTaskRequest{
			RunnerID:     r.config.RunnerID,
			SessionID:    sessionID,
			Capacity:     r.config.Concurrency,
			Capabilities: r.config.Capabilities,
		})
		if err != nil {
			return runContextError(ctx, err)
		}
		if resp.Lease == nil {
			wait := resp.Wait
			if wait <= 0 {
				wait = r.config.PollWait
			}
			if err := sleepContext(ctx, wait); err != nil {
				return runContextError(ctx, err)
			}
			continue
		}
		inFlight.Add(1)
		select {
		case leaseCh <- resp.Lease:
		case <-ctx.Done():
			inFlight.Add(-1)
			return nil
		}
	}
}

// workerLoop drains leaseCh and executes one lease at a time per worker.
func (r *Runner) workerLoop(ctx context.Context, sessionID string, leaseCh <-chan *engine.TaskLease, inFlight *atomic.Int32, signalError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case lease := <-leaseCh:
			if lease == nil {
				return
			}
			r.executeAndReport(ctx, sessionID, lease, inFlight, signalError)
		}
	}
}

// executeAndReport runs the handler and reports the result. It closes the
// server→runner→server trace graph: the lease carries a W3C carrier injected
// at dispatch; the runner extracts the remote parent, starts an
// xflow.task.execute span, and injects a report carrier from the execute
// context so the server's commit span is properly parented.
//
// The report uses a context detached from the execute context
// (context.WithoutCancel + WithTimeout) so that cancelling the run (e.g.
// SIGTERM) does not discard a computed result the server still needs — but
// the detached context PRESERVES the SpanContext, so the report/commit trace
// is not broken. A bare context.Background() would lose the SpanContext.
func (r *Runner) executeAndReport(ctx context.Context, sessionID string, lease *engine.TaskLease, inFlight *atomic.Int32, signalError func(error)) {
	defer inFlight.Add(-1)

	// Extract the remote parent from the lease carrier (injected by the
	// control plane at dispatch). Creates an xflow.task.execute span as a
	// child of the dispatch span — same trace, remote parent.
	execCtx := tracing.ExtractCarrier(ctx, lease.TraceCarrier)
	execCtx, span := r.tracer.Start(execCtx, "xflow.task.execute",
		"execution_id", string(lease.Task.ExecutionID),
		"node_name", lease.Task.NodeName,
		"node_type", lease.NodeType,
		"attempt", lease.Attempt,
	)
	defer span.End()

	result, execErr := r.executor.Execute(execCtx, lease)
	if execErr != nil {
		result = engine.TaskResult{Error: execErr}
		span.RecordError(execErr)
	}

	// Detach from the execute context so run cancellation cannot discard the
	// result, but keep the SpanContext so the report carrier links the
	// commit span to this execute span. context.WithoutCancel preserves the
	// context.Value (incl. span) while dropping cancellation/deadline.
	detached := context.WithoutCancel(execCtx)
	reportCtx, cancel := context.WithTimeout(detached, defaultReportTimeout)
	defer cancel()

	req := protocol.ReportResultRequest{
		RunnerID:  r.config.RunnerID,
		SessionID: sessionID,
		Lease:     lease,
		Result:    result,
		// Inject the execute span context so the server's report/commit span
		// is a child of xflow.task.execute rather than a fresh root.
		TraceCarrier: tracing.InjectCarrier(reportCtx),
	}
	reportResp, err := r.client.ReportResult(reportCtx, req)
	if err != nil {
		signalError(runContextError(ctx, err))
		return
	}
	if !reportResp.Accepted {
		signalError(fmt.Errorf("task result rejected: %s", reportResp.Error))
	}
}

// heartbeatLoop sends heartbeats on its own ticker, independent of task
// execution. A heartbeat failure signals the run to exit so the caller can
// reconnect.
func (r *Runner) heartbeatLoop(ctx context.Context, sessionID string, inFlight *atomic.Int32, signalError func(error)) {
	if err := r.heartbeat(ctx, sessionID, int(inFlight.Load())); err != nil {
		signalError(err)
		return
	}
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.heartbeat(ctx, sessionID, int(inFlight.Load())); err != nil {
				signalError(err)
				return
			}
		}
	}
}

func (r *Runner) heartbeat(ctx context.Context, sessionID string, inFlight int) error {
	_, err := r.client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID:  r.config.RunnerID,
		SessionID: sessionID,
		Capacity:  r.config.Concurrency,
		InFlight:  inFlight,
		Timestamp: time.Now().Unix(),
	})
	return err
}

func runContextError(ctx context.Context, err error) error {
	if ctx.Err() == nil {
		return err
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func namespaceStrings(namespaces []namespace.Namespace) []string {
	if len(namespaces) == 0 {
		return []string{string(namespace.Default)}
	}
	out := make([]string, len(namespaces))
	for i, t := range namespaces {
		out[i] = string(t)
	}
	return out
}
