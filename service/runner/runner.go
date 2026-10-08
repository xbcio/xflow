package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
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
	RunnerID string
	// InstanceUID is sent on Register; see protocol.RegisterRunnerRequest.
	InstanceUID  string
	Concurrency  int
	Labels       map[string]string
	Capabilities []protocol.Capability
	// DescriptorsJSON is the node descriptor envelope for the declared
	// Capabilities (protocol.RunnerDescriptorSchema; build it with
	// protocol.EncodeRunnerDescriptors). It is forwarded verbatim on every
	// Register; control validates and bounds it. nil reports no descriptors,
	// which clears anything an earlier session of this runner reported.
	DescriptorsJSON   json.RawMessage
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
	// GroupRuntime executes group subgraphs locally. Non-nil is what makes this
	// runner able to run a group lease; nil means a group lease falls through to
	// the handler path, which has no handler registered for the group's synthetic
	// node name.
	//
	// Setting it is necessary but not sufficient: the control plane only assigns
	// a group task to a runner advertising the group.exec.v1 feature on an
	// engine.GroupNodeType capability, so a runtime without that advertisement is
	// never reached. cmd/runner sets both together for exactly this reason.
	GroupRuntime *GroupRuntime
	// SubgraphRuntime executes map expansion batches locally. Required to accept
	// batch leases; nil means a batch lease fails rather than silently running
	// through the ordinary handler path, which has neither an Input nor a
	// registered handler for the batch's synthetic node name.
	SubgraphRuntime *SubgraphRuntime
	// ActivationTracker, when set, processes activation directives piggybacked on
	// heartbeat responses. nil means activations are ignored (passive runner).
	ActivationTracker *ActivationTracker
	// SupplyRegistry, when set, is the process-local supply cache this runner
	// reports Observed() from on every heartbeat. nil means the heartbeat never
	// carries SupplyObserved — byte-identical to a runner with no supplies.
	SupplyRegistry *supply.Registry
	// SupplyGate, when set, receives heartbeat-piggybacked supply hints
	// (HeartbeatResponse.SupplyHints) and fetches once per changed hash. nil
	// means hints are silently ignored — this runner relies solely on the
	// activation-time fetch and TTL polling for convergence, which is still
	// correct, just slower for supplies whose activation already happened.
	SupplyGate *SupplyGate
	// ArtifactCodeResolver, when set, resolves script artifacts by content-
	// addressable digest. ScriptNode calls Input.ArtifactCode(ctx, digest) at
	// Execute time; the runner wires the read-through artifact cache here.
	ArtifactCodeResolver func(ctx context.Context, digest string) ([]byte, error)
	// SupportsEncryption, when true, declares to the server that this runner
	// can receive and decrypt AES-256-GCM encrypted supply content. The server
	// responds with a SupplyKey on registration and encrypts supply GET bodies.
	SupportsEncryption bool
	// MetricsReporter, when set, ships this runner's whole Prometheus registry
	// to the server on a cadence the server can adjust (see
	// protocol.HeartbeatResponse.MetricsReportIntervalSeconds). nil means the
	// runner never reports — byte-identical behavior to before this feature, and
	// what a gRPC-transport runner always gets, since the gRPC client does not
	// implement MetricsReportClient.
	MetricsReporter *MetricsReporter
	// Renewal tunes the per-task lease renewal loop. Zero values take the
	// defaults (interval min(TTL/3, 10s), 3 consecutive transport failures
	// before the handler is cancelled). Renewal only happens when the protocol
	// client implements leaseRenewClient — the gRPC client does not.
	Renewal RenewalConfig
	// TimeoutObserver, when set, records node execution timeout/duration
	// events from the in-process executor (handler deadline fired, abandoned
	// goroutine gauge, per-invocation duration). nil leaves the executor with
	// a no-op observer so behavior is byte-identical to before this feature.
	TimeoutObserver execution.TimeoutObserver
	// LifecycleObserver, when set, receives the connection-lifecycle
	// transitions a host process needs to answer a liveness/readiness probe.
	// Run is the only exported method, so without this a host cannot tell
	// "connected and heartbeating" from "started and failing to register" —
	// both look like a process that has not returned yet. nil leaves behavior
	// byte-identical to before this feature.
	LifecycleObserver LifecycleObserver
}

// LifecycleObserver receives the runner's connection-lifecycle transitions.
// Implementations must be non-blocking: every method is called on the
// registration or heartbeat path.
type LifecycleObserver interface {
	// OnRegistered fires once per successful registration, before the
	// heartbeat loop starts. supplyKeyIssued reports whether the server
	// returned a supply encryption key, which is the only place that answer
	// is observable outside this package.
	OnRegistered(ctx context.Context, runnerID string, supplyKeyIssued bool)
	// OnHeartbeat fires for every heartbeat attempt, true for success. The
	// first call is the first beat, before the ticker starts.
	OnHeartbeat(ctx context.Context, ok bool)
	// OnSupplyGateWired fires once during assembly, reporting whether this
	// runner has a supply readiness gate at all. A runner that hosts no
	// triggers has none, and must not be held un-ready waiting for a supply
	// fetch that will never happen.
	OnSupplyGateWired(present bool)
	// OnSupplyFetch mirrors SupplyGateObserver.OnSupplyFetch. result is "ok"
	// or "error".
	OnSupplyFetch(ctx context.Context, name, result string)
}

type Runner struct {
	client            ProtocolClient
	executor          *execution.Runner
	config            Config
	tracer            tracing.Tracer
	activationTracker *ActivationTracker
	supplyRegistry    *supply.Registry
	supplyGate        *SupplyGate
	metricsReporter   *MetricsReporter
	controlGate       *runnerControlGate
	// acker sends ActivationAck for activations the tracker failed to take.
	// nil when there is no ActivationTracker configured, or the configured
	// client's transport does not support acks (e.g. the gRPC transport,
	// whose HeartbeatResponse does not carry Activations at all yet — see
	// protocol.HeartbeatResponse). A nil acker leaves failures logged locally
	// only, same as before this feature existed.
	acker *activationAcker
	// active is the set of leases this process's workers hold. It is owned by
	// the Runner, not by one Run, because a transport-error reconnect re-enters
	// Run while workers of the previous session are still executing: the
	// server rebinds those leases to the new session and replays any lease a
	// poll does not name, so a per-Run set would hand a still-running lease to
	// a second worker. The old worker's eventual report carries the old
	// session and is refused as stale; the lease then leaves this set and the
	// server's replay is a sequential redelivery of unreported work.
	active *activeLeases
	// inFlight counts leases this process's workers hold, from poll to the end
	// of the report attempt. Runner-owned like active and for the same
	// reason: a reconnect re-enters Run while the previous session's workers
	// may still be executing, and a per-Run counter would let the new session
	// claim Concurrency more leases on top of them and heartbeat an InFlight
	// that omits them.
	inFlight atomic.Int32
	// shutdownTimeout bounds Run's wait for busy workers after an operator
	// stop; a session that fails does not wait. Always
	// defaultRunnerShutdownTimeout outside tests.
	shutdownTimeout time.Duration
	// heartbeatGrace is how long heartbeats may keep failing, measured from
	// the last success (or from registration), before the session is ended.
	// Always defaultHeartbeatGrace outside tests.
	heartbeatGrace time.Duration
	// now is the clock heartbeatGrace is measured on. Always time.Now outside
	// tests.
	now func() time.Time
}

// defaultHeartbeatGrace is half the server's live window. A transient blip
// (one dropped request, a brief server restart) must not end the session:
// re-registering invalidates every lease the session's workers still run.
// Half the window leaves time to re-register before the server stops counting
// this runner live and routes its work elsewhere.
const defaultHeartbeatGrace = protocol.RunnerLiveTTL / 2

func New(client ProtocolClient, registry engine.HandlerRegistry, config Config) *Runner {
	if config.InstanceUID == "" {
		config.InstanceUID = "proc:" + uuid.NewString()
	}
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
	r := &Runner{
		client:            client,
		executor:          execution.NewRunner(registry, execution.WithResourcePool(config.ResourcePool), execution.WithCredentialResolver(config.CredentialResolver), execution.WithArtifactCodeResolver(config.ArtifactCodeResolver), execution.WithTimeoutObserver(config.TimeoutObserver)),
		config:            config,
		tracer:            tracer,
		activationTracker: config.ActivationTracker,
		supplyRegistry:    config.SupplyRegistry,
		supplyGate:        config.SupplyGate,
		metricsReporter:   config.MetricsReporter,
		controlGate:       newRunnerControlGate(),
		active:            newActiveLeases(),
		shutdownTimeout:   defaultRunnerShutdownTimeout,
		heartbeatGrace:    defaultHeartbeatGrace,
		now:               time.Now,
	}
	if config.ActivationTracker != nil {
		if ackClient, ok := client.(activationAckClient); ok {
			r.acker = newActivationAcker(ackClient, config.RunnerID, slog.Default())
		}
	}
	return r
}

// Run drives register → poll → execute → report with a pool of Concurrency
// workers and an independent heartbeat goroutine. Heartbeats are delivered on
// their own ticker so a long-running handler cannot starve them (which would
// otherwise let the server's lease sweeper reclaim and re-execute the task).
// On any transport error Run returns and the caller (cmd/runner) reconnects.
func (r *Runner) Run(ctx context.Context) error {
	// Report any activations still hosted from a prior session so a reconnect
	// renews their leases instead of orphaning them. Empty when no tracker is
	// configured or nothing is currently hosted. Carries only identity +
	// generation, never secret params.
	var inventory []protocol.ActivationInventoryItem
	if r.activationTracker != nil {
		inventory = r.activationTracker.Inventory()
	}
	registerResp, err := r.client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:           r.config.RunnerID,
		Concurrency:        r.config.Concurrency,
		Capabilities:       r.config.Capabilities,
		Labels:             r.config.Labels,
		Namespaces:         NamespaceStrings(r.config.Namespaces),
		Activations:        inventory,
		SupportsEncryption: r.config.SupportsEncryption,
		InstanceUID:        r.config.InstanceUID,
		DescriptorsJSON:    r.config.DescriptorsJSON,
	})
	if err != nil {
		return runContextError(ctx, err)
	}
	sessionID := registerResp.SessionID
	r.applyRunnerControl(registerResp.Control)

	// Install the supply encryption keyring if the server provided a key.
	if registerResp.SupplyKey != "" {
		key, keyErr := supplyenc.KeyFromBase64(registerResp.SupplyKey)
		if keyErr != nil {
			slog.Default().Warn("supply key decode failed, encryption disabled", "err", keyErr)
		} else {
			r.installSupplyKey(key)
		}
	}

	if r.config.LifecycleObserver != nil {
		r.config.LifecycleObserver.OnRegistered(ctx, r.config.RunnerID, registerResp.SupplyKey != "")
	}

	// Wire the ack callback with this session's ID now that it is known. Safe
	// to call unconditionally on every (re)connect: SetOnActivateFailed itself
	// is a plain field assignment, and this happens-before the heartbeatLoop
	// goroutine below is started, so it can never race a concurrent
	// ProcessDirectives call.
	if r.activationTracker != nil && r.acker != nil {
		r.activationTracker.SetOnActivateFailed(func(d protocol.ActivateDirective, activateErr error) {
			r.acker.ackFailed(sessionID, d, activateErr)
		})
		r.activationTracker.SetOnDeactivated(func(d protocol.DeactivateDirective) {
			r.acker.ackDeactivated(sessionID, d)
		})
	}

	inFlight := &r.inFlight
	active := r.active
	leaseCh := make(chan *engine.TaskLease, r.config.Concurrency)
	// pollCtx lets a heartbeat failure end the session. Without it the error
	// sat in errCh until polling stopped on its own, and with every worker
	// busy the poll loop is idle, so a dead session went unnoticed until a
	// slot freed. Workers keep ctx: a busy one finishes its lease, which stays
	// in the Runner-owned active set and in-flight count for the next session
	// to report.
	pollCtx, pollCancel := context.WithCancel(ctx)
	defer pollCancel()
	var errOnce sync.Once
	errCh := make(chan error, 1)
	// signalError only records the error. Workers use it: a failed or refused
	// report is about one lease (deadline backstop, sweeper reclaim, report
	// timeout), not the session, and ending the session would make the
	// re-register invalidate every sibling lease still running.
	signalError := func(err error) {
		errOnce.Do(func() {
			select {
			case errCh <- err:
			default:
			}
		})
	}
	// endSession also stops polling. Only heartbeats keep the session alive
	// server-side, so only a heartbeat failure the loop stopped tolerating
	// means the session is gone.
	endSession := func(err error) {
		signalError(err)
		pollCancel()
	}

	// Independent heartbeat goroutine — survives while workers are blocked on
	// long handlers, reflecting the true in-flight count to the server.
	heartbeatCtx, hbCancel := context.WithCancel(ctx)
	go r.heartbeatLoop(heartbeatCtx, sessionID, inFlight, endSession)

	// Metrics reporting shares heartbeatCtx: both are session-scoped, and a
	// reconnect must restart the reporter with the new sessionID rather than
	// keep shipping under a session the server has already replaced.
	if r.metricsReporter != nil {
		go r.metricsReporter.Run(heartbeatCtx, sessionID)
	}

	// Worker pool of Concurrency goroutines executing leases in parallel.
	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.workerLoop(ctx, sessionID, leaseCh, inFlight, active, signalError)
		}()
	}

	pollErr := r.pollLoop(pollCtx, sessionID, leaseCh, inFlight, active)
	hbCancel()
	// pollLoop is the only sender, so closing here is safe. It is what ends the
	// workers on a transport error, where ctx stays live for the reconnect:
	// an idle worker exits at once, a busy one finishes its current lease and
	// then exits. Without it every reconnect parked Concurrency idle workers on
	// this channel for the life of ctx and Run sat out the shutdown timeout.
	close(leaseCh)

	// Graceful shutdown: stop polling, then wait (bounded) for workers to
	// finish in-flight tasks. Workers exit on ctx cancellation or once the
	// closed leaseCh is empty.
	//
	// Only an operator stop (ctx cancelled) waits. A session that ended on an
	// error returns at once so the caller re-registers: its busy workers keep
	// running under ctx, and their leases stay in the Runner-owned active set
	// and in-flight count, so the next session reports them. Waiting here
	// would only leave the runner unregistered for up to shutdownTimeout while
	// that work runs. The skipped workers touch nothing of the next Run: each
	// Run has its own leaseCh and errCh, and they leave active and inFlight
	// when their report attempt ends, then exit on the closed leaseCh.
	drained := false
	if ctx.Err() != nil {
		waitDone := make(chan struct{})
		go func() { wg.Wait(); close(waitDone) }()
		select {
		case <-waitDone:
			drained = true
		case <-time.After(r.shutdownTimeout):
		}
	}
	releaseUnstartedLeases(leaseCh, inFlight, active)

	// Shutdown activation tracker if configured.
	if r.activationTracker != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultRunnerShutdownTimeout)
		r.activationTracker.Shutdown(shutdownCtx)
		shutdownCancel()
	}

	// Give the live session up only on a clean, operator-requested stop. A
	// transport failure is followed by a reconnect of this same instance, and
	// a drain that timed out still has workers holding leases; in both cases
	// the session must stay protected until it ages out.
	if ctx.Err() != nil && drained {
		r.deregister(sessionID)
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
func (r *Runner) pollLoop(ctx context.Context, sessionID string, leaseCh chan<- *engine.TaskLease, inFlight *atomic.Int32, active *activeLeases) error {
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
			Labels:       r.config.Labels,
			Capabilities: r.config.Capabilities,
			RecoveryOnly: r.controlGate != nil && r.controlGate.recoveryOnly(),
			// Reported every poll rather than tracked server-side: the server
			// has no way to distinguish a lease this runner is executing from
			// one it never received, and replaying the former runs the node a
			// second time while the first worker is still on it.
			ActiveLeaseIDs: active.snapshot(),
		})
		if err != nil {
			return runContextError(ctx, err)
		}
		r.applyRunnerControl(resp.Control)
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
		// Marked here, not in the worker: the lease is this runner's the moment
		// the poll returns it, and the next poll can otherwise be issued while
		// it is still in leaseCh.
		active.add(string(resp.Lease.LeaseID))
		select {
		case leaseCh <- resp.Lease:
		case <-ctx.Done():
			active.remove(string(resp.Lease.LeaseID))
			inFlight.Add(-1)
			return nil
		}
	}
}

// releaseUnstartedLeases unmarks leases pollLoop accepted but no worker took
// before the run ended. The active set outlives the run, so a lease left in it
// would be reported on every later poll and suppress the server's replay of
// work this process never started. pollLoop must have returned: it is the only
// sender on leaseCh.
func releaseUnstartedLeases(leaseCh chan *engine.TaskLease, inFlight *atomic.Int32, active *activeLeases) {
	for {
		select {
		case lease, ok := <-leaseCh:
			if !ok || lease == nil {
				return
			}
			active.remove(string(lease.LeaseID))
			inFlight.Add(-1)
		default:
			return
		}
	}
}

// workerLoop drains leaseCh and executes one lease at a time per worker. It
// returns when ctx is cancelled or once leaseCh is closed and empty.
func (r *Runner) workerLoop(ctx context.Context, sessionID string, leaseCh <-chan *engine.TaskLease, inFlight *atomic.Int32, active *activeLeases, signalError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case lease, ok := <-leaseCh:
			if !ok || lease == nil {
				return
			}
			r.executeAndReport(ctx, sessionID, lease, inFlight, active, signalError)
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
//
// While the work runs, a background loop extends the lease. The engine stamps
// every lease with its default TTL and nothing clamps a node's own timeout
// (xflow.http's options.timeout, xflow.script's params.timeout) against it, so
// without renewal a legitimately slow handler is reclaimed by the sweeper and
// redelivered to a second runner while the first is still executing it. The
// loop cancels the execute context on refusal or on repeated transport
// failure: once the server says this runner no longer owns the node, finishing
// the work would produce a second, unfenced result.
func (r *Runner) executeAndReport(ctx context.Context, sessionID string, lease *engine.TaskLease, inFlight *atomic.Int32, active *activeLeases, signalError func(error)) {
	defer inFlight.Add(-1)
	// Held until the report attempt is over, not merely until the handler
	// returns: a lease whose report never landed is genuinely unreported work
	// the server should replay, and one whose report landed is already
	// released server-side, so neither case needs it marked any longer.
	defer active.remove(string(lease.LeaseID))

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

	// The renewal loop needs its own cancel handle over the execute context so
	// a lost lease stops the handler. It is stopped unconditionally when the
	// work returns, so a finished task's lease stops being extended and the
	// sweeper can still reclaim it if the report never lands.
	if renewer, ok := r.client.(leaseRenewClient); ok && lease.LeaseToken != "" {
		var stopRenewal context.CancelFunc
		execCtx, stopRenewal = context.WithCancel(execCtx)
		defer stopRenewal()
		go renewLeaseLoop(execCtx,
			protocolLeaseRenewer{client: renewer, runnerID: r.config.RunnerID, sessionID: sessionID},
			lease, renewalExtendFor(lease), r.config.Renewal, stopRenewal)
	}

	var result engine.TaskResult
	var groupResult *engine.GroupResult

	if lease.GroupPayload != nil && r.config.GroupRuntime != nil {
		// Group task — execute on embedded group runtime.
		gr, err := r.config.GroupRuntime.Execute(execCtx, lease)
		if err != nil {
			result = engine.TaskResult{Error: err}
			span.RecordError(err)
		} else {
			groupResult = &gr
		}
	} else if lease.SubgraphPayload != nil {
		// Map batch — the work is in the payload, not in an Input, and no
		// handler is registered for the batch's synthetic node name. Fail
		// explicitly without a runtime rather than falling through to the
		// handler path, where the failure would be an opaque lookup error.
		if r.config.SubgraphRuntime == nil {
			err := fmt.Errorf("runner has no SubgraphRuntime configured for batch task %q", lease.Task.NodeName)
			result = engine.TaskResult{Error: err}
			span.RecordError(err)
		} else {
			sr, err := r.config.SubgraphRuntime.Execute(execCtx, lease)
			if err != nil {
				result = engine.TaskResult{Error: err}
				span.RecordError(err)
			} else {
				result = sr
			}
		}
	} else {
		// Normal node task — execute via the handler registry.
		var execErr error
		result, execErr = r.executor.Execute(execCtx, lease)
		if execErr != nil {
			result = engine.TaskResult{Error: execErr}
			span.RecordError(execErr)
		}
	}

	// Detach from the execute context so run cancellation cannot discard the
	// result, but keep the SpanContext so the report carrier links the
	// commit span to this execute span. context.WithoutCancel preserves the
	// context.Value (incl. span) while dropping cancellation/deadline.
	detached := context.WithoutCancel(execCtx)
	reportCtx, cancel := context.WithTimeout(detached, defaultReportTimeout)
	defer cancel()

	req := protocol.ReportResultRequest{
		RunnerID:    r.config.RunnerID,
		SessionID:   sessionID,
		Lease:       lease,
		Result:      result,
		GroupResult: groupResult,
		// Inject the execute span context so the server's report/commit span
		// is a child of xflow.task.execute rather than a fresh root.
		TraceCarrier: tracing.InjectCarrier(reportCtx),
	}
	reportResp, err := r.client.ReportResult(reportCtx, req)
	if errors.Is(err, protocol.ErrRunnerRequestTooLarge) {
		// The same body can never be accepted, and an unlanded report leaves
		// the lease finalized for the server to replay, so retrying the
		// handler would loop forever. Fail the lease permanently instead.
		span.RecordError(err)
		slog.Default().Warn("task report too large; reporting permanent failure",
			"runner_id", r.config.RunnerID, "lease_id", string(lease.LeaseID), "err", err)
		reportResp, err = r.client.ReportResult(reportCtx, oversizeReport(req, err))
	}
	if err != nil {
		signalError(runContextError(ctx, err))
		return
	}
	if !reportResp.Accepted {
		signalError(fmt.Errorf("task result rejected: %s", reportResp.Error))
	}
}

// oversizeReport replaces a report the server cannot accept with a small
// permanent failure for the same lease. The echoed lease keeps only what the
// server fences the report against (task identity, lease ID, token, attempt,
// namespace); the server commits against its own authoritative lease, so the
// dropped input and payloads are never read from the echo.
func oversizeReport(req protocol.ReportResultRequest, cause error) protocol.ReportResultRequest {
	slim := *req.Lease
	slim.Input = nil
	slim.GroupPayload = nil
	slim.SubgraphPayload = nil
	if p := slim.Task.Payload; p != nil {
		// BuildAssignmentID reads only the signal name and trigger, plus a
		// map batch's parent_lease_id.
		slim.Task.Payload = &types.SignalPayload{Triggered: p.Triggered, Name: p.Name}
		if gen, ok := p.Data["parent_lease_id"]; ok {
			slim.Task.Payload.Data = map[string]any{"parent_lease_id": gen}
		}
	}
	req.Lease = &slim
	req.GroupResult = nil
	req.Result = engine.TaskResult{Error: errors.Join(types.ErrPermanent,
		fmt.Errorf("task result too large to report: %w", cause))}
	return req
}

// observeHeartbeat forwards a heartbeat attempt's outcome to the lifecycle
// observer, when one is configured.
func (r *Runner) observeHeartbeat(ctx context.Context, ok bool) {
	if r.config.LifecycleObserver != nil {
		r.config.LifecycleObserver.OnHeartbeat(ctx, ok)
	}
}

// heartbeatLoop sends heartbeats on its own ticker, independent of task
// execution. Activation directives piggybacked on the heartbeat response are
// forwarded to the activation tracker when configured; supply hints are
// forwarded to the supply gate.
//
// A failed heartbeat is tolerated and retried on the next tick until
// heartbeatGrace has passed since the last success (or since the loop started,
// right after registration); then endSession ends the run so the caller
// re-registers. An error that says the session itself is invalid ends it at
// once, since retrying cannot revive it; see sessionInvalid for which errors
// a transport lets the runner recognize.
func (r *Runner) heartbeatLoop(ctx context.Context, sessionID string, inFlight *atomic.Int32, endSession func(error)) {
	lastOK := r.now()
	failures := 0
	// beat reports whether the loop should keep going.
	beat := func() bool {
		resp, err := r.heartbeat(ctx, sessionID, int(inFlight.Load()))
		if err != nil {
			r.observeHeartbeat(ctx, false)
			if ctx.Err() != nil {
				// The run is stopping; a heartbeat cut short by it is not a
				// session failure.
				return false
			}
			failures++
			if sessionInvalid(err) {
				endSession(err)
				return false
			}
			if since := r.now().Sub(lastOK); since > r.heartbeatGrace {
				endSession(fmt.Errorf("heartbeat failed %d times over %s: %w", failures, since.Round(time.Millisecond), err))
				return false
			}
			slog.Default().Warn("runner heartbeat failed; keeping the session",
				"runner_id", r.config.RunnerID, "consecutive_failures", failures, "err", err)
			return true
		}
		failures = 0
		lastOK = r.now()
		r.observeHeartbeat(ctx, true)
		r.applyRunnerControl(resp.Control)
		r.processActivations(ctx, resp)
		r.processSupplyHints(ctx, resp)
		r.processSupplyKeyRotation(resp)
		r.processMetricsInterval(resp)
		return true
	}
	if !beat() {
		return
	}

	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !beat() {
				return
			}
		}
	}
}

// sessionInvalid reports whether a heartbeat error means the server no longer
// accepts this session, so retrying it within heartbeatGrace is pointless:
// the runner or session is unknown, the session was replaced by a newer
// registration, or the credential was refused (revoked, expired, unknown).
//
// Only the gRPC transport lets the runner recognize these: the server maps
// them to NotFound, FailedPrecondition and Unauthenticated, and GRPCClient
// returns the status unchanged. The HTTP Client returns a plain error that
// carries the status code only in its text (404, 409, 401), and the in-process
// client returns service/control sentinels this package does not import, so
// on those transports an invalid session is retried until heartbeatGrace
// runs out like any other failure.
func sessionInvalid(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

// processActivations forwards heartbeat activation directives to the tracker.
func (r *Runner) processActivations(ctx context.Context, resp protocol.HeartbeatResponse) {
	if resp.Activations != nil && r.activationTracker != nil {
		_ = r.activationTracker.ProcessDirectives(ctx, resp.Activations)
	}
}

// processSupplyHints reacts to piggybacked supply hints. Failures are not
// propagated: a hint is an optimization and must never take a runner offline.
//
// ApplyHints does a synchronous HTTP fetch per changed hash (up to
// supplyFetchTimeout = 15s in supply_client.go), which can exceed the default
// 5s heartbeat interval. Running it inline here would delay the NEXT
// heartbeat send, and a heartbeat that arrives late enough risks the
// server-side lease/liveness window lapsing and the runner's activations
// churning for a reason that has nothing to do with the activations
// themselves. So this is fired into its own goroutine — the heartbeat loop
// moves on immediately — and SupplyGate.ApplyHints' own hintsInFlight map is
// what stops two overlapping heartbeat rounds from double-fetching the same
// name while an earlier fetch is still outstanding.
func (r *Runner) processSupplyHints(ctx context.Context, resp protocol.HeartbeatResponse) {
	if len(resp.SupplyHints) == 0 || r.supplyGate == nil {
		return
	}
	hints := resp.SupplyHints
	go r.supplyGate.ApplyHints(ctx, hints)
}

func (r *Runner) heartbeat(ctx context.Context, sessionID string, inFlight int) (protocol.HeartbeatResponse, error) {
	req := protocol.HeartbeatRequest{
		RunnerID:       r.config.RunnerID,
		SessionID:      sessionID,
		Capacity:       r.config.Concurrency,
		InFlight:       inFlight,
		Timestamp:      time.Now().Unix(),
		SupplyObserved: r.observedSupplies(),
		SupplyKeyID:    r.supplyKeyID(),
	}
	// Report the hosted activation set on every heartbeat — the same
	// tracker-derived inventory the register path sends (see Run), reused as
	// the one conversion so the two reports cannot describe the same tracker
	// differently. Freshness is what makes the report useful: the server
	// compares each report against the assignment ledger to notice a directive
	// that never arrived, and a stale report would either trigger spurious
	// redeliveries or mask a real gap. The allocation is per-heartbeat and
	// proportional to the activation count (order of ten), the same cost the
	// drain observation already pays.
	if r.activationTracker != nil {
		req.HostedActivations = &protocol.HostedActivationsReport{Activations: r.activationTracker.Inventory()}
	}
	if generation, draining := r.controlGate.drainingGeneration(); draining {
		req.DrainObservation = &protocol.RunnerDrainObservation{
			Generation:        generation,
			RecoveryOnly:      true,
			ActiveActivations: r.activationTracker.ActiveCount(),
		}
	}
	return r.client.Heartbeat(ctx, req)
}

// supplyKeyID reports which supply encryption key this runner currently holds,
// so the server can hand back a rotation only when it differs. Empty when this
// runner has no keyring — which is the plaintext path, and also what keeps the
// heartbeat body byte-identical for runners that use no supply encryption.
func (r *Runner) supplyKeyID() string {
	if r.supplyGate == nil {
		return ""
	}
	f, ok := r.supplyGate.Fetcher().(*HTTPSupplyFetcher)
	if !ok {
		return ""
	}
	return f.Keyring.CurrentKeyID()
}

// observedSupplies reports the content hashes currently in effect here. nil
// when this runner has no SupplyRegistry configured or hosts no supply, so the
// heartbeat body is unchanged for runners that consume none.
func (r *Runner) observedSupplies() map[string]string {
	if r.supplyRegistry == nil {
		return nil
	}
	return r.supplyRegistry.Observed()
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

// NamespaceStrings renders the namespaces a runner serves for the wire, where
// an empty list means the default namespace rather than "none". Exported
// because a runner's registration is not the only thing that has to spell this
// out the same way — sdk/xflow.VerifyRunner registers with the same payload,
// and a second copy of the empty-means-default rule is how a preflight that
// passes ends up describing a runner the server would route differently.
func NamespaceStrings(namespaces []namespace.Namespace) []string {
	if len(namespaces) == 0 {
		return []string{string(namespace.Default)}
	}
	out := make([]string, len(namespaces))
	for i, t := range namespaces {
		out[i] = string(t)
	}
	return out
}

// installSupplyKey installs a supply encryption key into the supply fetcher's
// keyring. Called once on registration when the server provides a key.
func (r *Runner) installSupplyKey(key *supplyenc.Key) {
	if r.supplyGate == nil {
		return
	}
	fetcher := r.supplyGate.Fetcher()
	if f, ok := fetcher.(*HTTPSupplyFetcher); ok {
		if f.Keyring == nil {
			f.Keyring = supplyenc.NewKeyring(key)
		} else {
			f.Keyring.Rotate(key)
		}
	}
}

// processSupplyKeyRotation installs a rotated supply key delivered via the
// heartbeat response. The old current key becomes the previous key in the
// keyring (allowing in-flight encrypted responses to still decrypt).
func (r *Runner) processSupplyKeyRotation(resp protocol.HeartbeatResponse) {
	if resp.SupplyKeyRotation == "" {
		return
	}
	key, err := supplyenc.KeyFromBase64(resp.SupplyKeyRotation)
	if err != nil {
		slog.Default().Warn("supply key rotation decode failed", "err", err)
		return
	}
	r.installSupplyKey(key)
}

// processMetricsInterval adopts the server's reporting cadence. The three
// states are decided entirely by the sign, and the server has already clamped
// any positive value into range — so this never validates, it only converts.
//
// Zero means "no opinion" and must NOT reach SetInterval: passing 0 would
// suspend reporting, turning every old server (which never sets the field) into
// a silent kill switch.
func (r *Runner) processMetricsInterval(resp protocol.HeartbeatResponse) {
	if r.metricsReporter == nil || resp.MetricsReportIntervalSeconds == 0 {
		return
	}
	r.metricsReporter.SetInterval(time.Duration(resp.MetricsReportIntervalSeconds) * time.Second)
}

// deregisterTimeout bounds the goodbye call. It runs after ctx is cancelled,
// on the shutdown path, so it must not hold up process exit for long; a call
// that does not make it simply leaves the session to age out.
const deregisterTimeout = 3 * time.Second

// deregisterClient is the optional protocol capability behind deregister.
// Only the HTTP client implements it; for every other transport deregister is
// a no-op.
type deregisterClient interface {
	Deregister(ctx context.Context, req protocol.DeregisterRequest) error
}

func (r *Runner) deregister(sessionID string) {
	client, ok := r.client.(deregisterClient)
	if !ok || sessionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deregisterTimeout)
	defer cancel()
	if err := client.Deregister(ctx, protocol.DeregisterRequest{RunnerID: r.config.RunnerID, SessionID: sessionID}); err != nil {
		slog.Default().Warn("runner deregister failed; session will age out", "runner_id", r.config.RunnerID, "err", err)
	}
}
