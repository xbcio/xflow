package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

var (
	ErrControlPlaneStarted = errors.New("control: ControlPlane already started")
	ErrControlPlaneStopped = errors.New("control: ControlPlane already stopped")
)

// Config configures a ControlPlane.
type Config struct {
	// Backend supplies StateStore, TaskQueue, and queue binding. Required.
	Backend backend.Provider
	// RunnerDirectory overrides automatic directory selection. When nil,
	// NewControlPlane uses the backend Redis capability when available and
	// otherwise falls back to MemoryRunnerDirectory.
	RunnerDirectory RunnerDirectory
	// Auth is the runner-protocol authenticator. Nil means DisabledAuthenticator.
	Auth Authenticator
	// RequireRunnerAuth causes NewControlPlane to return an error when Auth is
	// nil (production fail-closed: the runner protocol must not be left open
	// with the permissive DisabledAuthenticator). When false the behavior is
	// unchanged for backward compatibility, but a nil Auth still emits a
	// prominent warning so an accidentally unauthenticated deployment is visible.
	RequireRunnerAuth bool
	// Logger receives engine, dispatcher, and sweeper diagnostics. Optional.
	Logger engine.Logger
	// Metrics, when set, wires Prometheus observers into engine hooks, the
	// dispatcher, auth decisions, and the LeaseSweeper. Optional.
	Metrics *metrics.Metrics
	// Tracer, when set, enables distributed tracing for the runner protocol
	// dispatch and commit path and injects W3C carriers into TaskLease.
	// Nil means no-op tracing (NoopTracer).
	Tracer tracing.Tracer
	// PollWait overrides the long-poll wait duration returned to runners when
	// no task is available. Zero means the Server/GRPCServer default (1s).
	PollWait time.Duration
	// RuntimeEvidenceBuffer, when non-nil, is wired into the internal engine as
	// a read-only evidence sink. NewControlPlane converts only this typed
	// buffer to an engine Option; it does not expose arbitrary []engine.Option.
	RuntimeEvidenceBuffer *engine.RuntimeEvidenceBuffer
	// LeaseTTL, when greater than zero, overrides the engine default lease TTL
	// (60s). This is an additive seam for deterministic A0 request-loss tests
	// that need a short TTL so the production LeaseSweeper reclaims the lease
	// synchronously. LeaseTTL == 0 preserves the existing 60s default.
	LeaseTTL time.Duration
	// EntryActivationStore, when non-nil, is the durable EntryActivation store
	// used to fence entry seeds by activation generation (spec §11.6) and to
	// drive the node-generic EntryActivationReconciler. Optional; nil disables
	// generation fencing on the seed path.
	EntryActivationStore engine.EntryActivationStore
	// WorkflowRegistry, when non-nil, is the durable registry that persists
	// compiled workflow graphs (by WorkflowID+Version). When nil, NewControlPlane
	// falls back to a registry exposed by the backend provider (if any). It backs
	// the explicit /v1/workflows/register endpoint so later tasks can resolve a
	// graph on seed and derive entry activations. Optional.
	WorkflowRegistry backend.WorkflowRegistry
}

type redisClientProvider interface {
	RedisClient() redis.Cmdable
}

func selectRunnerDirectory(cfg Config, observer RunnerClaimObserver) RunnerDirectory {
	if cfg.RunnerDirectory != nil {
		return cfg.RunnerDirectory
	}
	if provider, ok := cfg.Backend.(redisClientProvider); ok {
		if client := provider.RedisClient(); client != nil {
			return NewRedisRunnerDirectory(client, WithRedisRunnerDirectoryObserver(observer))
		}
	}
	return NewMemoryRunnerDirectory()
}

// workflowRegistryProvider is the optional backend capability that exposes a
// durable workflow registry. The distributed and local providers implement it;
// backends that do not simply leave the control-plane registry nil.
type workflowRegistryProvider interface {
	WorkflowRegistry() backend.WorkflowRegistry
}

// selectWorkflowRegistry resolves the registry the control plane exposes:
// Config.WorkflowRegistry wins when set, else the backend provider's registry
// when it exposes one, else nil.
func selectWorkflowRegistry(cfg Config) backend.WorkflowRegistry {
	if cfg.WorkflowRegistry != nil {
		return cfg.WorkflowRegistry
	}
	if provider, ok := cfg.Backend.(workflowRegistryProvider); ok {
		if reg := provider.WorkflowRegistry(); reg != nil {
			return reg
		}
	}
	return nil
}

// ControlPlane bundles the engine, Task Dispatcher, Runner Protocol servers,
// and LeaseSweeper into a single embeddable unit with Handler()/Start()/
// Shutdown() lifecycle methods, so it can be mounted into a host program's
// own http.Server instead of only running as the cmd/server binary.
type ControlPlane struct {
	backend    backend.Provider
	eng        *engine.Engine
	runners    RunnerDirectory
	dispatcher *Dispatcher
	httpServer *Server
	grpcServer *GRPCServer
	sweeper    *LeaseSweeper
	elector    backend.LeaderElector
	logger     engine.Logger

	// entryActivations is the optional durable EntryActivation store (node-generic
	// activation controller). Non-nil only when Config.EntryActivationStore is
	// provided. Used for generation fencing on seeds and lifecycle management.
	entryActivations engine.EntryActivationStore

	// entryManager translates workflow add/update/remove into desired
	// EntryActivation records. Non-nil only when Config.EntryActivationStore is
	// provided. Exposed via EntryActivationManager() for the register path.
	entryManager *EntryActivationManager

	// entryReconciler drives desired EntryActivations toward a live runner
	// assignment and produces node-generic activate/deactivate directives. Non-nil
	// only when Config.EntryActivationStore is provided. Its Run loop is launched
	// leader-gated in Start.
	entryReconciler *EntryActivationReconciler

	// workflowRegistry is the optional durable registry of compiled workflow
	// graphs. Resolved from Config.WorkflowRegistry, else from the backend
	// provider when it exposes one, else nil. Exposed via WorkflowRegistry().
	workflowRegistry backend.WorkflowRegistry

	lifecycleMu           sync.Mutex
	started               bool
	stopped               bool
	leaderCancel          context.CancelFunc
	sweeperCancel         context.CancelFunc
	claimRecoveryCancel   context.CancelFunc
	entryReconcilerCancel context.CancelFunc
	unbind                func()
	// wg tracks the background goroutines started by Start (leader campaign,
	// sweeper, claim recovery, activation controller). Shutdown cancels their
	// contexts and then waits for them to exit, bounded by the Shutdown context
	// so a stuck goroutine cannot hang shutdown.
	wg sync.WaitGroup
}

// NewControlPlane assembles a ControlPlane from cfg. It does not start any
// background goroutines or bind the queue — call Start for that.
func NewControlPlane(cfg Config) (*ControlPlane, error) {
	if cfg.Backend == nil {
		return nil, errors.New("control: Config.Backend is required")
	}
	// Runner-protocol auth fail-closed. A nil Auth falls back to the permissive
	// DisabledAuthenticator (every runner allowed). RequireRunnerAuth turns that
	// into a hard error so production cannot silently serve the runner protocol
	// unauthenticated; otherwise it is only a prominent warning (mirroring the
	// nil-directory warning in NewServer) to preserve backward compatibility.
	if cfg.Auth == nil {
		if cfg.RequireRunnerAuth {
			return nil, errors.New("control: Auth must be configured when RequireRunnerAuth is set")
		}
		if cfg.Logger != nil {
			cfg.Logger.Warn("control: runner-protocol Auth is nil; using permissive DisabledAuthenticator (all runners allowed); set Config.Auth (and RequireRunnerAuth) for production")
		} else {
			log.Printf("control: runner-protocol Auth is nil; using permissive DisabledAuthenticator (all runners allowed); set Config.Auth (and RequireRunnerAuth) for production")
		}
	}

	var engOpts []engine.Option
	if cfg.Logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.Logger))
	}
	if cfg.RuntimeEvidenceBuffer != nil {
		engOpts = append(engOpts, engine.WithRuntimeEvidenceBuffer(cfg.RuntimeEvidenceBuffer))
	}
	if cfg.LeaseTTL > 0 {
		engOpts = append(engOpts, engine.WithDefaultLeaseTTL(cfg.LeaseTTL))
	}
	if cfg.Metrics != nil {
		engOpts = append(engOpts,
			engine.WithHooks(metrics.NewMetricsHooks(cfg.Metrics)),
			engine.WithCommitObserver(metrics.NewCommitMetrics(cfg.Metrics)),
			engine.WithOutboxObserver(metrics.NewOutboxMetrics(cfg.Metrics)),
		)
	}
	eng := engine.New(cfg.Backend.State(), cfg.Backend.Queue(), engOpts...)

	var runnerClaimObserver RunnerClaimObserver
	if cfg.Metrics != nil {
		runnerClaimObserver = metrics.NewRunnerClaimMetrics(cfg.Metrics)
	}
	runners := selectRunnerDirectory(cfg, runnerClaimObserver)

	var dispatcherOpts []DispatcherOption
	if cfg.Metrics != nil {
		dispatcherOpts = append(dispatcherOpts, WithDispatcherObserver(metrics.NewDispatcherMetrics(cfg.Metrics)))
	}
	dispatcher := NewDispatcher(eng, runners, dispatcherOpts...)

	if cfg.Metrics != nil {
		node.SetScriptObserver(metrics.NewScriptMetrics(cfg.Metrics))
	}

	var serverOpts []ServerOption
	if cfg.Auth != nil {
		serverOpts = append(serverOpts, WithAuthenticator(cfg.Auth))
	}
	if cfg.Logger != nil {
		serverOpts = append(serverOpts, WithControlLogger(cfg.Logger))
	}
	if cfg.Metrics != nil {
		serverOpts = append(serverOpts, WithAuthObserver(metrics.NewAuthMetrics(cfg.Metrics)))
	}
	if cfg.Tracer != nil {
		serverOpts = append(serverOpts, WithTracer(cfg.Tracer))
	}
	if cfg.PollWait > 0 {
		serverOpts = append(serverOpts, WithHTTPPollWait(cfg.PollWait))
	}
	if cfg.EntryActivationStore != nil {
		serverOpts = append(serverOpts, WithEntryActivationStore(cfg.EntryActivationStore))
	}
	workflowRegistry := selectWorkflowRegistry(cfg)
	if workflowRegistry != nil {
		serverOpts = append(serverOpts, WithWorkflowRegistry(workflowRegistry))
	}
	httpServer := NewServer(eng, runners, serverOpts...)

	var grpcOpts []GRPCServerOption
	if cfg.Auth != nil {
		grpcOpts = append(grpcOpts, WithGRPCAuthenticator(cfg.Auth))
	}
	if cfg.Logger != nil {
		grpcOpts = append(grpcOpts, WithGRPCLogger(cfg.Logger))
	}
	if cfg.Metrics != nil {
		grpcOpts = append(grpcOpts, WithGRPCAuthObserver(metrics.NewAuthMetrics(cfg.Metrics)))
	}
	if cfg.Tracer != nil {
		grpcOpts = append(grpcOpts, WithGRPCTracer(cfg.Tracer))
	}
	if cfg.PollWait > 0 {
		grpcOpts = append(grpcOpts, WithGRPCPollWait(cfg.PollWait))
	}
	grpcServer := NewGRPCServer(eng, runners, grpcOpts...)

	elector, ok := cfg.Backend.(backend.LeaderElector)
	if !ok {
		elector = backend.AlwaysLeader{}
	}
	sweeperCfg := LeaseSweeperConfig{Elector: elector, Logger: cfg.Logger, RunnerDirectory: runners}
	if cfg.Metrics != nil {
		sweeperCfg.Observer = metrics.NewSweepMetrics(cfg.Metrics)
	}
	sweeper := NewLeaseSweeper(cfg.Backend.State(), eng, sweeperCfg)

	// Node-generic entry-activation controller: optional, created only when an
	// EntryActivationStore is provided. The manager writes desired-state on
	// workflow register/deregister; the reconciler assigns live runners, fences
	// stale/removed owners, and produces the activate/deactivate directives the
	// heartbeat handler piggybacks. Both HTTP and gRPC Core instances get the
	// reconciler so heartbeats on either transport deliver directives. The
	// reconciler's Run loop is launched leader-gated in Start.
	var entryManager *EntryActivationManager
	var entryReconciler *EntryActivationReconciler
	if cfg.EntryActivationStore != nil {
		entryManager = NewEntryActivationManager(cfg.EntryActivationStore)
		entrySelector := DefaultRunnerSelector()
		recCfg := EntryActivationReconcilerConfig{
			Store:    cfg.EntryActivationStore,
			Selector: &entrySelector,
			Logger:   cfg.Logger,
		}
		// The reconciler enumerates live runners via ActivationRunnerLister. The
		// runner directory supplies it when it implements the capability; a
		// directory that does not simply yields no live runners (fail-closed: the
		// reconciler leaves activations unassigned rather than misplacing them).
		if lister, ok := runners.(ActivationRunnerLister); ok {
			recCfg.Lister = lister
		}
		entryReconciler = NewEntryActivationReconciler(recCfg)
		httpServer.core.entryReconciler = entryReconciler
		grpcServer.core.entryReconciler = entryReconciler
	}

	return &ControlPlane{
		backend:          cfg.Backend,
		eng:              eng,
		runners:          runners,
		dispatcher:       dispatcher,
		httpServer:       httpServer,
		grpcServer:       grpcServer,
		sweeper:          sweeper,
		elector:          elector,
		logger:           cfg.Logger,
		entryActivations: cfg.EntryActivationStore,
		entryManager:     entryManager,
		entryReconciler:  entryReconciler,
		workflowRegistry: workflowRegistry,
	}, nil
}

// Handler returns the HTTP Runner Protocol + workflow API mux. Mount it into
// a host program's own http.ServeMux/http.Server, or serve it directly.
func (cp *ControlPlane) Handler() http.Handler { return cp.httpServer.Handler() }

// GRPCServer returns the gRPC Runner Protocol implementation for hosts that
// want to register it on their own grpc.Server.
func (cp *ControlPlane) GRPCServer() runnerpb.RunnerProtocolServer { return cp.grpcServer }

// RunnerHTTPHandler returns the runner-protocol HTTP adapter for module-based
// route mounting (the apiserver runner-protocol module delegates to it).
func (cp *ControlPlane) RunnerHTTPHandler() protocol.RunnerHTTPHandler {
	return cp.httpServer
}

// Engine returns the engine facade for control API modules (submit/invoke/
// inspect/signal/revoke-signal/cancel). The *engine.Engine satisfies
// control.EngineFacade, so no adapter is required.
func (cp *ControlPlane) Engine() EngineFacade { return cp.eng }

// SeedExecutionFromEntry admits an entry-unit seed through the control Core so
// the server-side namespace resolution AND generation fence (spec §11.6) always
// apply — the apiserver seed module MUST route through this rather than calling
// the raw engine, otherwise a forged/stale-generation seed would fail open. The
// authoritative namespace is taken from ctx (injected by the authz wrapper), not
// the request body.
func (cp *ControlPlane) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	return cp.httpServer.core.SeedExecutionFromEntry(ctx, req)
}

// EntryActivationStore returns the durable EntryActivation store, or nil when
// none was configured. Used by lifecycle wiring to create/fence/deactivate
// activations on workflow add/update/remove.
func (cp *ControlPlane) EntryActivationStore() engine.EntryActivationStore {
	return cp.entryActivations
}

// EntryActivationManager returns the node-generic entry-activation manager, or
// nil when no EntryActivationStore is configured. The apiserver register/
// deregister handlers use it to derive/clear desired activations on workflow
// add/update/remove. Callers MUST nil-guard: an embedded/in-process control
// plane without an activation store returns nil.
func (cp *ControlPlane) EntryActivationManager() *EntryActivationManager {
	return cp.entryManager
}

// WorkflowRegistry returns the durable registry of compiled workflow graphs, or
// nil when none was configured. The apiserver register/deregister handlers use
// it to persist/remove compiled graphs; later tasks resolve a graph on seed to
// derive entry activations.
func (cp *ControlPlane) WorkflowRegistry() backend.WorkflowRegistry {
	return cp.workflowRegistry
}

// RunnerDirectory exposes the runner directory for management/observability
// modules. It is intended for read-only single-runner lookup (the directory
// interface has no list API), so management endpoints can answer
// /v1/management/runners/{id} without re-implementing directory access.
func (cp *ControlPlane) RunnerDirectory() RunnerDirectory { return cp.runners }

// Backend exposes the backend provider for management modules that need a
// capability of the StateStore beyond the engine facade — e.g. the
// DeadLetterStore capability for the dead-letter list/replay management API.
// It is intended for read-only capability checks; the StateStore remains the
// authoritative execution state.
func (cp *ControlPlane) Backend() backend.Provider { return cp.backend }

// Sweeper exposes the production LeaseSweeper for tests and admin tooling that
// need to drive a synchronous sweep without waiting for the background loop.
// It mirrors the read-only accessor pattern of RunnerDirectory() and Backend().
func (cp *ControlPlane) Sweeper() *LeaseSweeper { return cp.sweeper }

// EntryActivationReconciler exposes the node-generic entry-activation reconciler,
// or nil when no EntryActivationStore is configured. It mirrors Sweeper(): a
// read-only seam so integration tests can drive a single deterministic reconcile
// pass (assign/renew/revoke + directive enqueue) without waiting for the
// leader-gated background loop. Production code uses the internal Run loop.
func (cp *ControlPlane) EntryActivationReconciler() *EntryActivationReconciler {
	return cp.entryReconciler
}

// Start binds the Task Dispatcher onto the backend's queue, begins leader
// election (if the backend supports it), and starts the LeaseSweeper loop.
// It does not block.
func (cp *ControlPlane) Start(ctx context.Context) error {
	cp.lifecycleMu.Lock()
	if cp.started {
		cp.lifecycleMu.Unlock()
		return ErrControlPlaneStarted
	}
	if cp.stopped {
		cp.lifecycleMu.Unlock()
		return ErrControlPlaneStopped
	}
	cp.started = true
	cp.lifecycleMu.Unlock()

	unbind, err := cp.bindDispatcher()
	if err != nil {
		cp.lifecycleMu.Lock()
		cp.started = false
		cp.lifecycleMu.Unlock()
		return err
	}
	cp.unbind = unbind

	leaderCtx, leaderCancel := context.WithCancel(ctx)
	cp.leaderCancel = leaderCancel
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.runLeaderCampaign(leaderCtx)
	}()

	sweepCtx, cancel := context.WithCancel(context.Background())
	cp.sweeperCancel = cancel
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.sweeper.Run(sweepCtx)
	}()

	if reclaimer, ok := cp.runners.(ClaimReclaimer); ok {
		claimCtx, claimCancel := context.WithCancel(context.Background())
		cp.claimRecoveryCancel = claimCancel
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.runClaimRecovery(claimCtx, reclaimer)
		}()
	}

	// Launch the node-generic entry-activation reconcile loop. It ticks every
	// ReconcilePeriod and is leader-gated: only the elected leader assigns,
	// fences, renews, and produces directives. A non-leader replica skips the
	// pass entirely.
	if cp.entryReconciler != nil {
		recCtx, recCancel := context.WithCancel(context.Background())
		cp.entryReconcilerCancel = recCancel
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.runEntryReconciler(recCtx)
		}()
	}

	return nil
}

// runEntryReconciler drives the node-generic entry-activation reconcile loop
// until ctx is cancelled. It is leader-gated: a non-leader replica skips the
// pass so only one replica assigns/fences activations at a time.
func (cp *ControlPlane) runEntryReconciler(ctx context.Context) {
	ticker := time.NewTicker(DefaultEntryActivationReconcilePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !cp.elector.IsLeader() {
				continue
			}
			if err := cp.entryReconciler.Reconcile(ctx, time.Now()); err != nil && ctx.Err() == nil {
				if cp.logger != nil {
					cp.logger.Error("entry activation reconcile failed", "err", err)
				}
			}
		}
	}
}

func (cp *ControlPlane) runClaimRecovery(ctx context.Context, reclaimer ClaimReclaimer) {
	const interval = time.Second
	recover := func() {
		if err := reclaimer.ReclaimExpiredClaims(ctx); err != nil && ctx.Err() == nil && cp.logger != nil {
			cp.logger.Error("recover expired runner claims failed", "err", err)
		}
	}
	recover()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recover()
		}
	}
}

// IsLeader reports whether this ControlPlane replica currently holds
// leadership. Backends without real leader election (e.g. memory) always
// report true via backend.AlwaysLeader. Useful for health checks and
// observability in multi-replica deployments.
func (cp *ControlPlane) IsLeader() bool { return cp.elector.IsLeader() }

func (cp *ControlPlane) runLeaderCampaign(ctx context.Context) {
	const retryDelay = time.Second
	notify := cp.elector.Notify()
	for {
		if err := cp.elector.Campaign(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				if cp.logger != nil {
					cp.logger.Info("leader campaign stopped", "err", err)
				}
				return
			}
			if cp.logger != nil {
				cp.logger.Error("leader campaign failed", "err", err)
			}
			if err := sleepWithContext(ctx, retryDelay); err != nil {
				return
			}
			continue
		}

		for {
			select {
			case <-ctx.Done():
				return
			case isLeader := <-notify:
				if !isLeader {
					goto recampaign
				}
			}
		}

	recampaign:
	}
}

// Shutdown stops the sweeper, resigns leadership (if held), and unwinds the
// backend queue binding. It attempts every step even if an earlier one
// fails, aggregating all errors encountered.
func (cp *ControlPlane) Shutdown(ctx context.Context) error {
	var errs []error
	cp.lifecycleMu.Lock()
	cp.started = false
	cp.stopped = true
	cp.lifecycleMu.Unlock()

	if cp.sweeperCancel != nil {
		cp.sweeperCancel()
	}
	if cp.claimRecoveryCancel != nil {
		cp.claimRecoveryCancel()
	}
	if cp.entryReconcilerCancel != nil {
		cp.entryReconcilerCancel()
	}
	if cp.leaderCancel != nil {
		cp.leaderCancel()
	}
	// Wait for the background goroutines to observe their cancelled contexts
	// and return, but bound the wait by ctx so a stuck goroutine cannot hang
	// shutdown. sweeper.Run exits when its sleepFunc returns ctx.Err(); the
	// leader and claim-recovery loops select on ctx.Done().
	waitDone := make(chan struct{})
	go func() { cp.wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-ctx.Done():
	}
	if err := cp.elector.Resign(ctx); err != nil {
		errs = append(errs, err)
	}
	if cp.unbind != nil {
		cp.unbind()
	}
	return errors.Join(errs...)
}

// bindDispatcher wires the control-plane dispatcher's task handler into the
// backend's queue/transport via the TaskHandlerBinder capability. Backends
// that do not implement TaskHandlerBinder cannot serve a control plane —
// falling back to Provider.Bind would run the embedded execution dispatcher
// in-process (silently executing handlers inside the server instead of
// dispatching to remote runners), so we fail closed with a configuration
// error instead.
func (cp *ControlPlane) bindDispatcher() (func(), error) {
	binder, ok := cp.backend.(backend.TaskHandlerBinder)
	if !ok {
		return nil, fmt.Errorf("control: backend %T does not implement backend.TaskHandlerBinder; this backend cannot serve a control plane (configure a local or distributed backend)", cp.backend)
	}
	return binder.BindTaskHandler(cp.eng, cp.dispatcher.HandleTask)
}
