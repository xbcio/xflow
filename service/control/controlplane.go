package control

import (
	"context"
	"errors"
	"fmt"
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

	lifecycleMu         sync.Mutex
	started             bool
	stopped             bool
	leaderCancel        context.CancelFunc
	sweeperCancel       context.CancelFunc
	claimRecoveryCancel context.CancelFunc
	unbind              func()
	// wg tracks the background goroutines started by Start (leader campaign,
	// sweeper, claim recovery). Shutdown cancels their contexts and then waits
	// for them to exit, bounded by the Shutdown context so a stuck goroutine
	// cannot hang shutdown.
	wg sync.WaitGroup
}

// NewControlPlane assembles a ControlPlane from cfg. It does not start any
// background goroutines or bind the queue — call Start for that.
func NewControlPlane(cfg Config) (*ControlPlane, error) {
	if cfg.Backend == nil {
		return nil, errors.New("control: Config.Backend is required")
	}

	var engOpts []engine.Option
	if cfg.Logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.Logger))
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
	var expiredLeaseReleaser ExpiredLeaseReleaser
	if r, ok := runners.(ExpiredLeaseReleaser); ok {
		expiredLeaseReleaser = r
	}
	sweeperCfg := LeaseSweeperConfig{Elector: elector, Logger: cfg.Logger, RunnerDirectory: expiredLeaseReleaser}
	if cfg.Metrics != nil {
		sweeperCfg.Observer = metrics.NewSweepMetrics(cfg.Metrics)
	}
	sweeper := NewLeaseSweeper(cfg.Backend.State(), eng, sweeperCfg)

	return &ControlPlane{
		backend:    cfg.Backend,
		eng:        eng,
		runners:    runners,
		dispatcher: dispatcher,
		httpServer: httpServer,
		grpcServer: grpcServer,
		sweeper:    sweeper,
		elector:    elector,
		logger:     cfg.Logger,
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

	return nil
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
