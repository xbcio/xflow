package control

import (
	"context"
	"errors"
	"net/http"
	"time"

	backendasynq "github.com/xbcio/xflow/backend/asynq"
	backendmemory "github.com/xbcio/xflow/backend/memory"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// Config configures a ControlPlane.
type Config struct {
	// Backend supplies StateStore, TaskQueue, and queue binding. Required.
	Backend backend.Provider
	// Auth is the runner-protocol authenticator. Nil means DisabledAuthenticator.
	Auth Authenticator
	// Logger receives engine, dispatcher, and sweeper diagnostics. Optional.
	Logger engine.Logger
	// Metrics, when set, wires Prometheus observers into engine hooks, the
	// dispatcher, auth decisions, and the LeaseSweeper. Optional.
	Metrics *metrics.Metrics
	// PollWait overrides the long-poll wait duration returned to runners when
	// no task is available. Zero means the Server/GRPCServer default (1s).
	PollWait time.Duration
}

// ControlPlane bundles the engine, Task Dispatcher, Runner Protocol servers,
// and LeaseSweeper into a single embeddable unit with Handler()/Start()/
// Shutdown() lifecycle methods, so it can be mounted into a host program's
// own http.Server instead of only running as the cmd/server binary.
type ControlPlane struct {
	backend    backend.Provider
	eng        *engine.Engine
	runners    *RunnerPool
	dispatcher *Dispatcher
	httpServer *Server
	grpcServer *GRPCServer
	sweeper    *LeaseSweeper
	elector    backend.LeaderElector

	sweeperCancel context.CancelFunc
	unbind        func()
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
		engOpts = append(engOpts, engine.WithHooks(observability.NewMetricsHooks(cfg.Metrics)))
	}
	eng := engine.New(cfg.Backend.State(), cfg.Backend.Queue(), engOpts...)

	runners := NewRunnerPool()

	var dispatcherOpts []DispatcherOption
	if cfg.Metrics != nil {
		dispatcherOpts = append(dispatcherOpts, WithDispatcherObserver(observability.NewDispatcherMetrics(cfg.Metrics)))
	}
	dispatcher := NewDispatcher(eng, runners, dispatcherOpts...)

	var serverOpts []ServerOption
	if cfg.Auth != nil {
		serverOpts = append(serverOpts, WithAuthenticator(cfg.Auth))
	}
	if cfg.Logger != nil {
		serverOpts = append(serverOpts, WithControlLogger(cfg.Logger))
	}
	if cfg.Metrics != nil {
		serverOpts = append(serverOpts, WithAuthObserver(observability.NewAuthMetrics(cfg.Metrics)))
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
		grpcOpts = append(grpcOpts, WithGRPCAuthObserver(observability.NewAuthMetrics(cfg.Metrics)))
	}
	if cfg.PollWait > 0 {
		grpcOpts = append(grpcOpts, WithGRPCPollWait(cfg.PollWait))
	}
	grpcServer := NewGRPCServer(eng, runners, grpcOpts...)

	elector, ok := cfg.Backend.(backend.LeaderElector)
	if !ok {
		elector = backend.AlwaysLeader{}
	}
	sweeperCfg := LeaseSweeperConfig{Elector: elector, Logger: cfg.Logger}
	if cfg.Metrics != nil {
		sweeperCfg.Observer = observability.NewSweepMetrics(cfg.Metrics)
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
	}, nil
}

// Handler returns the HTTP Runner Protocol + workflow API mux. Mount it into
// a host program's own http.ServeMux/http.Server, or serve it directly.
func (cp *ControlPlane) Handler() http.Handler { return cp.httpServer.Handler() }

// GRPCServer returns the gRPC Runner Protocol implementation for hosts that
// want to register it on their own grpc.Server.
func (cp *ControlPlane) GRPCServer() runnerpb.RunnerProtocolServer { return cp.grpcServer }

// Start binds the Task Dispatcher onto the backend's queue, begins leader
// election (if the backend supports it), and starts the LeaseSweeper loop.
// It does not block.
func (cp *ControlPlane) Start(ctx context.Context) error {
	cp.unbind = cp.bindDispatcher()

	go func() { _ = cp.elector.Campaign(ctx) }()

	sweepCtx, cancel := context.WithCancel(context.Background())
	cp.sweeperCancel = cancel
	go cp.sweeper.Run(sweepCtx)

	return nil
}

// Shutdown stops the sweeper, resigns leadership (if held), and unwinds the
// backend queue binding. It attempts every step even if an earlier one
// fails, aggregating all errors encountered.
func (cp *ControlPlane) Shutdown(ctx context.Context) error {
	var errs []error
	if cp.sweeperCancel != nil {
		cp.sweeperCancel()
	}
	if err := cp.elector.Resign(ctx); err != nil {
		errs = append(errs, err)
	}
	if cp.unbind != nil {
		cp.unbind()
	}
	return errors.Join(errs...)
}

func (cp *ControlPlane) bindDispatcher() func() {
	switch b := cp.backend.(type) {
	case *backendmemory.Backend:
		return b.BindHandler(cp.dispatcher.HandleTask)
	case *backendasynq.Backend:
		return b.BindHandler(cp.eng, cp.dispatcher.HandleTask)
	default:
		return cp.backend.Bind(cp.eng)
	}
}
