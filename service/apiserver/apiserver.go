package apiserver

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc"

	"github.com/xbcio/xflow/backend/distributed"
	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/store"
)

// Config configures an APIServer. Transport-facing fields (HTTPAddr, etc.)
// are declared here but only consumed in stage 2 when APIServer.Run lands.
type Config struct {
	// RedisAddr is the legacy single-node Redis address. It is used when
	// RedisConfig is nil to preserve backwards compatibility. An empty value
	// selects the in-memory backend.
	RedisAddr string
	// RedisConfig is the optional Redis HA configuration (single/sentinel/cluster).
	// When non-nil it takes precedence over RedisAddr and is passed to
	// distributed.New via WithRedisConfig.
	RedisConfig *distributed.RedisConfig
	Store       store.Store
	Concurrency int
	Auth        control.Authenticator
	Logger      engine.Logger
	Metrics     *metrics.Metrics
	// Tracer, when non-nil, enables OTel HTTP middleware and wires distributed
	// tracing through the runner dispatch/commit path. Nil means no tracing.
	Tracer tracing.Tracer
	// WorkflowAuth authenticates callers of the workflow/control API
	// (/v1/workflows, /v1/executions/*). Nil uses DisabledWorkflowAuth
	// (allow all). Set RequireWorkflowAuth to enforce that a real authenticator
	// is present; a nil WorkflowAuth with RequireWorkflowAuth=true causes New
	// to return an error.
	WorkflowAuth WorkflowAuthenticator
	// RequireWorkflowAuth causes New to return an error when WorkflowAuth is
	// nil. Use this in production deployments to prevent accidentally serving
	// the workflow API without authentication.
	RequireWorkflowAuth bool
	// PrincipalAuth, when set, enables the B3 resource/operation-level authz
	// path: the module authenticates to a Principal, runs the Authorizer
	// (default-deny), and writes append-only audit via AuditSink before each
	// handler. When nil, the module falls back to WorkflowAuth (bearer-only).
	// RequireWorkflowAuth=true with nil PrincipalAuth is allowed for backward
	// compatibility; production should set PrincipalAuth + Authorizer + Audit.
	PrincipalAuth PrincipalAuthenticator
	// Authorizer decides allow/deny per operation+resource. Defaults to
	// ScopeAuthorizer (G1 single-tenant reference) when PrincipalAuth is set.
	Authorizer Authorizer
	// AuditSink records append-only authorization/mutation events. In
	// production this must be a durable sink (SQL projection reconciled
	// against the authoritative operation receipts). Mutations fail-closed
	// when the admission audit cannot be persisted.
	AuditSink AuditSink

	// Transport configuration. Stage 1 declares but does not use these.
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string
	MetricsPath string
	TLS         *TLSConfig
}

// TLSConfig holds optional TLS material for the HTTP and gRPC listeners.
// Stage 1 declares but does not use it.
type TLSConfig struct {
	Cert     string
	Key      string
	ClientCA string
}

// APIServer is the aggregation layer over service/control.ControlPlane. In
// stage 1 it is a transparent passthrough: Handler() returns the control
// plane's HTTP handler and RegisterGRPC registers the runner protocol
// service directly. Module aggregation and transport hosting arrive in
// later stages.
type APIServer struct {
	cp         *control.ControlPlane
	ownsCP     bool
	modules    []Module
	middleware []func(http.Handler) http.Handler
	cfg        Config
	timeouts   HTTPTimeouts

	// enableManagement gates registration of the ops read-only management
	// module. It is set by WithManagement and consumed at the end of New (after
	// s.cp is guaranteed to be non-nil) so the module always receives a ready
	// ControlPlane.
	enableManagement bool
}

// New assembles an APIServer from cfg. If no ControlPlane is injected via
// WithControlPlane, New builds one from cfg via buildControlPlane and owns
// its lifecycle. New always prepends the default runner-protocol and
// workflow-control modules so the Runner Protocol and workflow/control APIs
// are wired without callers having to register them explicitly (stage 3
// moved the workflow routes out of control.Server and into the
// workflow-control module).
//
// New returns an error immediately when cfg.RequireWorkflowAuth is true but
// cfg.WorkflowAuth is nil (production fail-closed: the workflow API must not
// be left open without an authenticator).
func New(cfg Config, opts ...Option) (*APIServer, error) {
	if cfg.RequireWorkflowAuth && cfg.WorkflowAuth == nil && cfg.PrincipalAuth == nil {
		return nil, errors.New("apiserver: WorkflowAuth (or PrincipalAuth) must be configured when RequireWorkflowAuth is set")
	}
	// B3 production fail-closed: when PrincipalAuth is configured for the
	// resource/operation authz path, an Authorizer and a durable AuditSink are
	// also required. A missing authorizer would default-deny everything; a
	// missing audit sink would leave mutations unaudited.
	if cfg.PrincipalAuth != nil {
		if cfg.Authorizer == nil {
			return nil, errors.New("apiserver: PrincipalAuth requires an Authorizer (use ScopeAuthorizer for the G1 single-tenant reference)")
		}
		if cfg.AuditSink == nil {
			return nil, errors.New("apiserver: PrincipalAuth requires an AuditSink (mutations must be audited before execution)")
		}
	}

	s := &APIServer{cfg: cfg, timeouts: defaultHTTPTimeouts()}
	for _, o := range opts {
		o(s)
	}
	if s.cp == nil {
		cp, err := buildControlPlane(cfg)
		if err != nil {
			return nil, err
		}
		s.cp = cp
		s.ownsCP = true
	}

	workflowAuth := cfg.WorkflowAuth
	ctrlModule := newWorkflowControlModule(s.cp, workflowAuth, cfg.Logger)
	if cfg.PrincipalAuth != nil {
		ctrlModule.principalAuth = cfg.PrincipalAuth
		ctrlModule.authorizer = cfg.Authorizer
		ctrlModule.audit = cfg.AuditSink
	}

	// Default modules are prepended so a caller that also uses WithModule to
	// add a custom module still gets the core API surface; the default
	// modules' paths (/v1/runners/*, /v1/workflows, /v1/executions/) do not
	// overlap with each other.
	s.modules = append([]Module{
		newRunnerProtocolModule(s.cp),
		ctrlModule,
	}, s.modules...)
	// The management module is opt-in (R5): it is only registered when
	// WithManagement was passed. Registration happens here, after s.cp is
	// guaranteed non-nil, so the module never sees a nil ControlPlane even
	// when WithManagement is ordered before an injected/built ControlPlane.
	if s.enableManagement {
		mgmt := newManagementModule(s.cp)
		mgmt.metrics = cfg.Metrics
		if cfg.PrincipalAuth != nil {
			mgmt.principalAuth = cfg.PrincipalAuth
			mgmt.authorizer = cfg.Authorizer
			mgmt.audit = cfg.AuditSink
		}
		s.modules = append(s.modules, mgmt)
	}
	return s, nil
}

// buildControlPlane assembles a *control.ControlPlane from cfg. It mirrors
// cmd/server's buildControlPlane for backend selection (memory when neither
// RedisAddr nor RedisConfig is set, distributed otherwise) but does NOT start
// a metrics HTTP server — cfg.Metrics is passed through verbatim and is the
// caller's responsibility to construct.
func buildControlPlane(cfg Config) (*control.ControlPlane, error) {
	ccfg := control.Config{
		Auth:    cfg.Auth,
		Logger:  cfg.Logger,
		Metrics: cfg.Metrics,
		Tracer:  cfg.Tracer,
	}

	useRedis := cfg.RedisConfig != nil || cfg.RedisAddr != ""
	if !useRedis {
		ccfg.Backend = backendlocal.New(backendlocal.WithConcurrency(cfg.Concurrency))
	} else {
		opts := []distributed.Option{
			distributed.WithConcurrency(cfg.Concurrency),
			distributed.WithStateLogger(cfg.Logger),
			distributed.WithConsumer(true),
		}
		if cfg.Metrics != nil {
			opts = append(opts,
				distributed.WithAuditObserver(metrics.NewAuditMetrics(cfg.Metrics)),
				distributed.WithLeaseObserver(metrics.NewLeaseMetrics(cfg.Metrics)),
			)
		}
		var b *distributed.Backend
		var err error
		if cfg.RedisConfig != nil {
			opts = append(opts, distributed.WithRedisConfig(*cfg.RedisConfig))
			b, err = distributed.New("", cfg.Store, opts...)
		} else {
			b, err = distributed.New(cfg.RedisAddr, cfg.Store, opts...)
		}
		if err != nil {
			return nil, err
		}
		ccfg.Backend = b
	}

	return control.NewControlPlane(ccfg)
}

// Handler returns the HTTP handler exposing the Runner Protocol and workflow
// APIs. With no HTTPModule registered it is a transparent passthrough to the
// control plane's handler; with HTTPModules present the routes are mounted
// onto a fresh mux. Middleware is applied outermost-last so the first
// registered middleware runs first. When a Tracer is configured, OTel
// request tracing is applied as the outermost layer.
func (s *APIServer) Handler() http.Handler {
	var h http.Handler = s.cp.Handler()
	if hasHTTPModule(s.modules) {
		mux := http.NewServeMux()
		for _, m := range s.modules {
			if hm, ok := m.(HTTPModule); ok {
				hm.RegisterHTTP(mux)
			}
		}
		h = mux
	}
	for i := len(s.middleware) - 1; i >= 0; i-- {
		h = s.middleware[i](h)
	}
	if s.cfg.Tracer != nil {
		h = tracing.Middleware(s.cfg.Tracer, h)
	}
	return h
}

// RegisterGRPC registers the Runner Protocol service onto g. With no
// GRPCModule registered it registers the control plane's runner protocol
// server directly; with GRPCModules present each module owns its own
// registration.
func (s *APIServer) RegisterGRPC(g *grpc.Server) {
	if !hasGRPCModule(s.modules) {
		runnerpb.RegisterRunnerProtocolServer(g, s.cp.GRPCServer())
		return
	}
	for _, m := range s.modules {
		if gm, ok := m.(GRPCModule); ok {
			gm.RegisterGRPC(g)
		}
	}
}

// Start binds the dispatcher, begins leader election, and starts the lease
// sweeper. Transparent passthrough to the underlying ControlPlane.
func (s *APIServer) Start(ctx context.Context) error {
	return s.cp.Start(ctx)
}

// Shutdown stops the sweeper, resigns leadership, and unwinds the queue
// binding. Transparent passthrough to the underlying ControlPlane.
func (s *APIServer) Shutdown(ctx context.Context) error {
	return s.cp.Shutdown(ctx)
}

// IsLeader reports whether this replica currently holds leadership.
// Transparent passthrough to the underlying ControlPlane.
func (s *APIServer) IsLeader() bool { return s.cp.IsLeader() }

// hasHTTPModule reports whether any registered module implements HTTPModule.
func hasHTTPModule(modules []Module) bool {
	for _, m := range modules {
		if _, ok := m.(HTTPModule); ok {
			return true
		}
	}
	return false
}

// hasGRPCModule reports whether any registered module implements GRPCModule.
func hasGRPCModule(modules []Module) bool {
	for _, m := range modules {
		if _, ok := m.(GRPCModule); ok {
			return true
		}
	}
	return false
}
