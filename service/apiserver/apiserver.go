package apiserver

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/distributed"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
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
	// Supplies backs the /v1/supplies endpoints. When nil the supply module is
	// not registered at all (the routes 404). A *sqlstore.Provider satisfies it.
	Supplies    store.Supplies
	// Artifacts backs GET/HEAD /v1/artifacts/{digest}, the endpoint runners use
	// to fetch script bytes they have not cached. When nil the artifact module
	// is not registered at all (the route 404s). Build it with
	// store.NewArtifactStore(provider.ArtifactObjects(), provider.ArtifactIndex())
	// — the index is required here, since a nil index makes HasReference answer
	// false for everything and the route would 404 unconditionally.
	Artifacts   *store.ArtifactStore
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
	// ScopeAuthorizer (G1 single-namespace reference) when PrincipalAuth is set.
	Authorizer Authorizer
	// AuditSink records append-only authorization/mutation events. In
	// production this must be a durable sink (SQL projection reconciled
	// against the authoritative operation receipts). Mutations fail-closed
	// when the admission audit cannot be persisted.
	AuditSink AuditSink
	// SupplyEncryptor, when set, enables AES-256-GCM encryption of supply
	// content on GET /v1/supplies/{name} for runners that request it via the
	// Accept: application/x-xflow-encrypted header. Wired from ControlPlane.
	SupplyEncryptor SupplyContentEncryptor
	// EnableSupplyEncryption turns on AES-256-GCM encryption of supply content
	// on the wire. The key is resolved through Redis when a distributed backend
	// is configured, so every replica encrypts with the same key.
	EnableSupplyEncryption bool
	// EnableRunnerMetricsProxy lets runners in other network domains ship their
	// Prometheus registry here, and merges what they ship into this server's own
	// /metrics. Off by default; see control.Config.EnableMetricsProxy.
	EnableRunnerMetricsProxy bool

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
			return nil, errors.New("apiserver: PrincipalAuth requires an Authorizer (use ScopeAuthorizer for the G1 single-namespace reference)")
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
	ctrlModule := newWorkflowControlModule(s.cp, workflowAuth, cfg.Logger, cfg.Tracer)
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
	// The supply module registers only when BOTH a PrincipalAuthenticator and a
	// supply store are configured. There is no unauthenticated fallback: an
	// endpoint that rewrites production cleansing rules must 404 rather than
	// serve open. See module_supply.go.
	if cfg.PrincipalAuth != nil && cfg.Supplies != nil {
		sup := newSupplyModule(cfg.Supplies)
		sup.principalAuth = cfg.PrincipalAuth
		sup.authorizer = cfg.Authorizer
		sup.audit = cfg.AuditSink
		sup.encryptor = supplyEncryptorFor(cfg, s.cp)
		s.modules = append(s.modules, sup)
	}
	// The artifact module registers under the same conditions and for the same
	// reason: without a PrincipalAuthenticator there is no namespace, and
	// without a namespace the per-tenant reference check that is the endpoint's
	// entire authorization cannot run. See module_artifact.go.
	if cfg.PrincipalAuth != nil && cfg.Artifacts != nil {
		art := newArtifactModule(cfg.Artifacts)
		art.principalAuth = cfg.PrincipalAuth
		art.authorizer = cfg.Authorizer
		art.audit = cfg.AuditSink
		s.modules = append(s.modules, art)
	}
	return s, nil
}

// supplyEncryptorFor decides which SupplyContentEncryptor the supply module
// should use. An explicitly configured cfg.SupplyEncryptor always wins (tests
// set it directly and must not be overridden). Otherwise it falls back to
// cp.SupplyEncryptor(), which is populated whether cp was built internally by
// buildControlPlane or injected via WithControlPlane -- both paths must reach
// the same wiring, since the e2e harness exercises the latter.
//
// cp.SupplyEncryptor() returns a *control.SupplyEncryptor, which is nil when
// encryption was never enabled on that control plane. That nil must be
// checked on the concrete pointer type BEFORE any assignment to the
// SupplyContentEncryptor interface: assigning a nil *control.SupplyEncryptor
// to an interface variable produces a non-nil interface value (the typed-nil
// trap), which would make module_supply.go's `m.encryptor != nil` guard pass
// and then panic calling Encrypt on a nil receiver.
func supplyEncryptorFor(cfg Config, cp *control.ControlPlane) SupplyContentEncryptor {
	if cfg.SupplyEncryptor != nil {
		return cfg.SupplyEncryptor
	}
	if enc := cp.SupplyEncryptor(); enc != nil {
		return enc
	}
	return nil
}

// metricsInboxFor returns the control plane's proxied-metrics gatherer, or nil.
//
// The nil check is on the CONCRETE pointer before it becomes an interface: a
// typed nil assigned to prometheus.Gatherer yields a non-nil interface whose
// method calls would then panic inside promhttp on every scrape. Same trap
// documented at supplyEncryptorFor.
func metricsInboxFor(cp *control.ControlPlane) prometheus.Gatherer {
	if cp == nil {
		return nil
	}
	inbox := cp.MetricsInbox()
	if inbox == nil {
		return nil
	}
	return inbox
}

// entryActivationStoreTTL bounds how long an untouched EntryActivation record
// survives in the durable (Redis) store. It comfortably exceeds the reconcile
// period + lease TTL so a live-but-idle activation is never evicted between
// reconcile passes; every write refreshes it.
const entryActivationStoreTTL = 24 * time.Hour

// buildControlPlane assembles a *control.ControlPlane from cfg. It mirrors
// cmd/server's buildControlPlane for backend selection (memory when neither
// RedisAddr nor RedisConfig is set, distributed otherwise) but does NOT start
// a metrics HTTP server — cfg.Metrics is passed through verbatim and is the
// caller's responsibility to construct.
func buildControlPlane(cfg Config) (*control.ControlPlane, error) {
	ccfg := control.Config{
		Auth:                   cfg.Auth,
		Logger:                 cfg.Logger,
		Metrics:                cfg.Metrics,
		Tracer:                 cfg.Tracer,
		Supplies:               cfg.Supplies,
		EnableSupplyEncryption: cfg.EnableSupplyEncryption,
		EnableMetricsProxy:     cfg.EnableRunnerMetricsProxy,
	}

	useRedis := cfg.RedisConfig != nil || cfg.RedisAddr != ""
	if !useRedis {
		ccfg.Backend = backendlocal.New(backendlocal.WithConcurrency(cfg.Concurrency))
		// In-memory EntryActivation store so the node-generic reconciler runs and
		// register/deregister derive activations even on the single-node path.
		ccfg.EntryActivationStore = control.NewMemoryEntryActivationStore()
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
		// Redis-backed EntryActivation store so the reconciler fences seeds by
		// generation and delivers directives across replicas. The TTL bounds how
		// long an untouched activation record survives; every write refreshes it.
		ccfg.EntryActivationStore = b.NewEntryActivationStore(entryActivationStoreTTL)
	}

	// Select the workflow registry the same way the backend is selected: reuse
	// the backend provider's registry (memory registry for the in-memory path,
	// the durable one for the distributed path). control.NewControlPlane also
	// falls back to the backend registry when Config.WorkflowRegistry is nil, but
	// selecting it explicitly here keeps the apiserver's wiring self-describing.
	ccfg.WorkflowRegistry = ccfg.Backend.WorkflowRegistry()

	return control.NewControlPlane(ccfg)
}

// Handler returns the HTTP handler exposing the Runner Protocol and workflow
// APIs. With no HTTPModule registered it is a transparent passthrough to the
// control plane's handler; with HTTPModules present the routes are mounted
// onto a fresh mux. Middleware is applied outermost-last so the first
// registered middleware runs first. When a Tracer is configured, OTel
// request tracing is applied as the outermost layer.
func (s *APIServer) Handler() http.Handler {
	h := s.cp.Handler()
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

// Backend returns the control-plane backend provider. It is the authoritative
// execution-state store (engine StateStore) and, for the distributed backend,
// the Redis leader elector. Exposed so a host program (e.g. cmd/server) can
// wire leader-gated background workers — the T9 audit reconcile worker uses
// Backend().State() as its AdmissionAuthority and IsLeader() as its leader
// gate. Returns the backend even when the APIServer does not own the control
// plane (an injected one); callers type-assert the capabilities they need.
func (s *APIServer) Backend() backend.Provider { return s.cp.Backend() }

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
