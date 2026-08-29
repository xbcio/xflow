// Package xflow server.go: the embeddable control-plane server entry point.
//
// NewServer mirrors NewLocal / NewCluster's factory-plus-Option shape but
// returns a *Server rather than an *Engine, because a server does not
// execute node handlers itself — it dispatches them to remote runners over
// the Runner Protocol. See docs/design/DEPLOYMENT-TOPOLOGIES.md.
//
// As of stage 4 (SDK convergence) Server is a thin facade over
// service/apiserver.APIServer, so an embedded SDK server exposes the same
// module surface (Runner Protocol + workflow/control API) as the standalone
// cmd/server binary. Callers that only need Handler/Start/Shutdown/IsLeader
// keep their existing code; callers that want the apiserver to host its own
// transports can use Run with the WithServerHTTPAddr / WithServerGRPCAddr /
// WithServerTLS / WithServerMetricsAddr options.
package xflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ServerConfig configures an embedded xflow control-plane server.
type ServerConfig struct {
	// RedisAddr is the Redis address for the Asynq/Redis backend. Empty means
	// an in-memory backend: single-process use, no external dependency, no
	// leader election (there is only ever one replica).
	RedisAddr string
	// RedisConfig connects to a password-protected, sentinel or cluster Redis.
	// When non-nil it takes precedence over RedisAddr.
	//
	// RedisAddr alone carries no credentials, so an embedded host whose Redis
	// requires AUTH — which is every managed Redis — has no way to reach it
	// through RedisAddr. Set Mode to distributed.RedisModeSingle for the
	// ordinary "one address plus a password" case.
	RedisConfig *distributed.RedisConfig
	// Store is an optional durable metadata store (see ClusterConfig.Store).
	Store store.Store
}

type serverConfig struct {
	auth                control.Authenticator
	logger              engine.Logger
	metrics             *metrics.Metrics
	httpAddr            string
	grpcAddr            string
	metricsAddr         string
	metricsPath         string
	tls                 *apiserver.TLSConfig
	artifacts           *store.ArtifactStore
	principalAuth       apiserver.PrincipalAuthenticator
	authorizer          apiserver.Authorizer
	auditSink           apiserver.AuditSink
	workflowAuth        apiserver.WorkflowAuthenticator
	requireWorkflowAuth bool

	tracer                   tracing.Tracer
	concurrency              int
	enableRunnerMetricsProxy bool
	runnerMetricsInterval    time.Duration
	enableManagement         bool
	supplyKeyRotation        time.Duration
	middleware               []func(http.Handler) http.Handler
}

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

// WithServerAuth installs a runner-protocol authenticator. Default accepts
// every runner (dev/MVP behavior), matching control.DisabledAuthenticator.
func WithServerAuth(auth control.Authenticator) ServerOption {
	return func(c *serverConfig) { c.auth = auth }
}

// WithServerLogger sets the logger used by the engine, dispatcher, and
// LeaseSweeper.
func WithServerLogger(l engine.Logger) ServerOption {
	return func(c *serverConfig) { c.logger = l }
}

// WithServerMetrics wires Prometheus observers into the engine and
// dispatcher. When WithServerMetricsAddr is also set, the same Metrics
// instance backs the scrape endpoint.
func WithServerMetrics(m *metrics.Metrics) ServerOption {
	return func(c *serverConfig) { c.metrics = m }
}

// WithServerHTTPAddr sets the HTTP listen address for Server.Run. When empty
// (the default) Run does not host an HTTP listener; mount Handler() into a
// host mux instead.
func WithServerHTTPAddr(addr string) ServerOption {
	return func(c *serverConfig) { c.httpAddr = addr }
}

// WithServerGRPCAddr sets the gRPC Runner Protocol listen address for
// Server.Run. When empty Run does not host a gRPC listener.
func WithServerGRPCAddr(addr string) ServerOption {
	return func(c *serverConfig) { c.grpcAddr = addr }
}

// WithServerMetricsAddr sets the Prometheus scrape listen address for
// Server.Run. Requires WithServerMetrics to also be set; otherwise the
// metrics server is skipped.
func WithServerMetricsAddr(addr string, path string) ServerOption {
	return func(c *serverConfig) {
		c.metricsAddr = addr
		c.metricsPath = path
	}
}

// WithServerTLS configures TLS material for the HTTP and gRPC listeners
// started by Server.Run. When cert is empty no TLS is applied. cert and key
// must be provided together; clientCA is optional (enables mTLS when set).
func WithServerTLS(cert, key, clientCA string) ServerOption {
	return func(c *serverConfig) {
		if cert == "" && key == "" && clientCA == "" {
			c.tls = nil
			return
		}
		c.tls = &apiserver.TLSConfig{Cert: cert, Key: key, ClientCA: clientCA}
	}
}

// WithServerArtifacts backs GET/HEAD /v1/artifacts/{digest}, the endpoint a
// runner uses to fetch script bytes it has not cached — the only practical way
// to ship a multi-megabyte wasm guest, since inlining it as a code parameter
// puts a copy of it in every queued message.
//
// The route registers only when WithServerPrincipalAuth is also set: the
// endpoint's entire authorization is "does this caller's namespace reference
// this digest", and without a principal there is no namespace to check. Build
// the store with an index (store.NewArtifactStore(p.ArtifactObjects(),
// p.ArtifactIndex())) — a nil index makes every reference check answer false,
// so the route would 404 unconditionally.
func WithServerArtifacts(as *store.ArtifactStore) ServerOption {
	return func(c *serverConfig) { c.artifacts = as }
}

// WithServerPrincipalAuth enables resource/operation-level authorization for
// the workflow, supply and artifact APIs: requests authenticate to a principal,
// the authorizer decides per operation (default-deny), and the sink records an
// append-only audit entry before each mutation.
//
// All three arrive together because apiserver.New rejects a principal
// authenticator without an authorizer or an audit sink — a missing authorizer
// would deny everything and a missing sink would leave mutations unaudited.
// Separate options would only defer that certain error to run time.
//
// apiserver.NamespaceAwareAuthorizer and apiserver.NewSQLAuditSink are the
// stock implementations; see cmd/server for the reference wiring.
func WithServerPrincipalAuth(auth apiserver.PrincipalAuthenticator, authz apiserver.Authorizer, sink apiserver.AuditSink) ServerOption {
	return func(c *serverConfig) {
		c.principalAuth = auth
		c.authorizer = authz
		c.auditSink = sink
	}
}

// WithServerWorkflowAuth guards the workflow/control API (/v1/workflows,
// /v1/executions/*) behind a bearer authenticator. Without it those routes
// accept every caller, and that API registers definitions and seeds
// executions — an unauthenticated remote code-execution surface, since a
// submitted workflow runs on every connected runner.
//
// require makes a nil authenticator a construction error rather than an open
// API. Pass it whenever the token is sourced from configuration: an env var
// that resolves to empty otherwise yields a silently open server from a config
// that reads as authenticated.
//
// WithServerPrincipalAuth supersedes this for callers that want per-operation
// authorization with audit; this is the bearer-only path, and the one to use
// when the host program already authenticates its callers upstream.
func WithServerWorkflowAuth(auth apiserver.WorkflowAuthenticator, require bool) ServerOption {
	return func(c *serverConfig) {
		c.workflowAuth = auth
		c.requireWorkflowAuth = require
	}
}

// WithServerTracer installs the OTel tracer used for HTTP middleware and
// distributed trace propagation into dispatched tasks. Without one the
// embedded server is a gap in the trace: the caller's span ends at the request
// and the runner's begins with no parent.
//
// The tracer provider's lifecycle stays with the host program — it owns a
// process-global exporter and a shutdown that must outlive the server's
// context, which is why this takes a tracer rather than provider config.
func WithServerTracer(t tracing.Tracer) ServerOption {
	return func(c *serverConfig) { c.tracer = t }
}

// WithServerConcurrency bounds how many tasks the backend dispatches at once.
// Zero (the default) leaves the backend's own default in place.
func WithServerConcurrency(n int) ServerOption {
	return func(c *serverConfig) { c.concurrency = n }
}

// WithServerRunnerMetricsProxy accepts metrics pushed by runners and merges
// them into this server's /metrics.
//
// This is for runners that cannot be scraped — a runner in another network
// domain, which is also the runner least likely to be listening on a metrics
// port. Without it their metrics are simply absent rather than reported as
// missing. Only the HTTP transport can report; a gRPC runner ignores it.
//
// The reporting cadence is a separate option: see
// WithServerRunnerMetricsInterval.
func WithServerRunnerMetricsProxy() ServerOption {
	return func(c *serverConfig) {
		c.enableRunnerMetricsProxy = true
	}
}

// WithServerRunnerMetricsInterval pushes a reporting cadence to runners on
// every heartbeat response. Zero leaves each runner on its own default;
// negative suspends reporting fleet-wide without restarting anything.
//
// Independent of WithServerRunnerMetricsProxy, because the control plane
// annotates heartbeat responses whether or not the inbox is enabled — the
// suspend case is precisely the one an operator reaches for when the proxy is
// off, and binding the two would make it unreachable.
func WithServerRunnerMetricsInterval(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.runnerMetricsInterval = d }
}

// WithServerManagement registers the read-only ops API (/v1/management/*:
// leader identity, runner status, execution lookup). Opt-in, because it is an
// operator surface rather than part of the workflow API.
func WithServerManagement() ServerOption {
	return func(c *serverConfig) { c.enableManagement = true }
}

// WithServerSupplyKeyRotation sets how often the supply transport key rotates.
// Zero (the default) adopts the apiserver's own default; negative disables
// rotation.
func WithServerSupplyKeyRotation(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.supplyKeyRotation = d }
}

// WithServerHTTPMiddleware wraps the server's HTTP handler, outermost first.
//
// The management API is the case that needs it: /v1/management/* is registered
// by its own module and does not consult the workflow authenticator, so
// enabling management without wrapping it in
// apiserver.ManagementAuthMiddleware exposes leader identity, runner status and
// execution lookup to every caller that can reach the port. Liveness probes
// (/healthz, /readyz) stay open — the middleware only guards the management
// paths.
func WithServerHTTPMiddleware(mw ...func(http.Handler) http.Handler) ServerOption {
	return func(c *serverConfig) { c.middleware = append(c.middleware, mw...) }
}

// Server is the embeddable xflow control-plane server: it accepts workflow
// submissions and dispatches node execution to remote runners over the
// Runner Protocol. It does not execute node handlers itself.
//
// Mount Handler() into a host program's own http.Server / http.ServeMux, or
// serve it directly via Run. Call Start before serving traffic and Shutdown
// when the host program is stopping.
type Server struct {
	api        *apiserver.APIServer
	supplies   store.Supplies
	artifacts  *store.ArtifactStore
	reconciler *control.AuditReconcileWorker
	// reconcileOnce keeps the worker to a single goroutine when a caller uses
	// both Start and Run, or calls either twice.
	reconcileOnce sync.Once
}

// NewServer creates an embeddable control-plane server. RedisAddr empty means
// an in-memory backend (no external dependency, single process only).
//
// The server delegates to service/apiserver.APIServer so it exposes the same
// module surface (Runner Protocol + workflow/control API) as cmd/server.
//
// Example:
//
//	srv, err := xflow.NewServer(xflow.ServerConfig{RedisAddr: "localhost:6379"})
//	if err != nil { ... }
//	if err := srv.Start(ctx); err != nil { ... }
//	mux.Handle("/xflow/", srv.Handler())
//	defer srv.Shutdown(ctx)
func NewServer(cfg ServerConfig, opts ...ServerOption) (*Server, error) {
	sc := &serverConfig{}
	for _, o := range opts {
		o(sc)
	}

	apiCfg := buildServerAPIConfig(cfg, sc)
	// The management module is registered through an apiserver Option rather
	// than a Config field, so it is forwarded here instead of in
	// buildServerAPIConfig.
	var apiOpts []apiserver.Option
	if sc.enableManagement {
		apiOpts = append(apiOpts, apiserver.WithManagement())
	}
	if len(sc.middleware) > 0 {
		apiOpts = append(apiOpts, apiserver.WithHTTPMiddleware(sc.middleware...))
	}
	api, err := apiserver.New(apiCfg, apiOpts...)
	if err != nil {
		return nil, err
	}
	return &Server{
		api:        api,
		supplies:   apiCfg.Supplies,
		artifacts:  sc.artifacts,
		reconciler: newAuditReconciler(cfg.Store, api, sc),
	}, nil
}

// newAuditReconciler builds the crash-safe audit reconcile worker, or returns
// nil when there is nothing to reconcile against.
//
// The worker settles admissions that were audited before execution and then
// lost their outcome row to a process exit — a crash between a successful
// mutation and its outcome append. It never re-executes anything: it probes
// authoritative state (the control-plane backend's StateStore) and appends the
// outcome that state implies, idempotently.
//
// It is built here rather than left to the caller because its three
// dependencies are all internal to the server: the store's reconcile
// capability, the backend's StateStore, and the leader gate. A caller who
// passed a durable audit sink and no reconciler got pending rows that nothing
// would ever settle, which is visible only by querying the audit table.
//
// nil is returned when the store is absent or does not implement the reconcile
// scans — in both cases there are no durable admissions to settle, so a worker
// would scan nothing. Callers that require one (production) should check.
func newAuditReconciler(st store.Store, api *apiserver.APIServer, sc *serverConfig) *control.AuditReconcileWorker {
	if st == nil {
		return nil
	}
	ar, ok := st.(store.AuditReconciler)
	if !ok {
		return nil
	}
	cfg := control.AuditReconcileConfig{
		Logger: sc.logger,
		// Leader-gated so only one replica scans. Idempotent appends make a
		// leader switch safe; the gate is about not doing the work N times.
		Elector: leaderGate(api.IsLeader),
	}
	if sc.metrics != nil {
		cfg.Observer = metrics.NewReconcileMetrics(sc.metrics)
	}
	return control.NewAuditReconcileWorker(ar, control.NewExecutionAuthority(api.Backend().State()), cfg)
}

// leaderGate adapts APIServer.IsLeader to control.LeaderGate without requiring
// the apiserver to implement the full elector surface.
type leaderGate func() bool

func (g leaderGate) IsLeader() bool {
	if g == nil {
		return false
	}
	return g()
}

// buildServerAPIConfig translates the SDK's config plus options into the
// apiserver config. It is separate from NewServer so the resulting posture can
// be asserted directly: several of its fields have no observable effect until a
// runner fetches a supply or an unauthenticated caller reaches a route, and a
// test that has to stand up both halves to notice a downgrade is a test that
// will not be written for the next field.
func buildServerAPIConfig(cfg ServerConfig, sc *serverConfig) apiserver.Config {
	// Resolve the supply store: cfg.Store satisfies store.Supplies when non-nil.
	var supplies store.Supplies
	if cfg.Store != nil {
		supplies = cfg.Store
	}

	return apiserver.Config{
		RedisAddr:           cfg.RedisAddr,
		RedisConfig:         cfg.RedisConfig,
		Store:               cfg.Store,
		Supplies:            supplies,
		Artifacts:           sc.artifacts,
		Auth:                sc.auth,
		WorkflowAuth:        sc.workflowAuth,
		RequireWorkflowAuth: sc.requireWorkflowAuth,
		PrincipalAuth:       sc.principalAuth,
		Authorizer:          sc.authorizer,
		AuditSink:           sc.auditSink,
		Logger:              sc.logger,
		Metrics:             sc.metrics,
		HTTPAddr:            sc.httpAddr,
		GRPCAddr:            sc.grpcAddr,
		MetricsAddr:         sc.metricsAddr,
		MetricsPath:         sc.metricsPath,
		TLS:                 sc.tls,
		Tracer:              sc.tracer,
		Concurrency:         sc.concurrency,

		EnableRunnerMetricsProxy: sc.enableRunnerMetricsProxy,
		RunnerMetricsInterval:    sc.runnerMetricsInterval,
		// Unconditional, and deliberately not an option. This encrypts supply
		// content on the server→runner hop, which is the hop that crosses a
		// network boundary and the content that carries credentials. It is
		// independent of at-rest encryption: at-rest needs a master key, this
		// does not, so there is no configuration a caller could be missing that
		// would justify leaving it off — only the chance of forgetting to turn
		// it on. cmd/server sets it the same way for the same reason.
		EnableSupplyEncryption:  true,
		SupplyKeyRotationPeriod: sc.supplyKeyRotation,
	}
}

// Reconciler returns the crash-safe audit reconcile worker, or nil when no
// durable audit store is configured (nothing to reconcile against).
//
// Start already runs it in the background, so a caller needs this only to
// assert its presence — production must not run with a durable audit sink and
// no reconciler — or to drive a sweep explicitly via ReconcileOnce.
func (s *Server) Reconciler() *control.AuditReconcileWorker { return s.reconciler }

// Handler returns the HTTP Runner Protocol + workflow submission/query API.
func (s *Server) Handler() http.Handler { return s.api.Handler() }

// Start begins dispatching queued tasks to runners and starts background
// maintenance (lease sweeping, leader election, audit reconciliation). Does not
// block.
//
// The audit reconcile worker runs here rather than being left to the caller: a
// worker that is constructed and never driven settles nothing while reading as
// wired from every angle a caller can check. It stops when ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	if err := s.api.Start(ctx); err != nil {
		return err
	}
	s.startReconciler(ctx)
	return nil
}

// Shutdown stops background maintenance and releases backend resources.
func (s *Server) Shutdown(ctx context.Context) error { return s.api.Shutdown(ctx) }

// IsLeader reports whether this Server replica currently holds leadership.
// Single-replica in-memory deployments always report true. Useful for health
// checks and observability in multi-replica Redis-backed deployments.
func (s *Server) IsLeader() bool { return s.api.IsLeader() }

// RegisterGRPC registers the Runner Protocol gRPC service onto g, matching
// the service surface exposed by cmd/server. Optional: only needed when the
// host program owns its own grpc.Server.
func (s *Server) RegisterGRPC(g *grpc.Server) { s.api.RegisterGRPC(g) }

// Run starts the server's transports (gRPC, metrics, HTTP — whichever
// addresses were configured via WithServerHTTPAddr / WithServerGRPCAddr /
// WithServerMetricsAddr) and blocks until ctx is cancelled or a listener
// fails. On exit it drains in-flight requests and tears down the control
// plane. This is the self-hosting mode for callers that do not want to wire
// Handler() into their own http.Server.
func (s *Server) Run(ctx context.Context) error {
	// apiserver.Run calls the apiserver's own Start, not this type's, so the
	// reconciler is started here too. Both entry points must drive it: the
	// self-hosting caller is the one that most resembles cmd/server, and it
	// would otherwise accumulate the same silent audit backlog.
	s.startReconciler(ctx)
	return s.api.Run(ctx)
}

// startReconciler runs the audit reconcile worker in the background, at most
// once per Server. Both Start and Run call it because neither is a superset of
// the other; Run reaches the apiserver's Start directly.
func (s *Server) startReconciler(ctx context.Context) {
	if s.reconciler == nil {
		return
	}
	s.reconcileOnce.Do(func() {
		go func() { _ = s.reconciler.Run(ctx) }()
	})
}

// AddWorkflow registers a workflow built with Workflow(...) on this server, the
// in-process equivalent of POST /v1/workflows/register. It returns the
// server-assigned workflow ID.
//
// An embedded host holds its definition as a Go value and has no HTTP client
// pointed at itself, so without this it would have to serialize the definition
// and call its own handler over loopback. Registration goes through the same
// apiserver path the HTTP route takes, so both agree on the registry key, the
// definition hash and the entry-activation derivation.
//
// When an artifact store is configured (WithServerArtifacts), ScriptFile nodes
// are resolved first: their file contents are stored and the node rewritten to
// carry an artifact_digest, which is what a runner then fetches.
//
// A workflow carrying LocalNode handlers is rejected. A Server dispatches every
// node to a remote runner and executes nothing itself, so accepting one would
// register a workflow whose local nodes have no executor anywhere — it would
// register cleanly and then stall at the first such node.
func (s *Server) AddWorkflow(ctx context.Context, wf *WorkflowBuilder) (types.WorkflowID, error) {
	return s.addWorkflow(ctx, wf, false)
}

// ReplaceWorkflow is AddWorkflow, except that a DIFFERENT definition already
// registered under the same name and version is removed first rather than
// rejected as a conflict. Re-registering an unchanged workflow is still
// idempotent and removes nothing.
//
// It is the call an embedded host makes when its workflow is built from its own
// configuration. Such a definition changes whenever the configuration does — a
// different Kafka topic, a rebuilt wasm guest, a new batch size — while its name
// and version stay put, and AddWorkflow answers that with a conflict the host
// cannot clear from inside its own process. The result is a host that fails to
// start on that boot and every boot after it, with the superseded definition
// still registered and its triggers still consuming.
//
// The replacement deactivates the old definition's entry units before removing
// it, so its triggers stop. Do not use it where several independent publishers
// share one workflow name: each would evict the others in turn.
func (s *Server) ReplaceWorkflow(ctx context.Context, wf *WorkflowBuilder) (types.WorkflowID, error) {
	return s.addWorkflow(ctx, wf, true)
}

func (s *Server) addWorkflow(ctx context.Context, wf *WorkflowBuilder, replace bool) (types.WorkflowID, error) {
	if wf == nil {
		return "", errors.New("xflow: workflow must not be nil")
	}
	if len(wf.directHandlers()) > 0 {
		return "", fmt.Errorf("xflow: workflow %q declares local node handlers, "+
			"which a control-plane Server cannot execute: register the node types on "+
			"the runner instead", wf.name)
	}
	def, err := wf.build()
	if err != nil {
		return "", err
	}
	if s.artifacts != nil {
		if err := resolveArtifacts(ctx, def, s.artifacts); err != nil {
			return "", err
		}
	}
	register := s.api.RegisterWorkflow
	if replace {
		register = s.api.ReplaceWorkflow
	}
	id, _, err := register(ctx, namespace.Namespace(def.Namespace), def)
	if err != nil {
		return "", err
	}
	return id, nil
}

// UpdateSupply writes (or replaces) supply content for a named supply node.
// Connected runners discover the change via heartbeat hints and re-fetch the
// content automatically. Namespace defaults to "default" when empty.
//
// This is the ONLY write path for supply content: HTTP PUT /v1/supplies/{name}
// is sealed (spec appendix Z.5), so an embedder is expected to wrap this call
// with its own authentication, authorization, and audit. Use
// UpdateSupplyIfMatch when a concurrent publisher must not be silently
// overwritten.
//
// Example:
//
//	srv.UpdateSupply(ctx, "", "kafka-creds", credsJSON)
func (s *Server) UpdateSupply(ctx context.Context, ns, name string, content []byte) error {
	if s.supplies == nil {
		return errors.New("xflow: supply store not configured (ServerConfig.Store is nil)")
	}
	if name == "" {
		return errors.New("xflow: supply name must not be empty")
	}
	if ns == "" {
		ns = string(namespace.Default)
	}
	_, err := s.supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace:   ns,
		Name:        name,
		Content:     content,
		ContentType: "application/json",
		UpdatedAt:   time.Now(),
		UpdatedBy:   "sdk",
	}, nil) // nil ifMatch = unconditional write
	return err
}

// GetSupply reads a supply's current content, revision, and content hash.
//
// The content comes back as PLAINTEXT: the at-rest layer decrypts on read
// (store/sqlstore/supply.go:47), and the encryption on GET /v1/supplies/{name}
// is transport encryption for runners, not storage encryption. Supply content
// can carry credentials — never log rec.Content.
//
// ns == "" is normalized to the default namespace, matching UpdateSupply. The
// two must agree: a write path that folds "" into default while the read path
// does not makes "write then read back with ns=\"\"" return not-found forever.
//
// Returns store.ErrNotFound when the namespace/name pair has never been written.
func (s *Server) GetSupply(ctx context.Context, ns, name string) (*store.SupplyResource, error) {
	if s.supplies == nil {
		return nil, errors.New("xflow: supply store not configured (ServerConfig.Store is nil)")
	}
	if name == "" {
		return nil, errors.New("xflow: supply name must not be empty")
	}
	if ns == "" {
		ns = string(namespace.Default)
	}
	return s.supplies.GetSupply(ctx, ns, name)
}

// UpdateSupplyIfMatch writes supply content under optimistic concurrency and
// returns the resulting record.
//
// ifMatch is the revision the caller believes is current, as read from
// GetSupply. A mismatch returns store.ErrRevisionConflict and leaves the stored
// content untouched. ifMatch == 0 means "create only": the write succeeds only
// when the supply does not exist yet. There is deliberately no "unconditional"
// value here — UpdateSupply is that entry point, and an unconditional write
// spelled as a zero would turn a stale read into a silent overwrite.
//
// This is the entry point for pointer flips (a supply whose content names an
// artifact digest): two operators publishing concurrently must not silently
// lose one of the two publishes.
func (s *Server) UpdateSupplyIfMatch(ctx context.Context, ns, name string, content []byte, ifMatch uint64) (*store.SupplyResource, error) {
	if s.supplies == nil {
		return nil, errors.New("xflow: supply store not configured (ServerConfig.Store is nil)")
	}
	if name == "" {
		return nil, errors.New("xflow: supply name must not be empty")
	}
	if ns == "" {
		ns = string(namespace.Default)
	}
	return s.supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace:   ns,
		Name:        name,
		Content:     content,
		ContentType: "application/json",
		UpdatedAt:   time.Now(),
		UpdatedBy:   "sdk",
	}, &ifMatch)
}
