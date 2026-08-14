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
	"time"

	"google.golang.org/grpc"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
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
	// Store is an optional durable metadata store (see ClusterConfig.Store).
	Store store.Store
}

type serverConfig struct {
	auth          control.Authenticator
	logger        engine.Logger
	metrics       *metrics.Metrics
	httpAddr      string
	grpcAddr      string
	metricsAddr   string
	metricsPath   string
	tls           *apiserver.TLSConfig
	artifacts     *store.ArtifactStore
	principalAuth apiserver.PrincipalAuthenticator
	authorizer    apiserver.Authorizer
	auditSink     apiserver.AuditSink
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

// Server is the embeddable xflow control-plane server: it accepts workflow
// submissions and dispatches node execution to remote runners over the
// Runner Protocol. It does not execute node handlers itself.
//
// Mount Handler() into a host program's own http.Server / http.ServeMux, or
// serve it directly via Run. Call Start before serving traffic and Shutdown
// when the host program is stopping.
type Server struct {
	api       *apiserver.APIServer
	supplies  store.Supplies
	artifacts *store.ArtifactStore
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

	// Resolve the supply store: cfg.Store satisfies store.Supplies when non-nil.
	var supplies store.Supplies
	if cfg.Store != nil {
		supplies = cfg.Store
	}

	apiCfg := apiserver.Config{
		RedisAddr:     cfg.RedisAddr,
		Store:         cfg.Store,
		Supplies:      supplies,
		Artifacts:     sc.artifacts,
		Auth:          sc.auth,
		PrincipalAuth: sc.principalAuth,
		Authorizer:    sc.authorizer,
		AuditSink:     sc.auditSink,
		Logger:        sc.logger,
		Metrics:       sc.metrics,
		HTTPAddr:      sc.httpAddr,
		GRPCAddr:      sc.grpcAddr,
		MetricsAddr:   sc.metricsAddr,
		MetricsPath:   sc.metricsPath,
		TLS:           sc.tls,
	}
	api, err := apiserver.New(apiCfg)
	if err != nil {
		return nil, err
	}
	return &Server{api: api, supplies: supplies, artifacts: sc.artifacts}, nil
}

// Handler returns the HTTP Runner Protocol + workflow submission/query API.
func (s *Server) Handler() http.Handler { return s.api.Handler() }

// Start begins dispatching queued tasks to runners and starts background
// maintenance (lease sweeping, leader election). Does not block.
func (s *Server) Start(ctx context.Context) error { return s.api.Start(ctx) }

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
func (s *Server) Run(ctx context.Context) error { return s.api.Run(ctx) }

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
	id, _, err := s.api.RegisterWorkflow(ctx, namespace.Namespace(def.Namespace), def)
	if err != nil {
		return "", err
	}
	return id, nil
}

// UpdateSupply writes (or replaces) supply content for a named supply node.
// Connected runners discover the change via heartbeat hints and re-fetch the
// content automatically. Namespace defaults to "default" when empty.
//
// This is the programmatic equivalent of HTTP PUT /v1/supplies/{name} — use it
// when the server is embedded and a direct method call is simpler than an HTTP
// round-trip.
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
