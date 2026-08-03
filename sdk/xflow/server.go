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
	"net/http"
	"time"

	"google.golang.org/grpc"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
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
	auth        control.Authenticator
	logger      engine.Logger
	metrics     *metrics.Metrics
	httpAddr    string
	grpcAddr    string
	metricsAddr string
	metricsPath string
	tls         *apiserver.TLSConfig
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

// Server is the embeddable xflow control-plane server: it accepts workflow
// submissions and dispatches node execution to remote runners over the
// Runner Protocol. It does not execute node handlers itself.
//
// Mount Handler() into a host program's own http.Server / http.ServeMux, or
// serve it directly via Run. Call Start before serving traffic and Shutdown
// when the host program is stopping.
type Server struct {
	api      *apiserver.APIServer
	supplies store.Supplies
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
		RedisAddr:   cfg.RedisAddr,
		Store:       cfg.Store,
		Supplies:    supplies,
		Auth:        sc.auth,
		Logger:      sc.logger,
		Metrics:     sc.metrics,
		HTTPAddr:    sc.httpAddr,
		GRPCAddr:    sc.grpcAddr,
		MetricsAddr: sc.metricsAddr,
		MetricsPath: sc.metricsPath,
		TLS:         sc.tls,
	}
	api, err := apiserver.New(apiCfg)
	if err != nil {
		return nil, err
	}
	return &Server{api: api, supplies: supplies}, nil
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
