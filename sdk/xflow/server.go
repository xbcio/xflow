// Package xflow server.go: the embeddable control-plane server entry point.
//
// NewServer mirrors NewLocal / NewCluster's factory-plus-Option shape but
// returns a *Server rather than an *Engine, because a server does not
// execute node handlers itself — it dispatches them to remote runners over
// the Runner Protocol. See docs/design/DEPLOYMENT-TOPOLOGIES.md.
package xflow

import (
	"context"
	"net/http"

	backendasynq "github.com/xbcio/xflow/backend/asynq"
	backendmemory "github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
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
	auth    control.Authenticator
	logger  engine.Logger
	metrics *metrics.Metrics
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
// dispatcher.
func WithServerMetrics(m *metrics.Metrics) ServerOption {
	return func(c *serverConfig) { c.metrics = m }
}

// Server is the embeddable xflow control-plane server: it accepts workflow
// submissions and dispatches node execution to remote runners over the
// Runner Protocol. It does not execute node handlers itself.
//
// Mount Handler() into a host program's own http.Server / http.ServeMux, or
// serve it directly. Call Start before serving traffic and Shutdown when the
// host program is stopping.
type Server struct {
	cp *control.ControlPlane
}

// NewServer creates an embeddable control-plane server. RedisAddr empty means
// an in-memory backend (no external dependency, single process only).
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

	ccfg := control.Config{
		Auth:    sc.auth,
		Logger:  sc.logger,
		Metrics: sc.metrics,
	}

	if cfg.RedisAddr == "" {
		ccfg.Backend = backendmemory.New()
	} else {
		b, err := backendasynq.New(cfg.RedisAddr, cfg.Store, backendasynq.WithConsumer(false))
		if err != nil {
			return nil, err
		}
		ccfg.Backend = b
	}

	cp, err := control.NewControlPlane(ccfg)
	if err != nil {
		return nil, err
	}
	return &Server{cp: cp}, nil
}

// Handler returns the HTTP Runner Protocol + workflow submission/query API.
func (s *Server) Handler() http.Handler { return s.cp.Handler() }

// Start begins dispatching queued tasks to runners and starts background
// maintenance (lease sweeping, leader election). Does not block.
func (s *Server) Start(ctx context.Context) error { return s.cp.Start(ctx) }

// Shutdown stops background maintenance and releases backend resources.
func (s *Server) Shutdown(ctx context.Context) error { return s.cp.Shutdown(ctx) }
