// cmd/server is the xflow control-plane server.
//
// Responsibilities:
//   - Accept workflow submissions via HTTP/gRPC API
//   - Compile WorkflowDef into Graph IR
//   - Enqueue node tasks via TaskQueue
//   - Dispatch queued node tasks to runners via Runner Protocol
//   - Track execution lifecycle (status, completion, cancellation)
//   - Deliver signals to suspended nodes
//   - Serve query APIs (execution status, pending approvals)
//   - Reclaim expired runner leases via LeaseSweeper
//
// It does NOT execute node handlers — that is the runner's job. Redis, Asynq,
// and StateStore access stay on this side of the boundary.
//
// Transport hosting (listeners, TLS, timeouts, metrics server, graceful
// shutdown) lives in service/apiserver; this command only parses flags,
// builds the logger and authenticator, and invokes APIServer.Run.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xbcio/xflow/engine"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"go.uber.org/zap"
)

type serverConfig struct {
	addr        string
	grpcAddr    string
	redis       string
	memory      bool
	concurrency int
	// authPolicy is the path to runners.yaml. Empty means DisabledAuthenticator
	// (dev / MVP behavior).
	authPolicy string
	// authDryRun logs auth violations but lets the request proceed. Meant for
	// the rollout window between adding runners.yaml and enforcing it.
	authDryRun bool
	// apiAuthToken, when non-empty, enables BearerTokenAuth on the workflow/
	// control API (/v1/workflows, /v1/executions/*). The same token must be
	// supplied by callers in the Authorization: Bearer <token> header.
	apiAuthToken string
	// requireAPIAuth causes the server to fail to start if no workflow API
	// authenticator is configured. Use in production to prevent accidentally
	// serving the workflow API without authentication.
	requireAPIAuth bool
	// management enables the ops read-only management module (/healthz,
	// /readyz, /v1/management/*). The /v1/management/* surface is gated by
	// the management authz middleware (reuses the workflow API authenticator
	// when --api-auth-token is set); /healthz and /readyz stay open for
	// Kubernetes probes. Opt-in because the management surface exposes
	// runner directory and execution state.
	management bool
	// TLS: server cert/key enable TLS on the HTTP + gRPC listeners; presence
	// of tlsClientCA additionally requires a client cert (mTLS).
	tlsCert     string
	tlsKey      string
	tlsClientCA string
	logFormat   string
	metricsAddr string
	metricsPath string
	// traceMode is one of "disabled", "stdout", or "otlp".
	traceMode     string
	traceEndpoint string
	traceInsecure bool
}

func main() {
	cfg, err := parseServerConfig(nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := runServer(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseServerConfig(args []string) (serverConfig, error) {
	fs := flag.NewFlagSet("xflow-server", flag.ContinueOnError)
	cfg := serverConfig{addr: ":8080", concurrency: 10}
	fs.StringVar(&cfg.addr, "addr", cfg.addr, "HTTP listen address")
	fs.StringVar(&cfg.grpcAddr, "grpc-addr", "", "gRPC Runner Protocol listen address (empty disables gRPC)")
	fs.StringVar(&cfg.redis, "redis", "", "Redis address for Asynq backend")
	fs.BoolVar(&cfg.memory, "memory", false, "Use in-memory backend")
	fs.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Queue consumer concurrency")
	fs.StringVar(&cfg.authPolicy, "auth-policy", "", "Path to runners.yaml (empty = auth disabled)")
	fs.BoolVar(&cfg.authDryRun, "auth-dry-run", false, "Log auth violations but let requests through (rollout aid)")
	fs.StringVar(&cfg.apiAuthToken, "api-auth-token", "", "Static bearer token for workflow API authentication (sets Authorization: Bearer guard on /v1/workflows and /v1/executions/*)")
	fs.BoolVar(&cfg.requireAPIAuth, "require-api-auth", false, "Fail to start if no workflow API authenticator is configured (production fail-closed)")
	fs.BoolVar(&cfg.management, "management", false, "Enable ops management module (/healthz /readyz /v1/management/*); /v1/management/* gated by --api-auth-token")
	fs.StringVar(&cfg.tlsCert, "tls-cert", "", "Path to server TLS certificate (enables TLS)")
	fs.StringVar(&cfg.tlsKey, "tls-key", "", "Path to server TLS private key (required with --tls-cert)")
	fs.StringVar(&cfg.tlsClientCA, "tls-client-ca", "", "Path to CA bundle to verify runner certs (enables mTLS)")
	fs.StringVar(&cfg.logFormat, "log-format", "text", "Log format: text or json")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", "", "Prometheus metrics listen address (empty disables metrics)")
	fs.StringVar(&cfg.metricsPath, "metrics-path", "/metrics", "Prometheus metrics path")
	fs.StringVar(&cfg.traceMode, "trace", "disabled", "Tracing mode: disabled|stdout|otlp")
	fs.StringVar(&cfg.traceEndpoint, "trace-endpoint", "localhost:4317", "OTLP collector gRPC endpoint (--trace=otlp)")
	fs.BoolVar(&cfg.traceInsecure, "trace-insecure", false, "Disable TLS verification for OTLP connection")
	if args == nil {
		args = os.Args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	switch cfg.traceMode {
	case "disabled", "stdout", "otlp":
		// valid
	default:
		return serverConfig{}, fmt.Errorf("--trace must be one of: disabled|stdout|otlp")
	}
	if cfg.redis == "" {
		cfg.memory = true
	}
	return cfg, nil
}

func runServer(cfg serverConfig) error {
	logger, err := buildLogger(cfg)
	if err != nil {
		return err
	}
	auth, err := buildAuthenticator(cfg)
	if err != nil {
		return err
	}

	var m *metrics.Metrics
	if cfg.metricsAddr != "" {
		m = metrics.New()
	}

	tracer, shutdownTracing, err := tracing.NewTracerProvider(context.Background(), tracing.ProviderConfig{
		Mode:        cfg.traceMode,
		Endpoint:    cfg.traceEndpoint,
		Insecure:    cfg.traceInsecure,
		ServiceName: "xflow-server",
	})
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer shutdownTracing(context.Background())

	var workflowAuth apiserver.WorkflowAuthenticator
	if cfg.apiAuthToken != "" {
		workflowAuth = apiserver.NewBearerTokenAuth(cfg.apiAuthToken)
	}

	apiCfg := apiserver.Config{
		RedisAddr:           cfg.redis, // empty => in-memory backend
		Concurrency:         cfg.concurrency,
		Auth:                auth,
		Logger:              logger,
		Metrics:             m,
		Tracer:              tracer,
		WorkflowAuth:        workflowAuth,
		RequireWorkflowAuth: cfg.requireAPIAuth,
		HTTPAddr:            cfg.addr,
		GRPCAddr:            cfg.grpcAddr,
		MetricsAddr:         cfg.metricsAddr,
		MetricsPath:         cfg.metricsPath,
	}
	if cfg.tlsCert != "" || cfg.tlsKey != "" || cfg.tlsClientCA != "" {
		apiCfg.TLS = &apiserver.TLSConfig{Cert: cfg.tlsCert, Key: cfg.tlsKey, ClientCA: cfg.tlsClientCA}
	}

	var apiOpts []apiserver.Option
	if cfg.management {
		apiOpts = append(apiOpts, apiserver.WithManagement())
		// Gate /v1/management/* with the workflow API authenticator when
		// configured; /healthz and /readyz stay open for probes. When no
		// token is set the management surface is open (dev / behind an
		// external gateway) — log a warning so production mis-config is loud.
		if workflowAuth != nil {
			apiOpts = append(apiOpts, apiserver.WithHTTPMiddleware(apiserver.ManagementAuthMiddleware(workflowAuth)))
			log.Println("xflow-server: management module enabled; /v1/management/* gated by --api-auth-token")
		} else {
			log.Println("xflow-server: WARNING management module enabled without --api-auth-token; /v1/management/* is open (dev only)")
		}
	}

	srv, err := apiserver.New(apiCfg, apiOpts...)
	if err != nil {
		return err
	}

	// signal.NotifyContext so SIGINT/SIGTERM trigger graceful shutdown: the
	// HTTP server drains in-flight requests, gRPC GracefulStops, and the
	// control plane's background goroutines (dispatcher, lease sweeper,
	// leader election) exit cleanly instead of being killed mid-transition.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return srv.Run(ctx)
}

func buildLogger(cfg serverConfig) (engine.Logger, error) {
	var zapCfg zap.Config
	switch cfg.logFormat {
	case "", "text":
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.Encoding = "console"
	case "json":
		zapCfg = zap.NewProductionConfig()
	default:
		return nil, fmt.Errorf("--log-format must be text or json")
	}
	zapCfg.OutputPaths = []string{"stderr"}
	zapCfg.ErrorOutputPaths = []string{"stderr"}
	log, err := zapCfg.Build()
	if err != nil {
		return nil, err
	}
	return obslogger.NewZapLogger(log), nil
}

// buildAuthenticator resolves the runner-protocol authenticator from CLI
// flags. Empty --auth-policy falls back to the permissive dev default.
func buildAuthenticator(cfg serverConfig) (control.Authenticator, error) {
	if cfg.authPolicy == "" {
		if cfg.authDryRun {
			log.Println("xflow-server: --auth-dry-run has no effect without --auth-policy; auth remains disabled")
		}
		return control.DisabledAuthenticator{}, nil
	}
	store, err := control.NewFilePolicyStore(cfg.authPolicy, cfg.authDryRun)
	if err != nil {
		return nil, err
	}
	mode := "enforcing"
	if cfg.authDryRun {
		mode = "dry-run"
	}
	log.Printf("xflow-server: runner auth policy loaded from %q (%s)", cfg.authPolicy, mode)
	return store, nil
}
