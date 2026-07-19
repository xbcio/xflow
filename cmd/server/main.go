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
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"go.uber.org/zap"
)

type serverConfig struct {
	addr        string
	grpcAddr    string
	redis       string
	memory      bool
	concurrency int
	// redisMode selects the Redis deployment topology: single (default),
	// sentinel, or cluster.
	redisMode string
	// redisSentinelMaster is the name of the Redis master monitored by sentinels.
	redisSentinelMaster string
	// redisClusterAddrs is a comma-separated list of Redis cluster node addresses.
	redisClusterAddrs string
	// redisSentinelAddrs is a comma-separated list of Sentinel node addresses.
	redisSentinelAddrs string
	redisUsername      string
	redisPassword      string
	redisDB            int
	redisTLS           bool
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
	traceSampler  string
	traceRatio    float64
	traceBaggage  bool
	// mysqlDSN, when non-empty, opens a MySQL-backed store.Store for durable
	// execution state AND a durable SQL audit sink (replaces the in-memory
	// audit projection). Empty keeps the in-memory store + in-memory audit
	// (dev / single-process preview only).
	mysqlDSN string
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
	fs.StringVar(&cfg.redis, "redis", "", "Redis address for Asynq backend (single-node; legacy compatible)")
	fs.StringVar(&cfg.redisMode, "redis-mode", "", "Redis deployment mode: single|sentinel|cluster (default single)")
	fs.StringVar(&cfg.redisSentinelMaster, "redis-sentinel-master", "", "Redis sentinel master name (required for --redis-mode=sentinel)")
	fs.StringVar(&cfg.redisClusterAddrs, "redis-cluster-addrs", "", "Comma-separated Redis cluster node addresses (required for --redis-mode=cluster)")
	fs.StringVar(&cfg.redisSentinelAddrs, "redis-sentinel-addrs", "", "Comma-separated Redis sentinel node addresses (required for --redis-mode=sentinel)")
	fs.StringVar(&cfg.redisUsername, "redis-username", "", "Redis username (ACL)")
	fs.StringVar(&cfg.redisPassword, "redis-password", "", "Redis password")
	fs.IntVar(&cfg.redisDB, "redis-db", 0, "Redis logical database (single/sentinel)")
	fs.BoolVar(&cfg.redisTLS, "redis-tls", false, "Enable TLS for Redis connections")
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
	fs.StringVar(&cfg.traceSampler, "trace-sampler", "parentbased", "OTel sampler: parentbased|always_on|always_off|traceidratio")
	fs.Float64Var(&cfg.traceRatio, "trace-ratio", 1.0, "Sampling ratio for --trace-sampler=traceidratio, in [0,1]")
	fs.BoolVar(&cfg.traceBaggage, "trace-baggage", false, "Propagate W3C baggage in addition to tracecontext (opt-in; bound accepted keys)")
	fs.StringVar(&cfg.mysqlDSN, "mysql-dsn", "", "MySQL DSN for durable execution state + durable SQL audit sink (parseTime=true required). Empty = in-memory store + in-memory audit (dev only)")
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
	switch cfg.redisMode {
	case "", distributed.RedisModeSingle, distributed.RedisModeSentinel, distributed.RedisModeCluster:
		// valid
	default:
		return serverConfig{}, fmt.Errorf("--redis-mode must be one of: single|sentinel|cluster")
	}
	// Backwards compatibility: when no Redis address and no HA topology flags
	// are provided, default to the in-memory backend.
	haConfigured := cfg.redisMode != "" || cfg.redisSentinelAddrs != "" || cfg.redisClusterAddrs != ""
	if cfg.redis == "" && !haConfigured {
		cfg.memory = true
	}
	return cfg, nil
}

// buildRedisConfig assembles a distributed.RedisConfig from CLI flags. It
// returns nil when the legacy single-address path should be used, preserving
// backwards compatibility for deployments that only pass --redis. It is
// fail-closed: invalid mode/address combinations return an error.
func buildRedisConfig(cfg serverConfig) (*distributed.RedisConfig, error) {
	mode := cfg.redisMode
	if mode == "" {
		mode = distributed.RedisModeSingle
	}

	// If only --redis is provided (no HA-specific flags), keep the legacy
	// RedisAddr path so existing deployments are untouched.
	haConfigured := cfg.redisMode != "" ||
		cfg.redisSentinelAddrs != "" ||
		cfg.redisClusterAddrs != "" ||
		cfg.redisSentinelMaster != "" ||
		cfg.redisUsername != "" ||
		cfg.redisPassword != "" ||
		cfg.redisDB != 0 ||
		cfg.redisTLS

	switch mode {
	case distributed.RedisModeSingle:
		if !haConfigured {
			return nil, nil
		}
		if cfg.redis == "" {
			return nil, fmt.Errorf("--redis is required for --redis-mode=single")
		}
		rc := &distributed.RedisConfig{
			Mode:     distributed.RedisModeSingle,
			Addrs:    []string{cfg.redis},
			Username: cfg.redisUsername,
			Password: cfg.redisPassword,
			DB:       cfg.redisDB,
		}
		if cfg.redisTLS {
			rc.TLSConfig = &tls.Config{}
		}
		return rc, nil

	case distributed.RedisModeSentinel:
		if cfg.redisSentinelMaster == "" {
			return nil, fmt.Errorf("--redis-sentinel-master is required for --redis-mode=sentinel")
		}
		if cfg.redisSentinelAddrs == "" {
			return nil, fmt.Errorf("--redis-sentinel-addrs is required for --redis-mode=sentinel")
		}
		rc := &distributed.RedisConfig{
			Mode:             distributed.RedisModeSentinel,
			Addrs:            splitAddrs(cfg.redisSentinelAddrs),
			MasterName:       cfg.redisSentinelMaster,
			Username:         cfg.redisUsername,
			Password:         cfg.redisPassword,
			SentinelUsername: cfg.redisUsername,
			SentinelPassword: cfg.redisPassword,
			DB:               cfg.redisDB,
		}
		if cfg.redisTLS {
			rc.TLSConfig = &tls.Config{}
		}
		return rc, nil

	case distributed.RedisModeCluster:
		if cfg.redisClusterAddrs == "" {
			return nil, fmt.Errorf("--redis-cluster-addrs is required for --redis-mode=cluster")
		}
		rc := &distributed.RedisConfig{
			Mode:     distributed.RedisModeCluster,
			Addrs:    splitAddrs(cfg.redisClusterAddrs),
			Username: cfg.redisUsername,
			Password: cfg.redisPassword,
		}
		if cfg.redisTLS {
			rc.TLSConfig = &tls.Config{}
		}
		return rc, nil

	default:
		return nil, fmt.Errorf("unsupported redis mode %q", mode)
	}
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
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
		Sampler:     tracing.SamplerMode(cfg.traceSampler),
		SampleRatio: cfg.traceRatio,
		Baggage:     cfg.traceBaggage,
	})
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer shutdownTracing(context.Background())

	var workflowAuth apiserver.WorkflowAuthenticator
	var principalAuth apiserver.PrincipalAuthenticator
	if cfg.apiAuthToken != "" {
		workflowAuth = apiserver.NewBearerTokenAuth(cfg.apiAuthToken)
		// B3: map the static token to a principal with the G1 single-tenant
		// operator scopes so resource/operation authz + audit are enforced.
		// The subject is server-configured; callers cannot self-report it.
		principalAuth = apiserver.NewBearerPrincipalAuth(cfg.apiAuthToken, "xflow-operator",
			[]string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.write"})
	}
	// G1 audit projection. When --mysql-dsn is set, a durable SQL sink is the
	// authoritative audit target (admission audit persisted before mutations,
	// fail-closed on sink error). Without MySQL, the in-memory sink is the
	// G0/prod-preview projection and is NOT authoritative — production must
	// configure --mysql-dsn. See docs/design/RELEASE-GATES.md §4.
	var sqlStore store.Store
	var audit apiserver.AuditSink
	if cfg.mysqlDSN != "" {
		p, err := mysqlstore.New(cfg.mysqlDSN)
		if err != nil {
			return fmt.Errorf("open mysql store: %w", err)
		}
		sqlStore = p
		audit = apiserver.NewSQLAuditSink(p)
		log.Println("xflow-server: durable SQL store + audit sink enabled (MySQL)")
	} else {
		audit = apiserver.NewInMemoryAuditSink()
		log.Println("xflow-server: WARNING --mysql-dsn not set; using in-memory store + in-memory audit (dev only; not production)")
	}

	redisConfig, err := buildRedisConfig(cfg)
	if err != nil {
		return fmt.Errorf("redis config: %w", err)
	}
	if cfg.memory {
		log.Println("xflow-server: using in-memory backend")
	} else if redisConfig != nil {
		log.Printf("xflow-server: using distributed backend (mode=%s addrs=%d master=%q tls=%v db=%d)", redisConfig.Mode, len(redisConfig.Addrs), redisConfig.MasterName, redisConfig.TLSConfig != nil, redisConfig.DB)
	} else if cfg.redis != "" {
		log.Printf("xflow-server: using distributed backend (redis=%s)", cfg.redis)
	}

	apiCfg := apiserver.Config{
		RedisAddr:           cfg.redis, // legacy single-node path
		RedisConfig:         redisConfig,
		Store:               sqlStore,
		Concurrency:         cfg.concurrency,
		Auth:                auth,
		Logger:              logger,
		Metrics:             m,
		Tracer:              tracer,
		WorkflowAuth:        workflowAuth,
		RequireWorkflowAuth: cfg.requireAPIAuth,
		PrincipalAuth:       principalAuth,
		Authorizer:          apiserver.ScopeAuthorizer{},
		AuditSink:           audit,
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
