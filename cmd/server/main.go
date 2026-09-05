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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	obslogger "github.com/xbcio/xflow/observability/logger"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/crypto/masterkey"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
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
	// redisSentinelUsername and redisSentinelPassword allow sentinel deployments
	// to use ACL credentials that differ from the Redis master credentials.
	// When empty, they fall back to redisUsername/redisPassword for backwards
	// compatibility with shared-credentials setups.
	redisSentinelUsername string
	redisSentinelPassword string
	redisDB               int
	redisTLS              bool
	// authPolicy is the path to runners.yaml. Empty means DisabledAuthenticator
	// (dev / MVP behavior).
	authPolicy string
	// authDryRun logs auth violations but lets the request proceed. Meant for
	// the rollout window between adding runners.yaml and enforcing it.
	authDryRun bool
	// enroll turns on the runner enrollment endpoint. Registration codes are
	// created through the management API; this flag only decides whether the
	// endpoint exists.
	enroll bool
	// apiAuthToken, when non-empty, enables BearerTokenAuth on the workflow/
	// control API (/v1/workflows, /v1/executions/*). The same token must be
	// supplied by callers in the Authorization: Bearer <token> header. When set
	// the token is mapped to a principal in namespace.Default (single-namespace
	// compatibility). For multi-namespace operation use --auth-tokens-file.
	apiAuthToken string
	// authTokensFile, when non-empty, loads a JSON array of token→principal
	// mappings (see apiserver.TokenPrincipalMapping) so each token binds to its
	// own (subject, namespace, scopes). This is the multi-namespace path (design §2.3
	// scheme A). When set it takes precedence over --api-auth-token. The file
	// contains sensitive bearer tokens: it must be 0600 and never logged.
	authTokensFile string
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
	// masterKeyFile is the path to a 0600 file holding the base64 master key.
	// XFLOW_MASTER_KEY takes precedence when both are set.
	masterKeyFile string
	logFormat     string
	metricsAddr   string
	metricsPath   string
	// enableRunnerMetricsProxy turns on the runner metrics proxy: runners push
	// their registry to this server, which merges it into /metrics. Off by
	// default because single-domain deployments can scrape runners directly.
	enableRunnerMetricsProxy bool
	// runnerMetricsInterval is the cadence pushed to runners for metrics
	// reporting. 0 = let each runner use its own default; negative suspends.
	runnerMetricsInterval time.Duration
	// supplyKeyRotation is how often the supply transport key rotates. 0 =
	// the built-in default (24h); negative disables rotation entirely.
	supplyKeyRotation time.Duration
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
	// mode selects the runtime posture: "dev" or "production". Production
	// (the default, fail-closed) requires PrincipalAuthenticator (--auth-tokens-
	// file), Authorizer, a durable AuditSink (--mysql-dsn), and a running
	// Reconciler; missing any one fails to start. Dev allows the in-memory
	// audit sink, the single-token (--api-auth-token) authenticator, and
	// anonymous/allow-all auth, but prints a loud stderr warning. The single-
	// token path is NOT allowed in production: production requires a token→
	// principal/scopes registry so one token cannot self-grant all scopes.
	mode string
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
	fs.StringVar(&cfg.redisSentinelUsername, "redis-sentinel-username", "", "Redis sentinel username (ACL); falls back to --redis-username if empty")
	fs.StringVar(&cfg.redisSentinelPassword, "redis-sentinel-password", "", "Redis sentinel password; falls back to --redis-password if empty")
	fs.IntVar(&cfg.redisDB, "redis-db", 0, "Redis logical database (single/sentinel)")
	fs.BoolVar(&cfg.redisTLS, "redis-tls", false, "Enable TLS for Redis connections")
	fs.BoolVar(&cfg.memory, "memory", false, "Use in-memory backend")
	fs.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Queue consumer concurrency")
	fs.StringVar(&cfg.authPolicy, "auth-policy", "", "Path to runners.yaml (empty = auth disabled)")
	fs.BoolVar(&cfg.authDryRun, "auth-dry-run", false, "Log auth violations but let requests through (rollout aid)")
	fs.BoolVar(&cfg.enroll, "enroll", false, "Enable the runner enrollment endpoint (/v1/runners/enroll)")
	fs.StringVar(&cfg.apiAuthToken, "api-auth-token", "", "Static bearer token for workflow API authentication (sets Authorization: Bearer guard on /v1/workflows and /v1/executions/*); single-namespace → default namespace. For multi-namespace use --auth-tokens-file.")
	fs.StringVar(&cfg.authTokensFile, "auth-tokens-file", "", "JSON file of [{token,subject,namespace,scopes}] mappings; each token binds to its own namespace (multi-namespace). Takes precedence over --api-auth-token. File must be 0600.")
	fs.BoolVar(&cfg.requireAPIAuth, "require-api-auth", false, "Fail to start if no workflow API authenticator is configured (production fail-closed)")
	fs.BoolVar(&cfg.management, "management", false, "Enable ops management module (/healthz /readyz /v1/management/*); /v1/management/* gated by --api-auth-token")
	fs.StringVar(&cfg.tlsCert, "tls-cert", "", "Path to server TLS certificate (enables TLS)")
	fs.StringVar(&cfg.tlsKey, "tls-key", "", "Path to server TLS private key (required with --tls-cert)")
	fs.StringVar(&cfg.tlsClientCA, "tls-client-ca", "", "Path to CA bundle to verify runner certs (enables mTLS)")
	fs.StringVar(&cfg.masterKeyFile, "master-key-file", "", "Path to a 0600 file holding the base64-encoded 32-byte master encryption key; XFLOW_MASTER_KEY takes precedence. Required in production: without it supply content is stored in plaintext. Generate with: openssl rand -base64 32")
	fs.StringVar(&cfg.logFormat, "log-format", "text", "Log format: text or json")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", "", "Prometheus metrics listen address (empty disables metrics)")
	fs.StringVar(&cfg.metricsPath, "metrics-path", "/metrics", "Prometheus metrics path")
	fs.BoolVar(&cfg.enableRunnerMetricsProxy, "enable-runner-metrics-proxy", false,
		"Accept metrics reports from runners that cannot be scraped directly, and merge them into /metrics")
	fs.DurationVar(&cfg.runnerMetricsInterval, "runner-metrics-interval", 0,
		"Cadence pushed to runners for metrics reporting (0 = let each runner use its own default; negative suspends reporting)")
	fs.DurationVar(&cfg.supplyKeyRotation, "supply-key-rotation", 0,
		"How often the supply transport key rotates (0 = 24h default; negative disables rotation)")
	fs.StringVar(&cfg.traceMode, "trace", "disabled", "Tracing mode: disabled|stdout|otlp")
	fs.StringVar(&cfg.traceEndpoint, "trace-endpoint", "localhost:4317", "OTLP collector gRPC endpoint (--trace=otlp)")
	fs.BoolVar(&cfg.traceInsecure, "trace-insecure", false, "Disable TLS verification for OTLP connection")
	fs.StringVar(&cfg.traceSampler, "trace-sampler", "parentbased", "OTel sampler: parentbased|always_on|always_off|traceidratio")
	fs.Float64Var(&cfg.traceRatio, "trace-ratio", 1.0, "Sampling ratio for --trace-sampler=traceidratio, in [0,1]")
	fs.BoolVar(&cfg.traceBaggage, "trace-baggage", false, "Propagate W3C baggage in addition to tracecontext (opt-in; bound accepted keys)")
	fs.StringVar(&cfg.mysqlDSN, "mysql-dsn", "", "MySQL DSN for durable execution state + durable SQL audit sink (parseTime=true required). Empty = in-memory store + in-memory audit (dev only)")
	fs.StringVar(&cfg.mode, "mode", "production", "Runtime posture: dev|production. Production (default, fail-closed) requires --auth-tokens-file + --mysql-dsn + reconciler. Dev allows in-memory audit + single-token with a stderr warning.")
	if args == nil {
		args = os.Args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	switch cfg.mode {
	case "dev", "production":
		// valid
	default:
		return serverConfig{}, fmt.Errorf("--mode must be one of: dev|production")
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
		cfg.redisSentinelUsername != "" ||
		cfg.redisSentinelPassword != "" ||
		cfg.redisDB != 0 ||
		cfg.redisTLS

	// Sentinel deployments may use credentials that differ from the Redis
	// master credentials. Fall back to the master credentials when the
	// sentinel-specific flags are not supplied.
	sentinelUsername := cfg.redisSentinelUsername
	if sentinelUsername == "" {
		sentinelUsername = cfg.redisUsername
	}
	sentinelPassword := cfg.redisSentinelPassword
	if sentinelPassword == "" {
		sentinelPassword = cfg.redisPassword
	}

	var rc *distributed.RedisConfig
	switch mode {
	case distributed.RedisModeSingle:
		if !haConfigured {
			return nil, nil
		}
		if cfg.redis != "" {
			rc = &distributed.RedisConfig{
				Mode:     distributed.RedisModeSingle,
				Addrs:    []string{cfg.redis},
				Username: cfg.redisUsername,
				Password: cfg.redisPassword,
				DB:       cfg.redisDB,
			}
		} else {
			rc = &distributed.RedisConfig{
				Mode:     distributed.RedisModeSingle,
				Username: cfg.redisUsername,
				Password: cfg.redisPassword,
				DB:       cfg.redisDB,
			}
		}

	case distributed.RedisModeSentinel:
		rc = &distributed.RedisConfig{
			Mode:             distributed.RedisModeSentinel,
			Addrs:            splitAddrs(cfg.redisSentinelAddrs),
			MasterName:       cfg.redisSentinelMaster,
			Username:         cfg.redisUsername,
			Password:         cfg.redisPassword,
			SentinelUsername: sentinelUsername,
			SentinelPassword: sentinelPassword,
			DB:               cfg.redisDB,
		}

	case distributed.RedisModeCluster:
		rc = &distributed.RedisConfig{
			Mode:     distributed.RedisModeCluster,
			Addrs:    splitAddrs(cfg.redisClusterAddrs),
			Username: cfg.redisUsername,
			Password: cfg.redisPassword,
		}

	default:
		return nil, fmt.Errorf("unsupported redis mode %q", mode)
	}

	if cfg.redisTLS {
		rc.TLSConfig = &tls.Config{}
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	return rc, nil
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

// resolveBackendTarget decides which backend the server actually connects to.
//
// --memory is a safety net, not a preference: an operator who passes it is
// saying "do not touch a shared Redis", and the flags that name one are
// frequently inherited from an environment file rather than typed on the
// command line. So --memory has to win over both the legacy --redis address
// and the HA config built from the sentinel/cluster flags — dropping either
// one silently points a throwaway process at a real, possibly production,
// Redis instance and lets it write leases and queues there.
//
// It is a separate function because runServer needs a listener, a store, a
// tracer and a Redis before it reaches this branch, which is why nothing
// executed the branch: cmd/server has no test that calls runServer at all.
// overrode reports whether a Redis configuration was actually discarded, so
// the caller only warns when there was something to ignore.
func resolveBackendTarget(memory bool, redisAddr string, redisConfig *distributed.RedisConfig) (addr string, rc *distributed.RedisConfig, overrode bool) {
	if !memory {
		return redisAddr, redisConfig, false
	}
	return "", nil, redisAddr != "" || redisConfig != nil
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
	singleToken := false
	if mappings, err := loadAuthTokenMappings(cfg); err != nil {
		return err
	} else if len(mappings) > 0 {
		// Multi-namespace path (design §2.3 scheme A): each token binds to its
		// own (subject, namespace, scopes). The same multi-token registry gates
		// the outer management middleware (via WorkflowAuthenticator) and the
		// route-level authz wrapper (via PrincipalAuthenticator). Plaintext
		// tokens are hashed in the constructor and never retained or logged.
		auth := apiserver.NewBearerPrincipalAuthMulti(mappings)
		workflowAuth = auth
		principalAuth = auth
		log.Printf("xflow-server: multi-namespace principal auth enabled (%d token(s))", len(mappings))
	} else if cfg.apiAuthToken != "" {
		// Single-token path: DEV ONLY. Production must use --auth-tokens-file
		// so one token cannot self-grant all scopes (Task 8 blocker 4). The
		// single token is mapped to the G1 operator scopes under the default
		// namespace; the subject is server-configured, callers cannot self-report
		// it. runServer rejects this in production mode (see validateProduction).
		auth := apiserver.NewBearerPrincipalAuth(cfg.apiAuthToken, "xflow-operator",
			[]string{"workflow", "execution", "deadletter.list", "deadletter.replay",
				"management.read", "management.write",
				"management.leader.read", "management.runner.read"})
		workflowAuth = auth
		principalAuth = auth
		singleToken = true
	}
	// Master key: env wins over file. ErrNotConfigured is not fatal here —
	// validateProduction decides, so dev keeps working without a key.
	var supplyAtRest *supplyenc.AtRest
	mk, mkErr := masterkey.Load(os.Getenv("XFLOW_MASTER_KEY"), cfg.masterKeyFile)
	switch {
	case mkErr == nil:
		dek := mk.Derive(supplyenc.SupplyContentInfo)
		supplyAtRest = supplyenc.NewAtRest(dek)
	case errors.Is(mkErr, masterkey.ErrNotConfigured):
		// Handled by validateProduction below.
	default:
		// A key that was supplied but is unusable is always fatal, in every
		// mode: continuing would silently write plaintext after the operator
		// explicitly asked for encryption.
		return mkErr
	}

	// G1 audit projection. When --mysql-dsn is set, a durable SQL sink is the
	// authoritative audit target (admission audit persisted before mutations,
	// fail-closed on sink error). Without MySQL, the in-memory sink is the
	// G0/prod-preview projection and is NOT authoritative — production must
	// configure --mysql-dsn. See docs/design/RELEASE-GATES.md §4.
	var sqlStore store.Store
	// artifactStore serves GET/HEAD /v1/artifacts/{digest}. It needs the two
	// concrete artifact repos rather than the store.Store interface (object
	// storage cannot join a MySQL transaction, so those repos are deliberately
	// outside store.Store — see store/sqlstore/provider.go), which is why it is
	// built from the *sqlstore.Provider directly and stays nil in the in-memory
	// dev mode. A nil store leaves the route unregistered, which is the right
	// outcome: without MySQL there is nowhere authoritative to serve bytes from.
	var artifactStore *store.ArtifactStore
	var audit apiserver.AuditSink
	durableAudit := false
	if cfg.mysqlDSN != "" {
		p, err := mysqlstore.New(cfg.mysqlDSN, mysqlstore.WithSupplyEncryption(supplyAtRest))
		if err != nil {
			return fmt.Errorf("open mysql store: %w", err)
		}
		sqlStore = p
		artifactStore = store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
		audit = apiserver.NewSQLAuditSink(p)
		durableAudit = true
		log.Println("xflow-server: durable SQL store + audit sink enabled (MySQL)")
	} else {
		audit = apiserver.NewInMemoryAuditSink()
		log.Println("xflow-server: WARNING --mysql-dsn not set; using in-memory store + in-memory audit (dev only; not production)")
	}

	redisConfig, err := buildRedisConfig(cfg)
	if err != nil {
		return fmt.Errorf("redis config: %w", err)
	}

	redisAddr, redisConfig, overrodeRedis := resolveBackendTarget(cfg.memory, cfg.redis, redisConfig)
	if cfg.memory {
		log.Println("xflow-server: using in-memory backend")
		if overrodeRedis {
			log.Println("xflow-server: WARNING memory flag set, ignoring redis configuration")
		}
	} else if redisConfig != nil {
		log.Printf("xflow-server: using distributed backend (mode=%s addrs=%d master=%q tls=%v db=%d)", redisConfig.Mode, len(redisConfig.Addrs), redisConfig.MasterName, redisConfig.TLSConfig != nil, redisConfig.DB)
	} else if cfg.redis != "" {
		log.Printf("xflow-server: using distributed backend (redis=%s)", cfg.redis)
	}

	// The assembly lives in sdk/xflow, not here. Every option below is a
	// translation of a flag; the wiring those options drive — supply wire
	// encryption, the audit reconcile worker and its leader gate, the module
	// set — is the SDK's, so an embedded host and this binary cannot end up
	// with different postures. Before this, they did: an SDK server had no
	// workflow-API authenticator at all, left supply content unencrypted on the
	// runner hop, and built no reconciler.
	//
	// Options are passed unconditionally wherever the zero value already means
	// "off" — a nil metrics registry, an empty artifact store, an unset address.
	// The conditionals below are only the ones a flag genuinely gates. Wrapping
	// the rest in nil checks would reintroduce, one `if` at a time, the same
	// per-field transcription this refactor removes.
	serverOpts := []xflowsdk.ServerOption{
		xflowsdk.WithServerAuth(auth),
		xflowsdk.WithServerLogger(logger),
		xflowsdk.WithServerMetrics(m),
		xflowsdk.WithServerMetricsAddr(cfg.metricsAddr, cfg.metricsPath),
		xflowsdk.WithServerTracer(tracer),
		xflowsdk.WithServerConcurrency(cfg.concurrency),
		xflowsdk.WithServerWorkflowAuth(workflowAuth, cfg.requireAPIAuth),
		xflowsdk.WithServerPrincipalAuth(principalAuth, apiserver.NamespaceAwareAuthorizer{}, audit),
		xflowsdk.WithServerArtifacts(artifactStore),
		xflowsdk.WithServerHTTPAddr(cfg.addr),
		xflowsdk.WithServerGRPCAddr(cfg.grpcAddr),
		xflowsdk.WithServerTLS(cfg.tlsCert, cfg.tlsKey, cfg.tlsClientCA),
		xflowsdk.WithServerSupplyKeyRotation(cfg.supplyKeyRotation),
		// The two runner-metrics flags are deliberately unbound:
		// --enable-runner-metrics-proxy opens the inbox, --runner-metrics-interval
		// annotates every heartbeat response. A negative interval suspends
		// reporting fleet-wide, which is exactly the case an operator reaches
		// for while the inbox is off.
		xflowsdk.WithServerRunnerMetricsInterval(cfg.runnerMetricsInterval),
	}
	if cfg.enableRunnerMetricsProxy {
		serverOpts = append(serverOpts, xflowsdk.WithServerRunnerMetricsProxy())
	}
	if cfg.management {
		serverOpts = append(serverOpts, xflowsdk.WithServerManagement())
		// Gate /v1/management/* with the workflow API authenticator when
		// configured; /healthz and /readyz stay open for probes. When no
		// token is set the management surface is open (dev / behind an
		// external gateway) — log a warning so production mis-config is loud.
		if workflowAuth != nil {
			serverOpts = append(serverOpts,
				xflowsdk.WithServerHTTPMiddleware(apiserver.ManagementAuthMiddleware(workflowAuth)))
			log.Println("xflow-server: management module enabled; /v1/management/* gated by --api-auth-token")
		} else {
			log.Println("xflow-server: WARNING management module enabled without --api-auth-token; /v1/management/* is open (dev only)")
		}
	}

	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{
		RedisAddr:   redisAddr, // legacy single-node path
		RedisConfig: redisConfig,
		Store:       sqlStore,
	}, serverOpts...)
	if err != nil {
		return err
	}

	// Reconciler (T9): the crash-safe audit reconcile worker, built and run by
	// the SDK. It scans admitted mutations that never received a post-handler
	// outcome (e.g. a crash between a successful mutation and its outcome audit
	// append), consults authoritative state WITHOUT re-executing the mutation,
	// and appends the missing outcome idempotently. Nil (dev) when no durable
	// audit store is configured; production requires a non-nil one so a
	// mis-config fails closed.
	rec := reconcilerOrNil(srv.Reconciler())

	// Task 8 blocker 3: production posture enforcement. Production fails
	// closed when any of PrincipalAuthenticator, Authorizer, durable AuditSink,
	// or Reconciler is missing. Dev allows the in-memory audit sink, single-
	// token, and anonymous auth with a loud stderr warning.
	if err := validateProduction(cfg.mode, productionDeps{
		principalAuth:        principalAuth,
		authorizer:           apiserver.NamespaceAwareAuthorizer{},
		auditSink:            audit,
		durableAudit:         durableAudit,
		reconciler:           rec,
		singleToken:          singleToken,
		masterKey:            supplyAtRest != nil,
		runnerAuthConfigured: runnerAuthConfigured(cfg),
	}); err != nil {
		return err
	}
	if cfg.mode == "dev" {
		fmt.Fprintln(os.Stderr, "xflow-server: WARNING --mode=dev: in-memory audit / single-token / anonymous auth are non-production; do not run in production")
	}
	if cfg.mode == "dev" && supplyAtRest == nil {
		fmt.Fprintln(os.Stderr, "xflow-server: WARNING no XFLOW_MASTER_KEY: supply content is stored in plaintext")
	}

	// signal.NotifyContext so SIGINT/SIGTERM trigger graceful shutdown: the
	// HTTP server drains in-flight requests, gRPC GracefulStops, and the
	// control plane's background goroutines (dispatcher, lease sweeper,
	// leader election) exit cleanly instead of being killed mid-transition.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run starts the reconcile worker along with the transports.
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

// runnerAuthConfigured reports whether the runner protocol has a real
// authenticator. Either a static policy file or the enrollment endpoint counts:
// an enroll-only production server is a supported deployment, and keying the
// gate on --auth-policy alone would push operators into creating an empty
// policy file just to start — auth theater that passes the check.
func runnerAuthConfigured(cfg serverConfig) bool {
	return cfg.authPolicy != "" || cfg.enroll
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

// loadAuthTokenMappings resolves the multi-namespace token→principal registry
// from --auth-tokens-file. The file is a JSON array of objects with fields
// token, subject, namespace, scopes. It returns nil (no error) when neither
// --auth-tokens-file nor --api-auth-token is set so the caller falls back to
// the legacy single-token path. Plaintext tokens are read only here and hashed
// inside NewBearerPrincipalAuthMulti; they are never logged.
//
// File permissions are checked: a world- or group-readable token file is
// rejected (0600 recommended) to avoid leaking bearer tokens.
func loadAuthTokenMappings(cfg serverConfig) ([]apiserver.TokenPrincipalMapping, error) {
	if cfg.authTokensFile == "" {
		return nil, nil
	}
	info, err := os.Stat(cfg.authTokensFile)
	if err != nil {
		return nil, fmt.Errorf("auth-tokens-file: %w", err)
	}
	if mode := info.Mode().Perm(); mode&0077 != 0 {
		return nil, fmt.Errorf("auth-tokens-file %s is group/world readable (mode %o); chmod 0600", cfg.authTokensFile, mode)
	}
	data, err := os.ReadFile(cfg.authTokensFile)
	if err != nil {
		return nil, fmt.Errorf("auth-tokens-file: %w", err)
	}
	var raw []struct {
		Token     string   `json:"token"`
		Subject   string   `json:"subject"`
		Namespace string   `json:"namespace"`
		Scopes    []string `json:"scopes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("auth-tokens-file: invalid JSON: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("auth-tokens-file: no token mappings")
	}
	out := make([]apiserver.TokenPrincipalMapping, 0, len(raw))
	for _, r := range raw {
		if r.Token == "" || r.Subject == "" || r.Namespace == "" {
			return nil, fmt.Errorf("auth-tokens-file: each mapping requires token, subject, and namespace")
		}
		out = append(out, apiserver.TokenPrincipalMapping{
			Token: r.Token, Subject: r.Subject, Namespace: r.Namespace, Scopes: r.Scopes,
		})
	}
	return out, nil
}

// reconciler is a leader-gated background loop that durably settles
// admission/outcome audit for mutations that did not reconcile before a
// process exit (e.g. a crash between a successful mutation and its outcome
// audit append). The real crash-safe worker
// (service/control.AuditReconcileWorker) is built and run by the SDK; this
// interface exists so validateProduction can require its presence — production
// fails closed on a nil reconciler.
type reconciler interface {
	Run(ctx context.Context) error
}

// productionDeps bundles the production-required components so validateProduction
// can assert each is present in a single, testable call.
type productionDeps struct {
	principalAuth apiserver.PrincipalAuthenticator
	authorizer    apiserver.Authorizer
	auditSink     apiserver.AuditSink
	durableAudit  bool // auditSink is backed by a durable store (SQL)
	reconciler    reconciler
	// singleToken indicates the PrincipalAuthenticator was built from the
	// single-token --api-auth-token path (no multi-namespace registry). Production
	// forbids this: one static token must not self-grant operator scopes
	// (Task 8 blocker 4). Production requires --auth-tokens-file.
	singleToken bool
	// masterKey reports whether a usable master encryption key was loaded.
	masterKey bool
	// runnerAuthConfigured reports whether the runner protocol authenticator
	// was built from --auth-policy rather than defaulting to
	// control.DisabledAuthenticator{}. DisabledAuthenticator is a non-nil
	// Authenticator, so a nil check on the authenticator itself cannot tell
	// "configured" apart from "explicitly disabled" — this field is set at the
	// one call site that knows which one buildAuthenticator returned.
	runnerAuthConfigured bool
}

// validateProduction enforces the Task 8 blocker 3 production posture. In
// production mode it fails closed when any of the following is missing:
//   - PrincipalAuthenticator (production must use --auth-tokens-file so a
//     single token cannot self-grant all scopes; the single-token
//     --api-auth-token path is dev-only),
//   - Authorizer (default-deny per operation+resource),
//   - durable AuditSink (admission audit persisted before mutations; the
//     in-memory sink is dev-only and not authoritative),
//   - Reconciler (the T8 seam; T9 provides the crash-safe worker).
//   - a master encryption key (XFLOW_MASTER_KEY or --master-key-file); without
//     it supply content would be stored in plaintext.
//   - a configured runner protocol authenticator (--auth-policy); without it
//     the runner-facing endpoints accept any runner via
//     control.DisabledAuthenticator{}, which is a non-nil Authenticator and so
//     does not trip NewServer's own posture check.
//
// dev mode allows every combination above (in-memory audit, single-token,
// anonymous) and is expected to print a stderr warning at startup.
func validateProduction(mode string, deps productionDeps) error {
	if mode != "production" {
		return nil
	}
	if deps.principalAuth == nil {
		return fmt.Errorf("production mode requires a PrincipalAuthenticator (--auth-tokens-file); --api-auth-token is dev-only")
	}
	if deps.singleToken {
		return fmt.Errorf("production mode requires --auth-tokens-file (multi-namespace token→principal/scopes registry); --api-auth-token is dev-only")
	}
	if deps.authorizer == nil {
		return fmt.Errorf("production mode requires an Authorizer")
	}
	if deps.auditSink == nil || !deps.durableAudit {
		return fmt.Errorf("production mode requires a durable AuditSink (--mysql-dsn); the in-memory sink is dev-only")
	}
	if deps.reconciler == nil {
		return fmt.Errorf("production mode requires a Reconciler (T8 seam; T9 provides the crash-safe worker)")
	}
	if !deps.masterKey {
		return fmt.Errorf("production mode requires a master encryption key (XFLOW_MASTER_KEY or --master-key-file); without it supply content is stored in plaintext. Generate with: openssl rand -base64 32")
	}
	if !deps.runnerAuthConfigured {
		return fmt.Errorf("production mode requires runner protocol authentication (--auth-policy); without it any runner can register and claim work")
	}
	return nil
}

// reconcilerOrNil converts the SDK's concrete worker into the productionDeps
// interface field, mapping a nil pointer to a nil interface.
//
// Assigning the pointer directly would defeat the production check: a nil
// *AuditReconcileWorker stored in an interface is not a nil interface, so
// `deps.reconciler != nil` would be true for an absent worker and a production
// server with a durable audit sink and nothing to settle its crash-orphaned
// admissions would start clean.
func reconcilerOrNil(w *control.AuditReconcileWorker) reconciler {
	if w == nil {
		return nil
	}
	return w
}
