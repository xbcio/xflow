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
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"go.uber.org/zap"
	"gorm.io/gorm"
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
	// enrollDB is the same *gorm.DB the SQL store.Store above is built on
	// (Provider.DB()), lifted to this outer scope so the registrationCodeStore /
	// issuedIdentityStore wiring below — which needs a *gorm.DB, not a
	// store.Store — can reach it without opening a second connection pool.
	// nil in the in-memory (--mysql-dsn unset) case, same as sqlStore.
	var enrollDB *gorm.DB
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
		enrollDB = p.DB()
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

	// registrationCodeStore / issuedIdentityStore back the runner enrollment
	// endpoint. Both are constructed here, in one place, so there is a single
	// pair of variables regardless of which implementation backs them.
	//
	// SQL-backed when --mysql-dsn is set (Task 7): issued runner identities
	// then survive a server restart, using the same *gorm.DB connection pool
	// as the durable execution store above (enrollDB, aliased from
	// sqlStore's Provider.DB()) rather than opening a second one.
	// In-memory otherwise (dev only): every enrolled runner must re-enroll
	// after a restart.
	var registrationCodeStore control.RegistrationCodeStore
	var issuedIdentityStore control.IssuedIdentityStore
	if cfg.enroll {
		if enrollDB != nil {
			registrationCodeStore = sqlstore.NewRegistrationCodeStore(enrollDB)
			issuedIdentityStore = sqlstore.NewIssuedIdentityStore(enrollDB)
		} else {
			registrationCodeStore = control.NewMemoryRegistrationCodeStore()
			issuedIdentityStore = control.NewMemoryIssuedIdentityStore()
		}
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
	// Runner-auth posture. buildAuthenticator returns DisabledAuthenticator{}
	// for an empty --auth-policy, which control.IsConfigured rejects — so
	// passing it unconditionally made NewServer fail with
	// ErrRunnerAuthPostureUndeclared, whose remedy names SDK options an
	// operator of this binary cannot reach. Declaring the posture in the
	// binary's own terms instead means --mode=dev starts, and --mode=production
	// reaches the gate below, which answers in flag names.
	switch runnerAuthPostureFor(auth, cfg.enroll) {
	case posturePolicy:
		serverOpts = append(serverOpts, xflowsdk.WithServerAuth(auth))
	case postureInsecure:
		serverOpts = append(serverOpts, xflowsdk.WithServerInsecureNoRunnerAuth())
	}
	if cfg.enroll {
		serverOpts = append(serverOpts, xflowsdk.WithServerEnroll(registrationCodeStore, issuedIdentityStore))
	}
	// Production posture (Task 8 blocker 3), enforced by apiserver.New — the
	// one layer both this binary and every SDK embedder pass through. The
	// declaration carries the three facts that layer cannot see for itself:
	// they describe how the dependencies below were BUILT, not anything the
	// built objects expose. Everything else the gate checks (principal auth,
	// authorizer, audit sink, runner auth, reconcilable store) it reads
	// directly off the config it is handed.
	if cfg.mode == "production" {
		serverOpts = append(serverOpts, xflowsdk.WithServerProduction(
			productionDeclaration(principalAuth, singleToken, durableAudit, supplyAtRest != nil)))
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
		// Rewrite the production posture error into this binary's flags. The
		// gate itself lives in apiserver, which is also what an SDK embedder
		// passes through, so both get the same enforcement and each gets
		// remediation in a vocabulary it has.
		return explainProductionGate(err)
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

// runnerAuthPosture names which of the SDK's three runner-auth declarations
// this binary's flags amount to. NewServer requires exactly one of them and
// rejects combinations: WithServerAuth conflicts with
// WithServerInsecureNoRunnerAuth on `sc.auth != nil` — note, not on
// IsConfigured, so handing it a DisabledAuthenticator still counts as a
// conflict — and WithServerEnroll conflicts with it as well. Deciding once,
// here, is what keeps that invariant checkable.
type runnerAuthPosture int

const (
	// posturePolicy: --auth-policy loaded a real policy store.
	posturePolicy runnerAuthPosture = iota
	// postureEnroll: --enroll declares the posture on its own; runners obtain
	// identities at enrollment time, so no authenticator is passed.
	postureEnroll
	// postureInsecure: neither is set. The SDK requires this to be said out
	// loud rather than inferred from a permissive default.
	postureInsecure
)

func runnerAuthPostureFor(auth control.Authenticator, enroll bool) runnerAuthPosture {
	switch {
	case control.IsConfigured(auth):
		return posturePolicy
	case enroll:
		return postureEnroll
	default:
		return postureInsecure
	}
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

// productionDeclaration derives the three facts apiserver's production gate
// cannot observe for itself from how this binary built its dependencies.
//
// It is a function rather than a struct literal at the call site so it can be
// tested: a declaration is only worth what its derivation is worth, and
// hard-coding any field to true would silently disable the corresponding
// check with nothing to catch it.
func productionDeclaration(principalAuth apiserver.PrincipalAuthenticator, singleToken, durableAudit, masterKeyLoaded bool) apiserver.ProductionDeclaration {
	return apiserver.ProductionDeclaration{
		DurableAudit: durableAudit,
		// A nil principalAuth is caught by the gate's own principal-auth
		// requirement; declaring "multi-token" for something that does not
		// exist would be a claim about nothing.
		MultiTokenPrincipalAuth: principalAuth != nil && !singleToken,
		SupplyEncryptionAtRest:  masterKeyLoaded,
	}
}

// productionFlagHint maps each production requirement to the flags that
// satisfy it here. apiserver states requirements in front-end-neutral terms
// because an embedded host has no flags; this is where they become the
// vocabulary of THIS binary's operator. TestProductionFlagHintsAreExhaustive
// keeps it in step with apiserver.AllProductionRequirements.
var productionFlagHint = map[apiserver.ProductionRequirement]string{
	apiserver.RequireRunnerAuth:              "--auth-policy or --enroll",
	apiserver.RequirePrincipalAuth:           "--auth-tokens-file",
	apiserver.RequireMultiTokenPrincipalAuth: "--auth-tokens-file (--api-auth-token is a single shared token and is dev-only)",
	apiserver.RequireAuthorizer:              "no flag: this binary always wires one, so reaching this is a bug worth reporting",
	apiserver.RequireAuditSink:               "--mysql-dsn",
	apiserver.RequireDurableAudit:            "--mysql-dsn (the in-memory sink is dev-only)",
	apiserver.RequireAuditReconciler:         "--mysql-dsn",
	apiserver.RequireSupplyEncryptionAtRest:  "XFLOW_MASTER_KEY or --master-key-file (generate with: openssl rand -base64 32)",
}

// explainProductionGate rewrites apiserver's production posture error into
// this binary's terms: every unmet requirement, why it matters, and the flag
// that fixes it. Any other error passes through untouched.
//
// The operator sees the whole list, not the first item — a server that named
// one missing piece per start would turn a fresh production deploy into a
// sequence of start-fix-restart cycles.
func explainProductionGate(err error) error {
	var gate *apiserver.ProductionGateError
	if !errors.As(err, &gate) {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "xflow-server: --mode=production is not satisfied (%d requirement(s)); pass --mode=dev to run without them:", len(gate.Unmet))
	for _, r := range gate.Unmet {
		fmt.Fprintf(&b, "\n  - %s\n      why:  %s\n      set:  %s", r, r.Reason(), productionFlagHint[r])
	}
	return errors.New(b.String())
}
