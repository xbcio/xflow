package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	xnode "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/node/supply"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	transportHTTP = "http"
	transportGRPC = "grpc"
)

var (
	reconnectMinBackoff = 2 * time.Second
	reconnectMaxBackoff = 30 * time.Second
)

type runFunc func(ctx context.Context) error

var errStop = errors.New("stop reconnect loop")

type runnerConfig struct {
	configPath        string
	serverURL         string
	transport         string
	grpcTarget        string
	runnerID          string
	concurrency       int
	changed           map[string]bool
	resolutionIssues  map[string]error
	capRaw            string
	capabilities      []protocol.Capability
	labelRaw          []string
	labels            map[string]string
	namespaceRaw      []string
	namespaces        []namespace.Namespace
	heartbeatInterval string
	pollWait          string
	// token is the runner's bearer token (matched against the server's
	// runners.yaml policy). Empty means "no auth", which the server accepts
	// only when running with --auth-mode disabled or dry-run.
	token string
	// TLS: serverCA verifies the server's certificate; clientCert/clientKey
	// present a client cert for mTLS. All three empty means plaintext.
	tlsServerCA   string
	tlsClientCert string
	tlsClientKey  string
	// tracing
	traceMode     string
	traceEndpoint string
	traceInsecure bool
	traceSampler  string
	traceRatio    float64
	traceBaggage  bool
	tracer        tracing.Tracer
	// metrics
	metricsAddr string // Prometheus scrape endpoint (e.g. ":9091")
	// reportMetrics ships this runner's whole registry to the server, which
	// merges it into its own /metrics. Independent of metricsAddr on purpose:
	// a runner in another network domain cannot be scraped, and is also the
	// runner least likely to want a listening port. Either, both, or neither.
	reportMetrics         bool
	reportMetricsInterval string
	// credentials holds named credential maps (driver/dsn, token/base_url, …)
	// with string leaves already env-expanded at load time. Passed to the
	// runner as a CredentialResolver closure. nil/empty means no resolver.
	credentials map[string]map[string]any
	// resourcePoolConfig carries the parsed resource_pool tunables. The zero
	// value signals "use defaults" — the pool is only constructed when
	// credentials are present or a db/grpc capability is declared.
	resourcePoolConfig types.ResourcePoolConfig
}

func newRunCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the xflow task runner",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			recordChangedFlags(cmd, cfg)
			resolved, err := resolveRunnerConfig(*cfg)
			if err != nil {
				return err
			}
			return opts.runFunc(resolved)
		},
	}
	bindRunnerFlags(cmd, cfg)
	return cmd
}

func bindRunnerFlags(cmd *cobra.Command, cfg *runnerConfig) {
	cmd.Flags().StringVar(&cfg.serverURL, "server", cfg.serverURL, "xflow-server base URL (http transport)")
	cmd.Flags().StringVar(&cfg.transport, "transport", cfg.transport, "Runner Protocol transport: http or grpc")
	cmd.Flags().StringVar(&cfg.grpcTarget, "grpc-target", cfg.grpcTarget, "xflow-server gRPC target host:port (grpc transport)")
	cmd.Flags().StringVar(&cfg.runnerID, "id", cfg.runnerID, "Runner ID")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Runner concurrency")
	cmd.Flags().StringVar(&cfg.capRaw, "cap", cfg.capRaw, "Comma-separated node type capabilities")
	cmd.Flags().StringArrayVar(&cfg.labelRaw, "label", cfg.labelRaw, "Runner label as key=value; repeatable")
	cmd.Flags().StringArrayVar(&cfg.namespaceRaw, "namespace", cfg.namespaceRaw, "Namespace this runner serves; repeatable (default: default)")
	cmd.Flags().StringVar(&cfg.heartbeatInterval, "heartbeat-interval", cfg.heartbeatInterval, "Heartbeat interval")
	cmd.Flags().StringVar(&cfg.pollWait, "poll-wait", cfg.pollWait, "Poll wait duration when no task is available")
	cmd.Flags().StringVar(&cfg.token, "token", cfg.token, "Runner bearer token (prefer XFLOW_RUNNER_TOKEN env)")
	cmd.Flags().StringVar(&cfg.tlsServerCA, "tls-server-ca", cfg.tlsServerCA, "Path to server CA bundle (enables TLS)")
	cmd.Flags().StringVar(&cfg.tlsClientCert, "tls-client-cert", cfg.tlsClientCert, "Path to client TLS certificate (enables mTLS)")
	cmd.Flags().StringVar(&cfg.tlsClientKey, "tls-client-key", cfg.tlsClientKey, "Path to client TLS private key")
	cmd.Flags().StringVar(&cfg.traceMode, "trace", "disabled", "Tracing mode: disabled|stdout|otlp")
	cmd.Flags().StringVar(&cfg.traceEndpoint, "trace-endpoint", "localhost:4317", "OTLP collector gRPC endpoint (--trace=otlp)")
	cmd.Flags().BoolVar(&cfg.traceInsecure, "trace-insecure", false, "Disable TLS verification for OTLP connection")
	cmd.Flags().StringVar(&cfg.traceSampler, "trace-sampler", "parentbased", "OTel sampler: parentbased|always_on|always_off|traceidratio")
	cmd.Flags().Float64Var(&cfg.traceRatio, "trace-ratio", 1.0, "Sampling ratio for --trace-sampler=traceidratio, in [0,1]")
	cmd.Flags().BoolVar(&cfg.traceBaggage, "trace-baggage", false, "Propagate W3C baggage in addition to tracecontext (opt-in)")
	cmd.Flags().StringVar(&cfg.metricsAddr, "metrics-addr", cfg.metricsAddr, "Prometheus metrics listen address (e.g. :9091); empty disables metrics")
	cmd.Flags().BoolVar(&cfg.reportMetrics, "report-metrics", cfg.reportMetrics,
		"Ship this runner's metrics to the server so they appear on the server's /metrics (for runners that cannot be scraped directly)")
	cmd.Flags().StringVar(&cfg.reportMetricsInterval, "report-metrics-interval", cfg.reportMetricsInterval,
		"Local metrics reporting cadence; the server can override it at runtime (--report-metrics)")
}

func recordChangedFlags(cmd *cobra.Command, cfg *runnerConfig) {
	if cfg.changed == nil {
		cfg.changed = map[string]bool{}
	}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		cfg.changed[f.Name] = true
	})
}

func parseCapabilities(raw string) []protocol.Capability {
	parts := strings.Split(raw, ",")
	capabilities := make([]protocol.Capability, 0, len(parts))
	for _, part := range parts {
		nodeType := strings.TrimSpace(part)
		if nodeType == "" {
			continue
		}
		capabilities = append(capabilities, protocol.Capability{NodeType: nodeType})
	}
	return capabilities
}

type runnerService interface {
	Run(context.Context) error
}

// withGroupExecCapability returns the operator's declared capabilities with the
// group execution capability guaranteed present and carrying its feature. It is
// supplied by the binary rather than typed by the operator for two reasons:
// --cap has no syntax for a feature list at all (parseCapabilities emits only a
// NodeType), and omitting it fails silently — a group unit's routing requires
// group.exec.v1, so a runner without it is filtered out during assignment and
// the task waits in the queue indefinitely with nothing logged on either side.
//
// An operator who does declare `--cap xflow.group` is completed rather than
// deferred to. A bare entry with no Features is the single worst shape
// available: canRunRouting ignores Features and accepts it, MatchCapabilities
// does not and rejects it, so the runner reads as correctly configured on the
// command line while still receiving nothing. Appending a second entry would
// not fix it either — hasCapabilityForRequirement stops at the first NodeType
// match, so the bare one would shadow the real one.
//
// This does not make the runner eligible for groups it cannot execute. A group's
// routing requirements list every member node type alongside this feature (see
// engine.RequirementsFromGraphPackage), and MatchCapabilities requires all of
// them, so a runner missing any member's handler is still rejected.
//
// The input slice is never mutated: cfg.capabilities is shared with verify.go
// and the config-resolution tests, and append would write through to its backing
// array whenever spare capacity happened to exist.
func withGroupExecCapability(declared []protocol.Capability) []protocol.Capability {
	out := make([]protocol.Capability, 0, len(declared)+1)
	found := false
	for _, c := range declared {
		if c.NodeType == engine.GroupNodeType {
			found = true
			c.Features = withFeature(c.Features, engine.FeatureGroupExecV1)
		}
		out = append(out, c)
	}
	if found {
		return out
	}
	return append(out, protocol.Capability{
		NodeType: engine.GroupNodeType,
		Features: []string{engine.FeatureGroupExecV1},
	})
}

// withFeature returns features with name present exactly once, copying rather
// than appending in place so the caller's slice is never aliased.
func withFeature(features []string, name string) []string {
	for _, f := range features {
		if f == name {
			return features
		}
	}
	out := make([]string, 0, len(features)+1)
	out = append(out, features...)
	return append(out, name)
}

// batchBodyCacheEntries bounds the compiled-body cache. A body compiles once per
// distinct PackageHash, and every batch of one map node carries the same hash, so
// the entry count tracks distinct map nodes this runner serves — not batches.
const batchBodyCacheEntries = 64

// groupPackageCacheEntries bounds the compiled group-package cache. A group
// package compiles once per distinct PackageHash, and every execution of one
// group carries the same hash, so the entry count tracks distinct groups this
// runner serves — not executions. Same reasoning, same size as the body cache.
const groupPackageCacheEntries = 64

var newRunnerService = func(client runnersvc.ProtocolClient, registry engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
	return runnersvc.New(client, registry, cfg)
}

func runRunner(ctx context.Context, cfg runnerConfig) error {
	registry := execution.NewRegistry()
	// Built before runnerServiceConfig (not after, as before this fix) so the
	// TriggerActivationHandler it wires can be given WithGroupRuntime — a
	// runner that both hosts triggers and executes groups needs the SAME
	// GroupRuntime instance in both places; constructing it after
	// runnerServiceConfig had already returned meant the handler could never
	// see it, and every trigger-group activation on a production runner
	// failed closed (spec 2026-08-07 §3, the gap this whole feature closes).
	//
	// Unconditional, and paired with an unconditional capability advertisement
	// in runnerServiceConfig, because the two are only meaningful together: a
	// group unit's routing requires the group.exec.v1 feature, so a runner
	// that does not advertise it is never SENT a group lease — the task stays
	// queued with no error anywhere. Gating this behind a flag would preserve
	// exactly that silent failure for every operator who did not know to set
	// it.
	//
	// Advertising unconditionally does not attract work this runner cannot
	// do. engine.RequirementsFromGraphPackage puts every member node type in
	// the same requirement set, so MatchCapabilities still rejects a runner
	// missing any member's handler. The group capability widens nothing on
	// its own.
	//
	// Suspend is disabled inside a group for the same reason as inside a map
	// body: a suspended member would park a sub-execution the outer lease
	// cannot resume.
	// One artifact resolver, three consumers: the group runtime below, the
	// subgraph runtime further down, and the top-level dispatcher inside
	// runnerServiceConfig. Built here because the two runtimes are constructed
	// before that function is called — see newArtifactCodeResolver for why a
	// resolver on the dispatcher alone leaves nested scripts unable to fetch.
	artifactCode, err := newArtifactCodeResolver(cfg)
	if err != nil {
		return err
	}

	groupRuntime := runnersvc.NewGroupRuntime(
		registry,
		runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: groupPackageCacheEntries}),
		runnersvc.WithSuspendDisabled(),
		runnersvc.WithGroupArtifactCodeResolver(artifactCode))

	serviceCfg, err := runnerServiceConfig(cfg, groupRuntime, artifactCode)
	if err != nil {
		return err
	}
	// Initialize tracing so the runner extracts the remote parent from the
	// lease carrier, starts xflow.task.execute, and injects the report carrier.
	tracer, shutdownTracing, err := tracing.NewTracerProvider(ctx, tracing.ProviderConfig{
		Mode:        cfg.traceMode,
		Endpoint:    cfg.traceEndpoint,
		Insecure:    cfg.traceInsecure,
		ServiceName: "xflow-runner",
		Sampler:     tracing.SamplerMode(cfg.traceSampler),
		SampleRatio: cfg.traceRatio,
		Baggage:     cfg.traceBaggage,
	})
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer shutdownTracing(context.Background())
	cfg.tracer = tracer
	serviceCfg.Tracer = tracer

	client, cleanup, err := newProtocolClient(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	// The ResourcePool is process-scoped (caches *sql.DB and *grpc.ClientConn).
	// Close it on runner shutdown with a bounded context so a hung close cannot
	// pin the process. Mirrors the parity tests' cleanup pattern. Idempotent.
	if serviceCfg.ResourcePool != nil {
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = serviceCfg.ResourcePool.Close(closeCtx)
		}()
	}
	// registry is already constructed above.
	// A batch lease needs a runtime to run: it names a synthetic node
	// ("m/_batch/0") that carries no Input and has no registered handler, so the
	// ordinary node path has nothing to execute. The runtime resolves the body's
	// member handlers out of this same registry.
	//
	// This does not widen what the runner claims. Batch routing advertises the map
	// node's own type (engine.TaskRouting returns meta.Type), so a runner only ever
	// sees a batch if the operator already listed xflow.map in --cap.
	serviceCfg.SubgraphRuntime = runnersvc.NewSubgraphRuntime(
		registry, runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: batchBodyCacheEntries}),
		runnersvc.WithSubgraphArtifactCodeResolver(artifactCode))
	serviceCfg.GroupRuntime = groupRuntime
	// Absorb script-engine cold start before the first lease arrives: qjs pays a
	// ~330 ms QuickJS-wasm compile and the wasm reactor opens its runtime
	// (resolving the on-disk compilation cache). A failure here is not fatal —
	// each engine still warms lazily — so log and carry on.
	if err := xnode.WarmupScriptEngines(ctx); err != nil {
		slog.Warn("script engine warmup failed; engines will warm on first use", "error", err)
	}

	// Metrics: the registry and the observer wiring are unconditional —
	// metrics.New() allocates a Prometheus registry and nothing else (no
	// listener, no goroutine), and an unobserved registry is what makes
	// reporting return an empty payload. Only the two *exits* are optional:
	// --metrics-addr opens a scrape port, --report-metrics ships to the server.
	// Binding the registry to --metrics-addr, as the pre-proxy code did, would
	// have made the cross-domain runner — the one case that cannot be scraped —
	// the one case that also cannot report.
	m := metrics.New()
	sm := metrics.NewSupplyMetrics(m)
	if serviceCfg.SupplyGate != nil {
		serviceCfg.SupplyGate.SetObserver(sm)
	}
	supply.Default.SetObserver(sm)
	xnode.SetWasmObserver(sm)
	kafkatrigger.SetObserver(metrics.NewTriggerMetrics(m))
	xnode.SetScriptObserver(metrics.NewScriptMetrics(m))
	// Wire the node execution timeout/duration observer into the in-process
	// executor so runner-detected timeouts, the abandoned-goroutine gauge, and
	// per-invocation duration are emitted into this runner's registry (which
	// is then either scraped directly via --metrics-addr or proxied to the
	// server via the MetricsReporter below).
	serviceCfg.TimeoutObserver = metrics.NewNodeTimeoutMetrics(m)

	var metricsServer *http.Server
	if cfg.metricsAddr != "" {
		metricsServer = &http.Server{Addr: cfg.metricsAddr, Handler: m.Handler()}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server failed", "error", err)
			}
		}()
		slog.Info("metrics server started", "addr", cfg.metricsAddr)
	}
	defer func() {
		if metricsServer != nil {
			shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(shutCtx)
		}
	}()

	reporter, err := buildMetricsReporter(client, m, cfg)
	if err != nil {
		return err
	}
	serviceCfg.MetricsReporter = reporter
	if reporter != nil {
		slog.Info("metrics reporting to server enabled", "interval", cfg.reportMetricsInterval)
	} else if cfg.reportMetrics {
		slog.Warn("--report-metrics set but this transport cannot report metrics; continuing without reporting",
			"transport", cfg.transport)
	}

	runner := newRunnerService(client, registry, serviceCfg)
	return runner.Run(ctx)
}

// newProtocolClient builds the Runner Protocol client for the configured
// transport. The returned cleanup releases any transport-owned resources (e.g.
// the gRPC connection); it is a no-op for HTTP.
func newProtocolClient(cfg runnerConfig) (runnersvc.ProtocolClient, func(), error) {
	tlsCfg, err := buildRunnerTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	switch cfg.transport {
	case transportGRPC:
		var creds credentials.TransportCredentials
		if tlsCfg != nil {
			creds = credentials.NewTLS(tlsCfg)
		} else {
			creds = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(cfg.grpcTarget, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, nil, fmt.Errorf("dial gRPC server %q: %w", cfg.grpcTarget, err)
		}
		client := protocol.NewGRPCClient(conn)
		if cfg.token != "" {
			return client.WithToken(cfg.token), func() { _ = conn.Close() }, nil
		}
		return client, func() { _ = conn.Close() }, nil
	default:
		httpClient, err := newRunnerHTTPClient(cfg, 0)
		if err != nil {
			return nil, nil, err
		}
		client := protocol.NewClient(cfg.serverURL, httpClient)
		if cfg.token != "" {
			return client.WithToken(cfg.token), func() {}, nil
		}
		return client, func() {}, nil
	}
}

// buildRunnerTLSConfig resolves the runner-side TLS config from CLI flags.
// Returns nil when no CA or cert was configured (plaintext, dev default).
func buildRunnerTLSConfig(cfg runnerConfig) (*tls.Config, error) {
	if cfg.tlsServerCA == "" && cfg.tlsClientCert == "" && cfg.tlsClientKey == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.tlsServerCA != "" {
		caPEM, err := os.ReadFile(cfg.tlsServerCA)
		if err != nil {
			return nil, fmt.Errorf("read server CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("server CA %q contains no valid certs", cfg.tlsServerCA)
		}
		tlsCfg.RootCAs = pool
	}
	switch {
	case cfg.tlsClientCert == "" && cfg.tlsClientKey == "":
		// TLS only, no client auth.
	case cfg.tlsClientCert != "" && cfg.tlsClientKey != "":
		cert, err := tls.LoadX509KeyPair(cfg.tlsClientCert, cfg.tlsClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	default:
		return nil, fmt.Errorf("--tls-client-cert and --tls-client-key must be provided together")
	}
	return tlsCfg, nil
}

// newRunnerHTTPClient builds an *http.Client that honours the runner's --tls-*
// flags, with the given absolute timeout.
//
// Every HTTP client the runner points at the control plane must go through
// here. There are three of them (Runner Protocol, artifact fetch, entry-seed +
// supply fetch) and they all talk to the same origin, so a client that silently
// used http.DefaultTransport would ignore --tls-server-ca and --tls-client-cert
// and fail against a private CA or an mTLS-requiring server — the supply fetch
// failing that way makes the readiness gate decline forever, so the runner never
// hosts its triggers at all.
func newRunnerHTTPClient(cfg runnerConfig, timeout time.Duration) (*http.Client, error) {
	tlsCfg, err := buildRunnerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	c := &http.Client{Timeout: timeout}
	if tlsCfg != nil {
		c.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	return c, nil
}

// newArtifactCodeResolver builds the digest -> script bytes resolver every
// script execution path in this runner shares: a read-through cache that serves
// from local disk and falls back to GET /v1/artifacts/{digest} on the server.
//
// One instance, three consumers — the top-level dispatcher (Config.
// ArtifactCodeResolver), the group runtime, and the subgraph (map body)
// runtime. The latter two build their own inner backends per attempt, so a
// resolver installed only on the dispatcher never reaches a script nested
// inside a group or a map body; that node then reads a nil resolver and fails
// permanently with script.artifact_unavailable. Sharing one instance also keeps
// a single on-disk cache rather than one per runtime.
func newArtifactCodeResolver(cfg runnerConfig) (func(ctx context.Context, digest string) ([]byte, error), error) {
	artifactClient, err := newRunnerHTTPClient(cfg, 60*time.Second)
	if err != nil {
		return nil, err
	}
	httpOrigin := &objectstore.HTTPStore{
		BaseURL: triggerSeedBaseURL(cfg),
		Token:   cfg.token,
		Client:  artifactClient,
	}
	readThrough := objectstore.NewReadThrough(objectstore.NewFSStore(artifactCacheDir()), httpOrigin)
	artifactStore := store.NewArtifactStore(readThrough, nil)
	return func(ctx context.Context, digest string) ([]byte, error) {
		rc, _, err := artifactStore.Open(ctx, digest)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}, nil
}

func runnerServiceConfig(cfg runnerConfig, groupRuntime *runnersvc.GroupRuntime, artifactCode func(ctx context.Context, digest string) ([]byte, error)) (runnersvc.Config, error) {
	_, err := parsePositiveDuration("heartbeat interval", cfg.heartbeatInterval)
	if err != nil {
		return runnersvc.Config{}, err
	}
	pollWait, err := parsePositiveDuration("poll wait", cfg.pollWait)
	if err != nil {
		return runnersvc.Config{}, err
	}
	poolCfg := cfg.resourcePoolConfig
	// If the file overrode nothing, fall back to the recommended defaults so
	// the pool gets sane conn limits. normalizeConfig (inside the pool) also
	// fills zeros, but using the documented default here keeps the intent
	// explicit and testable.
	if poolCfg == (types.ResourcePoolConfig{}) {
		poolCfg = types.DefaultResourcePoolConfig()
	}
	svcCfg := runnersvc.Config{
		RunnerID:     cfg.runnerID,
		Concurrency:  cfg.concurrency,
		Labels:       cloneStringMap(cfg.labels),
		Capabilities: withGroupExecCapability(cfg.capabilities),
		PollWait:     pollWait,
		Tracer:       cfg.tracer,
		Namespaces:   cfg.namespaces,
	}
	if shouldConstructPool(cfg) {
		svcCfg.ResourcePool = resource.NewDefaultResourcePool(poolCfg)
	}
	if len(cfg.credentials) > 0 {
		// Capture credentials in the closure; never log or surface in errors.
		// The config currently stores credentials as map[name]map[key]value,
		// so the resolver looks up by credential name only. Per-namespace credential
		// namespaces are not supported by the current YAML schema; the
		// namespace argument is kept because the runner service hook is already in
		// place and future config work only needs to update this closure.
		creds := cfg.credentials
		svcCfg.CredentialResolver = func(t namespace.Namespace, name string) map[string]any {
			return creds[name]
		}
	}
	// Artifact store: read-through cache for script artifacts (wasm modules).
	// The runner fetches by digest from the server (GET /v1/artifacts/{digest})
	// and caches locally on disk. The resolver closure is what ScriptNode.Execute
	// calls at runtime via Input.ArtifactCode.
	//
	// Built once in Run and passed in, not constructed here: the group and
	// subgraph runtimes need the SAME resolver, and they are constructed before
	// this function is called. See newArtifactCodeResolver.
	svcCfg.ArtifactCodeResolver = artifactCode
	// Trigger hosting: when the runner advertises at least one registered trigger
	// node type AND a seed base URL is reachable, construct the ActivationTracker
	// over the production TriggerActivationHandler (Task 8) so activate/deactivate
	// directives piggybacked on heartbeats start/stop real trigger subscriptions
	// whose seeds are stamped with the assigned generation. When the runner hosts
	// no triggers (no trigger capability) the tracker is left nil (passive runner)
	// so the existing no-activation behavior is preserved.
	if seedBaseURL := triggerSeedBaseURL(cfg); seedBaseURL != "" && hostsTriggers(cfg) {
		lookup := registryTriggerLookup{}
		// The seed HTTP client timeout (30s) is intentionally larger than the
		// per-request context timeout (entrySeedRequestTimeout = 15s in
		// entry_seed_runtime.go). The context deadline governs normal
		// cancellation; the client timeout is an absolute safety net covering
		// connection setup and full body read, preventing leaked connections if
		// the context is not propagated correctly.
		seedClient, err := newRunnerHTTPClient(cfg, 30*time.Second)
		if err != nil {
			return runnersvc.Config{}, err
		}
		// The supply fetch client shares the seed origin: both talk to the control
		// plane's HTTP API. The gate publishes into supply.Default, the same
		// registry node handlers read through $supplies.
		supplyFetcher := &runnersvc.HTTPSupplyFetcher{
			BaseURL: seedBaseURL,
			Token:   cfg.token,
			Client:  seedClient,
		}
		gate := runnersvc.NewSupplyGate(supplyFetcher, supply.Default, slog.Default())
		handler := runnersvc.NewTriggerActivationHandler(seedBaseURL, cfg.token, lookup,
			runnersvc.WithSeedHTTPClient(seedClient),
			runnersvc.WithSupplyGate(gate),
			runnersvc.WithGroupRuntime(groupRuntime),
			// Same resolver the executor uses (installed unconditionally above),
			// so activation can compile a wasm module before registering its
			// supply consumer without a second client or cache.
			runnersvc.WithArtifactCodeResolver(svcCfg.ArtifactCodeResolver))
		svcCfg.ActivationTracker = runnersvc.NewActivationTracker(handler, slog.Default())
		// Same gate/registry pair feeds the heartbeat's two supply channels: the
		// registry is what Observed() reads out to report applied hashes, and
		// the gate is what ApplyHints fetches into on a piggybacked hint. Both
		// nil whenever this runner hosts no triggers (no supply-consuming
		// workflow can be activated here either), which keeps heartbeat bodies
		// byte-identical to before this wiring existed for that runner shape.
		svcCfg.SupplyRegistry = supply.Default
		svcCfg.SupplyGate = gate
	}
	svcCfg.SupportsEncryption = true
	return svcCfg, nil
}

// registryTriggerLookup adapts the global node registry's LookupTrigger to the
// runner's TriggerHandlerLookup interface, so the TriggerActivationHandler can
// resolve any registered trigger node type without hardcoding Kafka. The
// node package is imported for side effects in this binary, so every built-in
// trigger (kafka/timer/cron/webhook/redis-hub) is registered by init time.
type registryTriggerLookup struct{}

func (registryTriggerLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	return registry.LookupTrigger(nodeType)
}

// triggerSeedBaseURL returns the control-plane origin the hosted trigger's seed
// runtime posts entry admissions to. The HTTP server URL is the seed endpoint
// origin (POST <base>/v1/executions). Entry seeding is always an HTTP round-trip
// even under the gRPC runner-protocol transport, so it requires a configured
// --server URL; an empty base disables trigger hosting (no reachable seed path).
func triggerSeedBaseURL(cfg runnerConfig) string {
	return strings.TrimRight(cfg.serverURL, "/")
}

// hostsTriggers reports whether the runner advertises any registered trigger
// node type among its declared capabilities. Only such a runner should host
// activations; a runner with no trigger capability stays passive.
func hostsTriggers(cfg runnerConfig) bool {
	for _, c := range cfg.capabilities {
		if _, ok := registry.LookupTrigger(c.NodeType); ok {
			return true
		}
	}
	return false
}

// shouldConstructPool reports whether the production runner should construct a
// ResourcePool. Per the design: construct when a credentials section is present
// OR a db/grpc capability is declared. Otherwise leave the pool nil to
// preserve the no-pool contract (TestRunner_NoPoolLeavesCtxWithoutPool).
func shouldConstructPool(cfg runnerConfig) bool {
	if len(cfg.credentials) > 0 {
		return true
	}
	for _, c := range cfg.capabilities {
		if c.NodeType == "xflow.database" || c.NodeType == "xflow.grpc" {
			return true
		}
	}
	return false
}
func runWithSignals(cfg runnerConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.runnerID == "" {
		cfg.runnerID = fmt.Sprintf("runner-%d", os.Getpid())
	}
	return runWithReconnect(ctx, func(ctx context.Context) error {
		return runRunner(ctx, cfg)
	})
}

func runWithReconnect(ctx context.Context, fn runFunc) error {
	backoff := reconnectMinBackoff
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, errStop) {
			return err
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
	}
}

// artifactCacheDir returns the local directory for caching artifact blobs.
// Priority: XFLOW_ARTIFACT_CACHE_DIR env > os.UserCacheDir()/xflow/artifacts.
func artifactCacheDir() string {
	if dir := os.Getenv("XFLOW_ARTIFACT_CACHE_DIR"); dir != "" {
		return dir
	}
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "xflow", "artifacts")
}

// buildMetricsReporter assembles the server-side metrics reporter, or returns
// nil when this runner should not report.
//
// Two paths return (nil, nil) rather than an error:
//   - reporting was not requested;
//   - the transport's client does not implement MetricsReportClient. Only the
//     HTTP client does (see spec §4.3: gRPC is not a target deployment and
//     crosses clouds via the Relay Gateway). A gRPC runner told to report keeps
//     running without reporting — refusing to start would turn a config that is
//     merely ineffective into an outage. The Warn at the call site is the signal.
func buildMetricsReporter(client runnersvc.ProtocolClient, m *metrics.Metrics, cfg runnerConfig) (*runnersvc.MetricsReporter, error) {
	if !cfg.reportMetrics {
		return nil, nil
	}
	interval := runnersvc.DefaultMetricsReportInterval
	if cfg.reportMetricsInterval != "" {
		d, err := parsePositiveDuration("report metrics interval", cfg.reportMetricsInterval)
		if err != nil {
			return nil, err
		}
		interval = d
	}
	reportClient, ok := client.(runnersvc.MetricsReportClient)
	if !ok {
		return nil, nil
	}
	return runnersvc.NewMetricsReporter(runnersvc.MetricsReporterConfig{
		Gatherer: m.Registry(),
		Client:   reportClient,
		RunnerID: cfg.runnerID,
		Interval: interval,
		Metrics:  m,
		Logger:   slog.Default(),
	}), nil
}
