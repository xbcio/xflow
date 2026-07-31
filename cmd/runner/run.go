package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
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

var newRunnerService = func(client runnersvc.ProtocolClient, registry engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
	return runnersvc.New(client, registry, cfg)
}

func runRunner(ctx context.Context, cfg runnerConfig) error {
	serviceCfg, err := runnerServiceConfig(cfg)
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
	registry := execution.NewRegistry()
	// Absorb script-engine cold start before the first lease arrives: qjs pays a
	// ~330 ms QuickJS-wasm compile and the wasm reactor opens its runtime
	// (resolving the on-disk compilation cache). A failure here is not fatal —
	// each engine still warms lazily — so log and carry on.
	if err := xnode.WarmupScriptEngines(ctx); err != nil {
		slog.Warn("script engine warmup failed; engines will warm on first use", "error", err)
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
		httpClient := http.DefaultClient
		if tlsCfg != nil {
			httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
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

func runnerServiceConfig(cfg runnerConfig) (runnersvc.Config, error) {
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
		Capabilities: cfg.capabilities,
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
		seedClient := &http.Client{Timeout: 30 * time.Second}
		handler := runnersvc.NewTriggerActivationHandler(seedBaseURL, cfg.token, lookup,
			runnersvc.WithSeedHTTPClient(seedClient))
		svcCfg.ActivationTracker = runnersvc.NewActivationTracker(handler, slog.Default())
	}
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
