package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

const (
	transportHTTP = "http"
	transportGRPC = "grpc"
)

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
	Close() error
}

var newRunnerService = func(cfg xflowsdk.RunnerConfig, opts ...xflowsdk.RunnerOption) (runnerService, error) {
	return xflowsdk.NewRunner(cfg, opts...)
}

// runRunner translates the resolved CLI/YAML config into the SDK's RunnerConfig
// and runs the embedded runner.
//
// The assembly itself lives in sdk/xflow, not here. It is order-critical in
// several places whose failure modes are all silent — a missing GroupRuntime
// means group leases are never SENT rather than failing, an artifact resolver
// that reaches only the dispatcher leaves nested scripts unable to fetch, an
// HTTP client that skips the TLS material makes the readiness gate decline
// forever — and a second hand-maintained copy of it in package main is how a
// field added to one and not the other goes unnoticed. This function's whole
// job is field translation plus the two process-level concerns the SDK
// deliberately leaves to its host: the tracer provider's lifecycle and the
// local scrape listener.
func runRunner(ctx context.Context, cfg runnerConfig) error {
	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		return err
	}

	// Tracing is initialized here rather than in the SDK because the provider
	// owns a process-global exporter and a shutdown that must outlive ctx.
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

	// The registry and the observer wiring are unconditional — metrics.New()
	// allocates a Prometheus registry and nothing else (no listener, no
	// goroutine), and an unobserved registry is what makes reporting return an
	// empty payload. Only the two *exits* are optional: --metrics-addr opens a
	// scrape port, --report-metrics ships to the server. Binding the registry to
	// --metrics-addr, as the pre-proxy code did, would have made the
	// cross-domain runner — the one case that cannot be scraped — the one case
	// that also cannot report.
	m := metrics.New()

	runner, err := newRunnerService(sdkCfg,
		xflowsdk.WithRunnerTracer(tracer),
		xflowsdk.WithRunnerMetrics(m),
		xflowsdk.WithRunnerLogger(slog.Default()))
	if err != nil {
		return err
	}
	defer runner.Close()

	// Which transports can actually report is the SDK's decision (only the HTTP
	// protocol client implements MetricsReportClient), so this only says what
	// was asked for. Restating the rule here would be a second copy of it, free
	// to drift from the one that decides.
	if cfg.reportMetrics {
		slog.Info("metrics reporting to server requested", "interval", cfg.reportMetricsInterval,
			"transport", cfg.transport)
	}

	if cfg.metricsAddr != "" {
		metricsServer := &http.Server{Addr: cfg.metricsAddr, Handler: m.Handler()}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server failed", "error", err)
			}
		}()
		slog.Info("metrics server started", "addr", cfg.metricsAddr)
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(shutCtx)
		}()
	}

	return runner.Run(ctx)
}

// toSDKRunnerConfig converts the resolved CLI/YAML config into the SDK's shape.
//
// The durations are re-parsed here rather than carried as time.Duration through
// runnerConfig because the CLI keeps them as strings all the way through
// file/env/flag precedence resolution. They were already validated by
// validateRunnerConfig; parsing again is how the validated value actually
// reaches the runner, which is what the previous code failed to do for the
// heartbeat interval — it parsed, discarded the result, and every runner
// heartbeated at the service's hardcoded 5s no matter what was configured.
func toSDKRunnerConfig(cfg runnerConfig) (xflowsdk.RunnerConfig, error) {
	heartbeat, err := parsePositiveDuration("heartbeat interval", cfg.heartbeatInterval)
	if err != nil {
		return xflowsdk.RunnerConfig{}, err
	}
	pollWait, err := parsePositiveDuration("poll wait", cfg.pollWait)
	if err != nil {
		return xflowsdk.RunnerConfig{}, err
	}
	reportInterval := time.Duration(0)
	if cfg.reportMetrics && cfg.reportMetricsInterval != "" {
		reportInterval, err = parsePositiveDuration("report metrics interval", cfg.reportMetricsInterval)
		if err != nil {
			return xflowsdk.RunnerConfig{}, err
		}
	}
	return xflowsdk.RunnerConfig{
		ServerURL:             cfg.serverURL,
		Transport:             cfg.transport,
		GRPCTarget:            cfg.grpcTarget,
		RunnerID:              cfg.runnerID,
		Concurrency:           cfg.concurrency,
		Capabilities:          capabilityNodeTypes(cfg.capabilities),
		Labels:                cloneStringMap(cfg.labels),
		Namespaces:            cfg.namespaces,
		Token:                 cfg.token,
		TLSServerCA:           cfg.tlsServerCA,
		TLSClientCert:         cfg.tlsClientCert,
		TLSClientKey:          cfg.tlsClientKey,
		HeartbeatInterval:     heartbeat,
		PollWait:              pollWait,
		Credentials:           cfg.credentials,
		ResourcePoolConfig:    cfg.resourcePoolConfig,
		ArtifactCacheDir:      os.Getenv("XFLOW_ARTIFACT_CACHE_DIR"),
		ArtifactCacheMaxBytes: artifactCacheMaxBytesFromEnv(),
		ReportMetrics:         cfg.reportMetrics,
		ReportMetricsInterval: reportInterval,
	}, nil
}

// artifactCacheMaxBytesFromEnv resolves XFLOW_ARTIFACT_CACHE_MAX_BYTES. Unset
// returns 0, which tells RunnerConfig to use the SDK's default cap. A
// malformed value also returns 0 rather than taking the runner down over a
// tuning-knob typo -- the same fail-open choice the sibling wasm compilation
// cache's XFLOW_WASM_CACHE_MAX_BYTES makes for the same reason.
func artifactCacheMaxBytesFromEnv() int64 {
	v := strings.TrimSpace(os.Getenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES"))
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		slog.Warn("runner: ignoring malformed XFLOW_ARTIFACT_CACHE_MAX_BYTES, using the default artifact cache cap",
			"value", v, "error", err)
		return 0
	}
	return n
}

// capabilityNodeTypes flattens the parsed capabilities back to node type names,
// which is all --cap can express: it has no syntax for a feature list, so the
// Features field is always empty here. The SDK re-adds the group execution
// feature, which is the only one that exists and the one an operator could not
// spell even if they knew of it.
func capabilityNodeTypes(caps []protocol.Capability) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		out = append(out, c.NodeType)
	}
	return out
}

// runWithSignals runs the runner until SIGINT/SIGTERM.
//
// Reconnect-after-transport-failure is the SDK's, not this file's: Runner.Run
// owns the loop, so an embedded host gets the same durability as this binary
// without writing its own. A second copy here is exactly how the two drift.
func runWithSignals(cfg runnerConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.runnerID == "" {
		cfg.runnerID = fmt.Sprintf("runner-%d", os.Getpid())
	}
	return runRunner(ctx, cfg)
}
