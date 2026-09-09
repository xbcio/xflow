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
	// autoLabels adds environment-derived xflow.io/* labels to the manual set.
	autoLabels bool
	// token is the runner's bearer token (matched against the server's
	// runners.yaml policy). Empty means "no auth", which the server accepts
	// only when running with --auth-mode disabled or dry-run.
	token string
	// TLS: serverCA verifies the server's certificate; clientCert/clientKey
	// present a client cert for mTLS. All three empty means plaintext.
	tlsServerCA   string
	tlsClientCert string
	tlsClientKey  string
	// identityStoreKind / identityFile select where this runner keeps the
	// identity it was issued at enrollment. "ephemeral" (the default) keeps it
	// in memory only, which is byte-identical to the pre-enrollment behavior:
	// nothing is written to disk unless asked.
	identityStoreKind string
	identityFile      string
	// registrationCode bootstraps enrollment when no identity is stored yet.
	// Never logged.
	registrationCode string
	// allowPlaintext opts out of the transport-security gate. Without it a
	// runner whose control-plane connection carries no TLS material at all
	// refuses to start, because its bearer token would cross the wire in the
	// clear.
	allowPlaintext bool
	// requireSupplyEncryption refuses to keep running if the control plane
	// issued no supply encryption key at registration.
	requireSupplyEncryption bool
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
	cmd.Flags().StringVar(&cfg.serverURL, "server", cfg.serverURL, "xflow-server base URL; carries task traffic under --transport=http, and always carries enrollment even under --transport=grpc")
	cmd.Flags().StringVar(&cfg.transport, "transport", cfg.transport, "Runner Protocol transport: http or grpc")
	cmd.Flags().StringVar(&cfg.grpcTarget, "grpc-target", cfg.grpcTarget, "xflow-server gRPC target host:port (grpc transport)")
	cmd.Flags().StringVar(&cfg.runnerID, "id", cfg.runnerID, "Runner ID")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Runner concurrency")
	cmd.Flags().StringVar(&cfg.capRaw, "cap", cfg.capRaw, "Comma-separated node type capabilities")
	cmd.Flags().StringArrayVar(&cfg.labelRaw, "label", cfg.labelRaw, "Runner label as key=value; repeatable")
	cmd.Flags().StringArrayVar(&cfg.namespaceRaw, "namespace", cfg.namespaceRaw, "Namespace this runner serves; repeatable (default: default)")
	cmd.Flags().BoolVar(&cfg.autoLabels, "auto-labels", cfg.autoLabels, "Add environment-derived xflow.io/* labels (os, arch, env, hostname)")
	cmd.Flags().StringVar(&cfg.heartbeatInterval, "heartbeat-interval", cfg.heartbeatInterval, "Heartbeat interval")
	cmd.Flags().StringVar(&cfg.pollWait, "poll-wait", cfg.pollWait, "Poll wait duration when no task is available")
	cmd.Flags().StringVar(&cfg.token, "token", cfg.token, "Runner bearer token (prefer XFLOW_RUNNER_TOKEN env)")
	cmd.Flags().StringVar(&cfg.tlsServerCA, "tls-server-ca", cfg.tlsServerCA, "Path to server CA bundle (enables TLS)")
	cmd.Flags().StringVar(&cfg.tlsClientCert, "tls-client-cert", cfg.tlsClientCert, "Path to client TLS certificate (enables mTLS)")
	cmd.Flags().StringVar(&cfg.tlsClientKey, "tls-client-key", cfg.tlsClientKey, "Path to client TLS private key")
	cmd.Flags().StringVar(&cfg.identityStoreKind, "identity-store", cfg.identityStoreKind, "Where to keep the enrolled identity: ephemeral or file")
	cmd.Flags().StringVar(&cfg.identityFile, "identity-file", cfg.identityFile, "Path to the identity file (--identity-store=file)")
	cmd.Flags().StringVar(&cfg.registrationCode, "registration-code", cfg.registrationCode, "One-time code used to enroll when no identity is stored; enrollment dials --server over HTTP regardless of --transport (prefer XFLOW_RUNNER_REGISTRATION_CODE)")
	cmd.Flags().BoolVar(&cfg.allowPlaintext, "allow-plaintext", cfg.allowPlaintext, "Permit an unencrypted control-plane connection (no TLS material configured)")
	cmd.Flags().BoolVar(&cfg.requireSupplyEncryption, "require-supply-encryption", cfg.requireSupplyEncryption, "Exit if the control plane issues no supply encryption key at registration")
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
	// Identity is settled before anything else: it rewrites cfg.runnerID and
	// cfg.token, and every client built below reads them.
	store, err := newIdentityStore(cfg)
	if err != nil {
		return err
	}
	cfg, err = resolveRunnerIdentity(ctx, cfg, store)
	if err != nil {
		return err
	}

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
	lifecycle := newLifecycleState()
	lifecycle.requireSupplyEncryption = cfg.requireSupplyEncryption
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	lifecycle.SetOnFatal(func(error) { cancelRun() })

	runner, err := newRunnerService(sdkCfg,
		xflowsdk.WithRunnerTracer(tracer),
		xflowsdk.WithRunnerMetrics(m),
		xflowsdk.WithRunnerLogger(slog.Default()),
		xflowsdk.WithRunnerLifecycleObserver(lifecycle))
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
		// One listener for all three: an operator who exposed the scrape port
		// has exposed the probes, and a second port is one more thing to get
		// wrong in a NetworkPolicy. Probes are therefore unavailable when
		// --metrics-addr is empty — documented, not silent.
		probeMux := http.NewServeMux()
		probeMux.Handle("GET /metrics", m.Handler())
		registerLifecycleProbes(probeMux, lifecycle)
		metricsServer := &http.Server{Addr: cfg.metricsAddr, Handler: probeMux}
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

	// The renewal loop needs the protocol client, which the SDK owns, plus
	// the identity store, the run context, and the server URL -- runRunner is
	// the only place that holds all four at once. decideIdentityRenewal is
	// the gate; every one of its "do not start" outcomes fails open (at most
	// a Warn), because none of them is a reason an already-valid identity
	// should stop serving traffic.
	if rc, start, warnMsg, warnErr := decideIdentityRenewal(cfg, store); warnErr != nil {
		slog.Warn(warnMsg, "runner_id", cfg.runnerID, "error", warnErr)
	} else if start {
		go runIdentityRenewal(runCtx, rc, cfg.runnerID, cfg.token, slog.Default())
	}

	err = runner.Run(runCtx)
	// A fatal startup condition cancels runCtx; Run's reconnect loop treats a
	// cancelled context as a clean stop, so it returns nil here regardless of
	// what actually went wrong (sdk/xflow/runner.go's runWithReconnect has
	// three exits and all three return nil). err therefore carries none of
	// the reason, which is why lifecycle.Fatal() is not redundant with it and
	// must win.
	if fatal := lifecycle.Fatal(); fatal != nil {
		return fatal
	}
	return err
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

// renewClient is the seam the renewal loop is tested through. cmd/runner has
// no business dialing a real server in a unit test.
type renewClient interface {
	RenewIdentity(context.Context, protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error)
}

// renewOutcome is why renewOnce returned. The three cases must stay distinct:
// collapsing "the server reports no expiry" into "the call failed" would
// retire the renewal loop on the first transient error and let the identity
// lapse silently -- the exact failure this whole task exists to prevent.
type renewOutcome int

const (
	renewOK       renewOutcome = iota // ExpiresAt is valid and in the future
	renewNoExpiry                     // server issues no expiry; retire the loop
	renewFailed                       // transient; keep the loop, retry later
	renewAborted                      // context done; unwind
)

// renewRetryWait is how long to wait after a failed renewal when no expiry is
// known yet (or after a deadline has lapsed -- see runIdentityRenewal).
// Without a deadline to divide, there is nothing to derive a cadence from, so
// this is the one fixed interval in the loop. A package-level var, not a
// const, so a test can shrink it instead of waiting on a real 30s clock.
var renewRetryWait = 30 * time.Second

// renewMinWait is the floor the TTL/3 cadence is never allowed to fall below
// for a deadline still in the future. A package-level var for the same
// testability reason as renewRetryWait.
var renewMinWait = time.Second

// runnerHasIssuedIdentity reports whether store holds an identity obtained
// through enrollment, as opposed to a static --token configuration. Only an
// issued identity has anything on the server to renew: a runner started with
// --id/--token has never enrolled, RenewIdentity would fail against it every
// time, and renewFailed deliberately never retires the loop -- so running it
// unconditionally would warn every 30s forever on every statically configured
// deployment in the fleet. store.Load() is side-effect-free and safe to call
// again here after resolveRunnerIdentity already called it once.
func runnerHasIssuedIdentity(store identityStore) (bool, error) {
	_, hasIssued, err := store.Load()
	return hasIssued, err
}

// renewClientFor builds the HTTP protocol client the renewal loop calls
// through.
//
// This mirrors the client sdk/xflow's newRunnerProtocolClient builds for the
// HTTP transport -- TLS material via NewRunnerHTTPClient, then WithToken --
// and NOT the enrollment client (resolveRunnerIdentity, above), which has no
// token to attach because enrollment is the one call made before a token
// exists. Renewal already holds one, and an already-enrolled runner's request
// must carry it or the server authenticates against an empty string and
// renewal fails every time with no crash and no red test to catch it.
func renewClientFor(cfg runnerConfig) (renewClient, error) {
	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		return nil, err
	}
	httpClient, err := xflowsdk.NewRunnerHTTPClient(sdkCfg, enrollHTTPTimeout)
	if err != nil {
		return nil, fmt.Errorf("renew: build http client: %w", err)
	}
	client := protocol.NewClient(cfg.serverURL, httpClient)
	if cfg.token != "" {
		client = client.WithToken(cfg.token)
	}
	return client, nil
}

// decideIdentityRenewal is the gate runRunner asks before starting the
// renewal loop. It is a pure decision function -- no goroutine, no logging --
// specifically so the gate itself is directly assertable in a unit test
// without standing up a full runRunner: a mutation that deletes or weakens
// any one of its checks must turn a test red here, not only pass silently
// through an untested wiring block.
//
// Returns:
//   - start=true, rc set: the caller should launch runIdentityRenewal(rc, ...).
//   - start=false, warnErr=nil: a silent no-op (the static --token case:
//     nothing on the server to renew, and warning every 30s forever on every
//     such deployment would be noise, not signal).
//   - start=false, warnErr!=nil: the caller should log warnMsg with warnErr,
//     then proceed to run the rest of the runner unaffected -- none of these
//     checks failing is a reason an already-valid identity should stop
//     serving traffic.
func decideIdentityRenewal(cfg runnerConfig, store identityStore) (rc renewClient, start bool, warnMsg string, warnErr error) {
	hasIssued, herr := runnerHasIssuedIdentity(store)
	if herr != nil {
		// Unable to tell whether this identity was issued; the identity is
		// valid right now regardless, so stay quiet rather than guess.
		return nil, false, "runner identity renewal disabled: cannot determine whether an identity was issued", herr
	}
	if !hasIssued {
		// Only an issued identity can be renewed. A runner configured with a
		// static --token has nothing on the server to extend, and running
		// the loop for it would log a warning every 30s forever --
		// renewFailed deliberately never retires the loop.
		return nil, false, "", nil
	}
	if verr := validateEnrollTransportSecurity(cfg); verr != nil {
		// Reusing enrollment's gate: it judges only the URL scheme against
		// --allow-plaintext, never the registration code. Its message is
		// written for enrollment though, so do not surface it as the
		// headline here.
		return nil, false, "runner identity renewal disabled: renewing over a plaintext --server would send the runner token in the clear; use https or --allow-plaintext", verr
	}
	client, cerr := renewClientFor(cfg)
	if cerr != nil {
		// A renewal client that cannot be built is not a reason to refuse to
		// run: taking the fleet down over a mis-typed TLS path would turn a
		// config error into an outage.
		return nil, false, "runner identity renewal disabled: cannot build renewal client", cerr
	}
	return client, true, "", nil
}

// runIdentityRenewal keeps this runner's issued identity alive.
//
// It lives here, in package main, and not in service/runner's heartbeat loop,
// for a structural reason: runRunner is the only place that holds the
// identity store, the run context, and the server URL at the same time. The
// heartbeat loop is a layer below and cannot reach any of them.
//
// The first call happens immediately at startup rather than after one tick.
// That single call does three jobs the local identity file cannot do: it
// returns the authoritative ExpiresAt (which the file deliberately does not
// store -- identity file schema is unchanged by this task), it proves the
// identity has not been revoked while this runner was down, and -- when the
// server reports no expiry at all -- it retires the loop.
func runIdentityRenewal(ctx context.Context, c renewClient, runnerID, token string, log *slog.Logger) {
	var deadline time.Time
	for {
		next, outcome := renewOnce(ctx, c, runnerID, token, log)
		switch outcome {
		case renewAborted:
			return
		case renewNoExpiry:
			// Nothing to renew, ever. Exiting is not an error path -- it is
			// the configuration the whole fleet runs in until an operator
			// sets --runner-identity-ttl.
			log.Info("runner identity has no expiry; renewal loop disabled")
			return
		case renewOK:
			deadline = next
		case renewFailed:
			// Keep the previous deadline. The identity is still valid until
			// it, so one failure is not fatal; taking the runner down over a
			// transient server error would turn a blip into an outage.
		}

		// Renew once validity drops below a third of the remaining window, so
		// a transient outage has two more attempts before the identity
		// lapses.
		wait := renewRetryWait
		if !deadline.IsZero() {
			// A deadline in the past must not drive the cadence: time.Until
			// goes negative, and a naive clamp to renewMinWait would make a
			// fleet whose identities have all lapsed hammer the server once
			// per runner per second, forever -- Renew (T4) refuses an
			// already-expired identity by design, so every one of those
			// calls fails and deadline never moves. Falling back to
			// renewRetryWait instead: the identity is already invalid, so
			// there is nothing left to renew "in time" for.
			if remaining := time.Until(deadline); remaining > 0 {
				wait = remaining / 3
				if wait < renewMinWait {
					wait = renewMinWait
				}
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// renewOnce performs one renewal attempt.
//
// The log line carries the runner id and the error and nothing else -- never
// the token, which exists on this side only inside the protocol client.
func renewOnce(ctx context.Context, c renewClient, runnerID, token string, log *slog.Logger) (time.Time, renewOutcome) {
	resp, err := c.RenewIdentity(ctx, protocol.RenewIdentityRequest{RunnerID: runnerID, AuthToken: token})
	if ctx.Err() != nil {
		return time.Time{}, renewAborted
	}
	if err != nil {
		log.Warn("runner identity renewal failed", "runner_id", runnerID, "error", err)
		return time.Time{}, renewFailed
	}
	if resp.ExpiresAt == "" {
		return time.Time{}, renewNoExpiry
	}
	t, perr := time.Parse(time.RFC3339, resp.ExpiresAt)
	if perr != nil {
		// An unparsable expiry is a server bug, not a "no expiry" signal.
		// Treating it as the latter would disable renewal on a typo.
		log.Warn("runner identity renewal returned an unparsable expiry",
			"runner_id", runnerID, "value", resp.ExpiresAt, "error", perr)
		return time.Time{}, renewFailed
	}
	return t.UTC(), renewOK
}
