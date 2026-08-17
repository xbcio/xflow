// Package xflow runner.go: the embeddable runner entry point.
//
// NewRunner is the execution-plane counterpart to NewServer. A host program
// registers its own node types (node.Define + registry.Register, or a
// types.ActionHandler implementation), calls NewRunner, and runs it; the
// runner connects to a control plane over the Runner Protocol, claims leases
// for the node types it advertises, and executes them in-process.
//
// The assembly this file performs is not a convenience wrapper. Several of its
// steps fail silently when omitted, which is why it lives here rather than in
// each host program:
//
//   - A missing GroupRuntime does not fail a group lease, it never receives
//     one: a group unit's routing requires the group.exec.v1 feature, so a
//     runner that does not advertise it is filtered out during assignment and
//     the task waits in the queue with nothing logged on either side.
//   - The GroupRuntime must exist before the TriggerActivationHandler is built,
//     because a runner that both hosts triggers and executes groups needs the
//     same instance in both places.
//   - The artifact resolver must reach three consumers (dispatcher, group
//     runtime, subgraph runtime), because the latter two build their own inner
//     backends per attempt. A resolver installed only on the dispatcher leaves
//     a script nested inside a group or a map body failing permanently with
//     script.artifact_unavailable.
//   - Every HTTP client pointed at the control plane must carry the runner's
//     TLS material. One that silently used http.DefaultTransport would ignore
//     a private CA, and a failing supply fetch makes the readiness gate decline
//     forever, so the runner never hosts its triggers at all.
//
// cmd/runner is a CLI/YAML front end over this same assembly.
package xflow

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
)

// Runner transport names for RunnerConfig.Transport.
const (
	RunnerTransportHTTP = "http"
	RunnerTransportGRPC = "grpc"
)

// Cache bounds, shared by the group-package and map-body caches. A package
// compiles once per distinct PackageHash and every execution of one group (or
// every batch of one map node) carries the same hash, so the entry count tracks
// distinct groups/map nodes this runner serves — not executions.
const runnerPackageCacheEntries = 64

// RunnerConfig configures an embedded xflow runner. ServerURL is the only
// required field; everything else has a working default.
type RunnerConfig struct {
	// ServerURL is the control plane's HTTP origin. Required even under the
	// gRPC transport: entry seeding and artifact fetch are always HTTP
	// round-trips to this origin.
	ServerURL string

	// Transport selects the Runner Protocol channel, RunnerTransportHTTP
	// (default) or RunnerTransportGRPC. GRPCTarget is required for the latter.
	Transport  string
	GRPCTarget string

	// RunnerID identifies this runner to the control plane. Empty lets the
	// runner service generate one.
	RunnerID string

	// Concurrency is how many leases this runner executes at once, and the
	// capacity it reports. Zero uses the runner service default.
	Concurrency int

	// Capabilities lists the node types this runner claims leases for. The
	// control plane matches against this list, not against the process's node
	// registry, so a registered handler that is not listed here never receives
	// work. The group execution capability is added automatically.
	Capabilities []string

	// Labels are matched against a workflow's, node's, or group's
	// RunnerSelector. A runner with no labels is only eligible for unpinned
	// work and for "default"-mode selectors after their fallback grace.
	Labels map[string]string

	// Namespaces restricts which namespaces' assignments this runner may
	// claim. Empty means no restriction.
	Namespaces []namespace.Namespace

	// Token is the runner's bearer token, matched against the server's runner
	// policy. Empty means no auth, which the server accepts only when running
	// with auth disabled or in dry-run.
	Token string

	// TLS material for every connection to the control plane. TLSServerCA
	// verifies the server; TLSClientCert and TLSClientKey present a client
	// certificate for mTLS and must be set together. All empty means plaintext.
	TLSServerCA   string
	TLSClientCert string
	TLSClientKey  string

	// HeartbeatInterval and PollWait override the runner service defaults when
	// non-zero.
	HeartbeatInterval time.Duration
	PollWait          time.Duration

	// ReportMetrics ships this runner's whole Prometheus registry to the server,
	// which merges it into its own /metrics. Requires WithRunnerMetrics, and is
	// deliberately independent of whether the host exposes a scrape endpoint: a
	// runner in another network domain cannot be scraped, and is also the runner
	// least likely to want a listening port.
	//
	// Only the HTTP transport can report. A gRPC runner with this set keeps
	// running without reporting rather than refusing to start — an ineffective
	// config should not become an outage.
	ReportMetrics bool

	// ReportMetricsInterval is the local reporting cadence; the server can
	// override it at runtime. Zero uses runnersvc.DefaultMetricsReportInterval.
	ReportMetricsInterval time.Duration

	// Credentials holds named credential maps (driver/dsn, token/base_url, …)
	// exposed to node handlers by name. Its presence also causes a
	// ResourcePool to be constructed.
	Credentials map[string]map[string]any

	// ResourcePoolConfig tunes the pooled *sql.DB / *grpc.ClientConn cache used
	// by DatabaseNode and GRPCNode. The zero value uses documented defaults.
	// A pool is constructed when Credentials is non-empty or a database/grpc
	// capability is declared.
	ResourcePoolConfig types.ResourcePoolConfig

	// ArtifactCacheDir is where fetched script artifacts are cached on disk.
	// Empty uses the process default.
	ArtifactCacheDir string
}

type runnerOptions struct {
	logger           *slog.Logger
	tracer           tracing.Tracer
	metrics          *metrics.Metrics
	artifactResolver func(ctx context.Context, digest string) ([]byte, error)
	nodeRegistry     *execution.Registry
}

// RunnerOption configures a Runner.
type RunnerOption func(*runnerOptions)

// WithRunnerLogger sets the logger used by the supply gate, activation tracker,
// and runner service. Defaults to slog.Default().
func WithRunnerLogger(l *slog.Logger) RunnerOption {
	return func(o *runnerOptions) { o.logger = l }
}

// WithRunnerTracer installs a tracer so the runner extracts the remote parent
// from each lease carrier and injects the report carrier.
func WithRunnerTracer(t tracing.Tracer) RunnerOption {
	return func(o *runnerOptions) { o.tracer = t }
}

// WithRunnerMetrics wires Prometheus observers into the supply gate, wasm and
// script engines, and the trigger consumer. Pass the same registry to
// Runner.MetricsHandler to expose a scrape endpoint.
func WithRunnerMetrics(m *metrics.Metrics) RunnerOption {
	return func(o *runnerOptions) { o.metrics = m }
}

// WithRunnerArtifactResolver replaces the default digest -> script bytes
// resolver. The default reads through a local disk cache to
// GET /v1/artifacts/{digest} on the control plane, which is what a host program
// wants unless it ships artifacts by another route entirely.
//
// Whatever is installed here reaches all three consumers — the top-level
// dispatcher, the group runtime, and the subgraph runtime — so a script nested
// inside a group or a map body resolves its module the same way a top-level one
// does.
func WithRunnerArtifactResolver(fn func(ctx context.Context, digest string) ([]byte, error)) RunnerOption {
	return func(o *runnerOptions) { o.artifactResolver = fn }
}

// WithRunnerNodeRegistry supplies the handler registry the runner resolves
// leases against. Defaults to a fresh execution.NewRegistry(), which resolves
// through the process-global node registry that node.Define and
// registry.Register write to.
func WithRunnerNodeRegistry(reg *execution.Registry) RunnerOption {
	return func(o *runnerOptions) { o.nodeRegistry = reg }
}

// Runner is an embeddable xflow execution-plane runner. It connects to a
// control plane, claims leases for its advertised node types, executes them
// with the handlers registered in this process, and reports results.
type Runner struct {
	svc     *runnersvc.Runner
	cleanup func()
	pool    types.ResourcePool
	metrics *metrics.Metrics
}

// NewRunner creates an embeddable runner. Register node handlers before
// calling Run — the runner resolves each lease against the process's node
// registry at execution time.
//
// Example:
//
//	registry.Register(&myAnalyseNode{})
//	r, err := xflow.NewRunner(xflow.RunnerConfig{
//		ServerURL:    "https://xflow.internal:8080",
//		Capabilities: []string{"my.analyse", "xflow.trigger.kafka"},
//		Labels:       map[string]string{"env": "test"},
//	})
//	if err != nil { ... }
//	defer r.Close()
//	return r.Run(ctx)
func NewRunner(cfg RunnerConfig, opts ...RunnerOption) (*Runner, error) {
	if strings.TrimSpace(cfg.ServerURL) == "" {
		return nil, fmt.Errorf("xflow: RunnerConfig.ServerURL is required: it is the " +
			"control plane server origin for leases, entry seeding, and artifact fetch")
	}
	o := runnerOptionsFrom(opts)

	svcCfg, err := buildRunnerServiceConfig(cfg, opts...)
	if err != nil {
		return nil, err
	}
	client, cleanup, err := newRunnerProtocolClient(cfg)
	if err != nil {
		return nil, err
	}
	reporter, err := newRunnerMetricsReporter(client, o.metrics, cfg)
	if err != nil {
		cleanup()
		return nil, err
	}
	svcCfg.MetricsReporter = reporter
	reg := o.nodeRegistry
	if reg == nil {
		reg = execution.NewRegistry()
	}
	return &Runner{
		svc:     runnersvc.New(client, reg, svcCfg),
		cleanup: cleanup,
		pool:    svcCfg.ResourcePool,
		metrics: o.metrics,
	}, nil
}

// newRunnerMetricsReporter assembles the server-side metrics reporter, or
// returns nil when this runner should not report.
//
// Two paths return (nil, nil) rather than an error: reporting was not
// requested, and the transport's client does not implement MetricsReportClient
// (only the HTTP one does — gRPC is not a target deployment and crosses clouds
// via the Relay Gateway). A gRPC runner told to report keeps running without
// reporting, because refusing to start would turn a config that is merely
// ineffective into an outage.
func newRunnerMetricsReporter(client runnersvc.ProtocolClient, m *metrics.Metrics, cfg RunnerConfig) (*runnersvc.MetricsReporter, error) {
	if !cfg.ReportMetrics {
		return nil, nil
	}
	if m == nil {
		return nil, fmt.Errorf("xflow: RunnerConfig.ReportMetrics requires WithRunnerMetrics: " +
			"there is no registry to report")
	}
	interval := runnersvc.DefaultMetricsReportInterval
	if cfg.ReportMetricsInterval != 0 {
		if cfg.ReportMetricsInterval < 0 {
			return nil, fmt.Errorf("xflow: RunnerConfig.ReportMetricsInterval must be positive, got %s",
				cfg.ReportMetricsInterval)
		}
		interval = cfg.ReportMetricsInterval
	}
	reportClient, ok := client.(runnersvc.MetricsReportClient)
	if !ok {
		slog.Warn("metrics reporting was requested but this transport cannot report; "+
			"continuing without it", "transport", cfg.Transport)
		return nil, nil
	}
	return runnersvc.NewMetricsReporter(runnersvc.MetricsReporterConfig{
		Gatherer: m.Registry(),
		Client:   reportClient,
		RunnerID: cfg.RunnerID,
		Interval: interval,
		Metrics:  m,
		Logger:   slog.Default(),
	}), nil
}

// Run registers with the control plane and blocks, claiming and executing
// leases until ctx is cancelled, reconnecting after any transport failure.
//
// Script engines are warmed before the first lease arrives: QuickJS pays a
// ~330ms compile and the wasm reactor opens its runtime. A warmup failure is
// logged, not fatal — each engine still warms lazily on first use.
//
// The reconnect loop is not optional resilience, it is what makes this a
// runner rather than a one-shot session. runnersvc.Run returns on the FIRST
// error from any of its loops — a ReportResult timeout, a heartbeat failure, a
// non-2xx poll — and does not distinguish a control plane that is gone from one
// that is restarting. Calling it once therefore turns any momentary fault into a
// permanently dead runner, and nothing about that looks broken: the host process
// keeps serving and any hosted trigger stays activated in the control plane's
// view, while messages simply stop being consumed. Both cmd/runner and the SAS
// embedded host reached that failure in production and each grew its own copy of
// this loop before it lived here.
//
// Re-entering Run is safe and is the point: it opens with client.Register,
// derives a fresh session, and reports the activations it still hosts in that
// request, so a reconnect renews those leases instead of orphaning them.
func (r *Runner) Run(ctx context.Context) error {
	if err := xnode.WarmupScriptEngines(ctx); err != nil {
		slog.Warn("script engine warmup failed; engines will warm on first use", "error", err)
	}
	return runWithReconnect(ctx, r.svc.Run)
}

// Reconnect backoff bounds. Doubling from 2s to a 30s ceiling: fast enough that
// a control-plane restart costs one missed poll cycle, slow enough that a fleet
// riding out a longer outage does not amount to a retry storm.
const (
	reconnectMinBackoff = 2 * time.Second
	reconnectMaxBackoff = 30 * time.Second
)

// runWithReconnect re-enters run after each failure until ctx is cancelled.
//
// Whether to stop is decided by the caller's ctx alone, never by the shape of
// the error. runnersvc.Run cancels its own session-scoped heartbeat context as
// soon as the poll loop fails, so an in-flight heartbeat fails with a wrapped
// context.Canceled — and Run prefers that error over the poll error that
// actually caused the teardown. Treating context.Canceled as "the caller asked
// us to stop" therefore reads a plain transport fault as a shutdown and leaves
// the runner permanently silent, which is exactly the failure this loop exists
// to prevent. ctx.Err() is the only signal that means what it says.
//
// A nil return means run decided it was done, which nothing in the runner
// service currently does; looping on it would be a busy loop rather than a
// visible failure, so it ends the loop.
//
// The wait is randomized across the upper half of the current backoff so that
// several runners that lost the same control plane do not re-register in
// lockstep the moment it comes back.
func runWithReconnect(ctx context.Context, run func(context.Context) error) error {
	backoff := reconnectMinBackoff
	for {
		err := run(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		slog.Error("runner stopped; reconnecting",
			"error", err, "retry_in", wait.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
	}
}

// MetricsHandler serves the Prometheus registry passed to WithRunnerMetrics,
// for a host program that wants to mount a scrape endpoint. Returns nil when no
// metrics registry was configured.
func (r *Runner) MetricsHandler() http.Handler {
	if r.metrics == nil {
		return nil
	}
	return r.metrics.Handler()
}

// Close releases transport-owned resources (the gRPC connection) and the
// process-scoped ResourcePool. Idempotent; safe to defer immediately after
// NewRunner.
func (r *Runner) Close() error {
	if r.cleanup != nil {
		r.cleanup()
		r.cleanup = nil
	}
	if r.pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := r.pool.Close(ctx)
		r.pool = nil
		return err
	}
	return nil
}

func runnerOptionsFrom(opts []RunnerOption) *runnerOptions {
	o := &runnerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	return o
}

// buildRunnerServiceConfig performs the whole assembly and is the seam the
// tests assert on: everything that can silently go missing is observable in the
// returned Config.
//
// Construction order is load-bearing. The artifact resolver and the
// GroupRuntime are built first because the trigger activation handler needs
// both, and building that handler before the GroupRuntime existed is what made
// every trigger-group activation on a production runner fail closed.
func buildRunnerServiceConfig(cfg RunnerConfig, opts ...RunnerOption) (runnersvc.Config, error) {
	o := runnerOptionsFrom(opts)

	artifactCode := o.artifactResolver
	if artifactCode == nil {
		var err error
		artifactCode, err = newRunnerArtifactResolver(cfg)
		if err != nil {
			return runnersvc.Config{}, err
		}
	}

	reg := o.nodeRegistry
	if reg == nil {
		reg = execution.NewRegistry()
	}

	// Suspend is disabled inside a group for the same reason as inside a map
	// body: a suspended member would park a sub-execution the outer lease
	// cannot resume.
	groupRuntime := runnersvc.NewGroupRuntime(
		reg,
		runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: runnerPackageCacheEntries}),
		runnersvc.WithSuspendDisabled(),
		runnersvc.WithGroupArtifactCodeResolver(artifactCode))

	svcCfg := runnersvc.Config{
		RunnerID:     cfg.RunnerID,
		Concurrency:  cfg.Concurrency,
		Labels:       cloneStringMap(cfg.Labels),
		Capabilities: runnerCapabilities(cfg.Capabilities),
		PollWait:     cfg.PollWait,
		Tracer:       o.tracer,
		Namespaces:   cfg.Namespaces,
		GroupRuntime: groupRuntime,
		// A batch lease names a synthetic node ("m/_batch/0") that carries no
		// Input and has no registered handler, so the ordinary node path has
		// nothing to execute; the runtime resolves the body's member handlers
		// out of this same registry.
		SubgraphRuntime: runnersvc.NewSubgraphRuntime(
			reg,
			runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: runnerPackageCacheEntries}),
			runnersvc.WithSubgraphArtifactCodeResolver(artifactCode)),
		ArtifactCodeResolver: artifactCode,
		SupportsEncryption:   true,
	}
	if cfg.HeartbeatInterval > 0 {
		svcCfg.HeartbeatInterval = cfg.HeartbeatInterval
	}
	if runnerNeedsPool(cfg) {
		poolCfg := cfg.ResourcePoolConfig
		if poolCfg == (types.ResourcePoolConfig{}) {
			poolCfg = types.DefaultResourcePoolConfig()
		}
		svcCfg.ResourcePool = resource.NewDefaultResourcePool(poolCfg)
	}
	if len(cfg.Credentials) > 0 {
		// Captured in the closure; never logged or surfaced in errors.
		creds := cfg.Credentials
		svcCfg.CredentialResolver = func(_ namespace.Namespace, name string) map[string]any {
			return creds[name]
		}
	}

	if err := wireRunnerTriggerHosting(&svcCfg, cfg, o, groupRuntime); err != nil {
		return runnersvc.Config{}, err
	}
	wireRunnerMetrics(&svcCfg, o)
	return svcCfg, nil
}

// wireRunnerTriggerHosting builds the activation tracker and supply gate when
// this runner advertises a registered trigger node type. A runner with no
// trigger capability stays passive: the tracker and both supply channels are
// left nil, which keeps its heartbeat bodies identical to a runner shape that
// predates this wiring.
func wireRunnerTriggerHosting(svcCfg *runnersvc.Config, cfg RunnerConfig, o *runnerOptions, groupRuntime *runnersvc.GroupRuntime) error {
	if !runnerHostsTriggers(cfg.Capabilities) {
		return nil
	}
	seedBaseURL := runnerSeedBaseURL(cfg)
	if seedBaseURL == "" {
		return nil
	}
	// The seed client timeout is deliberately larger than the per-request
	// context timeout: the context deadline governs normal cancellation, this
	// is an absolute safety net covering connection setup and full body read.
	seedClient, err := newRunnerHTTPClient(cfg, 30*time.Second)
	if err != nil {
		return err
	}
	// The supply fetch client shares the seed origin: both talk to the control
	// plane's HTTP API. The gate publishes into supply.Default, the same
	// registry node handlers read through $supplies.
	gate := runnersvc.NewSupplyGate(&runnersvc.HTTPSupplyFetcher{
		BaseURL: seedBaseURL,
		Token:   cfg.Token,
		Client:  seedClient,
	}, supply.Default, o.logger)

	handler := runnersvc.NewTriggerActivationHandler(seedBaseURL, cfg.Token, runnerTriggerLookup{},
		runnersvc.WithSeedHTTPClient(seedClient),
		runnersvc.WithSupplyGate(gate),
		runnersvc.WithGroupRuntime(groupRuntime),
		// The same resolver the executor uses, so activation can compile a wasm
		// module before registering its supply consumer without a second client
		// or cache.
		runnersvc.WithArtifactCodeResolver(svcCfg.ArtifactCodeResolver))

	svcCfg.ActivationTracker = runnersvc.NewActivationTracker(handler, o.logger)
	// The same gate/registry pair feeds the heartbeat's two supply channels:
	// the registry is what Observed() reads out to report applied hashes, and
	// the gate is what ApplyHints fetches into on a piggybacked hint.
	svcCfg.SupplyRegistry = supply.Default
	svcCfg.SupplyGate = gate
	return nil
}

// wireRunnerMetrics installs observers on every component that reports. The
// registry is inert without them — an unobserved registry is what makes a
// scrape or a report return an empty payload.
func wireRunnerMetrics(svcCfg *runnersvc.Config, o *runnerOptions) {
	if o.metrics == nil {
		return
	}
	sm := metrics.NewSupplyMetrics(o.metrics)
	if svcCfg.SupplyGate != nil {
		svcCfg.SupplyGate.SetObserver(sm)
	}
	supply.Default.SetObserver(sm)
	xnode.SetWasmObserver(sm)
	xnode.SetScriptObserver(metrics.NewScriptMetrics(o.metrics))
	// The trigger observer covers the discards, dead letters and batch
	// admissions on the Kafka ingest path. Without it a producer emitting
	// malformed records looks exactly like an idle topic — offsets keep being
	// committed, so consumer-group lag stays at zero.
	kafkatrigger.SetObserver(metrics.NewTriggerMetrics(o.metrics))
	// The node execution timeout observer reports runner-detected timeouts, the
	// abandoned-goroutine gauge, and per-invocation duration. Without it a
	// runner that is shedding work on deadline looks identical to one that is
	// simply idle — the tasks end as failures on the server with nothing here
	// to say the deadline is what ended them.
	svcCfg.TimeoutObserver = metrics.NewNodeTimeoutMetrics(o.metrics)
}

// runnerCapabilities converts declared node types and guarantees the group
// execution capability is present and carries its feature.
//
// The feature is supplied here rather than by the caller because omitting it
// fails silently: a group unit's routing requires group.exec.v1, so a runner
// without it is filtered out during assignment and the task waits in the queue
// indefinitely with nothing logged on either side. A caller who does declare
// the group node type is completed rather than deferred to — a bare entry with
// no Features is the worst available shape, since canRunRouting ignores
// Features and accepts it while MatchCapabilities rejects it, so the runner
// reads as correctly configured while still receiving nothing.
//
// This does not make the runner eligible for groups it cannot execute: a
// group's routing requirements list every member node type alongside this
// feature, and all of them must match.
func runnerCapabilities(declared []string) []protocol.Capability {
	out := make([]protocol.Capability, 0, len(declared)+1)
	found := false
	for _, nodeType := range declared {
		nodeType = strings.TrimSpace(nodeType)
		if nodeType == "" {
			continue
		}
		c := protocol.Capability{NodeType: nodeType}
		if nodeType == engine.GroupNodeType {
			found = true
			c.Features = []string{engine.FeatureGroupExecV1}
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

// runnerTriggerLookup adapts the global node registry to the runner's
// TriggerHandlerLookup, so the activation handler resolves any registered
// trigger node type without hardcoding one.
type runnerTriggerLookup struct{}

func (runnerTriggerLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	return registry.LookupTrigger(nodeType)
}

// runnerHostsTriggers reports whether any declared capability names a
// registered trigger node type.
func runnerHostsTriggers(declared []string) bool {
	for _, nodeType := range declared {
		if _, ok := registry.LookupTrigger(strings.TrimSpace(nodeType)); ok {
			return true
		}
	}
	return false
}

// runnerNeedsPool reports whether to construct a ResourcePool: when credentials
// are present or a database/grpc capability is declared. Otherwise the pool is
// left nil to preserve the no-pool contract.
func runnerNeedsPool(cfg RunnerConfig) bool {
	if len(cfg.Credentials) > 0 {
		return true
	}
	for _, nodeType := range cfg.Capabilities {
		switch strings.TrimSpace(nodeType) {
		case "xflow.database", "xflow.grpc":
			return true
		}
	}
	return false
}

// runnerSeedBaseURL returns the control-plane origin the hosted trigger's seed
// runtime posts entry admissions to. Entry seeding is an HTTP round-trip even
// under the gRPC transport, so it always uses ServerURL.
func runnerSeedBaseURL(cfg RunnerConfig) string {
	return strings.TrimRight(cfg.ServerURL, "/")
}

func newRunnerProtocolClient(cfg RunnerConfig) (runnersvc.ProtocolClient, func(), error) {
	tlsCfg, err := buildRunnerTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	switch cfg.Transport {
	case RunnerTransportGRPC:
		creds := insecure.NewCredentials()
		if tlsCfg != nil {
			creds = credentials.NewTLS(tlsCfg)
		}
		conn, err := grpc.NewClient(cfg.GRPCTarget, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, nil, fmt.Errorf("dial gRPC server %q: %w", cfg.GRPCTarget, err)
		}
		client := protocol.NewGRPCClient(conn)
		closeConn := func() { _ = conn.Close() }
		if cfg.Token != "" {
			return client.WithToken(cfg.Token), closeConn, nil
		}
		return client, closeConn, nil
	default:
		httpClient, err := newRunnerHTTPClient(cfg, 0)
		if err != nil {
			return nil, nil, err
		}
		client := protocol.NewClient(cfg.ServerURL, httpClient)
		if cfg.Token != "" {
			return client.WithToken(cfg.Token), func() {}, nil
		}
		return client, func() {}, nil
	}
}

// buildRunnerTLSConfig resolves the runner-side TLS config. Returns nil when
// nothing was configured (plaintext).
func buildRunnerTLSConfig(cfg RunnerConfig) (*tls.Config, error) {
	if cfg.TLSServerCA == "" && cfg.TLSClientCert == "" && cfg.TLSClientKey == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLSServerCA != "" {
		caPEM, err := os.ReadFile(cfg.TLSServerCA)
		if err != nil {
			return nil, fmt.Errorf("read server CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("server CA %q contains no valid certs", cfg.TLSServerCA)
		}
		tlsCfg.RootCAs = pool
	}
	switch {
	case cfg.TLSClientCert == "" && cfg.TLSClientKey == "":
		// TLS only, no client auth.
	case cfg.TLSClientCert != "" && cfg.TLSClientKey != "":
		cert, err := tls.LoadX509KeyPair(cfg.TLSClientCert, cfg.TLSClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	default:
		return nil, fmt.Errorf("TLSClientCert and TLSClientKey must be provided together")
	}
	return tlsCfg, nil
}

// newRunnerHTTPClient builds an *http.Client honouring the runner's TLS
// material, with the given absolute timeout.
//
// Every HTTP client the runner points at the control plane goes through here.
// There are three of them (Runner Protocol, artifact fetch, entry-seed + supply
// fetch) and they all talk to the same origin, so one that silently used
// http.DefaultTransport would ignore a private CA and fail against an
// mTLS-requiring server — the supply fetch failing that way makes the readiness
// gate decline forever, so the runner never hosts its triggers at all.
func newRunnerHTTPClient(cfg RunnerConfig, timeout time.Duration) (*http.Client, error) {
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

// newRunnerArtifactResolver builds the digest -> script bytes resolver every
// script execution path shares: a read-through cache serving from local disk
// and falling back to GET /v1/artifacts/{digest} on the control plane.
//
// One instance, three consumers — the dispatcher, the group runtime, and the
// subgraph runtime. The latter two build their own inner backends per attempt,
// so a resolver installed only on the dispatcher never reaches a script nested
// inside a group or a map body; that node reads a nil resolver and fails
// permanently with script.artifact_unavailable. Sharing one instance also keeps
// a single on-disk cache rather than one per runtime.
func newRunnerArtifactResolver(cfg RunnerConfig) (func(ctx context.Context, digest string) ([]byte, error), error) {
	artifactClient, err := newRunnerHTTPClient(cfg, 60*time.Second)
	if err != nil {
		return nil, err
	}
	origin := &objectstore.HTTPStore{
		BaseURL: runnerSeedBaseURL(cfg),
		Token:   cfg.Token,
		Client:  artifactClient,
	}
	readThrough := objectstore.NewReadThrough(
		objectstore.NewFSStore(runnerArtifactCacheDir(cfg)), origin)
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

func runnerArtifactCacheDir(cfg RunnerConfig) string {
	if cfg.ArtifactCacheDir != "" {
		return cfg.ArtifactCacheDir
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "xflow", "artifacts")
	}
	return filepath.Join(base, "xflow", "artifacts")
}
