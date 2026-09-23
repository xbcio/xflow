package xflow

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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/namespace"
	xnode "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/node/supply"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"
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
	// RunnerTransportInProc dispatches the runner protocol straight into an
	// embedded control plane in this process, with no HTTP or gRPC hop. It
	// requires WithRunnerControlPlane; omitting that fails NewRunner rather than
	// silently falling back to HTTP.
	RunnerTransportInProc = "inproc"
)

// Cache bounds, shared by the group-package and map-body caches. A package
// compiles once per distinct PackageHash and every execution of one group (or
// every batch of one map node) carries the same hash, so the entry count tracks
// distinct groups/map nodes this runner serves — not executions.
const runnerPackageCacheEntries = 64

const browserCDPNodeType = "xflow.browser.cdp"

// newRunnerResourcePool is a test seam for constructor rollback coverage.
var newRunnerResourcePool = resource.NewDefaultResourcePool

// RunnerConfig configures an embedded xflow runner. ServerURL is the only
// required field; everything else has a working default.
type RunnerConfig struct {
	// ServerURL is the control plane's HTTP origin. Required under the HTTP and
	// gRPC transports: entry seeding and artifact fetch are always HTTP
	// round-trips to this origin. Under RunnerTransportInProc it may be empty —
	// the runner protocol no longer needs an origin — but any capability that
	// does reach the control plane over HTTP (hosted triggers seeding entries,
	// script/map artifact fetch) still requires it, so leaving it unset is only
	// valid for a runner that declares neither. See RunnerConfig.Transport.
	ServerURL string

	// Transport selects the Runner Protocol channel: RunnerTransportHTTP
	// (default), RunnerTransportGRPC, or RunnerTransportInProc. GRPCTarget is
	// required for the gRPC transport; WithRunnerControlPlane is required for
	// the in-process one.
	Transport  string
	GRPCTarget string

	// RunnerID identifies this runner to the control plane. Empty lets the
	// runner service generate one.
	RunnerID string

	// Concurrency is how many leases this runner executes at once, and the
	// capacity it reports. Zero uses the runner service default.
	Concurrency int

	// BrowserCDP configures the process-global remote-browser CDP client used
	// by xflow.browser.cdp nodes. Zero-valued scalar fields use node defaults.
	// Concurrent Browser-capable embedded runners must resolve to the same
	// configuration; NewRunner rejects a conflicting configuration instead of
	// replacing one a live Browser-capable runner is using. Other runners do not
	// acquire this process-global lease.
	BrowserCDP xnode.BrowserCDPConfig

	// MapBatchConcurrency caps the number of map batches actively executing in
	// this runner. It is separate from Kafka's emit/reorder window: queued emits
	// may remain ordered without letting every batch retain partially-computed
	// state. Zero defaults to GOMAXPROCS.
	MapBatchConcurrency int

	// MapItemConcurrency caps the total number of map body items executing in
	// this runner across all admitted batches and trigger-hosted groups. It is
	// separate from a map node's body_concurrency, which caps only one batch.
	// Zero defaults to GOMAXPROCS; set it higher for I/O-bound bodies or lower
	// for bodies backed by a narrower process-scoped resource pool.
	MapItemConcurrency int

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

	// SeedRequestTimeout bounds one entry-seed admission round trip — the POST
	// to /v1/executions that admits a whole trigger batch's results. Zero (the
	// default) keeps protocol.DefaultEntrySeedRequestTimeout, 15s.
	//
	// The window a deployment needs is a function of the batch size IT chose:
	// the request carries the batch's exits, so its latency grows with the
	// batch. At the 15s default a large batch fails with
	// context.DeadlineExceeded, which is treated as a transient failure — the
	// Kafka offset is left uncommitted and the whole batch is redelivered and
	// re-executed, so the failure amplifies load instead of shedding it. Raise
	// this to cover the batch the runner is configured to flush.
	//
	// It is a two-sided bound like LeaseTTL, with a milder cost for
	// overshooting: too low redelivers batches and burns downstream capacity on
	// work already done; too high makes a partition's flush wait longer before
	// it retries. This runner's seed HTTP client timeout is derived from it, so
	// the context deadline is what governs a normal admission.
	SeedRequestTimeout time.Duration

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

	// ArtifactCacheMaxBytes bounds the total size of ArtifactCacheDir. Zero
	// (the default) uses objectstore.DefaultFSStoreMaxBytes; a negative value
	// disables the bound entirely (matches every other "zero means default"
	// field in this struct needing a distinct signal for "explicitly
	// unbounded").
	//
	// This exists because the cache is a content-addressed store that is only
	// ever appended to: a script redeploy always mints a fresh digest, and
	// nothing ever deletes an old one on its own. The sibling wasm
	// compilation cache (node/internal/code/script/wasm/cache.go) had exactly
	// this shape and reached 20 GB on one development machine before it
	// gained the same kind of cap — see FSStore.MaxBytes for the full
	// history.
	ArtifactCacheMaxBytes int64
}

type runnerOptions struct {
	logger            *slog.Logger
	tracer            tracing.Tracer
	metrics           *metrics.Metrics
	artifactResolver  func(ctx context.Context, digest string) ([]byte, error)
	nodeRegistry      *execution.Registry
	lifecycleObserver runnersvc.LifecycleObserver
	// controlPlane is the embedded control plane an in-process runner
	// dispatches into. Nil unless WithRunnerControlPlane was supplied.
	controlPlane *control.Server
}

// RunnerOption configures a Runner.
type RunnerOption func(*runnerOptions)

// WithRunnerLogger sets the logger used by the supply gate, activation tracker,
// runner service, and the queues of the per-attempt/per-map-item backends the
// group and subgraph runtimes build. Defaults to slog.Default().
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

// WithRunnerLifecycleObserver installs an observer for the runner's
// registration / heartbeat / supply-readiness transitions, which is what a host
// process needs to answer a liveness or readiness probe. Runner.Run does not
// return until the connection ends, so these transitions are otherwise
// invisible from outside.
func WithRunnerLifecycleObserver(o runnersvc.LifecycleObserver) RunnerOption {
	return func(opts *runnerOptions) { opts.lifecycleObserver = o }
}

// WithRunnerControlPlane supplies the embedded control plane an in-process
// runner dispatches into. It is required when RunnerConfig.Transport is
// RunnerTransportInProc and unused by the HTTP and gRPC transports.
//
// srv is the runner-protocol server of an embedded control plane — for a host
// that built one with xflow.NewServer, that is Server.ControlServer(). An
// embedded host holds the control plane as a Go value and needs no client
// pointed at itself; without this the only way to reach its own protocol is a
// loopback HTTP call, which is a failure surface (see the loopback i/o timeout
// this transport exists to remove).
func WithRunnerControlPlane(srv *control.Server) RunnerOption {
	return func(opts *runnerOptions) { opts.controlPlane = srv }
}

// Runner is an embeddable xflow execution-plane runner. It connects to a
// control plane, claims leases for its advertised node types, executes them
// with the handlers registered in this process, and reports results.
type Runner struct {
	svc               *runnersvc.Runner
	cleanup           func()
	releaseObservers  func()
	releaseBrowserCDP func()
	pool              types.ResourcePool
	metrics           *metrics.Metrics
	closeOnce         sync.Once
	closeErr          error
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
	// ServerURL is the origin for every HTTP round-trip the runner makes to the
	// control plane (entry seeding, artifact fetch). The in-process transport
	// removes the runner-protocol round-trip but not those, so an empty
	// ServerURL is only permitted under it, and only for a runner that declares
	// no capability needing them — see the guarded warning in
	// wireRunnerTriggerHosting and the artifact resolver, which still build an
	// empty-origin HTTP client that fails at first use. Leaving ServerURL unset
	// with a hosted trigger or a script/map capability is therefore a
	// configuration error the runner reports at use time, not one this gate can
	// see without duplicating the capability analysis.
	if strings.TrimSpace(cfg.ServerURL) == "" && cfg.Transport != RunnerTransportInProc {
		return nil, fmt.Errorf("xflow: RunnerConfig.ServerURL is required: it is the " +
			"control plane server origin for leases, entry seeding, and artifact fetch")
	}
	o := runnerOptionsFrom(opts)

	svcCfg, err := buildRunnerServiceConfig(cfg, opts...)
	if err != nil {
		return nil, err
	}

	// From this point until ownership is transferred to Runner, every acquired
	// resource is rolled back on both ordinary errors and panics. In particular,
	// process-observer setters deliberately panic on a conflicting live install.
	var cleanup, releaseObservers, releaseBrowserCDP func()
	constructed := false
	defer func() {
		if !constructed {
			_ = releaseRunnerResources(releaseBrowserCDP, releaseObservers, cleanup, svcCfg.ResourcePool)
		}
	}()

	client, cleanup, err := newRunnerProtocolClient(cfg, o)
	if err != nil {
		return nil, err
	}
	reporter, err := newRunnerMetricsReporter(client, o.metrics, cfg)
	if err != nil {
		return nil, err
	}
	svcCfg.MetricsReporter = reporter
	reg := o.nodeRegistry
	if reg == nil {
		reg = execution.NewRegistry()
	}

	// Browser CDP is process-global, but only runners that can actually receive
	// xflow.browser.cdp work participate in its lease. A non-Browser runner must
	// neither validate nor reserve a Browser configuration it will never use.
	if runnerCapabilitiesContain(svcCfg.Capabilities, browserCDPNodeType) {
		releaseBrowserCDP, err = xnode.AcquireBrowserCDPConfig(resolveRunnerBrowserCDPConfig(cfg.BrowserCDP))
		if err != nil {
			return nil, fmt.Errorf("xflow: acquire BrowserCDP configuration: %w", err)
		}
	}

	// Install process-global observers last. installProcessObservers rolls back
	// any earlier slots if a later setter panics; the constructor-level defer
	// above then releases the CDP lease, protocol client, and resource pool while
	// preserving the public panic behavior of the observer setters.
	releaseObservers = installProcessObservers(o)
	r := &Runner{
		svc:               runnersvc.New(client, reg, svcCfg),
		cleanup:           cleanup,
		releaseObservers:  releaseObservers,
		releaseBrowserCDP: releaseBrowserCDP,
		pool:              svcCfg.ResourcePool,
		metrics:           o.metrics,
	}
	constructed = true
	return r, nil
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

// Close releases transport-owned resources (the gRPC connection), the
// process-scoped ResourcePool, and the process-wide Browser CDP and observer
// leases this runner installed. Idempotent; safe to defer immediately after
// NewRunner.
//
// Releasing the observers is what makes the install-once guard on those slots
// survivable: they panic on a second non-nil install, so a process that builds
// more than one runner over its lifetime depends on this half of the pair.
func (r *Runner) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = releaseRunnerResources(r.releaseBrowserCDP, r.releaseObservers, r.cleanup, r.pool)
	})
	return r.closeErr
}

func releaseRunnerResources(releaseBrowserCDP, releaseObservers, cleanup func(), pool types.ResourcePool) error {
	if releaseBrowserCDP != nil {
		releaseBrowserCDP()
	}
	if releaseObservers != nil {
		releaseObservers()
	}
	if cleanup != nil {
		cleanup()
	}
	if pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pool.Close(ctx)
}

// resolveRunnerBrowserCDPConfig applies node-owned defaults before acquiring
// the process-global lease. Negative values intentionally remain unchanged so
// node.AcquireBrowserCDPConfig can return its authoritative validation error.
// The allowlist is copied before handing it to process-global state so a caller
// cannot mutate the acquired configuration through the original slice.
func resolveRunnerBrowserCDPConfig(cfg xnode.BrowserCDPConfig) xnode.BrowserCDPConfig {
	defaults := xnode.BrowserCDPConfigDefaults()
	if cfg.MaxContexts == 0 {
		cfg.MaxContexts = defaults.MaxContexts
	}
	if cfg.QueueTimeout == 0 {
		cfg.QueueTimeout = defaults.QueueTimeout
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaults.ConnectTimeout
	}
	cfg.EndpointAllowlist = append([]string(nil), cfg.EndpointAllowlist...)
	return cfg
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
	if cfg.MapBatchConcurrency < 0 {
		return runnersvc.Config{}, fmt.Errorf("xflow: RunnerConfig.MapBatchConcurrency must be positive, got %d",
			cfg.MapBatchConcurrency)
	}
	if cfg.MapItemConcurrency < 0 {
		return runnersvc.Config{}, fmt.Errorf("xflow: RunnerConfig.MapItemConcurrency must be positive, got %d",
			cfg.MapItemConcurrency)
	}
	if cfg.SeedRequestTimeout < 0 {
		return runnersvc.Config{}, fmt.Errorf("xflow: RunnerConfig.SeedRequestTimeout must be positive, got %s",
			cfg.SeedRequestTimeout)
	}

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
	mapBatchConcurrency := cfg.MapBatchConcurrency
	if mapBatchConcurrency == 0 {
		mapBatchConcurrency = runtime.GOMAXPROCS(0)
	}
	mapItemConcurrency := cfg.MapItemConcurrency
	if mapItemConcurrency == 0 {
		mapItemConcurrency = runtime.GOMAXPROCS(0)
	}
	mapConcurrencyLimiter := subgraph.NewMapConcurrencyLimiter(mapBatchConcurrency, mapItemConcurrency)

	// Inner-engine hooks, when a metrics registry was configured. A map body
	// item and a group member run on their own engine, so without these their
	// nodes emit nothing at all — the per-item work a map exists to do was the
	// one part of an execution with no series behind it. They write the
	// xflow_subgraph_* family rather than xflow_node_*/xflow_execution_*,
	// because one outer execution fans out to one inner execution per item and
	// folding them together would inflate the outer counters by the fan-out
	// width.
	var groupHookOpts []runnersvc.GroupRuntimeOption
	var subgraphHookOpts []runnersvc.SubgraphRuntimeOption
	if o.metrics != nil {
		groupHookOpts = append(groupHookOpts, runnersvc.WithGroupHooks(metrics.NewSubgraphMetricsHooks(o.metrics)))
		subgraphHookOpts = append(subgraphHookOpts, runnersvc.WithSubgraphHooks(metrics.NewSubgraphMetricsHooks(o.metrics)))
	}

	// Suspend is disabled inside a group for the same reason as inside a map
	// body: a suspended member would park a sub-execution the outer lease
	// cannot resume.
	//
	// The package cache observer is wired only for this group cache, not for
	// the SubgraphRuntime cache below: xflow_group_package_cache_total is a
	// xflow_group_* series, and map body / subgraph package resolutions are a
	// distinct fan-out domain (see the comment above on why inner-engine
	// hooks keep xflow_subgraph_* separate from the outer counters). Folding
	// map body cache events into the group counter would repeat that same
	// mistake. Map body / subgraph package cache hit/miss has no metric of
	// its own today — that is an intentional gap, not an oversight; closing
	// it means adding a new xflow_subgraph_package_cache_total series plus
	// its own metricHelp entry.
	groupPackageCacheConfig := runnersvc.PackageCacheConfig{MaxEntries: runnerPackageCacheEntries}
	if o.metrics != nil {
		groupPackageCacheConfig.Observer = metrics.NewGroupMetrics(o.metrics)
	}
	groupRuntime := runnersvc.NewGroupRuntime(
		reg,
		runnersvc.NewPackageCache(groupPackageCacheConfig),
		append([]runnersvc.GroupRuntimeOption{
			runnersvc.WithSuspendDisabled(),
			runnersvc.WithGroupMapConcurrencyLimiter(mapConcurrencyLimiter),
			runnersvc.WithGroupArtifactCodeResolver(artifactCode),
			// o.logger is non-nil by this point (defaulted to slog.Default()
			// above), so every runner gets these dispatch failures on the
			// record rather than only the ones that opted in.
			runnersvc.WithGroupQueueLogger(newSlogLogger(o.logger)),
		}, groupHookOpts...)...)

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
			append([]runnersvc.SubgraphRuntimeOption{
				runnersvc.WithSubgraphMapConcurrencyLimiter(mapConcurrencyLimiter),
				runnersvc.WithSubgraphArtifactCodeResolver(artifactCode),
				runnersvc.WithSubgraphQueueLogger(newSlogLogger(o.logger)),
			}, subgraphHookOpts...)...),
		ArtifactCodeResolver: artifactCode,
		SupportsEncryption:   true,
		LifecycleObserver:    o.lifecycleObserver,
	}
	if cfg.HeartbeatInterval > 0 {
		svcCfg.HeartbeatInterval = cfg.HeartbeatInterval
	}
	if runnerNeedsPool(cfg) {
		poolCfg := cfg.ResourcePoolConfig
		if poolCfg == (types.ResourcePoolConfig{}) {
			poolCfg = types.DefaultResourcePoolConfig()
		}
		svcCfg.ResourcePool = newRunnerResourcePool(poolCfg)
	}
	if len(cfg.Credentials) > 0 {
		// Captured in the closure; never logged or surfaced in errors.
		creds := cfg.Credentials
		svcCfg.CredentialResolver = func(_ namespace.Namespace, name string) map[string]any {
			return creds[name]
		}
	}

	if err := wireRunnerTriggerHosting(&svcCfg, cfg, o, groupRuntime); err != nil {
		return runnersvc.Config{}, errors.Join(
			err,
			releaseRunnerResources(nil, nil, nil, svcCfg.ResourcePool),
		)
	}
	wireRunnerMetrics(&svcCfg, o)
	wireSupplyGateObserver(&svcCfg, o)
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
		// This runner advertises a trigger but has no control-plane origin to
		// seed entries through. Only RunnerTransportInProc permits an empty
		// ServerURL (NewRunner rejects it elsewhere), so this is an in-process
		// runner that declared a trigger capability without a seed origin.
		// Trigger hosting is skipped, and a skipped trigger is indistinguishable
		// from a quiet one from the outside — so say so rather than degrade in
		// silence. Entry seeding is still an HTTP round-trip; closing it in
		// process (the way the runner protocol was) is a separate change.
		o.logger.Warn("runner declares a trigger capability but has no control-plane origin to seed entries through; trigger hosting is disabled",
			"capabilities", cfg.Capabilities)
		return nil
	}
	// The seed client timeout is deliberately larger than the per-request
	// context timeout: the context deadline governs normal cancellation, this
	// is an absolute safety net covering connection setup and full body read.
	// It tracks SeedRequestTimeout, so raising the admission window does not
	// leave the client cutting the request off first and reporting a transport
	// error where the deadline would have reported a timeout.
	seedClient, err := newRunnerHTTPClient(cfg, seedClientTimeout(cfg))
	if err != nil {
		return err
	}
	// The supply fetch client shares the seed origin: both talk to the control
	// plane's HTTP API. The gate publishes into supply.Default, the same
	// registry node handlers read through $supplies.
	gate := runnersvc.NewSupplyGate(&runnersvc.HTTPSupplyFetcher{
		BaseURL:  seedBaseURL,
		Token:    cfg.Token,
		RunnerID: cfg.RunnerID,
		Client:   seedClient,
	}, supply.Default, o.logger)

	handler := runnersvc.NewTriggerActivationHandler(seedBaseURL, cfg.Token, runnerTriggerLookup{},
		runnersvc.WithSeedHTTPClient(seedClient),
		runnersvc.WithSeedRunnerID(cfg.RunnerID),
		runnersvc.WithSeedRequestTimeout(cfg.SeedRequestTimeout),
		runnersvc.WithSupplyGate(gate),
		runnersvc.WithGroupRuntime(groupRuntime),
		// The same resolver the executor uses, so activation can compile a wasm
		// module before registering its supply consumer without a second client
		// or cache.
		runnersvc.WithArtifactCodeResolver(svcCfg.ArtifactCodeResolver),
		// Seed admission observation rides the runtime the handler installs, not
		// a process-global slot, so a host with no metrics registry gets nil and
		// an observed one gets its own registry — no install-once guard and no
		// cross-runner arbitration.
		runnersvc.WithSeedObserver(runnerSeedObserver(o.metrics)))

	svcCfg.ActivationTracker = runnersvc.NewActivationTracker(handler, o.logger)
	// The same gate/registry pair feeds the heartbeat's two supply channels:
	// the registry is what Observed() reads out to report applied hashes, and
	// the gate is what ApplyHints fetches into on a piggybacked hint.
	svcCfg.SupplyRegistry = supply.Default
	svcCfg.SupplyGate = gate
	return nil
}

// seedClientTimeout is the absolute http.Client timeout for the seed client:
// twice the admission deadline, which is the ratio the 30s/15s pair always
// used. Seeding a positive SeedRequestTimeout therefore keeps the client above
// the deadline rather than pinning it at 30s — a 60s admission window under a
// 30s client timeout would fail as a transport error before the context
// deadline could classify it as the timeout it is.
func seedClientTimeout(cfg RunnerConfig) time.Duration {
	if cfg.SeedRequestTimeout > 0 {
		return 2 * cfg.SeedRequestTimeout
	}
	return 2 * protocol.DefaultEntrySeedRequestTimeout
}

// runnerSeedObserver returns the seed admission observer for this runner, or
// nil when no metrics registry was configured.
//
// The nil case is returned as an untyped nil rather than as a non-nil interface
// holding a nil *Metrics: the runtime installs whatever it is given into
// HTTPEntrySeedRuntime.Observer and calls it behind a `!= nil` test, and a
// typed-nil interface would pass that test and then panic on the first
// admission — on the hot path, inside the runner's trigger flush.
func runnerSeedObserver(m *metrics.Metrics) types.EntrySeedObserver {
	if m == nil {
		return nil
	}
	return metrics.NewEntrySeedMetrics(m)
}

// wireRunnerMetrics installs the observers that belong to THIS assembly — the
// ones reachable through svcCfg and owned by the returned config. The registry
// is inert without them: an unobserved registry is what makes a scrape or a
// report return an empty payload.
//
// The process-wide singletons (supply.Default, the wasm/script/kafka package
// observers) are deliberately NOT installed here. This function runs inside
// buildRunnerServiceConfig, a pure configuration constructor that a dozen tests
// call directly and that an embedder may call more than once per process.
// Installing a process-global from it made the second call panic on the
// install-once guard — sdk/xflow's own package went red that way, reading as
// "TestNewRunnerObservesNodeExecutionTimeouts fails" rather than as a lifecycle
// defect, because the panicking call sits three frames below the test. Those
// installs now live in installProcessObservers, called once from NewRunner and
// released by Close.
func wireRunnerMetrics(svcCfg *runnersvc.Config, o *runnerOptions) {
	if o.metrics == nil {
		return
	}
	// The node execution timeout observer reports runner-detected timeouts, the
	// abandoned-goroutine gauge, and per-invocation duration. Without it a
	// runner that is shedding work on deadline looks identical to one that is
	// simply idle — the tasks end as failures on the server with nothing here
	// to say the deadline is what ended them.
	svcCfg.TimeoutObserver = metrics.NewNodeTimeoutMetrics(o.metrics)
}

// wireSupplyGateObserver fills the supply gate's single observer slot.
//
// SupplyGate.SetObserver panics on a second non-nil install, deliberately, so
// two live observers cannot silently drop one side. That means metrics and the
// lifecycle observer cannot each install their own — they are fanned out
// through one value here. cmd/runner always passes WithRunnerMetrics, so this
// is the ordinary case, not a corner one.
func wireSupplyGateObserver(svcCfg *runnersvc.Config, o *runnerOptions) {
	if o.lifecycleObserver != nil {
		// Reported even when there is no gate: a runner that hosts no triggers
		// must not sit un-ready forever waiting for a fetch that cannot happen.
		o.lifecycleObserver.OnSupplyGateWired(svcCfg.SupplyGate != nil)
	}
	if svcCfg.SupplyGate == nil {
		return
	}
	fanout := supplyGateFanout{lifecycle: o.lifecycleObserver}
	if o.metrics != nil {
		fanout.metrics = metrics.NewSupplyMetrics(o.metrics)
	}
	if fanout.metrics == nil && fanout.lifecycle == nil {
		return
	}
	svcCfg.SupplyGate.SetObserver(fanout)
}

// supplyGateFanout forwards gate observations to the metrics observer and, for
// the one event a readiness probe cares about, to the lifecycle observer. The
// two gauge-shaped callbacks stay metrics-only: readiness is a latch on "a
// fetch succeeded at least once", not a live gauge.
type supplyGateFanout struct {
	metrics   runnersvc.SupplyGateObserver
	lifecycle runnersvc.LifecycleObserver
}

func (f supplyGateFanout) OnSupplyFetch(ctx context.Context, name, result string) {
	if f.metrics != nil {
		f.metrics.OnSupplyFetch(ctx, name, result)
	}
	if f.lifecycle != nil {
		f.lifecycle.OnSupplyFetch(ctx, name, result)
	}
}

func (f supplyGateFanout) OnSupplyNotReady(ctx context.Context, workflow, supplyName string, notReady bool) {
	if f.metrics != nil {
		f.metrics.OnSupplyNotReady(ctx, workflow, supplyName, notReady)
	}
}

func (f supplyGateFanout) OnSupplyServingUnavailable(ctx context.Context, name string, serving bool) {
	if f.metrics != nil {
		f.metrics.OnSupplyServingUnavailable(ctx, name, serving)
	}
}

// installProcessObservers installs the observers that live in process-global
// slots rather than in the runner's own config, and returns the function that
// releases them.
//
// Each of these slots holds exactly one observer and panics on a second non-nil
// install, so that two live runners in one process cannot silently drop one
// side's observations. That guard is only usable if install and release are
// symmetric: NewRunner installs, Close releases. Without the release, an
// embedder that builds a runner per test — the in-process embedded model, where
// the control plane and the runner share a binary — crashes on the second
// construction.
//
// Returns nil when there is nothing to release, so Close can call it
// unconditionally.
func installProcessObservers(o *runnerOptions) (release func()) {
	if o.metrics == nil {
		return nil
	}

	// Each successful setter adds its inverse immediately. If a later setter
	// panics because that slot already belongs to another live runner, unwind
	// only the slots installed by this attempt and re-panic unchanged.
	releases := make([]func(), 0, 4)
	defer func() {
		if recovered := recover(); recovered != nil {
			releaseProcessObservers(releases)
			panic(recovered)
		}
	}()

	sm := metrics.NewSupplyMetrics(o.metrics)
	supply.Default.SetObserver(sm)
	releases = append(releases, func() { supply.Default.SetObserver(nil) })
	xnode.SetWasmObserver(sm)
	releases = append(releases, func() { xnode.SetWasmObserver(nil) })
	xnode.SetScriptObserver(metrics.NewScriptMetrics(o.metrics))
	releases = append(releases, func() { xnode.SetScriptObserver(nil) })
	// The trigger observer covers the discards, dead letters and batch
	// admissions on the Kafka ingest path. Without it a producer emitting
	// malformed records looks exactly like an idle topic — offsets keep being
	// committed, so consumer-group lag stays at zero.
	kafkatrigger.SetObserver(metrics.NewTriggerMetrics(o.metrics))
	releases = append(releases, func() { kafkatrigger.SetObserver(nil) })
	return func() { releaseProcessObservers(releases) }
}

func releaseProcessObservers(releases []func()) {
	for i := len(releases) - 1; i >= 0; i-- {
		releases[i]()
	}
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
		c := protocol.Capability{
			NodeType: nodeType,
			Features: []string{
				engine.FeatureEntryActivationReplicaV1,
				engine.FeatureWasmSupplyDeclarationV1,
			},
		}
		if nodeType == engine.GroupNodeType {
			found = true
			c.Features = []string{
				engine.FeatureGroupExecV1,
				engine.FeatureEntryActivationReplicaV1,
				engine.FeatureWasmSupplyDeclarationV1,
			}
		}
		out = append(out, c)
	}
	if found {
		return out
	}
	return append(out, protocol.Capability{
		NodeType: engine.GroupNodeType,
		Features: []string{
			engine.FeatureGroupExecV1,
			engine.FeatureEntryActivationReplicaV1,
			engine.FeatureWasmSupplyDeclarationV1,
		},
	})
}

func runnerCapabilitiesContain(capabilities []protocol.Capability, nodeType string) bool {
	for _, capability := range capabilities {
		if capability.NodeType == nodeType {
			return true
		}
	}
	return false
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

func newRunnerProtocolClient(cfg RunnerConfig, o *runnerOptions) (runnersvc.ProtocolClient, func(), error) {
	if cfg.Transport == RunnerTransportInProc {
		if o.controlPlane == nil {
			return nil, nil, fmt.Errorf("xflow: RunnerConfig.Transport is %q but no control plane was supplied: "+
				"pass WithRunnerControlPlane (a host built on xflow.NewServer uses Server.ControlServer()); "+
				"falling back to HTTP would silently reintroduce the loopback the in-process transport removes",
				RunnerTransportInProc)
		}
		// No TLS material is consulted: there is no connection to secure, and a
		// runner-proc TLS config here would be a misconfiguration, not a
		// transport option. The token is the only credential, carried the way the
		// wire transports carry it (see control.InProcessRunnerClient).
		return o.controlPlane.InProcessRunnerClient(cfg.Token), func() {}, nil
	}
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

// NewRunnerHTTPClient builds an *http.Client honoring cfg's TLS material
// (TLSServerCA / TLSClientCert / TLSClientKey) — the same client every internal
// runner subsystem uses to reach the control plane.
//
// Exported for a host that needs its own one-shot call to that same origin
// before a Runner exists to make it through: cmd/runner's enrollment bootstrap
// runs before registration, and hand-rolling a second copy of this wiring is
// how the two drift until one of them quietly stops presenting a client cert.
func NewRunnerHTTPClient(cfg RunnerConfig, timeout time.Duration) (*http.Client, error) {
	return newRunnerHTTPClient(cfg, timeout)
}

// newRunnerArtifactResolver builds the digest -> script bytes resolver every
// script execution path shares: a read-through cache serving from local disk
// and falling back to GET /v1/artifacts/{digest} on the control plane.
//
// The fallback is an HTTP round-trip, so under RunnerTransportInProc with an
// empty ServerURL the origin below is empty and the fallback fails at first
// use. Only a runner that declares no script/map capability avoids it —
// those are the only nodes that resolve artifacts. Closing this in process
// (serving guest bytes straight from the embedded artifact store, the way the
// seed fetch must also be closed) is a separate change; until then an in-process
// runner that does reach a script node needs ServerURL set, and picking the
// transport does not by itself make it self-contained.
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
		// RunnerID lets the server resolve this runner's RunnerPolicy when
		// validating the X-Xflow-Namespace declaration HTTPStore attaches to
		// every request (see HTTPStore.declareNamespace and
		// module_artifact.go's resolveNamespace). Empty is valid: it matches
		// RunnerConfig.RunnerID's own "empty lets the server generate one"
		// contract, and simply means no runner ID is declared.
		RunnerID: cfg.RunnerID,
	}
	// PartitionByNamespace: this is the runner-side read-through cache, the one
	// FSStore use site where a cache hit for one namespace's digest must never
	// be handed to another namespace's task on the same runner. See
	// FSStore.PartitionByNamespace's doc comment.
	cache := objectstore.NewFSStore(runnerArtifactCacheDir(cfg))
	cache.PartitionByNamespace = true
	// MaxBytes: this cache is append-only by construction (a fresh digest per
	// redeploy, nothing ever overwritten), so left unbounded it repeats the
	// wasm compilation cache's 20 GB history. See FSStore.MaxBytes and
	// RunnerConfig.ArtifactCacheMaxBytes.
	cache.MaxBytes = artifactCacheMaxBytes(cfg)
	readThrough := objectstore.NewReadThrough(cache, origin)
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

// artifactCacheMaxBytes resolves the effective FSStore.MaxBytes for the
// runner artifact cache: zero (RunnerConfig's unset value) defers to the
// package default, everything else -- including a negative value, which
// explicitly means unbounded -- is passed through verbatim.
func artifactCacheMaxBytes(cfg RunnerConfig) int64 {
	if cfg.ArtifactCacheMaxBytes != 0 {
		return cfg.ArtifactCacheMaxBytes
	}
	return objectstore.DefaultFSStoreMaxBytes
}
