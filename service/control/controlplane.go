package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	"github.com/xbcio/xflow/store"
)

var (
	ErrControlPlaneStarted = errors.New("control: ControlPlane already started")
	ErrControlPlaneStopped = errors.New("control: ControlPlane already stopped")
)

// Config configures a ControlPlane.
type Config struct {
	// Backend supplies StateStore, TaskQueue, and queue binding. Required.
	Backend backend.Provider
	// RunnerDirectory overrides automatic directory selection. When nil,
	// NewControlPlane uses the backend Redis capability when available and
	// otherwise falls back to MemoryRunnerDirectory.
	RunnerDirectory RunnerDirectory
	// Auth is the runner-protocol authenticator. Nil means DisabledAuthenticator.
	Auth Authenticator
	// RequireRunnerAuth causes NewControlPlane to return an error when Auth is
	// nil (production fail-closed: the runner protocol must not be left open
	// with the permissive DisabledAuthenticator). When false the behavior is
	// unchanged for backward compatibility, but a nil Auth still emits a
	// prominent warning so an accidentally unauthenticated deployment is visible.
	RequireRunnerAuth bool
	// Logger receives engine, dispatcher, and sweeper diagnostics. Optional.
	Logger engine.Logger
	// Metrics, when set, wires Prometheus observers into engine hooks, the
	// dispatcher, auth decisions, and the LeaseSweeper. Optional.
	Metrics *metrics.Metrics
	// Tracer, when set, enables distributed tracing for the runner protocol
	// dispatch and commit path and injects W3C carriers into TaskLease.
	// Nil means no-op tracing (NoopTracer).
	Tracer tracing.Tracer
	// PollWait overrides the long-poll wait duration returned to runners when
	// no task is available. Zero means the Server/GRPCServer default (1s).
	PollWait time.Duration
	// RuntimeEvidenceBuffer, when non-nil, is wired into the internal engine as
	// a read-only evidence sink. NewControlPlane converts only this typed
	// buffer to an engine Option; it does not expose arbitrary []engine.Option.
	RuntimeEvidenceBuffer *engine.RuntimeEvidenceBuffer
	// LeaseTTL, when greater than zero, overrides the engine default lease TTL
	// (60s). This is an additive seam for deterministic A0 request-loss tests
	// that need a short TTL so the production LeaseSweeper reclaims the lease
	// synchronously. LeaseTTL == 0 preserves the existing 60s default.
	LeaseTTL time.Duration
	// EntryActivationStore, when non-nil, is the durable EntryActivation store
	// used to fence entry seeds by activation generation (spec §11.6) and to
	// drive the node-generic EntryActivationReconciler. Optional; nil disables
	// generation fencing on the seed path.
	EntryActivationStore engine.EntryActivationStore
	// WorkflowRegistry, when non-nil, is the durable registry that persists
	// compiled workflow graphs (by WorkflowID+Version). When nil, NewControlPlane
	// falls back to a registry exposed by the backend provider (if any). It backs
	// the explicit /v1/workflows/register endpoint so later tasks can resolve a
	// graph on seed and derive entry activations. Optional.
	WorkflowRegistry backend.WorkflowRegistry
	// Supplies, when non-nil, backs the heartbeat-piggybacked supply hint
	// (server→runner) and observed-hash (runner→server) channels: a
	// SupplyHinter and a MemorySupplyObserved are constructed and wired into
	// both the HTTP and gRPC Core instances. Nil means neither is constructed
	// and heartbeat bodies are byte-identical to before this field existed —
	// this is the "wiring is optional" requirement: a deployment with no
	// store.Supplies configured (e.g. no PrincipalAuth for the supply HTTP
	// module) sees no behavior change at all.
	Supplies store.Supplies
	// EnableSupplyEncryption, when true, enables AES-256-GCM encryption of
	// supply content delivered to runners. The server generates a key at startup
	// and distributes it to runners on registration. Requires Supplies to be
	// non-nil for the encryption path to activate on GET /v1/supplies/{name}.
	EnableSupplyEncryption bool
	// SupplyKeyRotationPeriod is how often the supply transport key rotates.
	// Zero adopts DefaultSupplyKeyRotationPeriod; negative disables rotation.
	// Ignored unless EnableSupplyEncryption is set.
	//
	// Rotation is fleet-wide-at-most-once per period via a Redis lease, not
	// per-replica, so raising the replica count does not raise the key churn.
	SupplyKeyRotationPeriod time.Duration
	// EnableMetricsProxy turns on the runner metrics proxy: the
	// /v1/runners/metrics endpoint starts accepting reports and MetricsInbox()
	// returns a gatherer the host can merge into its own /metrics. It exists
	// because a runner deployed in another network domain cannot be scraped —
	// Prometheus pulls, and that direction of connectivity does not exist.
	//
	// Off by default: a single-domain deployment can scrape runners directly via
	// their own --metrics-addr and does not need the extra hop.
	EnableMetricsProxy bool
	// MetricsReportInterval is the cadence pushed to runners on every heartbeat
	// response (see protocol.HeartbeatResponse.MetricsReportIntervalSeconds).
	// Zero leaves every runner on its own default; negative suspends reporting
	// fleet-wide without restarting anything.
	MetricsReportInterval time.Duration
	// RegistrationCodes / IssuedIdentities turn on the enrollment endpoint
	// (§2.3.1). Both must be non-nil; either nil leaves enroll off and every
	// attempt gets the standard rejection.
	//
	// Enrollment does NOT replace Auth. When both are set, NewControlPlane
	// composes them with MultiAuthenticator so a runners.yaml runner and an
	// enrolled runner authenticate through the same Core.
	RegistrationCodes RegistrationCodeStore
	IssuedIdentities  IssuedIdentityStore
	// IdentityTTL is how long a newly enrolled identity authenticates before
	// it must renew (see WithIdentityTTL). Zero (the default) means never
	// expires — the pre-feature behavior. Only the HTTP Core receives this:
	// enroll has no gRPC endpoint (grpc_server.go has no Enroll method), so
	// there is nothing on that transport for a TTL to affect.
	IdentityTTL time.Duration
}

// EnrollDeclared reports whether both enrollment stores are present. It is
// exported so callers outside this package can ask "is enrollment configured"
// without re-deriving the same two-nil check — sdk/xflow's runner-auth
// posture gate (NewServer) is the first such caller: it must accept
// WithServerEnroll(...) as a declared posture using this exact predicate,
// not a hand-written `codes != nil && ids != nil` of its own, or the two
// packages could silently disagree about what "enrollment is on" means.
func EnrollDeclared(codes RegistrationCodeStore, ids IssuedIdentityStore) bool {
	return codes != nil && ids != nil
}

// enrollConfigured reports whether enrollment is turned on. Both stores are
// required: one without the other cannot issue an identity that anything can
// later authenticate. This is one function rather than the condition written
// twice because the two call sites — composing the authenticator and mounting
// the endpoint — must never disagree. A server that authenticates enrolled
// identities but exposes no enroll endpoint (or the reverse) is a half-wired
// state each site would consider correct on its own.
func enrollConfigured(cfg Config) bool {
	return EnrollDeclared(cfg.RegistrationCodes, cfg.IssuedIdentities)
}

type redisClientProvider interface {
	RedisClient() redis.Cmdable
}

func selectRunnerDirectory(cfg Config, observer RunnerClaimObserver) RunnerDirectory {
	if cfg.RunnerDirectory != nil {
		return cfg.RunnerDirectory
	}
	if provider, ok := cfg.Backend.(redisClientProvider); ok {
		if client := provider.RedisClient(); client != nil {
			return NewRedisRunnerDirectory(client, WithRedisRunnerDirectoryObserver(observer))
		}
	}
	return NewMemoryRunnerDirectory()
}

// workflowRegistryProvider is the optional backend capability that exposes a
// durable workflow registry. The distributed and local providers implement it;
// backends that do not simply leave the control-plane registry nil.
type workflowRegistryProvider interface {
	WorkflowRegistry() backend.WorkflowRegistry
}

// selectWorkflowRegistry resolves the registry the control plane exposes:
// Config.WorkflowRegistry wins when set, else the backend provider's registry
// when it exposes one, else nil.
func selectWorkflowRegistry(cfg Config) backend.WorkflowRegistry {
	if cfg.WorkflowRegistry != nil {
		return cfg.WorkflowRegistry
	}
	if provider, ok := cfg.Backend.(workflowRegistryProvider); ok {
		if reg := provider.WorkflowRegistry(); reg != nil {
			return reg
		}
	}
	return nil
}

// ControlPlane bundles the engine, Task Dispatcher, Runner Protocol servers,
// and LeaseSweeper into a single embeddable unit with Handler()/Start()/
// Shutdown() lifecycle methods, so it can be mounted into a host program's
// own http.Server instead of only running as the cmd/server binary.
type ControlPlane struct {
	backend    backend.Provider
	eng        *engine.Engine
	runners    RunnerDirectory
	dispatcher *Dispatcher
	httpServer *Server
	grpcServer *GRPCServer
	sweeper    *LeaseSweeper
	elector    backend.LeaderElector
	logger     engine.Logger

	// entryActivations is the optional durable EntryActivation store (node-generic
	// activation controller). Non-nil only when Config.EntryActivationStore is
	// provided. Used for generation fencing on seeds and lifecycle management.
	entryActivations engine.EntryActivationStore

	// entryManager translates workflow add/update/remove into desired
	// EntryActivation records. Non-nil only when Config.EntryActivationStore is
	// provided. Exposed via EntryActivationManager() for the register path.
	entryManager *EntryActivationManager

	// entryReconciler drives desired EntryActivations toward a live runner
	// assignment and produces node-generic activate/deactivate directives. Non-nil
	// only when Config.EntryActivationStore is provided. Its Run loop is launched
	// leader-gated in Start.
	entryReconciler *EntryActivationReconciler

	// workflowRegistry is the optional durable registry of compiled workflow
	// graphs. Resolved from Config.WorkflowRegistry, else from the backend
	// provider when it exposes one, else nil. Exposed via WorkflowRegistry().
	workflowRegistry backend.WorkflowRegistry

	// workflowProjectionWorker drains projection intents committed atomically
	// with workflow replacements. It is present whenever workflowRegistry
	// implements WorkflowActivationProjectionOutbox. When entryManager is nil,
	// its projector is a safe no-op, so committed intents are still acknowledged
	// and drained.
	workflowProjectionWorker *WorkflowActivationProjectionWorker

	// supplyObserved is the optional sink of runner-reported applied supply
	// hashes. Non-nil only when both Config.EntryActivationStore and
	// Config.Supplies are provided. Exposed via SupplyObserved() for
	// diagnostics/management reads.
	supplyObserved SupplyObservedSink

	// supplyEncryptor is the optional AES-256-GCM encryptor for supply content.
	// Non-nil only when Config.EnableSupplyEncryption is true. Exposed via
	// SupplyEncryptor() so the apiserver can encrypt GET responses.
	supplyEncryptor *SupplyEncryptor
	// supplyKeyRotationPeriod carries Config.SupplyKeyRotationPeriod through to
	// Start, which resolves it via clampSupplyKeyRotationPeriod.
	supplyKeyRotationPeriod time.Duration

	// metricsInbox retains proxied runner metrics. Non-nil only when
	// Config.EnableMetricsProxy is set. Exposed via MetricsInbox() so the
	// apiserver can merge it into the scrape endpoint.
	metricsInbox *MetricsInbox

	lifecycleMu              sync.Mutex
	started                  bool
	stopped                  bool
	leaderCancel             context.CancelFunc
	sweeperCancel            context.CancelFunc
	claimRecoveryCancel      context.CancelFunc
	entryReconcilerCancel    context.CancelFunc
	workflowProjectionCancel context.CancelFunc
	supplyKeyCancel          context.CancelFunc
	unbind                   func()
	// wg tracks the background goroutines started by Start (leader campaign,
	// sweeper, claim recovery, entry reconciliation, workflow projection, and
	// supply-key rotation). Shutdown cancels their contexts and then waits for
	// them to exit, bounded by the Shutdown context so a stuck goroutine cannot
	// hang shutdown.
	wg sync.WaitGroup
}

// NewControlPlane assembles a ControlPlane from cfg. It does not start any
// background goroutines or bind the queue — call Start for that.
func NewControlPlane(cfg Config) (*ControlPlane, error) {
	if cfg.Backend == nil {
		return nil, errors.New("control: Config.Backend is required")
	}
	// Enrollment-issued identities authenticate through the same Authenticator
	// seam as runners.yaml. Composing here (rather than at each call site) is
	// what makes ControlPlane.Authenticator() — the one the namespace-declaration
	// check reads — see both populations. This MUST happen before the
	// IsConfigured gate below: otherwise a server whose only runner auth is
	// enrollment (cfg.Auth == nil) would be rejected by RequireRunnerAuth
	// before the enroll-issued authenticator ever gets a chance to count.
	if enrollConfigured(cfg) {
		issued := NewIssuedIdentityAuthenticator(cfg.IssuedIdentities)
		// IsConfigured, not a plain nil check: cfg.Auth is frequently
		// DisabledAuthenticator{} (cmd/server's default when --auth-policy is
		// empty), which is non-nil. MultiAuthenticator.dispatch returns the
		// first member that succeeds, and DisabledAuthenticator always
		// succeeds — composing it in front of (or alongside) issued would make
		// the whole authenticator permissive for every runner, silently
		// defeating enrollment. An enroll-only deployment (the case this
		// exists for) must end up with issued alone, not
		// Multi(DisabledAuthenticator{}, issued).
		if IsConfigured(cfg.Auth) {
			cfg.Auth = NewMultiAuthenticator(cfg.Auth, issued)
		} else {
			cfg.Auth = issued
		}
	}
	// Runner-protocol auth fail-closed. A nil or explicitly-disabled Auth
	// falls back to the permissive DisabledAuthenticator (every runner
	// allowed) — DisabledAuthenticator{} is a non-nil Authenticator, so this
	// gate uses IsConfigured rather than a plain nil check, or passing the
	// sentinel explicitly would silently defeat RequireRunnerAuth. RequireRunnerAuth
	// turns that into a hard error so production cannot silently serve the
	// runner protocol unauthenticated; otherwise it is only a prominent warning
	// (mirroring the nil-directory warning in NewServer) to preserve backward
	// compatibility.
	if !IsConfigured(cfg.Auth) {
		if cfg.RequireRunnerAuth {
			return nil, errors.New("control: Auth must be configured when RequireRunnerAuth is set")
		}
		if cfg.Logger != nil {
			cfg.Logger.Warn("control: runner-protocol Auth is nil; using permissive DisabledAuthenticator (all runners allowed); set Config.Auth (and RequireRunnerAuth) for production")
		} else {
			log.Printf("control: runner-protocol Auth is nil; using permissive DisabledAuthenticator (all runners allowed); set Config.Auth (and RequireRunnerAuth) for production")
		}
	}

	var engOpts []engine.Option
	// The control plane is the one deployment that has a runner directory, so
	// it is the one that can let a batch escape to the runner the map node's
	// runnerSelector chose. Embedded engines keep executing batches in process
	// (see engine.WithRemoteBatchExecution).
	engOpts = append(engOpts, engine.WithRemoteBatchExecution())
	if cfg.Logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.Logger))
	}
	if cfg.RuntimeEvidenceBuffer != nil {
		engOpts = append(engOpts, engine.WithRuntimeEvidenceBuffer(cfg.RuntimeEvidenceBuffer))
	}
	if cfg.LeaseTTL > 0 {
		engOpts = append(engOpts, engine.WithDefaultLeaseTTL(cfg.LeaseTTL))
	}
	if cfg.Metrics != nil {
		engOpts = append(engOpts,
			engine.WithHooks(metrics.NewMetricsHooks(cfg.Metrics)),
			engine.WithCommitObserver(metrics.NewCommitMetrics(cfg.Metrics)),
			engine.WithOutboxObserver(metrics.NewOutboxMetrics(cfg.Metrics)),
			// Batches escape to runners here, so this engine never runs a body.
			// The observer still belongs on it: the runners' reported batches
			// commit through CommitSubgraphResult, which is where their failed
			// items are counted.
			engine.WithItemFailureObserver(metrics.NewSubgraphMetrics(cfg.Metrics)),
			engine.WithGroupObserver(metrics.NewGroupObserver(cfg.Metrics)),
		)
	}
	eng := engine.New(cfg.Backend.State(), cfg.Backend.Queue(), engOpts...)

	var runnerClaimObserver RunnerClaimObserver
	if cfg.Metrics != nil {
		runnerClaimObserver = metrics.NewRunnerClaimMetrics(cfg.Metrics)
	}
	runners := selectRunnerDirectory(cfg, runnerClaimObserver)

	var dispatcherOpts []DispatcherOption
	if cfg.Metrics != nil {
		dispatcherOpts = append(dispatcherOpts, WithDispatcherObserver(metrics.NewDispatcherMetrics(cfg.Metrics)))
	}
	dispatcher := NewDispatcher(eng, runners, dispatcherOpts...)

	var serverOpts []ServerOption
	if cfg.Auth != nil {
		serverOpts = append(serverOpts, WithAuthenticator(cfg.Auth))
	}
	if enrollConfigured(cfg) {
		serverOpts = append(serverOpts, WithEnroll(cfg.RegistrationCodes, cfg.IssuedIdentities))
	}
	// WithIdentityTTL no-ops for cfg.IdentityTTL <= 0, so this is unconditional
	// like the other options above that guard internally.
	serverOpts = append(serverOpts, WithIdentityTTL(cfg.IdentityTTL))
	if cfg.Logger != nil {
		serverOpts = append(serverOpts, WithControlLogger(cfg.Logger))
	}
	if cfg.Metrics != nil {
		serverOpts = append(serverOpts,
			WithAuthObserver(metrics.NewAuthMetrics(cfg.Metrics)),
			WithNodeTimeoutObserver(metrics.NewNodeTimeoutMetrics(cfg.Metrics)),
		)
	}
	if cfg.Tracer != nil {
		serverOpts = append(serverOpts, WithTracer(cfg.Tracer))
	}
	if cfg.PollWait > 0 {
		serverOpts = append(serverOpts, WithHTTPPollWait(cfg.PollWait))
	}
	if cfg.EntryActivationStore != nil {
		serverOpts = append(serverOpts, WithEntryActivationStore(cfg.EntryActivationStore))
	}
	workflowRegistry := selectWorkflowRegistry(cfg)
	if workflowRegistry != nil {
		serverOpts = append(serverOpts, WithWorkflowRegistry(workflowRegistry))
	}
	httpServer := NewServer(eng, runners, serverOpts...)

	var grpcOpts []GRPCServerOption
	if cfg.Auth != nil {
		grpcOpts = append(grpcOpts, WithGRPCAuthenticator(cfg.Auth))
	}
	if cfg.Logger != nil {
		grpcOpts = append(grpcOpts, WithGRPCLogger(cfg.Logger))
	}
	if cfg.Metrics != nil {
		grpcOpts = append(grpcOpts, WithGRPCAuthObserver(metrics.NewAuthMetrics(cfg.Metrics)))
	}
	if cfg.Tracer != nil {
		grpcOpts = append(grpcOpts, WithGRPCTracer(cfg.Tracer))
	}
	if cfg.PollWait > 0 {
		grpcOpts = append(grpcOpts, WithGRPCPollWait(cfg.PollWait))
	}
	grpcServer := NewGRPCServer(eng, runners, grpcOpts...)

	elector, ok := cfg.Backend.(backend.LeaderElector)
	if !ok {
		elector = backend.AlwaysLeader{}
	}
	sweeperCfg := LeaseSweeperConfig{Elector: elector, Logger: cfg.Logger, RunnerDirectory: runners}
	if cfg.Metrics != nil {
		sweeperCfg.Observer = metrics.NewSweepMetrics(cfg.Metrics)
	}
	sweeper := NewLeaseSweeper(cfg.Backend.State(), eng, sweeperCfg)

	// Node-generic entry-activation controller: optional, created only when an
	// EntryActivationStore is provided. The manager writes desired-state on
	// workflow register/deregister; the reconciler assigns live runners, fences
	// stale/removed owners, and produces the activate/deactivate directives the
	// heartbeat handler piggybacks. Both HTTP and gRPC Core instances get the
	// reconciler so heartbeats on either transport deliver directives. The
	// reconciler's Run loop is launched leader-gated in Start.
	var entryManager *EntryActivationManager
	var entryReconciler *EntryActivationReconciler
	// entryNamespaces is the reconciler's namespace list, captured outside the
	// block below so the supply hinter (assembled after) can enumerate the
	// SAME namespaces without a second configuration knob. nil here defaults
	// to {namespace.Default} in both NewEntryActivationReconciler and
	// NewSupplyHinter identically.
	var entryNamespaces []namespace.Namespace
	if cfg.EntryActivationStore != nil {
		entryManager = NewEntryActivationManager(cfg.EntryActivationStore)
		entrySelector := DefaultRunnerSelector()
		recCfg := EntryActivationReconcilerConfig{
			Store:            cfg.EntryActivationStore,
			Selector:         &entrySelector,
			Logger:           cfg.Logger,
			WorkflowRegistry: workflowRegistry,
		}
		if cfg.Metrics != nil {
			recCfg.Metrics = metrics.NewGroupMetrics(cfg.Metrics)
		}
		// The reconciler enumerates live runners via ActivationRunnerLister. The
		// runner directory supplies it when it implements the capability; a
		// directory that does not simply yields no live runners (fail-closed: the
		// reconciler leaves activations unassigned rather than misplacing them).
		if lister, ok := runners.(ActivationRunnerLister); ok {
			recCfg.Lister = lister
		}
		entryReconciler = NewEntryActivationReconciler(recCfg)
		httpServer.core.entryReconciler = entryReconciler
		grpcServer.core.entryReconciler = entryReconciler
		entryNamespaces = recCfg.Namespaces
	}

	var workflowProjectionWorker *WorkflowActivationProjectionWorker
	if outbox, ok := workflowRegistry.(backend.WorkflowActivationProjectionOutbox); ok {
		workflowProjectionWorker = NewWorkflowActivationProjectionWorker(WorkflowActivationProjectionWorkerConfig{
			Outbox:    outbox,
			Projector: NewWorkflowActivationProjector(entryManager),
			Leader:    elector,
			Logger:    cfg.Logger,
		})
	}

	// Supply hint/observed wiring: optional, and only meaningful once an
	// EntryActivationStore exists (the hinter reads activations to find "which
	// runner hosts which supply") AND a store.Supplies is configured (the source
	// of current content hashes). Either missing means both stay nil, and
	// heartbeat request/response bodies are exactly as before this field
	// existed — see the Config.Supplies doc comment.
	var supplyObserved SupplyObservedSink
	if cfg.EntryActivationStore != nil && cfg.Supplies != nil {
		hinter := NewSupplyHinter(cfg.EntryActivationStore, cfg.Supplies, entryNamespaces, cfg.Logger)
		observed := NewMemorySupplyObserved()
		httpServer.core.supplyHinter = hinter
		grpcServer.core.supplyHinter = hinter
		httpServer.core.supplyObserved = observed
		grpcServer.core.supplyObserved = observed
		supplyObserved = observed
	}

	// Supply content encryption: when enabled, resolve the transport key and
	// wire it into the Core (for key delivery on register/heartbeat) and the
	// apiserver supply module (for response encryption).
	var supplyEnc *SupplyEncryptor
	if cfg.EnableSupplyEncryption {
		enc, encErr := resolveSupplyEncryptor(context.Background(), cfg.Backend)
		if encErr != nil {
			return nil, fmt.Errorf("supply encryption: %w", encErr)
		}
		httpServer.core.supplyEncryptor = enc
		grpcServer.core.supplyEncryptor = enc
		supplyEnc = enc
	}

	// Runner metrics proxy: store selection mirrors selectRunnerDirectory —
	// Redis when the backend offers it (so a report that lands on replica A is
	// visible when Prometheus scrapes replica B), process memory otherwise (so
	// single-node and test deployments behave exactly as before).
	var metricsInbox *MetricsInbox
	if cfg.EnableMetricsProxy {
		var store MetricsStore = NewMemoryMetricsStore()
		if provider, ok := cfg.Backend.(redisClientProvider); ok {
			if client := provider.RedisClient(); client != nil {
				store = NewRedisMetricsStore(client, DefaultMetricsRetention)
			}
		}
		var self prometheus.Gatherer
		if cfg.Metrics != nil {
			self = cfg.Metrics.Registry()
		}
		metricsInbox = NewMetricsInbox(MetricsInboxConfig{
			Store:   store,
			Self:    self,
			Live:    NewDirectoryLiveness(runners, DefaultRunnerSelector()),
			Metrics: cfg.Metrics,
			Logger:  cfg.Logger,
		})
		// HTTP only: the gRPC core deliberately does NOT get the inbox, because
		// protocol.RunnerHTTPHandler is the only transport that carries this
		// call (gRPC is not a target shape; cross-cloud goes through the Relay
		// Gateway). Assigning it there would advertise a capability the
		// transport cannot deliver.
		httpServer.core.metricsInbox = metricsInbox
	}

	// Metrics report interval: both cores receive it (unlike the inbox, which is
	// HTTP-only). The field is a pure response annotation — safe on gRPC, and
	// omitting it there would make gRPC heartbeat responses inconsistent with HTTP.
	httpServer.core.metricsReportInterval = cfg.MetricsReportInterval
	grpcServer.core.metricsReportInterval = cfg.MetricsReportInterval

	return &ControlPlane{
		backend:                  cfg.Backend,
		eng:                      eng,
		runners:                  runners,
		dispatcher:               dispatcher,
		httpServer:               httpServer,
		grpcServer:               grpcServer,
		sweeper:                  sweeper,
		elector:                  elector,
		logger:                   cfg.Logger,
		entryActivations:         cfg.EntryActivationStore,
		entryManager:             entryManager,
		entryReconciler:          entryReconciler,
		workflowRegistry:         workflowRegistry,
		workflowProjectionWorker: workflowProjectionWorker,
		supplyObserved:           supplyObserved,
		supplyEncryptor:          supplyEnc,
		supplyKeyRotationPeriod:  cfg.SupplyKeyRotationPeriod,
		metricsInbox:             metricsInbox,
	}, nil
}

// SupplyObserved returns the sink of runner-reported applied supply hashes, or
// nil when no store.Supplies was configured (Config.Supplies). Read-only;
// intended for management/diagnostic surfaces that answer "has runner X
// applied revision Y yet".
func (cp *ControlPlane) SupplyObserved() SupplyObservedSink { return cp.supplyObserved }

// SupplyEncryptor returns the supply content encryptor, or nil when encryption
// is not enabled. The apiserver uses this to encrypt GET /v1/supplies/{name}
// responses for runners that request encrypted content.
func (cp *ControlPlane) SupplyEncryptor() *SupplyEncryptor { return cp.supplyEncryptor }

// MetricsInbox returns the proxied-runner-metrics gatherer, or nil when
// Config.EnableMetricsProxy was not set. The apiserver merges it into its own
// /metrics via prometheus.Gatherers so Prometheus scrapes one endpoint and
// needs no configuration change.
func (cp *ControlPlane) MetricsInbox() *MetricsInbox { return cp.metricsInbox }

// Authenticator returns the runner-protocol authenticator this control plane
// was built with, resolved through the same nil-to-DisabledAuthenticator
// fallback the runner-protocol servers themselves use (Core.authn()). It is
// never nil.
//
// The apiserver's artifact module uses this — rather than reading
// Config.Auth directly — so the namespace-declaration check (§5.3 of the
// runner-artifact-namespace-authorization design) sees the SAME authenticator
// instance the runner protocol itself enforces, whether this ControlPlane was
// built internally by apiserver.New or injected via WithControlPlane (the e2e
// harness path). Reading Config.Auth directly would silently see a stale nil
// in the latter case, the same typed-nil trap documented at
// apiserver.supplyEncryptorFor.
func (cp *ControlPlane) Authenticator() Authenticator { return cp.httpServer.core.authn() }

// Handler returns the HTTP Runner Protocol + workflow API mux. Mount it into
// a host program's own http.ServeMux/http.Server, or serve it directly.
func (cp *ControlPlane) Handler() http.Handler { return cp.httpServer.Handler() }

// GRPCServer returns the gRPC Runner Protocol implementation for hosts that
// want to register it on their own grpc.Server.
func (cp *ControlPlane) GRPCServer() runnerpb.RunnerProtocolServer { return cp.grpcServer }

// RunnerHTTPHandler returns the runner-protocol HTTP adapter for module-based
// route mounting (the apiserver runner-protocol module delegates to it).
func (cp *ControlPlane) RunnerHTTPHandler() protocol.RunnerHTTPHandler {
	return cp.httpServer
}

// Engine returns the engine facade for control API modules (submit/invoke/
// inspect/signal/revoke-signal/cancel). The *engine.Engine satisfies
// control.EngineFacade, so no adapter is required.
func (cp *ControlPlane) Engine() EngineFacade { return cp.eng }

// SeedExecutionFromEntry admits an entry-unit seed through the control Core so
// the server-side namespace resolution AND generation fence (spec §11.6) always
// apply — the apiserver seed module MUST route through this rather than calling
// the raw engine, otherwise a forged/stale-generation seed would fail open. The
// authoritative namespace is taken from ctx (injected by the authz wrapper), not
// the request body.
func (cp *ControlPlane) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	return cp.httpServer.core.SeedExecutionFromEntry(ctx, req)
}

// EntryActivationStore returns the durable EntryActivation store, or nil when
// none was configured. Used by lifecycle wiring to create/fence/deactivate
// activations on workflow add/update/remove.
func (cp *ControlPlane) EntryActivationStore() engine.EntryActivationStore {
	return cp.entryActivations
}

// EntryActivationManager returns the node-generic entry-activation manager, or
// nil when no EntryActivationStore is configured. The apiserver register/
// deregister handlers use it to derive/clear desired activations on workflow
// add/update/remove. Callers MUST nil-guard: an embedded/in-process control
// plane without an activation store returns nil.
func (cp *ControlPlane) EntryActivationManager() *EntryActivationManager {
	return cp.entryManager
}

// WorkflowRegistry returns the durable registry of compiled workflow graphs, or
// nil when none was configured. The apiserver register/deregister handlers use
// it to persist/remove compiled graphs; later tasks resolve a graph on seed to
// derive entry activations.
func (cp *ControlPlane) WorkflowRegistry() backend.WorkflowRegistry {
	return cp.workflowRegistry
}

// WorkflowActivationProjectionWorker returns the durable replacement
// projection worker, or nil when the registry does not expose the projection
// outbox capability. Without an activation manager, its projector is a safe
// no-op and the worker still acknowledges and drains intents. The apiserver
// uses the same worker for its low-latency fast path.
func (cp *ControlPlane) WorkflowActivationProjectionWorker() *WorkflowActivationProjectionWorker {
	return cp.workflowProjectionWorker
}

// RunnerDirectory exposes the runner directory for management/observability
// modules. It is intended for read-only single-runner lookup (the directory
// interface has no list API), so management endpoints can answer
// /v1/management/runners/{id} without re-implementing directory access.
func (cp *ControlPlane) RunnerDirectory() RunnerDirectory { return cp.runners }

// Backend exposes the backend provider for management modules that need a
// capability of the StateStore beyond the engine facade — e.g. the
// DeadLetterStore capability for the dead-letter list/replay management API.
// It is intended for read-only capability checks; the StateStore remains the
// authoritative execution state.
func (cp *ControlPlane) Backend() backend.Provider { return cp.backend }

// Sweeper exposes the production LeaseSweeper for tests and admin tooling that
// need to drive a synchronous sweep without waiting for the background loop.
// It mirrors the read-only accessor pattern of RunnerDirectory() and Backend().
func (cp *ControlPlane) Sweeper() *LeaseSweeper { return cp.sweeper }

// EntryActivationReconciler exposes the node-generic entry-activation reconciler,
// or nil when no EntryActivationStore is configured. It mirrors Sweeper(): a
// read-only seam so integration tests can drive a single deterministic reconcile
// pass (assign/renew/revoke + directive enqueue) without waiting for the
// leader-gated background loop. Production code uses the internal Run loop.
func (cp *ControlPlane) EntryActivationReconciler() *EntryActivationReconciler {
	return cp.entryReconciler
}

// Start binds the Task Dispatcher onto the backend's queue, begins leader
// election (if the backend supports it), and starts the LeaseSweeper loop.
// It does not block.
func (cp *ControlPlane) Start(ctx context.Context) error {
	cp.lifecycleMu.Lock()
	if cp.started {
		cp.lifecycleMu.Unlock()
		return ErrControlPlaneStarted
	}
	if cp.stopped {
		cp.lifecycleMu.Unlock()
		return ErrControlPlaneStopped
	}
	cp.started = true
	cp.lifecycleMu.Unlock()

	unbind, err := cp.bindDispatcher()
	if err != nil {
		cp.lifecycleMu.Lock()
		cp.started = false
		cp.lifecycleMu.Unlock()
		return err
	}
	cp.unbind = unbind

	leaderCtx, leaderCancel := context.WithCancel(ctx)
	cp.leaderCancel = leaderCancel
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.runLeaderCampaign(leaderCtx)
	}()

	sweepCtx, cancel := context.WithCancel(context.Background())
	cp.sweeperCancel = cancel
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.sweeper.Run(sweepCtx)
	}()

	if reclaimer, ok := cp.runners.(ClaimReclaimer); ok {
		claimCtx, claimCancel := context.WithCancel(context.Background())
		cp.claimRecoveryCancel = claimCancel
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.runClaimRecovery(claimCtx, reclaimer)
		}()
	}

	// Launch the node-generic entry-activation reconcile loop. It ticks every
	// ReconcilePeriod and is leader-gated: only the elected leader assigns,
	// fences, renews, and produces directives. A non-leader replica skips the
	// pass entirely.
	if cp.entryReconciler != nil {
		recCtx, recCancel := context.WithCancel(context.Background())
		cp.entryReconcilerCancel = recCancel
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.runEntryReconciler(recCtx)
		}()
	}

	// Recover registry commits whose activation projection was interrupted by
	// a process crash. The worker runs whenever the registry exposes the outbox;
	// a nil activation manager makes projection a safe no-op while intents are
	// still acknowledged and drained. Run immediately on startup and periodically
	// thereafter; each pass is leader-gated inside the worker.
	if cp.workflowProjectionWorker != nil {
		projectionCtx, projectionCancel := context.WithCancel(context.Background())
		cp.workflowProjectionCancel = projectionCancel
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.workflowProjectionWorker.Run(projectionCtx)
		}()
	}

	// Rotate the supply transport key on a schedule, and keep this replica's
	// copy current when a peer rotates instead. Only when encryption is on and
	// rotation is not disabled.
	if cp.supplyEncryptor != nil {
		if period := clampSupplyKeyRotationPeriod(cp.supplyKeyRotationPeriod); period > 0 {
			keyCtx, keyCancel := context.WithCancel(context.Background())
			cp.supplyKeyCancel = keyCancel
			cp.wg.Add(1)
			go func() {
				defer cp.wg.Done()
				cp.runSupplyKeyRotation(keyCtx, period)
			}()
		}
	}

	return nil
}

// runEntryReconciler drives the node-generic entry-activation reconcile loop
// until ctx is cancelled. It is leader-gated: a non-leader replica skips the
// pass so only one replica assigns/fences activations at a time.
func (cp *ControlPlane) runEntryReconciler(ctx context.Context) {
	ticker := time.NewTicker(DefaultEntryActivationReconcilePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !cp.elector.IsLeader() {
				continue
			}
			if err := cp.entryReconciler.Reconcile(ctx, time.Now()); err != nil && ctx.Err() == nil {
				if cp.logger != nil {
					cp.logger.Error("entry activation reconcile failed", "err", err)
				}
			}
		}
	}
}

func (cp *ControlPlane) runClaimRecovery(ctx context.Context, reclaimer ClaimReclaimer) {
	const interval = time.Second
	recover := func() {
		if err := reclaimer.ReclaimExpiredClaims(ctx); err != nil && ctx.Err() == nil && cp.logger != nil {
			cp.logger.Error("recover expired runner claims failed", "err", err)
		}
	}
	recover()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recover()
		}
	}
}

// IsLeader reports whether this ControlPlane replica currently holds
// leadership. Backends without real leader election (e.g. memory) always
// report true via backend.AlwaysLeader. Useful for health checks and
// observability in multi-replica deployments.
func (cp *ControlPlane) IsLeader() bool { return cp.elector.IsLeader() }

func (cp *ControlPlane) runLeaderCampaign(ctx context.Context) {
	const retryDelay = time.Second
	notify := cp.elector.Notify()
	for {
		if err := cp.elector.Campaign(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				if cp.logger != nil {
					cp.logger.Info("leader campaign stopped", "err", err)
				}
				return
			}
			if cp.logger != nil {
				cp.logger.Error("leader campaign failed", "err", err)
			}
			if err := sleepWithContext(ctx, retryDelay); err != nil {
				return
			}
			continue
		}

		for {
			select {
			case <-ctx.Done():
				return
			case isLeader := <-notify:
				if !isLeader {
					goto recampaign
				}
			}
		}

	recampaign:
	}
}

// Shutdown cancels all background workers, including workflow activation
// projection recovery, resigns leadership (if held), and unwinds the backend
// queue binding. It attempts every step even if an earlier one fails,
// aggregating all errors encountered.
func (cp *ControlPlane) Shutdown(ctx context.Context) error {
	var errs []error
	cp.lifecycleMu.Lock()
	cp.started = false
	cp.stopped = true
	cp.lifecycleMu.Unlock()

	if cp.sweeperCancel != nil {
		cp.sweeperCancel()
	}
	if cp.claimRecoveryCancel != nil {
		cp.claimRecoveryCancel()
	}
	if cp.entryReconcilerCancel != nil {
		cp.entryReconcilerCancel()
	}
	if cp.workflowProjectionCancel != nil {
		cp.workflowProjectionCancel()
	}
	if cp.supplyKeyCancel != nil {
		cp.supplyKeyCancel()
	}
	if cp.leaderCancel != nil {
		cp.leaderCancel()
	}
	// Wait for the background goroutines to observe their cancelled contexts
	// and return, but bound the wait by ctx so a stuck goroutine cannot hang
	// shutdown. The sweeper, leader, claim-recovery, entry-reconciliation,
	// workflow-projection, and supply-key loops all observe their contexts.
	waitDone := make(chan struct{})
	go func() { cp.wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-ctx.Done():
	}
	if err := cp.elector.Resign(ctx); err != nil {
		errs = append(errs, err)
	}
	if cp.unbind != nil {
		cp.unbind()
	}
	return errors.Join(errs...)
}

// bindDispatcher wires the control-plane dispatcher's task handler into the
// backend's queue/transport via the TaskHandlerBinder capability. Backends
// that do not implement TaskHandlerBinder cannot serve a control plane —
// falling back to Provider.Bind would run the embedded execution dispatcher
// in-process (silently executing handlers inside the server instead of
// dispatching to remote runners), so we fail closed with a configuration
// error instead.
func (cp *ControlPlane) bindDispatcher() (func(), error) {
	binder, ok := cp.backend.(backend.TaskHandlerBinder)
	if !ok {
		return nil, fmt.Errorf("control: backend %T does not implement backend.TaskHandlerBinder; this backend cannot serve a control plane (configure a local or distributed backend)", cp.backend)
	}
	return binder.BindTaskHandler(cp.eng, cp.dispatcher.HandleTask)
}
