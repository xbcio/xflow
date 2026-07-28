package distributed

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	asynqtransport "github.com/xbcio/xflow/backend/providers/distributed/internal/queue/asynq"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/rstate"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/timeout"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/trigger"
	"github.com/xbcio/xflow/backend/providers/distributed/internal/workflowreg"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// defaultLeaderLeaseTTL is the Redis lease TTL backing LeaderElector. Renewal
// runs at ttl/3, so a crashed leader is detected and replaced within roughly
// one TTL window.
const defaultLeaderLeaseTTL = 15 * time.Second

// QueueObserver receives producer-side enqueue outcomes from the task
// transport. It is a technology-neutral alias for queue.Observer so metrics
// code can implement it without importing a concrete broker package.
type QueueObserver = queue.Observer

// AuditObserver, LeaseObserver, and AuditStats are the state-store
// observability contracts. They live in the internal rstate package; these
// aliases preserve the distributed public API so existing metrics
// implementations keep satisfying them unchanged.
type (
	AuditObserver = rstate.AuditObserver
	LeaseObserver = rstate.LeaseObserver
	AuditStats    = rstate.AuditStats
)

// ShutdownReport captures the outcomes of releasing the backend's owned
// resources during normal shutdown. All fields are nil when shutdown cleanup
// succeeds; otherwise each field carries the error returned by the matching
// Close call.
//
// Observers and log sinks must not emit these errors verbatim in production:
// close errors may contain DSNs or credentials. The built-in fallback logging
// paths redact URL credentials and common query parameters before printing, but
// external observers receive the raw error and are responsible for their own
// sanitization.
type ShutdownReport struct {
	TransportErr error
	RedisErr     error
	PoolErr      error
}

// ShutdownObserver receives the ShutdownReport after a normal stop completes.
// Implementations must be non-blocking: the observer is called synchronously
// inside the sync.Once-guarded stop path. The report is delivered raw; observers
// and downstream log sinks must sanitize any sensitive substrings (DSNs,
// passwords, tokens) before emitting them.
type ShutdownObserver interface {
	OnShutdown(r ShutdownReport)
}

// Option configures the distributed backend.
type Option func(*config)

type config struct {
	concurrency            int
	execTTL                time.Duration
	consumer               bool
	resourcePool           types.ResourcePool
	auditObserver          AuditObserver
	leaseObserver          LeaseObserver
	shutdownObserver       ShutdownObserver
	logger                 engine.Logger
	transient              bool
	transientTTL           time.Duration
	transientCompletionTTL time.Duration
	transport              queue.Transport
	queueObserver          queue.Observer
	redisConfig            *RedisConfig
}

// WithConcurrency sets the number of task consumer goroutines. Default is 10.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithExecTTL sets the TTL for all Redis keys belonging to an execution.
// Default is 24 hours.
func WithExecTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.execTTL = d
		}
	}
}

// WithTransientMode enables transient Redis retention with a sliding active TTL
// and a shorter completion TTL.
func WithTransientMode(activeTTL, completionTTL time.Duration) Option {
	return func(c *config) {
		c.transient = true
		c.transientTTL = activeTTL
		c.transientCompletionTTL = completionTTL
	}
}

// WithConsumer controls whether Bind starts a task consumer and timeout
// monitor in this process. Disable it for API-only embedded SDK instances.
func WithConsumer(enabled bool) Option {
	return func(c *config) {
		c.consumer = enabled
	}
}

// WithResourcePool installs a process-scope ResourcePool. Worker pods that
// run DatabaseNode / GRPCNode benefit from a pool; API-only pods (consumer
// disabled) can leave it nil. Default is nil: resource-aware nodes
// (DatabaseNode/GRPCNode) error at runtime when invoked without a pool —
// production deployments should always inject a pool.
// See .claude/specs/resource-pool.md.
func WithResourcePool(p types.ResourcePool) Option {
	return func(c *config) { c.resourcePool = p }
}

// WithAuditObserver installs an external observer for audit-store dual-write
// outcomes. Per .claude/specs/dual-write-contract.md, Redis is the system
// of record and the sqlstore audit trail is best-effort; this observer is the
// hook for ops/metrics to count and reconcile audit failures. Composes with
// the built-in atomic counters reachable via (*Backend).AuditStats().
func WithAuditObserver(obs AuditObserver) Option {
	return func(c *config) {
		if obs != nil {
			c.auditObserver = obs
		}
	}
}

// WithLeaseObserver installs an external observer for Redis lease acquisition,
// expiry-index scans, and index repairs. It is optional and must not block the
// state-store hot path.
func WithLeaseObserver(obs LeaseObserver) Option {
	return func(c *config) {
		if obs != nil {
			c.leaseObserver = obs
		}
	}
}

// WithStateLogger installs a logger used by the audit wrapper for failed
// dual-writes. Optional; without one, audit failures are still counted via
// the observer/counters but not logged.
func WithStateLogger(l engine.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithShutdownObserver installs an observer that receives a ShutdownReport
// after normal shutdown completes. When no observer is configured, shutdown
// errors are still logged via the configured engine.Logger, or via the
// standard log package if no logger is configured, so close failures are never
// silently swallowed. The raw error is delivered to the observer; callers must
// ensure observer-side sinks do not emit sensitive substrings such as DSNs or
// credentials in production.
func WithShutdownObserver(obs ShutdownObserver) Option {
	return func(c *config) {
		if obs != nil {
			c.shutdownObserver = obs
		}
	}
}

// WithTransport injects a custom task transport, replacing the default Asynq
// transport entirely. This is the seam that makes the queue technology
// pluggable: a broker only has to implement queue.Transport.
func WithTransport(t queue.Transport) Option {
	return func(c *config) {
		if t != nil {
			c.transport = t
		}
	}
}

// WithQueueObserver installs a producer-side enqueue observer on the default
// Asynq transport. It is ignored when a custom transport is injected via
// WithTransport (that transport carries its own observer wiring).
func WithQueueObserver(obs QueueObserver) Option {
	return func(c *config) {
		if obs != nil {
			c.queueObserver = obs
		}
	}
}

// WithRedisConfig injects a Redis HA connection description. When present,
// New builds the appropriate go-redis client (single/sentinel/cluster) using
// redis.NewUniversalClient and ignores the redisAddr argument. When absent,
// New keeps the legacy single-address behavior.
func WithRedisConfig(rc RedisConfig) Option {
	return func(c *config) { c.redisConfig = &rc }
}

// Backend wires the Engine Core to Redis state (internal/rstate) and a
// pluggable task transport (default: Asynq). It is a thin facade over the
// internal state, timeout, trigger, and workflow-registry subpackages.
// Call Bind() after creating the engine to start the consumer and monitors.
type Backend struct {
	state            *rstate.Store
	transport        queue.Transport
	registry         *execution.Registry
	workflowReg      *workflowreg.Registry
	triggerRuntime   *trigger.Primitives
	rdb              redis.UniversalClient
	timeoutMonitor   *timeout.Monitor
	concurrency      int
	consumer         bool
	transient        bool
	resourcePool     types.ResourcePool
	leaderElector    backend.LeaderElector
	shutdownObserver ShutdownObserver
	logger           engine.Logger
	testHooks        bindStartHooks
}

// State returns the StateStore implementation.
func (b *Backend) State() engine.StateStore { return b.state }

// Queue returns the TaskQueue implementation (the injected or default transport).
func (b *Backend) Queue() engine.TaskQueue { return b.transport }

// Registry returns the HandlerRegistry implementation.
func (b *Backend) Registry() engine.HandlerRegistry { return b.registry }

// WorkflowRegistry returns the workflow metadata registry.
func (b *Backend) WorkflowRegistry() backend.WorkflowRegistry { return b.workflowReg }

// TriggerPrimitives returns trigger coordination primitives.
func (b *Backend) TriggerPrimitives() backend.TriggerPrimitives { return b.triggerRuntime }

// AuditStats returns a point-in-time snapshot of audit-store dual-write
// outcomes (ok and failed counts keyed by op). See
// .claude/specs/dual-write-contract.md.
func (b *Backend) AuditStats() AuditStats { return b.state.AuditStats() }

// RedisClient exposes the Redis command capability required by optional
// server-side coordination components. It does not add a dependency from the
// reusable backend package to service packages.
func (b *Backend) RedisClient() redis.Cmdable { return b.rdb }

// LeaderElector returns the Redis-backed leader election coordinator shared
// by all ControlPlane replicas pointed at the same Redis instance. Used to
// gate leader-only background work (e.g. LeaseSweeper) so only one replica
// runs it at a time.
func (b *Backend) LeaderElector() backend.LeaderElector { return b.leaderElector }

// Campaign, IsLeader, Resign, and Notify forward to the backend's
// RedisLeaderElector, so *Backend itself satisfies backend.LeaderElector —
// required for ControlPlane's type assertion (cfg.Backend.(backend.LeaderElector))
// to actually detect Redis-backed leader election instead of silently
// falling back to AlwaysLeader.
func (b *Backend) Campaign(ctx context.Context) error { return b.leaderElector.Campaign(ctx) }
func (b *Backend) IsLeader() bool                     { return b.leaderElector.IsLeader() }
func (b *Backend) Resign(ctx context.Context) error   { return b.leaderElector.Resign(ctx) }
func (b *Backend) Notify() <-chan bool                { return b.leaderElector.Notify() }

var _ backend.LeaderElector = (*Backend)(nil)
var _ backend.TaskHandlerBinder = (*Backend)(nil)
var _ backend.StartBinder = (*Backend)(nil)

// New creates a distributed backend connected to the given Redis address.
// db may be nil for pure-Redis mode (no MySQL persistence).
// Call Bind(eng) after creating the engine to start queue consumers.
func New(redisAddr string, db store.Store, opts ...Option) (*Backend, error) {
	cfg := &config{concurrency: 10, execTTL: rstate.DefaultExecTTL, consumer: true}
	for _, o := range opts {
		o(cfg)
	}

	var rdb redis.UniversalClient
	var err error
	if cfg.redisConfig != nil {
		if err := cfg.redisConfig.validate(); err != nil {
			return nil, fmt.Errorf("redis config: %w", err)
		}
		rdb, err = newRedisClient(*cfg.redisConfig)
		if err != nil {
			return nil, fmt.Errorf("redis client: %w", err)
		}
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	}

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	state := rstate.New(rdb, db, cfg.execTTL)
	state.SetAuditObserver(cfg.auditObserver)
	state.SetLeaseObserver(cfg.leaseObserver)
	state.SetLogger(cfg.logger)
	state.ConfigureTransient(cfg.transient, cfg.transientTTL, cfg.transientCompletionTTL)

	// Default to the Asynq transport; WithTransport can inject an alternative.
	// If a RedisConfig was injected, map it to the corresponding asynq HA
	// connection option (single/sentinel/cluster) and use NewWithConnOpt.
	// Otherwise keep the legacy single-address string path via New(redisAddr).
	transport := cfg.transport
	if transport == nil {
		var topts []asynqtransport.Option
		if cfg.queueObserver != nil {
			topts = append(topts, asynqtransport.WithObserver(cfg.queueObserver))
		}
		if cfg.redisConfig != nil {
			connOpt, err := cfg.redisConfig.AsAsynqConnOpt()
			if err != nil {
				// rdb has been Pinged successfully and is owned by this function
				// until a Backend is returned; on failure it must be released.
				_ = rdb.Close()
				return nil, fmt.Errorf("asynq redis conn opt: %w", err)
			}
			transport = asynqtransport.NewWithConnOpt(connOpt, topts...)
		} else {
			transport = asynqtransport.New(redisAddr, topts...)
		}
	}

	registry := execution.NewRegistry()

	leaderKey := "xflow:leader:control-plane"
	leaderElector := NewRedisLeaderElector(rdb, leaderKey, defaultLeaderLeaseTTL)

	return &Backend{
		state:            state,
		transport:        transport,
		registry:         registry,
		workflowReg:      workflowreg.New(rdb),
		triggerRuntime:   trigger.New(rdb),
		rdb:              rdb,
		concurrency:      cfg.concurrency,
		consumer:         cfg.consumer,
		transient:        cfg.transient,
		resourcePool:     cfg.resourcePool,
		leaderElector:    leaderElector,
		shutdownObserver: cfg.shutdownObserver,
		logger:           cfg.logger,
	}, nil
}

// Bind wires the embedded execution dispatcher into the task transport and
// starts the timeout monitor. Returns a stop function for graceful shutdown.
//
// Deprecated: Provider.Bind is the legacy SDK path that cannot propagate start
// errors. New code should use StartBinding, which returns an error so callers
// can fail closed when the consumer does not start.
func (b *Backend) Bind(eng *engine.Engine) func() {
	if !b.consumer {
		return b.nonConsumerStop()
	}

	var opts []execution.RunnerOption
	if b.resourcePool != nil {
		opts = append(opts, execution.WithResourcePool(b.resourcePool))
	}
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
	stop, err := b.bindHandler(eng, dispatcher.HandleTask)
	if err != nil {
		b.logBindError("Provider.Bind path", err)
		return noopStop()
	}
	return stop
}

// StartBinding implements backend.StartBinder. It wires the embedded execution
// dispatcher into the task transport and starts the durable outbox dispatcher
// and lease timeout monitor, returning an error if any component fails so the
// SDK factory (NewCluster/NewLocal) never returns a ready Engine while the
// consumer is down.
func (b *Backend) StartBinding(eng *engine.Engine) (func(), error) {
	if !b.consumer {
		return b.nonConsumerStop(), nil
	}

	var opts []execution.RunnerOption
	if b.resourcePool != nil {
		opts = append(opts, execution.WithResourcePool(b.resourcePool))
	}
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
	return b.bindHandler(eng, dispatcher.HandleTask)
}

// BindHandler wires a custom task handler into the task transport and starts
// the outbox dispatcher and timeout monitor. It is retained for compatibility
// with callers that bind a custom handler outside the control-plane contract.
//
// Deprecated: Prefer StartBinding for the SDK path or BindTaskHandler for the
// control-plane path. BindHandler swallows consumer start errors; production
// callers should use the error-returning alternatives.
func (b *Backend) BindHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) func() {
	stop, err := b.bindHandler(eng, handler)
	if err != nil {
		b.logBindError("BindHandler path", err)
		return noopStop()
	}
	return stop
}

// noopStop returns a truly empty stop func. It is the correct return value
// for the deprecated Bind/BindHandler paths when bindHandler has failed:
// closeOwnedResources has already released transport/Redis/pool, so the
// returned stop must NOT close them again. nonConsumerStop would double-close
// (regression 2026-07-21: ResourcePool.Close calls=2). Idempotent.
func noopStop() func() {
	var once sync.Once
	return func() {
		once.Do(func() {})
	}
}

var (
	// urlCredsRe matches scheme://[user:]password@host patterns. The username
	// is optional so the common redis://:password@host form is also covered.
	// The scheme group is restricted to a leading letter so it does not
	// over-match arbitrary text.
	urlCredsRe = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*)://(?:[^:@\s]*):([^@\s]+)@`)
	// queryCredsRe matches common credential-bearing parameters anywhere in
	// the text, including token-bearing names that commonly leak DSN/transport
	// credentials. The leading boundary accepts the start of the string or any
	// non-word character (space, ;, &, ?, comma, etc.) so parameters appearing
	// in free-form error text are masked, not just those in a query string.
	queryCredsRe = regexp.MustCompile(`(?i)(^|[^a-z0-9_])(password|pwd|secret|token|access_token|api_key|apikey|ak|sk)=([^\s&;]*)`)
	// queryCredsReplacement re-inserts the captured boundary and the param
	// name, masking only the credential value.
	queryCredsReplacement = "${1}${2}=***"
)

// sanitizeLogError masks URL/DSN-like credential substrings and common
// credential query parameters before an error is printed. It is intentionally
// conservative: it only removes substrings that look like credentials, leaving
// the rest of the error intact. Observers receive the raw error unchanged.
func sanitizeLogError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	s = urlCredsRe.ReplaceAllString(s, "$1://***:***@")
	s = queryCredsRe.ReplaceAllString(s, queryCredsReplacement)
	return s
}

// logBindError records a bind/consumer-start error through the engine.Logger
// when available, otherwise through the stdlib log. The error is sanitized
// first: bind failures can surface transport, Redis or pool errors whose
// message strings carry DSN/credential substrings. Deprecated Bind/BindHandler
// paths swallow the error return, so the only observability is this log line,
// which must not leak credentials.
func (b *Backend) logBindError(path string, err error) {
	if err == nil {
		return
	}
	if b.logger != nil {
		b.logger.Error("distributed backend: bind error", "path", path, "error", sanitizeLogError(err))
		return
	}
	log.Printf("xflow: bind error (%s): %v", path, sanitizeLogError(err))
}

// reportShutdown emits a ShutdownReport to the configured observer or logs any
// non-nil close errors. It is called synchronously from the sync.Once-guarded
// stop path so shutdown remains observable even when no external observer is
// installed. Errors sent to the built-in logger/stdlib log path are sanitized;
// when an observer is configured it receives the raw error. If that observer
// panics, the already-collected report is NOT lost: logShutdownFallback records
// the sanitized errors (and the panic) via the logger or stdlib log so the
// close errors remain observable instead of vanishing.
func (b *Backend) reportShutdown(r ShutdownReport) {
	if b.shutdownObserver != nil {
		// A panicking observer must not crash the shutdown path, and must not
		// cause the collected ShutdownReport to be silently dropped. On panic
		// we fall back to the sanitized logger/stdlib path so the close errors
		// (and a record of the panic) survive.
		panicked := true
		defer func() {
			if !panicked {
				return
			}
			rec := recover()
			if rec == nil {
				return
			}
			if b.logger != nil {
				b.logger.Error("distributed backend: shutdown observer panicked; falling back to sanitized log", "panic", rec)
			} else {
				log.Printf("xflow: distributed backend shutdown observer panicked: %v", rec)
			}
			logShutdownFallback(b.logger, r)
		}()
		b.shutdownObserver.OnShutdown(r)
		panicked = false
		return
	}
	logShutdownFallback(b.logger, r)
}

// logShutdownFallback emits the non-nil close errors of a ShutdownReport via
// the engine.Logger when available, otherwise via the stdlib log. All errors
// are sanitized before being written. It is the shared fallback used both when
// no observer is configured and when a configured observer panics, so a
// collected report is never lost.
func logShutdownFallback(logger engine.Logger, r ShutdownReport) {
	if logger != nil {
		if r.TransportErr != nil {
			logger.Error("distributed backend: transport close error", "error", sanitizeLogError(r.TransportErr))
		}
		if r.RedisErr != nil {
			logger.Error("distributed backend: redis close error", "error", sanitizeLogError(r.RedisErr))
		}
		if r.PoolErr != nil {
			logger.Error("distributed backend: resource pool close error", "error", sanitizeLogError(r.PoolErr))
		}
		return
	}
	if r.TransportErr != nil {
		log.Printf("xflow: distributed backend transport close error: %v", sanitizeLogError(r.TransportErr))
	}
	if r.RedisErr != nil {
		log.Printf("xflow: distributed backend redis close error: %v", sanitizeLogError(r.RedisErr))
	}
	if r.PoolErr != nil {
		log.Printf("xflow: distributed backend resource pool close error: %v", sanitizeLogError(r.PoolErr))
	}
}

// nonConsumerStop releases transport and Redis resources for a backend that is
// not configured to consume (API-only instances). It is idempotent and reports
// a ShutdownReport exactly once.
func (b *Backend) nonConsumerStop() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			var r ShutdownReport
			r.TransportErr = b.transport.Close()
			r.RedisErr = b.rdb.Close()
			if b.resourcePool != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				r.PoolErr = b.resourcePool.Close(ctx)
			}
			b.reportShutdown(r)
		})
	}
}

// bindStartHooks are test-only injection points for the binder lifecycle. They
// are nil in production. Each hook returns an error to simulate a component
// failing to start, so the reverse-order rollback path is covered even though
// the outbox dispatcher and timeout monitor cannot fail to start on their own.
type bindStartHooks struct {
	afterConsumerStart func() error // simulate outbox dispatcher start failure
	afterOutboxStart   func() error // simulate timeout monitor start failure
	onMonitorDone      func()       // nil in production; called after timeout monitor goroutine exits
}

// closeOwnedResources releases the resources owned by bindHandler in
// reverse-acquisition order: transport, Redis client, then the injected
// ResourcePool (if any). Used by all failed-start rollback paths so a failed
// bind leaves no resources behind, mirroring the normal stop order (the pool
// is closed last because it may depend on the Redis client and transport).
//
// The startup error is primary: cleanup errors are joined via errors.Join but
// never mask it — callers can still unwrap the original startup error.
// Each resource is closed at most once along this path because a failed bind
// returns a nil stop func, so the normal stop path is never reached.
func (b *Backend) closeOwnedResources(startupErr error) error {
	transportErr := b.transport.Close()
	rdbErr := b.rdb.Close()
	cleanupErrs := []error{startupErr, transportErr, rdbErr}
	if b.resourcePool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := b.resourcePool.Close(ctx); cerr != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("distributed: close resource pool: %w", cerr))
		}
	}
	joined := errors.Join(cleanupErrs...)
	if joined == nil {
		return nil
	}
	return joined
}

// bindHandler is the unified internal lifecycle entry: it wires a task handler
// into the transport and starts the durable outbox dispatcher and lease
// timeout monitor. Components are acquired in order; on any failure the
// already-started components are stopped in reverse order and their goroutines
// awaited before returning. The returned stop func is idempotent and waits for
// background goroutines to exit before releasing the resources they depend on.
//
// Fail-closed contract: a consumer start error is returned to the caller
// (BindTaskHandler propagates it to ControlPlane.Start) rather than logged and
// ignored, so readiness can never be reported while the consumer is down.
func (b *Backend) bindHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) (func(), error) {
	if !b.consumer {
		return b.nonConsumerStop(), nil
	}

	// F1 graph-aware unit-index resolver: the pure queue codec (queue.Unmarshal)
	// cannot access a StateStore/Graph, so it decodes a task with
	// UnitIdx == engine.UnitIdxUnknown when the wire payload has no _unit_idx
	// field (pre-group durable payload). This wrapper is the single owner of
	// graph-aware recovery: it loads the authoritative Graph for the task's
	// execution and resolves the real unit index via Graph.UnitIndexForNode
	// before the task reaches the dispatcher/handler. It never assumes
	// UnitIdx == NodeIdx (only true for graphs with no groups — buildUnits
	// places ungrouped nodes before group units, so a mixed graph's node unit
	// indexes are shifted). A group task (TaskTypeGroupExec) with an unknown
	// UnitIdx fails closed immediately without attempting graph-aware
	// recovery: a durable group payload missing its unit identity cannot be
	// trusted to resolve to the correct group unit even if a graph loads
	// successfully.
	resolvedHandler := func(ctx context.Context, task *engine.Task) error {
		if task != nil && task.UnitIdx == engine.UnitIdxUnknown {
			if task.Type == engine.TaskTypeGroupExec {
				return fmt.Errorf("distributed: group task %q/%q missing durable unit identity (stale payload)", task.ExecutionID, task.NodeName)
			}
			g, err := b.state.LoadGraph(context.Background(), task.ExecutionID)
			if err != nil {
				return fmt.Errorf("distributed: resolve unit index for %q/%q: load graph: %w", task.ExecutionID, task.NodeName, err)
			}
			if g == nil {
				return fmt.Errorf("distributed: resolve unit index for %q/%q: no graph available", task.ExecutionID, task.NodeName)
			}
			if task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
				return fmt.Errorf("distributed: resolve unit index for %q/%q: node index %d out of range", task.ExecutionID, task.NodeName, task.NodeIdx)
			}
			task.UnitIdx = g.UnitIndexForNode(task.NodeIdx)
		}
		return handler(ctx, task)
	}

	// 1. Start the consumer (broker-side task delivery). This is the only step
	//    that can fail against a real broker; its error must propagate.
	stopConsumer, err := b.transport.StartConsumer(
		queue.ConsumerConfig{Concurrency: b.concurrency, Transient: b.transient},
		queue.TaskHandler(resolvedHandler),
	)
	if err != nil {
		// Consumer never started; release the delivery transport, Redis
		// client, and the injected ResourcePool so a failed bind leaves no
		// resources behind. Mirrors the normal stop order.
		return nil, b.closeOwnedResources(fmt.Errorf("distributed: start consumer: %w", err))
	}
	if b.testHooks.afterConsumerStart != nil {
		if err := b.testHooks.afterConsumerStart(); err != nil {
			stopConsumer()
			return nil, b.closeOwnedResources(fmt.Errorf("distributed: start outbox dispatcher: %w", err))
		}
	}

	// 2. Start the durable outbox dispatcher. It exits when ctx is canceled.
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	outboxDispatcher := engine.NewOutboxDispatcher(eng, time.Second)
	outboxDone := make(chan struct{})
	go func() {
		defer close(outboxDone)
		outboxDispatcher.Run(outboxCtx)
	}()

	// 3. Start the lease timeout monitor (durable only). Transient mode has no
	//    suspend/timeout semantics, so the poll would be pure overhead.
	var tm *timeout.Monitor
	tmDone := make(chan struct{})
	if !b.transient {
		tm = timeout.New(b.rdb, eng, nil, b.logger, 5*time.Second)
		b.timeoutMonitor = tm
		go func() {
			defer close(tmDone)
			tm.Run()
		}()
		if b.testHooks.afterOutboxStart != nil {
			if err := b.testHooks.afterOutboxStart(); err != nil {
				// Reverse-order rollback: stop monitor and wait, stop outbox
				// dispatcher and wait, then stop consumer. The transport,
				// Redis client, and ResourcePool are released via
				// closeOwnedResources (mirroring the normal stop order).
				if tm != nil {
					tm.Stop()
					<-tmDone
					if b.testHooks.onMonitorDone != nil {
						b.testHooks.onMonitorDone()
					}
				}
				cancelOutbox()
				<-outboxDone
				if stopConsumer != nil {
					stopConsumer()
				}
				return nil, b.closeOwnedResources(fmt.Errorf("distributed: start timeout monitor: %w", err))
			}
		}
	} else {
		close(tmDone)
	}

	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			// Stop the consumer first to block new work, then cancel background
			// loops and wait for their goroutines to exit before releasing the
			// resources (Redis, transport, pool) they use.
			if stopConsumer != nil {
				stopConsumer()
			}
			cancelOutbox()
			<-outboxDone
			if tm != nil {
				tm.Stop()
				<-tmDone
				if b.testHooks.onMonitorDone != nil {
					b.testHooks.onMonitorDone()
				}
			}
			var r ShutdownReport
			r.TransportErr = b.transport.Close()
			r.RedisErr = b.rdb.Close()
			if b.resourcePool != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				r.PoolErr = b.resourcePool.Close(ctx)
			}
			b.reportShutdown(r)
		})
	}
	return stop, nil
}

// BindTaskHandler implements backend.TaskHandlerBinder. It is the control-plane
// binding path: the caller-supplied handler (the control-plane dispatcher) is
// wired into the transport, and the durable outbox dispatcher plus lease
// timeout monitor are started. A nil engine is a configuration error — the
// outbox dispatcher and timeout monitor both require it — so we fail closed
// rather than silently starting a backend that cannot make scheduling progress.
//
// A backend configured with WithConsumer(false) is also a configuration error
// for a control plane: without a consumer it cannot receive task results, so
// dispatch would silently go nowhere. We reject it here instead of returning a
// no-op stop.
//
// Consumer start errors propagate to ControlPlane.Start via the returned error
// (fail-closed): readiness can never be reported while the consumer is down.
func (b *Backend) BindTaskHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) (func(), error) {
	if eng == nil {
		return nil, errors.New("distributed: BindTaskHandler requires a non-nil engine")
	}
	if handler == nil {
		return nil, errors.New("distributed: BindTaskHandler requires a non-nil handler")
	}
	if !b.consumer {
		return nil, errors.New("distributed: BindTaskHandler requires a backend configured with WithConsumer(true); a non-consumer backend cannot serve a control plane")
	}
	return b.bindHandler(eng, handler)
}
