package distributed

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	asynqtransport "github.com/xbcio/xflow/backend/distributed/internal/queue/asynq"
	"github.com/xbcio/xflow/backend/distributed/internal/rstate"
	"github.com/xbcio/xflow/backend/distributed/internal/timeout"
	"github.com/xbcio/xflow/backend/distributed/internal/trigger"
	"github.com/xbcio/xflow/backend/distributed/internal/workflowreg"
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

// Option configures the distributed backend.
type Option func(*config)

type config struct {
	concurrency            int
	execTTL                time.Duration
	consumer               bool
	resourcePool           types.ResourcePool
	auditObserver          AuditObserver
	leaseObserver          LeaseObserver
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
	state          *rstate.Store
	transport      queue.Transport
	registry       *execution.Registry
	workflowReg    *workflowreg.Registry
	triggerRuntime *trigger.Primitives
	rdb            redis.UniversalClient
	timeoutMonitor *timeout.Monitor
	concurrency    int
	consumer       bool
	transient      bool
	resourcePool   types.ResourcePool
	leaderElector  backend.LeaderElector
	testHooks      bindStartHooks
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
		state:          state,
		transport:      transport,
		registry:       registry,
		workflowReg:    workflowreg.New(rdb),
		triggerRuntime: trigger.New(rdb),
		rdb:            rdb,
		concurrency:    cfg.concurrency,
		consumer:       cfg.consumer,
		transient:      cfg.transient,
		resourcePool:   cfg.resourcePool,
		leaderElector:  leaderElector,
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
		log.Printf("xflow: bind error (Provider.Bind path): %v", err)
		return b.nonConsumerStop()
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
		log.Printf("xflow: bind error (BindHandler path): %v", err)
		return b.nonConsumerStop()
	}
	return stop
}

// nonConsumerStop releases transport and Redis resources for a backend that is
// not configured to consume (API-only instances). It is idempotent.
func (b *Backend) nonConsumerStop() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = b.transport.Close()
			_ = b.rdb.Close()
			if b.resourcePool != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = b.resourcePool.Close(ctx)
			}
		})
	}
}

// bindStartHooks are test-only injection points for the binder lifecycle. They
// are nil in production. Each hook returns an error to simulate a component
// failing to start, so the reverse-order rollback path is covered even though
// the outbox dispatcher and timeout monitor cannot fail to start on their own.
type bindStartHooks struct {
	afterConsumerStart func() error  // simulate outbox dispatcher start failure
	afterOutboxStart   func() error  // simulate timeout monitor start failure
	onMonitorDone      func()        // nil in production; called after timeout monitor goroutine exits
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
	_ = b.transport.Close()
	_ = b.rdb.Close()
	if b.resourcePool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cerr := b.resourcePool.Close(ctx); cerr != nil {
			return errors.Join(startupErr, fmt.Errorf("distributed: close resource pool: %w", cerr))
		}
	}
	return startupErr
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

	// 1. Start the consumer (broker-side task delivery). This is the only step
	//    that can fail against a real broker; its error must propagate.
	stopConsumer, err := b.transport.StartConsumer(
		queue.ConsumerConfig{Concurrency: b.concurrency, Transient: b.transient},
		queue.TaskHandler(handler),
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
		tm = timeout.New(b.rdb, eng, nil, nil, 5*time.Second)
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
			_ = b.transport.Close()
			_ = b.rdb.Close()
			if b.resourcePool != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = b.resourcePool.Close(ctx)
			}
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
