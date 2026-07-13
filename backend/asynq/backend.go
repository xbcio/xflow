package asynq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	asynqlib "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// defaultLeaderLeaseTTL is the Redis lease TTL backing LeaderElector. Renewal
// runs at ttl/3, so a crashed leader is detected and replaced within roughly
// one TTL window.
const defaultLeaderLeaseTTL = 15 * time.Second

// Option configures the Asynq backend.
type Option func(*config)

type config struct {
	concurrency   int
	execTTL       time.Duration
	consumer      bool
	resourcePool  types.ResourcePool
	auditObserver AuditObserver
	logger        engine.Logger
}

// WithConcurrency sets the number of Asynq queue consumer goroutines. Default is 10.
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

// WithConsumer controls whether Bind starts an Asynq consumer and timeout
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

// Backend wires the Engine Core to Redis state and an Asynq task queue.
// Call Bind() after creating the engine to wire the Asynq server.
type Backend struct {
	state          *redisState
	queue          *asynqQueue
	registry       *execution.Registry
	workflowReg    *workflowRegistry
	triggerRuntime *triggerPrimitives
	rdb            *redis.Client
	timeoutMonitor *TimeoutMonitor
	redisAddr      string
	concurrency    int
	consumer       bool
	resourcePool   types.ResourcePool
	leaderElector  backend.LeaderElector
}

// State returns the StateStore implementation.
func (b *Backend) State() engine.StateStore { return b.state }

// Queue returns the TaskQueue implementation.
func (b *Backend) Queue() engine.TaskQueue { return b.queue }

// Registry returns the HandlerRegistry implementation.
func (b *Backend) Registry() engine.HandlerRegistry { return b.registry }

// WorkflowRegistry returns the workflow metadata registry.
func (b *Backend) WorkflowRegistry() backend.WorkflowRegistry { return b.workflowReg }

// TriggerPrimitives returns trigger coordination primitives.
func (b *Backend) TriggerPrimitives() backend.TriggerPrimitives { return b.triggerRuntime }

// AuditStats returns a point-in-time snapshot of audit-store dual-write
// outcomes (ok and failed counts keyed by op). See
// .claude/specs/dual-write-contract.md.
func (b *Backend) AuditStats() AuditStats { return b.state.auditCounters.snapshot() }

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

// New creates an Asynq backend connected to the given Redis address.
// db may be nil for pure-Redis mode (no MySQL persistence).
// Call Bind(eng) after creating the engine to start queue consumers.
func New(redisAddr string, db store.Store, opts ...Option) (*Backend, error) {
	cfg := &config{concurrency: 10, execTTL: defaultExecTTL, consumer: true}
	for _, o := range opts {
		o(cfg)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	state := newRedisState(rdb, db, cfg.execTTL)
	if cfg.auditObserver != nil {
		state.audit = cfg.auditObserver
	}
	state.logger = cfg.logger
	queue := newAsynqQueue(redisAddr)
	registry := execution.NewRegistry()

	leaderKey := "xflow:leader:control-plane"
	leaderElector := NewRedisLeaderElector(rdb, leaderKey, defaultLeaderLeaseTTL)

	return &Backend{
		state:          state,
		queue:          queue,
		registry:       registry,
		workflowReg:    newWorkflowRegistry(rdb),
		triggerRuntime: newTriggerPrimitives(rdb),
		rdb:            rdb,
		redisAddr:      redisAddr,
		concurrency:    cfg.concurrency,
		consumer:       cfg.consumer,
		resourcePool:   cfg.resourcePool,
		leaderElector:  leaderElector,
	}, nil
}

// Bind wires the embedded execution dispatcher into the Asynq server and
// starts the timeout monitor. Returns a stop function for graceful shutdown.
func (b *Backend) Bind(eng *engine.Engine) func() {
	if !b.consumer {
		return func() {
			_ = b.queue.Close()
			_ = b.rdb.Close()
		}
	}

	var opts []execution.RunnerOption
	if b.resourcePool != nil {
		opts = append(opts, execution.WithResourcePool(b.resourcePool))
	}
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
	return b.BindHandler(eng, dispatcher.HandleTask)
}

// BindHandler wires a custom task handler into the Asynq server and starts the
// timeout monitor. It is used by the control-plane server to dispatch tasks to
// remote runners instead of executing handlers in-process.
func (b *Backend) BindHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) func() {
	if !b.consumer {
		return func() {
			_ = b.queue.Close()
			_ = b.rdb.Close()
		}
	}

	srv := asynqlib.NewServer(
		asynqlib.RedisClientOpt{Addr: b.redisAddr},
		asynqlib.Config{Concurrency: b.concurrency},
	)
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(asynqTaskType, func(ctx context.Context, t *asynqlib.Task) error {
		task, err := unmarshalQueuedTask(t.Payload())
		if err != nil {
			return err
		}
		return asynqHandlerError(handler(ctx, task))
	})

	tm := NewTimeoutMonitor(b.rdb, eng, nil, nil, 5*time.Second)
	b.timeoutMonitor = tm

	go tm.Run()
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Printf("xflow: asynq server error: %v", err)
		}
	}()

	return func() {
		tm.Stop()
		srv.Shutdown()
		_ = b.queue.Close()
		_ = b.rdb.Close()
		if b.resourcePool != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = b.resourcePool.Close(ctx)
		}
	}
}

func asynqHandlerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, types.ErrPermanent) {
		return fmt.Errorf("%w: %w", asynqlib.SkipRetry, err)
	}
	return err
}
