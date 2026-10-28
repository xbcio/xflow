package local

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Option configures the memory backend.
type Option func(*config)

type config struct {
	concurrency             int
	queueCapacity           int
	resourcePool            types.ResourcePool
	artifactCode            func(ctx context.Context, digest string) ([]byte, error)
	registry                *execution.Registry
	logger                  engine.Logger
	store                   store.Store
	tracer                  tracing.Tracer
	deadlineExpiredObserver engine.DeadlineExpiredObserver
}

// WithConcurrency sets the number of in-memory queue consumer goroutines. Default is 4.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithQueueCapacity sets the buffered capacity of each in-memory queue lane.
// The default remains 1024 for general-purpose local backends. Short-lived
// embedded backends may use a smaller value to avoid paying that allocation on
// every execution; durable outbox delivery supplies backpressure when a lane
// fills.
func WithQueueCapacity(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueCapacity = n
		}
	}
}

// WithQueueLogger installs the logger the in-memory queue uses to report tasks
// it drops: one that failed with a permanent error, and one that exhausted its
// transient retries. Both paths end in a bare return, so without a logger a
// task disappears with no record anywhere that it existed.
//
// It is an option rather than a default because the queue sits below the
// logging layer — engine.Logger is an interface the caller supplies. Callers
// that already hold one (service/apiserver's Config.Logger, which the
// distributed backend beside it has always received) should pass it; the
// no-logger behaviour is unchanged for those that do not.
//
// The same logger reports a failed metadata-mirror write (see WithStore).
func WithQueueLogger(l engine.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithStore installs an optional durable metadata mirror. The memory state
// remains the sole scheduling authority and the only source for the engine's
// reads; every accepted transition is additionally projected into s, exactly
// as the Redis-backed distributed store projects into its SQL store
// (rstate/doc.go).
//
// The mirror is metadata only. Execution history, node status, signals, and
// the resulting audit rows survive a process exit and can be listed from the
// store, but in-flight work does not: a restarted local engine starts with
// empty in-memory state and will not resume, redeliver, or repair an execution
// it finds only in the store. Use the distributed backend for durable
// execution.
//
// Error semantics mirror the distributed backend:
//   - CreateExecution is required. If the store cannot persist the execution
//     row, the in-memory execution (and any initial intents / concurrency
//     slot it took) is rolled back and the create fails. Initial intents are
//     held until the row lands, so nothing is dispatched for an execution the
//     store refused.
//   - Every later projection is best-effort. A failed status, node, signal, or
//     seed-row write is logged and dropped; the in-memory transition has
//     already committed and is not rolled back.
//
// Projections for one execution are serialized and read the current in-memory
// state inside the serialized section, so a completion that races the required
// insert still converges on the terminal status rather than leaving the row
// running. While that insert is in flight, ordinary status/node/signal
// projections for the same execution are dropped (the insert reconciles live
// state afterwards), promotions keep the execution's intents held, and seeds
// hold theirs, so nothing can run against a row the store may still refuse.
// Mirror writes are bounded to two seconds and never block scheduling on a
// contended database.
//
// Per-workflow transient executions (WorkflowOptions.Transient or a
// per-execution transient hint) are never projected: no execution, node, or
// signal rows are written for them, matching the distributed backend's
// transient promise.
//
// A deterministic execution ID (engine.IdempotentExecutionID, entry seeds)
// whose row already exists from a previous process cannot be created again: a
// required create returns an error wrapping ErrStoreExecutionExists and rolls
// back, while a seed logs the collision at warn level and disables mirroring
// for that execution so the old run's rows are not overwritten. In-memory
// idempotency maps do not survive a restart, so callers reusing IDs across
// processes must treat that error as "already ran".
//
// nil (the default) leaves the backend purely in-memory, byte-for-byte as
// before this option existed.
func WithStore(s store.Store) Option {
	return func(c *config) { c.store = s }
}

// WithResourcePool installs a process-scope ResourcePool used by
// DatabaseNode / GRPCNode to reuse *sql.DB / *grpc.ClientConn across
// invocations. Default is nil: resource-aware nodes (DatabaseNode/GRPCNode)
// error at runtime when invoked without a pool — production deployments
// should always inject a pool.
func WithResourcePool(p types.ResourcePool) Option {
	return func(c *config) { c.resourcePool = p }
}

// WithArtifactCodeResolver installs a script-artifact resolver for the embedded
// dispatcher. ScriptNode calls Input.ArtifactCode(ctx, digest) at Execute time.
func WithArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) Option {
	return func(c *config) { c.artifactCode = fn }
}

// WithTracer installs the tracer the embedded execution dispatcher uses for
// xflow.local.task spans. Nil (the default) records nothing. This is the
// embedded-engine counterpart of service/runner's Config.Tracer: an in-process
// backend has no runner process, so without it a local execution is invisible
// to a trace backend even when the host installed a provider.
func WithTracer(t tracing.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithDeadlineExpiredObserver installs an observer for executions the embedded
// deadline sweeper drives into timeout. It is optional and must not block the
// sweep.
//
// Without one the sweeper still expires overdue executions; it just reports
// nothing, which leaves xflow_execution_deadline_expired_total{source="embedded"}
// at zero for an in-process deployment that has no control plane to report the
// transition.
//
// The report is at-least-once, not exactly-once. The timeout write is fenced
// and read back rather than assumed, so a concurrent expirer that won the same
// transition can still leave the sweep answering Expired, and the sweep reports
// the transition it observed either way. This backend's state is process-local,
// so a NewLocal deployment has a single reporter; where several sweepers drive
// one shared store, each can report the same expiry, and there the count is an
// upper bound on the expirations behind it rather than one report per expiry.
func WithDeadlineExpiredObserver(obs engine.DeadlineExpiredObserver) Option {
	return func(c *config) {
		if obs != nil {
			c.deadlineExpiredObserver = obs
		}
	}
}

// WithRegistry injects an external handler registry. Use this when the inner
// engine must resolve handlers registered outside its own backend (e.g. group
// runtime injecting the outer runner's member handlers).
func WithRegistry(reg *execution.Registry) Option {
	return func(c *config) { c.registry = reg }
}

// Backend bundles in-memory state, queue, registry, and lifecycle binding.
// Call Bind() after creating the engine to wire the queue handler.
type Backend struct {
	state            *memoryState
	queue            *memoryQueue
	registry         *execution.Registry
	workflowRegistry *workflowRegistry
	triggerRuntime   *triggerPrimitives
	resourcePool     types.ResourcePool
	artifactCode     func(ctx context.Context, digest string) ([]byte, error)
	logger           engine.Logger
	tracer           tracing.Tracer
	// deadlineExpiredObserver reports the executions the embedded deadline
	// sweeper drives into timeout. Nil reports nothing.
	deadlineExpiredObserver engine.DeadlineExpiredObserver
}

// New creates a memory backend with its components but does NOT start the queue.
// Call Bind(eng) to wire the embedded execution dispatcher and start queue consumers.
func New(opts ...Option) *Backend {
	cfg := &config{concurrency: 4, queueCapacity: defaultMemoryQueueCapacity}
	for _, o := range opts {
		o(cfg)
	}

	reg := cfg.registry
	if reg == nil {
		reg = execution.NewRegistry()
	}

	q := newMemoryQueueWithCapacity(cfg.concurrency, cfg.queueCapacity)
	q.SetLogger(cfg.logger)

	state := newMemoryState()
	state.db = cfg.store
	state.logger = cfg.logger

	return &Backend{
		state:            state,
		queue:            q,
		registry:         reg,
		workflowRegistry: newWorkflowRegistry(),
		triggerRuntime:   newTriggerPrimitives(state),
		resourcePool:     cfg.resourcePool,
		artifactCode:     cfg.artifactCode,
		logger:           cfg.logger,
		tracer:           cfg.tracer,

		deadlineExpiredObserver: cfg.deadlineExpiredObserver,
	}
}

// State returns the StateStore implementation.
func (b *Backend) State() engine.StateStore { return b.state }

// Queue returns the TaskQueue implementation.
func (b *Backend) Queue() engine.TaskQueue { return b.queue }

// Registry returns the handler registry.
func (b *Backend) Registry() engine.HandlerRegistry { return b.registry }

// WorkflowRegistry returns the workflow metadata registry.
func (b *Backend) WorkflowRegistry() backend.WorkflowRegistry { return b.workflowRegistry }

// TriggerPrimitives returns local trigger coordination primitives.
func (b *Backend) TriggerPrimitives() backend.TriggerPrimitives { return b.triggerRuntime }

// Bind wires the embedded execution dispatcher into the queue and starts queue consumers.
// Returns a stop function that drains the consumer pool. The stop function
// also closes the resource pool when one was provided.
func (b *Backend) Bind(eng *engine.Engine) func() {
	var opts []execution.RunnerOption
	if b.resourcePool != nil {
		opts = append(opts, execution.WithResourcePool(b.resourcePool))
	}
	if b.artifactCode != nil {
		opts = append(opts, execution.WithArtifactCodeResolver(b.artifactCode))
	}
	if b.tracer != nil {
		opts = append(opts, execution.WithTracer(b.tracer))
	}
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
	// The queue worker's ctx starts from context.Background() and is NOT
	// re-anchored to the engine's namespace here. Anchoring it looks right
	// for a single-namespace engine but is not: dispatch-time sub-workflow
	// resolution keys the registry by the registration namespace
	// (WorkflowDef.Namespace, default "default"), while an anchored ctx names
	// the deployment — so the lookup would miss every child registered
	// without an explicit namespace, and concurrency admission keys would
	// split between children (queue ctx) and top-level executions (caller
	// ctx). Dispatch now resolves sub-workflows from the registration
	// namespace persisted on the execution snapshot
	// (engine.ExecutionSnapshot.Namespace), which is what makes the ctx
	// unnecessary on that path; a single-namespace engine's lease stamp
	// (BuildTaskLease, see WithNamespace) remains the carrier for the
	// handler's execution context.
	queueStop := b.BindHandlerWithEngine(eng, dispatcher.HandleTask)
	stop := func() {
		queueStop()
	}
	if b.resourcePool != nil {
		return func() {
			stop()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = b.resourcePool.Close(ctx)
		}
	}
	return stop
}

// BindHandler wires a custom queue handler and starts queue consumers.
// Without an Engine, it cannot start the durable scheduling outbox dispatcher;
// callers that have an Engine should use BindHandlerWithEngine instead.
func (b *Backend) BindHandler(handler func(context.Context, *engine.Task) error) func() {
	b.queue.SetHandler(handler)
	b.queue.Start()
	return func() { b.queue.Stop() }
}

// BindHandlerWithEngine wires a custom queue handler and starts both queue
// consumers and the Engine's durable scheduling outbox dispatcher. It is used
// by process-level dispatchers that route tasks somewhere other than the
// embedded handler registry.
func (b *Backend) BindHandlerWithEngine(eng *engine.Engine, handler func(context.Context, *engine.Task) error) func() {
	if eng == nil {
		return b.BindHandler(handler)
	}
	return b.bindHandlerWithOutbox(eng, handler, time.Second)
}

// BindTaskHandler implements backend.TaskHandlerBinder. It is the control-plane
// binding path: the caller-supplied handler (the control-plane dispatcher)
// replaces the embedded execution dispatcher, and the durable outbox
// dispatcher is started so that scheduling intents survive queue handoff
// failures. A nil engine is a configuration error — without it the outbox
// dispatcher cannot run, so we fail closed instead of silently degrading to the
// no-outbox BindHandler path.
func (b *Backend) BindTaskHandler(eng *engine.Engine, handler func(context.Context, *engine.Task) error) (func(), error) {
	if eng == nil {
		return nil, errors.New("local: BindTaskHandler requires a non-nil engine")
	}
	if handler == nil {
		return nil, errors.New("local: BindTaskHandler requires a non-nil handler")
	}
	stop := b.bindHandlerWithOutbox(eng, handler, time.Second)
	// Wrap stop to also close the resourcePool (matching Bind lifecycle).
	if b.resourcePool != nil {
		return func() {
			stop()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = b.resourcePool.Close(ctx)
		}, nil
	}
	return stop, nil
}

func (b *Backend) bindHandlerWithOutbox(eng *engine.Engine, handler func(context.Context, *engine.Task) error, interval time.Duration) func() {
	b.queue.SetHandler(handler)
	b.queue.Start()

	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	outboxDone := make(chan struct{})
	go func() {
		defer close(outboxDone)
		engine.NewOutboxDispatcher(eng, interval).Run(outboxCtx)
	}()

	queueTimeoutDone := make(chan struct{})
	go func() {
		defer close(queueTimeoutDone)
		b.runQueueTimeoutSweeper(outboxCtx, interval)
	}()

	deadlineDone := make(chan struct{})
	go func() {
		defer close(deadlineDone)
		b.runDeadlineSweeper(outboxCtx, eng, interval)
	}()

	return func() {
		cancelOutbox()
		<-outboxDone
		<-queueTimeoutDone
		<-deadlineDone
		b.queue.Stop()
	}
}

// queueTimeoutSweepInterval is how often queue_timeout is enforced when the
// caller does not say otherwise. One second, matching the outbox dispatcher's
// default tick: a coarser sweep would stretch a short queue_timeout into a wait
// nobody could explain from the definition.
const queueTimeoutSweepInterval = time.Second

// runQueueTimeoutSweeper drives the store's queue-timeout expiry on a clock.
//
// Queue timeouts are enforced by whichever background loop the deployment owns.
// Cluster mode has the control plane's concurrency maintenance; the in-process
// backend has no control plane, so without this loop queue_timeout would be
// enforced in one deployment and silently ignored in the other — and a
// submission waiting forever for a slot is not a failure any caller can
// distinguish from a busy workflow.
//
// It holds no state: the deadlines live in the store, and every tick asks the
// store to advance the ones that passed.
func (b *Backend) runQueueTimeoutSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = queueTimeoutSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := b.state.ExpireQueuedExecutions(ctx, 0); err != nil && b.logger != nil {
				b.logger.Error("concurrency_queue_timeout_sweep_failed", "err", err)
			}
		}
	}
}

// deadlineSweepInterval is how often executions past their settings.timeout
// deadline are expired when the caller does not say otherwise. One second,
// matching the queue-timeout sweep: an execution can overrun its deadline by up
// to one tick.
const deadlineSweepInterval = time.Second

// deadlineSweepBatch bounds how many overdue executions one tick expires. The
// rest are still listed on the next tick, so a burst of simultaneous deadlines
// drains over a few ticks instead of holding one tick open.
const deadlineSweepBatch = 100

// runDeadlineSweeper expires the executions whose settings.timeout deadline has
// passed with no task left to notice it.
//
// A running node enforces the deadline through its own lease; an execution that
// is parked (suspended, paused, queued) or between tasks has no lease, so
// without this loop it would outlive its deadline indefinitely in-process.
// Unlike the queue-timeout sweep this one needs the engine, because expiring an
// execution is not only a status write: ExpireExecution also cascades the
// cancellation to what the execution still had suspended or running, and
// acknowledges the index entry only once that finished.
//
// The deadlines live in the store and every tick asks it which ones have
// passed. The loop body is engine.DeadlineSweep, which sets an execution that
// fails (or is canceling) aside for a growing window instead of retrying it every
// tick: the index lists the oldest deadlines first and keeps a failing entry, so
// a page of entries that never succeed would otherwise hold every slot and starve
// the later ones, logging one error per entry per second. Its warnings come at
// 5, 20 and 100 consecutive failures of one execution instead.
func (b *Backend) runDeadlineSweeper(ctx context.Context, expirer engine.ExecutionExpirer, interval time.Duration) {
	if interval <= 0 {
		interval = deadlineSweepInterval
	}
	var logger engine.DeadlineSweepLogger
	if b.logger != nil {
		logger = b.logger
	}
	sweepCfg := engine.DeadlineSweepConfig{
		Lister:  b.state,
		Expirer: expirer,
		Batch:   deadlineSweepBatch,
		Logger:  logger,
	}
	if b.deadlineExpiredObserver != nil {
		observer := b.deadlineExpiredObserver
		sweepCfg.OnExpired = func(execCtx context.Context, _ engine.DeadlineExpiredExecution) {
			// The observer is host-supplied: a panic here would otherwise leave
			// this goroutine and take every later deadline with it.
			defer func() { _ = recover() }()
			observer.OnExecutionDeadlineExpired(execCtx, engine.DeadlineExpiredSourceEmbedded)
		}
	}
	sweep := engine.NewDeadlineSweep(sweepCfg)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := sweep.Sweep(ctx); err != nil && b.logger != nil {
				b.logger.Error("execution_deadline_sweep_failed", "err", err)
			}
		}
	}
}

// WaitDone blocks until the execution reaches a terminal state or ctx is canceled.
// Implements the backend.Waiter interface.
func (b *Backend) WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	doneCh := b.state.waitDone(id)
	select {
	case <-ctx.Done():
		return types.Result{}, ctx.Err()
	case <-doneCh:
	}

	snap, err := b.state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return types.Result{ExecutionID: id, Status: types.ExecutionStatusFailed}, err
	}
	// Error travels alongside Status: this fast path bypasses Inspect (it reads
	// the snapshot directly), so without this the reason would be readable via
	// the SDK's polling fallback but not via the local backend's own Waiter.
	result := types.Result{ExecutionID: id, Status: snap.Status, Error: snap.Error}
	if snap.Status == types.ExecutionStatusSuccess {
		result.Output = b.state.GetAllOutputs(id)
		// A stored output also carries the node's private state, because that is
		// the map a resumption reads it back from -- so the store keeps it raw
		// and every reader strips it. This fast path builds the result without
		// Inspect, which means the engine never gets to strip for us, and a wait
		// that returned the slot would answer differently here than in cluster
		// mode for the same workflow.
		for name, out := range result.Output {
			if m, ok := out.(map[string]any); ok {
				result.Output[name] = engine.StripNodeState(m)
			}
		}
	}
	return result, nil
}

// WaitTimeout is a convenience wrapper that applies a deadline to WaitDone.
func (b *Backend) WaitTimeout(id types.ExecutionID, timeout time.Duration) (types.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return b.WaitDone(ctx, id)
}
