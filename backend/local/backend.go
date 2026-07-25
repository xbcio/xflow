package local

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// Option configures the memory backend.
type Option func(*config)

type config struct {
	concurrency  int
	resourcePool types.ResourcePool
}

// WithConcurrency sets the number of in-memory queue consumer goroutines. Default is 4.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithResourcePool installs a process-scope ResourcePool used by
// DatabaseNode / GRPCNode to reuse *sql.DB / *grpc.ClientConn across
// invocations. Default is nil: resource-aware nodes (DatabaseNode/GRPCNode)
// error at runtime when invoked without a pool — production deployments
// should always inject a pool.
// See .claude/specs/resource-pool.md.
func WithResourcePool(p types.ResourcePool) Option {
	return func(c *config) { c.resourcePool = p }
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
}

// New creates a memory backend with its components but does NOT start the queue.
// Call Bind(eng) to wire the embedded execution dispatcher and start queue consumers.
func New(opts ...Option) *Backend {
	cfg := &config{concurrency: 4}
	for _, o := range opts {
		o(cfg)
	}

	return &Backend{
		state:            newMemoryState(),
		queue:            newMemoryQueue(cfg.concurrency),
		registry:         execution.NewRegistry(),
		workflowRegistry: newWorkflowRegistry(),
		triggerRuntime:   newTriggerPrimitives(),
		resourcePool:     cfg.resourcePool,
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
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
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

	return func() {
		cancelOutbox()
		<-outboxDone
		b.queue.Stop()
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
	result := types.Result{ExecutionID: id, Status: snap.Status}
	if snap.Status == types.ExecutionStatusSuccess {
		result.Output = b.state.GetAllOutputs(id)
	}
	return result, nil
}

// WaitTimeout is a convenience wrapper that applies a deadline to WaitDone.
func (b *Backend) WaitTimeout(id types.ExecutionID, timeout time.Duration) (types.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return b.WaitDone(ctx, id)
}
