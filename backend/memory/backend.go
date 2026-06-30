package memory

import (
	"context"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// Option configures the memory backend.
type Option func(*config)

type config struct {
	concurrency int
}

// WithConcurrency sets the number of in-memory queue consumer goroutines. Default is 4.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// Backend bundles in-memory state, queue, registry, and lifecycle binding.
// Call Bind() after creating the engine to wire the queue handler.
type Backend struct {
	state            *memoryState
	queue            *memoryQueue
	registry         *execution.Registry
	workflowRegistry *workflowRegistry
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

// Bind wires the embedded execution dispatcher into the queue and starts queue consumers.
// Returns a stop function that drains the consumer pool.
func (b *Backend) Bind(eng *engine.Engine) func() {
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry)
	return b.BindHandler(dispatcher.HandleTask)
}

// BindHandler wires a custom queue handler and starts queue consumers.
func (b *Backend) BindHandler(handler func(context.Context, *engine.Task) error) func() {
	b.queue.SetHandler(handler)
	b.queue.Start()
	return func() { b.queue.Stop() }
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
