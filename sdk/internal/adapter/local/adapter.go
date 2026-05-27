package local

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Option configures the local adapter.
type Option func(*config)

type config struct {
	concurrency int
}

// WithConcurrency sets the number of worker goroutines. Default is 4.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// Adapter bundles local-mode components: in-memory state, queue, and registry.
// Call Bind() after creating the engine to wire the queue handler.
type Adapter struct {
	state    *memoryState
	queue    *memoryQueue
	registry *LocalRegistry
}

// New creates a local adapter with its components but does NOT start the queue.
// Call Bind(eng) to wire the engine's ExecuteNode and start workers.
func New(opts ...Option) *Adapter {
	cfg := &config{concurrency: 4}
	for _, o := range opts {
		o(cfg)
	}

	return &Adapter{
		state:    newMemoryState(),
		queue:    newMemoryQueue(cfg.concurrency),
		registry: NewLocalRegistry(),
	}
}

// State returns the StateBackend implementation.
func (a *Adapter) State() engine.StateBackend { return a.state }

// Queue returns the TaskQueue implementation.
func (a *Adapter) Queue() engine.TaskQueue { return a.queue }

// Registry returns the handler registry.
func (a *Adapter) Registry() *LocalRegistry { return a.registry }

// Bind wires the engine's ExecuteNode into the queue and starts workers.
// Returns a stop function that drains the worker pool.
func (a *Adapter) Bind(eng *engine.Engine) func() {
	a.queue.SetHandler(eng.ExecuteNode)
	a.queue.Start()
	return func() { a.queue.Stop() }
}

// WaitDone blocks until the execution reaches a terminal state or ctx is canceled.
// Implements the xflow.Waiter interface.
func (a *Adapter) WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	doneCh := a.state.waitDone(id)
	select {
	case <-ctx.Done():
		return types.Result{}, ctx.Err()
	case <-doneCh:
	}

	snap, err := a.state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return types.Result{ExecutionID: id, Status: types.StatusFailed}, err
	}
	result := types.Result{ExecutionID: id, Status: snap.Status}
	if snap.Status == types.StatusSuccess {
		result.Output = a.state.GetAllOutputs(id)
	}
	return result, nil
}

// WaitTimeout is a convenience wrapper that applies a deadline to WaitDone.
func (a *Adapter) WaitTimeout(id types.ExecutionID, timeout time.Duration) (types.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.WaitDone(ctx, id)
}
