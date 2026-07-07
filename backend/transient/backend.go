package transient

import (
	"context"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// Option configures the transient backend.
type Option func(*config)

type config struct {
	concurrency   int
	completionTTL time.Duration
	resourcePool  types.ResourcePool
}

// WithConcurrency sets the transient worker count. Default is 4.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithCompletionTTL sets how long completed executions remain readable.
func WithCompletionTTL(ttl time.Duration) Option {
	return func(c *config) {
		if ttl > 0 {
			c.completionTTL = ttl
		}
	}
}

// WithResourcePool installs a shared types.ResourcePool for embedded execution.
func WithResourcePool(p types.ResourcePool) Option {
	return func(c *config) { c.resourcePool = p }
}

// Backend bundles transient state, queue, registry, and lifecycle binding.
type Backend struct {
	state            *state
	queue            *queue
	registry         *execution.Registry
	workflowRegistry *workflowRegistry
	triggerRuntime   *triggerPrimitives
	resourcePool     types.ResourcePool
}

// New creates a local transient backend.
func New(opts ...Option) *Backend {
	cfg := &config{concurrency: 4, completionTTL: 30 * time.Second}
	for _, o := range opts {
		o(cfg)
	}
	st := newState(cfg.completionTTL)

	return &Backend{
		state:            st,
		queue:            newQueue(cfg.concurrency, st),
		registry:         execution.NewRegistry(),
		workflowRegistry: newWorkflowRegistry(),
		triggerRuntime:   newTriggerPrimitives(),
		resourcePool:     cfg.resourcePool,
	}
}

func (b *Backend) State() engine.StateStore { return b.state }

func (b *Backend) Queue() engine.TaskQueue { return b.queue }

func (b *Backend) Registry() engine.HandlerRegistry { return b.registry }

func (b *Backend) WorkflowRegistry() backend.WorkflowRegistry { return b.workflowRegistry }

func (b *Backend) TriggerPrimitives() backend.TriggerPrimitives { return b.triggerRuntime }

func (b *Backend) Bind(eng *engine.Engine) func() {
	var opts []execution.RunnerOption
	if b.resourcePool != nil {
		opts = append(opts, execution.WithResourcePool(b.resourcePool))
	}
	dispatcher := execution.NewEmbeddedDispatcher(eng, b.registry, opts...)
	b.queue.SetHandler(dispatcher.HandleTask)
	b.queue.Start()
	return func() {
		b.queue.Stop()
		if b.resourcePool != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = b.resourcePool.Close(ctx)
		}
	}
}

func (b *Backend) WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	return b.state.waitDone(ctx, id)
}
