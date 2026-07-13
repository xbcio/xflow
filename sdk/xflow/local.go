package xflow

import (
	backendmemory "github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/types"
)

// NewLocal creates an in-process engine backed by in-memory state and a
// goroutine worker pool.
//
// Use NewLocal for unit tests, examples, local development, and single-process
// embedded usage. It supports LocalNode and typed nodes, but all state is
// process memory: stopping the process loses executions, signals, outputs, and
// queued work. For distributed workers, durable Redis state, or API-only pods,
// use NewCluster instead.
func NewLocal(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 4, allowDirectHandlers: true}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}
	pool := resolveResourcePool(cfg)
	memOpts := []backendmemory.Option{backendmemory.WithConcurrency(cfg.concurrency)}
	if pool != nil {
		memOpts = append(memOpts, backendmemory.WithResourcePool(pool))
	}
	provider := backendmemory.New(memOpts...)
	return newFromConfig(cfg, provider)
}

// resolveResourcePool returns the SDK-managed default pool.
func resolveResourcePool(_ *engineConfig) types.ResourcePool {
	return resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
}
