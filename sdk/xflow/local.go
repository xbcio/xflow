package xflow

import (
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
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
//
// NewLocal only supports ExecutionModeDefault. Transient execution mode is a
// cluster/Redis-only concept: it needs Redis TTLs and Asynq's fire-and-forget
// dispatch, which the in-memory backend cannot provide. Requesting
// ExecutionModeTransient from NewLocal returns ErrTransientRequiresCluster; use
// NewCluster instead.
func NewLocal(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 4, allowDirectHandlers: true}
	for _, o := range opts {
		o(cfg)
	}
	if err := validateExecutionModeConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.executionMode == ExecutionModeTransient {
		return nil, ErrTransientRequiresCluster
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}
	pool := resolveResourcePool(cfg)
	memOpts := []backendlocal.Option{backendlocal.WithConcurrency(cfg.concurrency)}
	if pool != nil {
		memOpts = append(memOpts, backendlocal.WithResourcePool(pool))
	}
	provider := backendlocal.New(memOpts...)
	return newFromConfig(cfg, provider)
}

// resolveResourcePool returns the configured pool, the default pool built from
// resourcePoolConfig, or nil when the caller explicitly opted out via
// WithResourcePool(nil).
//
// Shared by NewLocal and NewCluster: both modes default to a connection pool
// for DatabaseNode/GRPCNode unless the caller overrides it.
func resolveResourcePool(cfg *engineConfig) types.ResourcePool {
	if cfg.resourcePoolSet {
		return cfg.resourcePool // may be nil — explicit opt-out
	}
	poolCfg := types.DefaultResourcePoolConfig()
	if cfg.resourcePoolConfig != nil {
		poolCfg = *cfg.resourcePoolConfig
	}
	return resource.NewDefaultResourcePool(poolCfg)
}
