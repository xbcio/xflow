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
// embedded usage. It supports LocalNode and typed nodes. State is process
// memory unless WithStore is supplied: stopping the process loses executions,
// signals, outputs, and queued work.
//
// With WithStore, execution metadata — the execution row, node status,
// signals, and the audit projection — is additionally mirrored into the
// supplied store after each accepted transition, exactly as the distributed
// backend mirrors Redis state into its SQL store. The mirror is metadata only:
// execution history survives process exit and can be listed from the store,
// but an in-flight execution does NOT resume, and no queued task is
// redelivered from it after a restart. The in-memory state remains the sole
// scheduling authority while the process runs. For durable execution use
// NewCluster instead.
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
	if err := validateNamespaceConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.executionMode == ExecutionModeTransient {
		return nil, ErrTransientRequiresCluster
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}
	pool := resolveResourcePool(cfg)
	// Keep the exact pool installed in the local provider on the owning SDK
	// Engine as well. Direct FAF dispatch uses the same process-local pool as
	// ordinary embedded execution; it creates no execution state of its own.
	cfg.resourcePool = pool
	memOpts := []backendlocal.Option{backendlocal.WithConcurrency(cfg.concurrency)}
	if pool != nil {
		memOpts = append(memOpts, backendlocal.WithResourcePool(pool))
	}
	if fn := artifactCodeResolverFor(cfg.artifactStore); fn != nil {
		memOpts = append(memOpts, backendlocal.WithArtifactCodeResolver(fn))
	}
	if cfg.metadataStore != nil {
		memOpts = append(memOpts, backendlocal.WithStore(cfg.metadataStore))
		if cfg.logger != nil {
			// The backend's logger reports failed best-effort mirror writes. It is
			// wired only when a mirror exists so the nil-store path stays
			// byte-for-byte what it was: previously NewLocal never handed the SDK
			// logger to the in-memory provider.
			memOpts = append(memOpts, backendlocal.WithQueueLogger(cfg.logger))
		}
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
