package xflow

import (
	"errors"
	"fmt"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/store"
)

// ClusterConfig holds Redis/Asynq adapter configuration for NewCluster.
//
// Common runtime settings such as concurrency, hooks, logger, and supported
// node definitions are passed with Option values.
type ClusterConfig struct {
	// RedisAddr is the Redis server address used by the Asynq queue and Redis
	// state store. It is required, for example "localhost:6379".
	RedisAddr string

	// Store is an optional durable metadata store, such as sqlstore over GORM
	// or memstore for tests. Nil means Redis-only runtime state with no SQL
	// persistence mirror.
	Store store.Store

	// DisableConsumer leaves this SDK instance as an API/control client only:
	// it can submit, inspect, cancel, and signal executions, but this process
	// will not consume Asynq tasks or run timeout monitoring. Use this for
	// API-only pods; worker pods should leave it false and register executable
	// node definitions with WithNodes.
	DisableConsumer bool
}

// NewCluster creates a distributed engine backed by Redis/Asynq and an optional
// persistent Store.
//
// Use NewCluster when executions may outlive a process, multiple instances need
// to share the queue/state, or API pods and worker pods are separated. Cluster
// workflows must use portable typed nodes (node.Define, built-ins); LocalNode
// submissions are rejected because Go function values cannot be serialized or
// executed by another process.
//
// Consumer-capable worker processes should pass xflow.WithNodes for every
// custom node type they may execute. API-only processes can set
// ClusterConfig.DisableConsumer=true to submit, signal, cancel, and inspect
// without consuming tasks.
//
// Example:
//
//	eng, err := xflow.NewCluster(xflow.ClusterConfig{RedisAddr: "localhost:6379"})
//	eng, err := xflow.NewCluster(xflow.ClusterConfig{RedisAddr: addr, Store: sqlstore.New(db)}, xflow.WithConcurrency(16))
func NewCluster(clusterCfg ClusterConfig, opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 10}
	for _, o := range opts {
		o(cfg)
	}
	if err := validateExecutionModeConfig(cfg); err != nil {
		return nil, err
	}
	if clusterCfg.RedisAddr == "" {
		return nil, errors.New("xflow.NewCluster: RedisAddr is required")
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 10
	}

	asynqOpts := []distributed.Option{
		distributed.WithConcurrency(cfg.concurrency),
		distributed.WithConsumer(!clusterCfg.DisableConsumer),
	}
	if cfg.executionMode == ExecutionModeTransient {
		asynqOpts = append(asynqOpts, distributed.WithTransientMode(cfg.transientTTL, cfg.transientCompletionTTL))
	}
	if pool := resolveResourcePool(cfg); pool != nil && !clusterCfg.DisableConsumer {
		// Only worker pods need a pool; API-only pods don't dispatch handlers.
		asynqOpts = append(asynqOpts, distributed.WithResourcePool(pool))
	}
	a, err := distributed.New(clusterCfg.RedisAddr, clusterCfg.Store, asynqOpts...)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	return newFromConfig(cfg, a)
}
