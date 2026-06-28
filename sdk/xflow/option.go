package xflow

import (
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/nodes/node"
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

// Option configures an Engine.
// Common options are valid for NewLocal and NewCluster.
type Option func(*engineConfig)

type engineConfig struct {
	state       engine.StateStore
	queue       engine.TaskQueue
	registry    engine.HandlerRegistry
	hooks       engine.Hooks
	logger      engine.Logger
	waiter      backend.Waiter
	concurrency int
	nodes       []*node.Definition
	stopFns     []func()

	allowDirectHandlers bool
}

// WithConcurrency sets the task consumer concurrency.
//
// For NewLocal this is the in-memory goroutine worker count. For NewCluster it
// is the Asynq consumer concurrency when DisableConsumer is false. Defaults are
// 4 for NewLocal and 10 for NewCluster. Non-positive values are ignored.
func WithConcurrency(n int) Option {
	return func(c *engineConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithHooks sets the lifecycle hook receiver.
//
// Hooks are best used for lightweight observation, metrics, and test
// synchronization. Hook implementations must be non-blocking; slow side effects
// should be handed off to another goroutine or queue.
func WithHooks(h engine.Hooks) Option {
	return func(c *engineConfig) { c.hooks = h }
}

// WithLogger sets the logger used by engine internals.
//
// The logger interface is intentionally minimal. It is currently used for
// engine-level diagnostics such as recovered hook panics.
func WithLogger(l engine.Logger) Option {
	return func(c *engineConfig) { c.logger = l }
}

// WithNodes declares custom node definitions this process can execute.
//
// Submit automatically registers typed handlers that appear in the submitted
// workflow in the current process. WithNodes is still required for cluster
// worker processes that may execute workflows submitted elsewhere, because
// workers need to resolve node types before seeing the workflow builder.
func WithNodes(defs ...*node.Definition) Option {
	return func(c *engineConfig) {
		c.nodes = append(c.nodes, defs...)
	}
}

// SubmitOption configures a single workflow submission.
type SubmitOption func(*submitConfig)

type submitConfig struct {
	execTTL time.Duration
}

// WithExecutionTTL overrides the backend execution TTL for this submission.
//
// It is mostly relevant for Redis-backed cluster mode. The TTL is extended
// while nodes are suspended, but it should still be set longer than the
// expected workflow lifetime for long-running approval processes.
func WithExecutionTTL(d time.Duration) SubmitOption {
	return func(c *submitConfig) {
		if d > 0 {
			c.execTTL = d
		}
	}
}
