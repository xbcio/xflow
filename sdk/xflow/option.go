package xflow

import (
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/store"
)

// ClusterConfig holds cluster-adapter-specific configuration.
// Common settings (concurrency, hooks, logger) are passed via Option.
type ClusterConfig struct {
	RedisAddr string // Required.
	// Store is any persistence implementation of store.Store
	// (e.g. sqlstore over GORM, or memstore for tests). Optional; nil =
	// pure Redis mode with no durable persistence.
	Store store.Store
	// DisableConsumer leaves this SDK instance as an API/control client only:
	// it can submit, inspect, cancel, and signal executions, but it will not
	// consume Asynq tasks or run timeout monitoring in this process.
	DisableConsumer bool
}

// Option configures an Engine.
// Common options are valid for New(), NewLocal(), and NewCluster().
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

// WithConcurrency sets the local queue consumer pool size.
// Default is 4 for NewLocal, 10 for NewCluster.
func WithConcurrency(n int) Option {
	return func(c *engineConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithHooks sets the lifecycle hook receiver.
func WithHooks(h engine.Hooks) Option {
	return func(c *engineConfig) { c.hooks = h }
}

// WithLogger sets the logger.
func WithLogger(l engine.Logger) Option {
	return func(c *engineConfig) { c.logger = l }
}

// WithNodes declares custom node definitions this process can execute. Use it
// on consumer-capable cluster processes so workers can resolve node types even
// before they submit any workflow themselves.
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

// WithExecutionTTL overrides the default execution TTL for this submission.
func WithExecutionTTL(d time.Duration) SubmitOption {
	return func(c *submitConfig) {
		if d > 0 {
			c.execTTL = d
		}
	}
}
