package xflow

import (
	"time"

	"github.com/xbcio/xflow/engine"
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
}

// Option configures an Engine.
// Common options are valid for New(), NewLocal(), and NewCluster().
type Option func(*engineConfig)

// Backend provides all components needed to run the engine.
// Both local.Adapter and cluster.Adapter satisfy this interface.
type Backend interface {
	State() engine.StateBackend
	Queue() engine.TaskQueue
	Registry() engine.HandlerRegistry
	Bind(eng *engine.Engine) func()
}

type engineConfig struct {
	state       engine.StateBackend
	queue       engine.TaskQueue
	registry    engine.HandlerRegistry
	hooks       engine.Hooks
	logger      engine.Logger
	waiter      Waiter
	backend     Backend
	concurrency int
	stopFns     []func()
}

// WithBackend sets the Backend that provides State, Queue, Registry, and lifecycle binding.
// For standard setups, use NewLocal or NewCluster instead.
func WithBackend(b Backend) Option {
	return func(c *engineConfig) { c.backend = b }
}

// WithState sets the StateBackend implementation directly.
func WithState(s engine.StateBackend) Option {
	return func(c *engineConfig) { c.state = s }
}

// WithQueue sets the TaskQueue implementation directly.
func WithQueue(q engine.TaskQueue) Option {
	return func(c *engineConfig) { c.queue = q }
}

// WithRegistry sets the handler registry.
func WithRegistry(r engine.HandlerRegistry) Option {
	return func(c *engineConfig) { c.registry = r }
}

// WithConcurrency sets the worker pool size.
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

// WithWaiter sets a Waiter for channel-based blocking.
func WithWaiter(w Waiter) Option {
	return func(c *engineConfig) { c.waiter = w }
}

// WithStopFunc registers a cleanup function called by Engine.Stop().
func WithStopFunc(fn func()) Option {
	return func(c *engineConfig) { c.stopFns = append(c.stopFns, fn) }
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
