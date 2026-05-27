package xflow

import (
	"time"

	"github.com/xbcio/xflow/engine"
)

// Option configures an Engine created via New().
type Option func(*engineConfig)

type engineConfig struct {
	state       engine.StateBackend
	queue       engine.TaskQueue
	registry    engine.HandlerRegistry
	hooks       engine.Hooks
	logger      engine.Logger
	waiter      Waiter
	concurrency int
	stopFns     []func()
}

// WithState sets the StateBackend implementation (required).
func WithState(s engine.StateBackend) Option {
	return func(c *engineConfig) { c.state = s }
}

// WithQueue sets the TaskQueue implementation (required).
func WithQueue(q engine.TaskQueue) Option {
	return func(c *engineConfig) { c.queue = q }
}

// WithRegistry sets the handler registry.
func WithRegistry(r engine.HandlerRegistry) Option {
	return func(c *engineConfig) { c.registry = r }
}

// WithHooks sets the lifecycle hook receiver.
func WithHooks(h engine.Hooks) Option {
	return func(c *engineConfig) { c.hooks = h }
}

// WithLogger sets the logger.
func WithLogger(l engine.Logger) Option {
	return func(c *engineConfig) { c.logger = l }
}

// WithWaiter sets a Waiter for channel-based blocking (local mode).
// If not set, Wait() falls back to polling StateBackend.
func WithWaiter(w Waiter) Option {
	return func(c *engineConfig) { c.waiter = w }
}

// WithConcurrency is a hint for convenience constructors. It has no effect
// when using New() directly — configure concurrency on your queue implementation.
func WithConcurrency(n int) Option {
	return func(c *engineConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithStopFunc registers a cleanup function called by Engine.Stop().
// Multiple calls append; functions are called in LIFO order.
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
