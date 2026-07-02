package xflow

import (
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
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
	nodes       []node.Handler
	stopFns     []func()

	allowDirectHandlers bool

	versionPolicy    execution.VersionPolicy
	versionPolicySet bool

	resourcePool       node.ResourcePool
	resourcePoolSet    bool
	resourcePoolConfig *node.ResourcePoolConfig
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
// AddWorkflow automatically registers typed handlers that appear in the
// workflow in the current process. WithNodes is still required for cluster
// worker processes that may execute workflows registered elsewhere, because
// workers need to resolve node types before seeing the workflow builder.
func WithNodes(defs ...node.Handler) Option {
	return func(c *engineConfig) {
		c.nodes = append(c.nodes, defs...)
	}
}

// WithResourcePool installs a custom node.ResourcePool. By default the SDK
// creates a default pool (node.NewDefaultResourcePool) so DatabaseNode and
// GRPCNode reuse connections automatically. Pass nil to disable pooling
// entirely; nodes will then fall back to per-call construction.
//
// See .claude/specs/resource-pool.md.
func WithResourcePool(p node.ResourcePool) Option {
	return func(c *engineConfig) {
		c.resourcePool = p
		c.resourcePoolSet = true
	}
}

// WithResourcePoolConfig customizes the default pool's SQL / gRPC tunables.
// Ignored when WithResourcePool is also supplied.
func WithResourcePoolConfig(cfg node.ResourcePoolConfig) Option {
	return func(c *engineConfig) {
		c.resourcePoolConfig = &cfg
	}
}

// WithVersionPolicy controls how the embedded handler registry responds when
// a workflow pins a specific handler version that is not registered.
//
//   - execution.VersionWarnFallback (default): warn and fall back to the latest
//     registered handler; safe rollout, surfaces the drift in logs.
//   - execution.VersionStrict: return an error; recommended once handlers and
//     workflows are version-aligned.
//   - execution.VersionSilentFallback: legacy behavior — silent fallback to
//     the latest handler. Avoid in production.
//
// AddWorkflow always pre-checks versions strictly regardless of this option;
// the policy only affects runtime resolution.
func WithVersionPolicy(p execution.VersionPolicy) Option {
	return func(c *engineConfig) {
		c.versionPolicy = p
		c.versionPolicySet = true
	}
}

// InvokeOption configures a single workflow invocation.
type InvokeOption func(*invokeConfig)

type invokeConfig struct {
	execTTL time.Duration
	runtime *types.Runtime
	traceID string
	spanID  string
}

// WithExecutionTTL overrides the backend execution TTL for this invocation.
//
// It is mostly relevant for Redis-backed cluster mode. The TTL is extended
// while nodes are suspended, but it should still be set longer than the
// expected workflow lifetime for long-running approval processes.
func WithExecutionTTL(d time.Duration) InvokeOption {
	return func(c *invokeConfig) {
		if d > 0 {
			c.execTTL = d
		}
	}
}

// WithRuntime sets per-execution runtime context for this invocation.
//
// Runtime is distinct from static workflow context: WorkflowContext.Vars belongs
// to the workflow definition, while Runtime.Vars can differ for every Invoke.
func WithRuntime(runtime *types.Runtime) InvokeOption {
	return func(c *invokeConfig) {
		c.runtime = cloneRuntime(runtime)
	}
}

// WithRuntimeVars is a convenience wrapper for setting Runtime.Vars.
func WithRuntimeVars(vars map[string]any) InvokeOption {
	return WithRuntime(&types.Runtime{Vars: vars})
}

// WithTraceID attaches a trace ID to this invocation. Node handlers receive it
// through types.Input.TraceID.
func WithTraceID(traceID string) InvokeOption {
	return func(c *invokeConfig) {
		c.traceID = traceID
	}
}

// WithSpanID attaches a span ID to this invocation. Node handlers receive it
// through types.Input.SpanID.
func WithSpanID(spanID string) InvokeOption {
	return func(c *invokeConfig) {
		c.spanID = spanID
	}
}

func cloneRuntime(runtime *types.Runtime) *types.Runtime {
	if runtime == nil {
		return nil
	}
	cp := &types.Runtime{}
	if runtime.Vars != nil {
		cp.Vars = cloneMap(runtime.Vars)
	}
	return cp
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
