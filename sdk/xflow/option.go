package xflow

import (
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Option configures an Engine.
// Common options are valid for NewLocal and NewCluster.
type Option func(*engineConfig)

// engineConfig backs Option. Fields fall into three groups, marked below:
//   - shared: read by both NewLocal and NewCluster (see engine.go)
//   - local-only: only meaningful for NewLocal (see local.go)
//   - cluster-only: only meaningful for NewCluster (see cluster.go)
type engineConfig struct {
	// shared
	state       engine.StateStore
	queue       engine.TaskQueue
	registry    engine.HandlerRegistry
	hooks       engine.Hooks
	logger      engine.Logger
	waiter      backend.Waiter
	concurrency int
	nodes       []types.Handler
	stopFns     []func()
	// subgraphHooks observes the inner engine a map body item runs on. Kept
	// apart from hooks so the two populations stay in separate series: a map
	// over N items is N inner executions beneath one outer one.
	subgraphHooks engine.Hooks

	versionPolicy    execution.VersionPolicy
	versionPolicySet bool

	resourcePool       types.ResourcePool
	resourcePoolSet    bool
	resourcePoolConfig *types.ResourcePoolConfig

	// artifactStore holds script artifacts (wasm modules, JS sources) in a
	// content-addressable store. Used by resolveArtifacts (AddWorkflow) to Put
	// files from ScriptFile, and by the embedded runner to resolve artifact_digest
	// at Execute time. nil = feature disabled.
	artifactStore *store.ArtifactStore

	executionMode             ExecutionMode
	executionModeSet          bool
	transientTTL              time.Duration
	transientTTLSet           bool
	transientCompletionTTL    time.Duration
	transientCompletionTTLSet bool

	// local-only: NewCluster always leaves this false; direct handlers are
	// rejected regardless (see node_registration.go registerDirectHandlers).
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

// WithSubgraphHooks sets the lifecycle hook receiver for the inner engine that
// runs a map body item.
//
// It is separate from WithHooks because the two observe different populations.
// A map over N items produces N inner executions beneath one outer execution,
// so routing both into one receiver would add N to whatever that receiver
// counts as an execution. Passing the same value to both is therefore a
// mistake, not a shortcut: use observability/metrics.NewSubgraphMetricsHooks,
// which writes the xflow_subgraph_* family.
//
// Nil (the default) leaves a body item's nodes unobserved, which is what they
// were before this existed.
func WithSubgraphHooks(h engine.Hooks) Option {
	return func(c *engineConfig) { c.subgraphHooks = h }
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
func WithNodes(defs ...types.Handler) Option {
	return func(c *engineConfig) {
		c.nodes = append(c.nodes, defs...)
	}
}

// WithResourcePool installs a custom ResourcePool. Pass nil to explicitly
// opt out of pooling — resource-aware nodes (DatabaseNode/GRPCNode) will
// error at runtime when invoked without a pool.
func WithResourcePool(p types.ResourcePool) Option {
	return func(c *engineConfig) {
		c.resourcePool = p
		c.resourcePoolSet = true
	}
}

// WithResourcePoolConfig tunes the SDK-managed default pool. Ignored if
// WithResourcePool is also set.
func WithResourcePoolConfig(cfg types.ResourcePoolConfig) Option {
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

// WithArtifactStore provides a content-addressable store for script artifacts.
// When set:
//   - AddWorkflow resolves ScriptFile nodes: reads the file, stores it, and
//     rewrites the parameter to artifact_digest.
//   - The embedded runner resolves artifact_digest at Execute time by reading
//     from this store.
//
// nil (the default) disables both: ScriptFile nodes fail at AddWorkflow, and
// artifact_digest params fail at Execute with a permanent config error.
func WithArtifactStore(s *store.ArtifactStore) Option {
	return func(c *engineConfig) { c.artifactStore = s }
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
