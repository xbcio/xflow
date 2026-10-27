package xflow

import (
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
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
	// namespace is the deployment's namespace for a single-namespace embedded
	// engine (see WithNamespace). Empty = unstamped, the multi-namespace
	// default where the control plane namespaces each lease per claim.
	namespace namespace.Namespace
	nodes     []types.Handler
	stopFns   []func()
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

	// metadataStore is the optional durable metadata mirror. NewLocal passes it
	// to the in-memory backend (backendlocal.WithStore); NewCluster uses it as
	// the ClusterConfig.Store value when that field is nil. nil = no mirror.
	metadataStore store.Store

	// scheduleOverrides holds the runtime overlays an operator has placed on
	// trigger nodes (a retimed cron expression, a pause). Read by the
	// in-process trigger host at activation time — see WithScheduleOverrides.
	// nil = no overlays, i.e. every trigger runs exactly what its definition
	// says.
	scheduleOverrides store.ScheduleOverrideStore

	executionMode             ExecutionMode
	executionModeSet          bool
	transientTTL              time.Duration
	transientTTLSet           bool
	transientCompletionTTL    time.Duration
	transientCompletionTTLSet bool

	// paramValidation is how AddWorkflow treats ParamSpec issues; empty means
	// types.DefaultParamValidationMode. See WithParamValidation.
	paramValidation types.ParamValidationMode

	// local-only: NewCluster always leaves this false; direct handlers are
	// rejected regardless (see node_registration.go registerDirectHandlers).
	allowDirectHandlers bool
	// tracer instruments in-process task attempts (xflow.local.task) and FAF
	// executions. Local-only: a cluster engine's runner is a separate process
	// with its own Config.Tracer, and the control plane already traces its
	// dispatch. Nil = no local spans. See WithTracer.
	tracer tracing.Tracer
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

// WithNamespace declares the namespace this engine's executions belong to.
//
// For an embedded single-namespace deployment — the process reads its tenant
// from configuration (xflow.namespace), never from a request — this is the
// only channel that can put one on a lease: the local queue's worker rebuilds
// ctx from context.Background(), so the submitting request's namespace is
// gone by the time the engine builds the lease, and a local engine has no
// claim record to read it from. Every lease this engine issues (and every
// inner lease its batch-body executor re-stamps) then carries the namespace,
// which is what scopes $supplies and the artifact-reference check on the
// executing side.
//
// Leave unset in a multi-namespace deployment: there the control plane names
// each lease per claim, and one engine-wide value would collapse them.
func WithNamespace(ns namespace.Namespace) Option {
	return func(c *engineConfig) { c.namespace = ns }
}

// WithTracer installs the tracer used for in-process execution spans.
//
// NewLocal uses it in two places: xflow.local.task around each embedded task
// attempt (so a local workflow execution joins the trace of the request that
// started it), and the xflow.local.task span a FireAndForget call runs under.
// Nil (the default) records nothing.
//
// It is local-only. NewCluster ignores it: a cluster runner is a separate
// process configured with its own tracer (xflow.RunnerConfig.Tracer), and the
// control plane's dispatch/report spans are already traced through
// Config.Tracer on the server side.
func WithTracer(t tracing.Tracer) Option {
	return func(c *engineConfig) {
		if t != nil {
			c.tracer = t
		}
	}
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

// WithStore installs an optional durable metadata store.
//
// For NewLocal the store is a mirror of the in-process state: executions, node
// status, signals, and the resulting audit rows are projected into it after
// each accepted transition, so they survive process exit and can be listed
// from the store. It does NOT make an in-flight execution resumable — a
// restarted local engine starts empty and never resumes, redelivers, or
// repairs work it finds only in the store. Use NewCluster for durable
// execution.
//
// Per-workflow transient executions are never mirrored. A deterministic
// execution ID that already has a store row from a previous process fails a
// required create with an error wrapping local.ErrStoreExecutionExists (the
// in-memory execution is rolled back); seed collisions are logged and stop
// further projection for that execution, leaving the old row untouched.
// Projections for one execution are serialized and converge on the latest
// in-memory status, so a completion racing the initial insert still ends
// terminal in the store.
//
// For NewCluster this is the same knob as ClusterConfig.Store. ClusterConfig
// wins when both are set, so an existing caller that passes the field is
// unaffected.
//
// nil (the default) leaves NewLocal purely in-memory, exactly as before this
// option existed.
func WithStore(s store.Store) Option {
	return func(c *engineConfig) { c.metadataStore = s }
}

// WithScheduleOverrides provides the store holding trigger-node overlays: the
// per-node runtime overrides that retime a cron schedule, or pause it, without
// re-registering the workflow.
//
// The engine reads it in the trigger host, at activation time: an overlay is
// merged into the node's activation parameters (a cron node's `expression`, its
// `timezone` and policies; a timer node's `interval`) on top of the values the
// definition rendered.
//
// It is the SAME store the schedule API writes through (control.ScheduleManager),
// and that is the point: a process that serves the API and hosts its own
// triggers must hand both the same store, or an operator's change is stored
// where nothing reads it.
//
// nil (the default) means no overlays exist: every trigger activates with
// exactly the parameters its definition declares.
func WithScheduleOverrides(overrides store.ScheduleOverrideStore) Option {
	return func(c *engineConfig) { c.scheduleOverrides = overrides }
}

// InvokeOption configures a single workflow invocation.
type InvokeOption func(*invokeConfig)

type invokeConfig struct {
	execTTL         time.Duration
	runtime         *types.Runtime
	fafRuntime      *types.Runtime
	traceID         string
	spanID          string
	idempotencyKey  string
	testRun         bool
	workflowVersion string
}

// WithIdempotencyKey makes this invocation safe to retry: a second Invoke of the
// same workflow, entry, input and key returns the execution the first one
// created instead of starting another.
//
// The key is scoped to the workflow, so one key means one execution of one
// workflow; the same key with a different entry or different input is refused
// (engine.ErrIdempotencyConflict) rather than answered with an execution of
// other work. A server whose state store cannot hold the claim refuses the
// keyed invocation (engine.ErrIdempotencyUnsupported) rather than silently
// dropping the key.
//
// A key stays valid for the execution's own retention (engine.WithIdempotencyTTL
// adjusts it), so a retry within that window — including after the execution
// reached a terminal status — is answered with the original execution.
//
// The fingerprint is taken from the graph the call actually runs, so combining
// it with WithWorkflowVersion is what makes a retry survive an intervening
// publish: a pinned retry resolves the same version and its original execution
// is returned, while an unpinned retry resolves the new version and is refused
// as a conflict.
func WithIdempotencyKey(key string) InvokeOption {
	return func(c *invokeConfig) {
		c.idempotencyKey = key
	}
}

// WithWorkflowVersion pins this invocation to one published version of the
// workflow addressed by the call.
//
// Omitted (the default) runs the addressed record — its current version — which
// is what Invoke did before versions existed. A version equal to the current
// one resolves to that same live record. A RETIRED version runs from the graph
// snapshot captured when it was retired, never a recompile, after its stored
// payload has been verified to hash to the value the version was published
// with; when the current compiler cannot reproduce that hash the call fails
// with backend.ErrWorkflowVersionUnavailable rather than execute a definition
// wearing the label of another. An unknown version fails with
// backend.ErrWorkflowVersionNotFound.
//
// Pinning is what makes a call independent of an intervening publish: the
// execution runs the named version, and the graph snapshot it records carries
// that version.
func WithWorkflowVersion(version string) InvokeOption {
	return func(c *invokeConfig) { c.workflowVersion = version }
}

// WithTestRun marks this invocation as a test run.
//
// A test run is what settings.pin_data_mode: test_only (the default) keys on:
// pinned nodes skip their handler and commit the pin_data mock as their output
// with status "pinned". Outside a test run those nodes execute for real. The
// flag is stored on the execution, so it holds for every node of the run,
// including ones scheduled after a restart.
func WithTestRun() InvokeOption {
	return func(c *invokeConfig) { c.testRun = true }
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
		// FAF snapshots this original payload immediately before it launches its
		// asynchronous runner. Ordinary Invoke continues to use runtime above.
		c.fafRuntime = runtime
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
