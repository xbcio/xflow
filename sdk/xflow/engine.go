package xflow

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// bindDeprecationOnce ensures the legacy Provider.Bind fallback warning is
// logged only once per process, not on every Engine construction.
var bindDeprecationOnce sync.Once

// Engine is the user-facing workflow engine.
//
// Engine itself is mode-agnostic: NewLocal and NewCluster both assemble it
// from a backend.Provider via newFromConfig, and every field below is used by
// both modes. Mode-specific setup lives in local.go / cluster.go.
type Engine struct {
	eng              *engine.Engine
	registry         engine.HandlerRegistry
	workflowRegistry backend.WorkflowRegistry
	triggerRuntime   *triggerRuntime
	waiter           backend.Waiter
	stopFns          []func()
	// nonOwning marks a facade over resources owned by Server. Its Stop method
	// is deliberately a no-op so callers cannot stop the Server's dispatcher,
	// consumers, registries, or other lifecycle-owned resources.
	nonOwning           bool
	allowDirectHandlers bool
	executionMode       ExecutionMode
	logger              engine.Logger
	// stopOnce guarantees the stopFns run at most once. The local queue's
	// shutdown closes a channel that panics on a second close, so Stop must be
	// idempotent for callers that defer Stop and also stop explicitly.
	stopOnce sync.Once
	// mu serializes workflow registration and protects the handler mirrors plus
	// per-workflow FAF handler bindings from concurrent mutation.
	mu sync.Mutex
	// directHandlerNames tracks LocalNode handler names this Engine has already
	// registered, keyed by node name with the registering workflow's name as the
	// value. LocalNode handlers are registered into a process-global map (see
	// execution.Registry.RegisterNodeHandler), so a second workflow reusing the
	// same node name silently shadows the first. This map surfaces that
	// collision as a warning instead of failing silently.
	directHandlerNames map[string]string
	// directHandlers and globalHandlers mirror the node-name and node-type
	// handlers this Engine registered into the process registry. The
	// HandlerRegistrar write API exposes neither reads nor unregistration, so
	// these mirrors let AddWorkflow restore a previously-overwritten handler
	// when a later step of the same call fails (rollback). Guarded by e.mu.
	directHandlers map[string]types.ActionHandler
	globalHandlers map[string]types.ActionHandler
	// fafHandlers pins a locally registered FAF workflow ID to its exact action
	// handler. It must not resolve through directHandlers at dispatch time:
	// direct handler names are process-global and can be shadowed by a later
	// workflow registration.
	fafHandlers map[types.WorkflowID]fafHandlerBinding

	// artifactStore for ScriptFile resolution (AddWorkflow) and embedded-runner
	// artifact_digest resolution (Execute). nil = disabled.
	artifactStore *store.ArtifactStore
	// resourcePool is the process-owned pool supplied to embedded execution.
	// FAF reuses it so resource-aware actions retain local execution parity
	// without introducing any execution persistence.
	resourcePool types.ResourcePool
}

// newFromConfig assembles an Engine from a resolved engineConfig and a backend provider.
func newFromConfig(cfg *engineConfig, provider backend.Provider) (*Engine, error) {
	if cfg.state == nil {
		cfg.state = provider.State()
	}
	if cfg.queue == nil {
		cfg.queue = provider.Queue()
	}
	if cfg.registry == nil {
		cfg.registry = provider.Registry()
	}
	if cfg.waiter == nil {
		if w, ok := provider.(backend.Waiter); ok {
			cfg.waiter = w
		}
	}

	var engOpts []engine.Option
	if cfg.hooks != nil {
		engOpts = append(engOpts, engine.WithHooks(cfg.hooks))
	}
	if cfg.logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.logger))
	}
	if cfg.executionMode == ExecutionModeTransient {
		engOpts = append(engOpts, engine.WithSuspendDisabled(ErrTransientSuspendUnsupported))
	}
	// A map node's batches run in this process, so this engine needs the
	// executor that runs their body. A body always forbids suspend: it runs once
	// per item with no external identity to resume against, so a suspended item
	// would park a sub-execution nothing can ever signal.
	//
	// The artifact resolver is threaded in explicitly: a body member is executed
	// by a fresh backend the body executor builds, not by the engine's own, so
	// the resolver NewLocal/NewCluster installed does not reach a ScriptFile
	// node nested inside a map body.
	if bodies := newBatchBodyExecutor(cfg.registry, true, artifactCodeResolverFor(cfg.artifactStore), cfg.subgraphHooks); bodies != nil {
		engOpts = append(engOpts, engine.WithBatchBodyExecutor(bodies))
	}

	eng := engine.New(cfg.state, cfg.queue, engOpts...)

	if vc, ok := cfg.registry.(execution.VersionConfigurator); ok {
		if cfg.versionPolicySet {
			vc.SetVersionPolicy(cfg.versionPolicy)
		}
		if cfg.logger != nil {
			vc.SetLogger(cfg.logger)
		}
	}

	e := &Engine{
		eng:                 eng,
		registry:            cfg.registry,
		workflowRegistry:    provider.WorkflowRegistry(),
		waiter:              cfg.waiter,
		stopFns:             cfg.stopFns,
		allowDirectHandlers: cfg.allowDirectHandlers,
		executionMode:       cfg.executionMode,
		logger:              cfg.logger,
		directHandlerNames:  make(map[string]string),
		directHandlers:      make(map[string]types.ActionHandler),
		globalHandlers:      make(map[string]types.ActionHandler),
		fafHandlers:         make(map[types.WorkflowID]fafHandlerBinding),
		artifactStore:       cfg.artifactStore,
		resourcePool:        cfg.resourcePool,
	}
	e.triggerRuntime = newTriggerRuntime(e, provider.TriggerPrimitives())

	if err := e.registerNodeDefinitions(cfg.nodes); err != nil {
		return nil, err
	}

	if sb, ok := provider.(backend.StartBinder); ok {
		stop, err := sb.StartBinding(eng)
		if err != nil {
			return nil, fmt.Errorf("xflow: start backend: %w", err)
		}
		e.stopFns = append(e.stopFns, stop)
	} else {
		// Deprecated compatibility path: legacy providers only expose the
		// error-swallowing Bind contract. We keep it so external backend
		// implementations continue to compile, but production backends should
		// implement backend.StartBinder.
		bindDeprecationOnce.Do(func() {
			log.Println("xflow: warning: backend.Provider does not implement backend.StartBinder; using deprecated Bind path that cannot propagate consumer start errors")
		})
		stop := provider.Bind(eng)
		e.stopFns = append(e.stopFns, stop)
	}
	e.stopFns = append(e.stopFns, func() { _ = e.triggerRuntime.Close(context.Background()) })

	return e, nil
}

// newNonOwningEngineFacade exposes an already-assembled control-plane core to
// trusted in-process callers without binding, starting, or owning anything.
// The Server remains responsible for all provider and dispatcher lifecycle.
func newNonOwningEngineFacade(core *engine.Engine, provider backend.Provider) *Engine {
	e := &Engine{
		eng:                core,
		registry:           provider.Registry(),
		workflowRegistry:   provider.WorkflowRegistry(),
		nonOwning:          true,
		directHandlerNames: make(map[string]string),
		directHandlers:     make(map[string]types.ActionHandler),
		globalHandlers:     make(map[string]types.ActionHandler),
		fafHandlers:        make(map[types.WorkflowID]fafHandlerBinding),
	}
	if waiter, ok := provider.(backend.Waiter); ok {
		e.waiter = waiter
	}
	return e
}

// Stop shuts down background services and releases resources.
// Stop functions are called in LIFO order. Stop is idempotent: repeated calls
// after the first are no-ops (the local queue panics on a double close, so the
// stopFns must run exactly once). For a non-owning Server facade, Stop is a
// no-op because Server owns the underlying lifecycle.
func (e *Engine) Stop() {
	if e == nil || e.nonOwning {
		return
	}
	e.stopOnce.Do(func() {
		for i := len(e.stopFns) - 1; i >= 0; i-- {
			e.stopFns[i]()
		}
	})
}

// WebhookHandler returns the http.Handler that serves the routes this engine's
// xflow.trigger.webhook nodes registered, or nil when the engine owns no
// trigger runtime (a non-owning Server facade does not).
//
// A host embedding the engine must mount the result on its own listener.
// Nothing else mounts it: the control-plane API has no webhook route, and a
// remote-hosted runner's trigger runtime fails closed on webhooks, so without
// this call a webhook workflow activates successfully and then never receives a
// request — the trigger's route exists but is unreachable.
func (e *Engine) WebhookHandler() http.Handler {
	if e == nil || e.triggerRuntime == nil || e.triggerRuntime.webhooks == nil {
		return nil
	}
	return e.triggerRuntime.webhooks
}

func cfgAllowsDirectHandlers(e *Engine) bool {
	if e == nil {
		return false
	}
	return e.allowDirectHandlers
}
