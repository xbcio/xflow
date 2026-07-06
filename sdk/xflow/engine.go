// Package xflow provides the public SDK entry points for embedding the
// workflow engine: mode factories (see local.go, cluster.go), the definition
// builder (builder.go), and runtime control APIs (engine_control.go).
package xflow

import (
	"context"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
)

// Engine is the user-facing workflow engine.
//
// Engine itself is mode-agnostic: NewLocal and NewCluster both assemble it
// from a backend.Provider via newFromConfig, and every field below is used by
// both modes. Mode-specific setup lives in local.go / cluster.go.
type Engine struct {
	eng                 *engine.Engine
	registry            engine.HandlerRegistry
	workflowRegistry    backend.WorkflowRegistry
	triggerRuntime      *triggerRuntime
	waiter              backend.Waiter
	stopFns             []func()
	allowDirectHandlers bool
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

	eng := engine.New(cfg.state, cfg.queue, engOpts...)

	if lr, ok := cfg.registry.(*execution.Registry); ok {
		if cfg.versionPolicySet {
			lr.SetVersionPolicy(cfg.versionPolicy)
		}
		if cfg.logger != nil {
			lr.SetLogger(cfg.logger)
		}
	}

	e := &Engine{
		eng:                 eng,
		registry:            cfg.registry,
		workflowRegistry:    provider.WorkflowRegistry(),
		waiter:              cfg.waiter,
		stopFns:             cfg.stopFns,
		allowDirectHandlers: cfg.allowDirectHandlers,
	}
	e.triggerRuntime = newTriggerRuntime(e, provider.TriggerPrimitives())

	if err := e.registerNodeDefinitions(cfg.nodes); err != nil {
		return nil, err
	}

	stop := provider.Bind(eng)
	e.stopFns = append(e.stopFns, stop)
	e.stopFns = append(e.stopFns, func() { _ = e.triggerRuntime.Close(context.Background()) })

	return e, nil
}

// Stop shuts down background services and releases resources.
// Stop functions are called in LIFO order.
func (e *Engine) Stop() {
	for i := len(e.stopFns) - 1; i >= 0; i-- {
		e.stopFns[i]()
	}
}

func cfgAllowsDirectHandlers(e *Engine) bool {
	if e == nil {
		return false
	}
	return e.allowDirectHandlers
}
