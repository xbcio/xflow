package xflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/backend"
	backendasynq "github.com/xbcio/xflow/backend/asynq"
	backendmemory "github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// Engine is the user-facing workflow engine.
type Engine struct {
	eng                 *engine.Engine
	registry            engine.HandlerRegistry
	waiter              backend.Waiter
	stopFns             []func()
	allowDirectHandlers bool
}

// NewLocal creates an in-process engine backed by in-memory state and a goroutine pool.
// Zero external dependencies — suitable for development, testing, and single-process use.
func NewLocal(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 4, allowDirectHandlers: true}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}

	provider := backendmemory.New(backendmemory.WithConcurrency(cfg.concurrency))
	return newFromConfig(cfg, provider)
}

// NewCluster creates a distributed engine backed by Redis (Asynq) and an
// optional persistent Store (any store.Store implementation). Node tasks are
// consumed through the reusable execution Dispatcher and embedded Runner so
// cluster mode uses the same lease/result boundary as the future server backend.
//
// Example:
//
//	eng, err := xflow.NewCluster(xflow.ClusterConfig{RedisAddr: "localhost:6379"})
//	eng, err := xflow.NewCluster(xflow.ClusterConfig{RedisAddr: addr, Store: sqlstore.New(db)}, xflow.WithConcurrency(16))
func NewCluster(clusterCfg ClusterConfig, opts ...Option) (*Engine, error) {
	if clusterCfg.RedisAddr == "" {
		return nil, errors.New("xflow.NewCluster: RedisAddr is required")
	}

	cfg := &engineConfig{concurrency: 10}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 10
	}

	a, err := backendasynq.New(clusterCfg.RedisAddr, clusterCfg.Store,
		backendasynq.WithConcurrency(cfg.concurrency),
		backendasynq.WithConsumer(!clusterCfg.DisableConsumer),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	return newFromConfig(cfg, a)
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

	e := &Engine{
		eng:                 eng,
		registry:            cfg.registry,
		waiter:              cfg.waiter,
		stopFns:             cfg.stopFns,
		allowDirectHandlers: cfg.allowDirectHandlers,
	}

	if err := e.registerNodeDefinitions(cfg.nodes); err != nil {
		return nil, err
	}

	stop := provider.Bind(eng)
	e.stopFns = append(e.stopFns, stop)

	return e, nil
}

// Stop shuts down background services and releases resources.
// Stop functions are called in LIFO order.
func (e *Engine) Stop() {
	for i := len(e.stopFns) - 1; i >= 0; i-- {
		e.stopFns[i]()
	}
}

func (e *Engine) registerNodeDefinitions(defs []*node.Definition) error {
	if len(defs) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	if !ok {
		return fmt.Errorf("registry does not support node definition registration")
	}
	for _, def := range defs {
		if def == nil {
			continue
		}
		lr.RegisterGlobal(def.Descriptor().Type, def)
	}
	return nil
}

// Submit builds the workflow definition and starts an asynchronous execution.
func (e *Engine) Submit(ctx context.Context, wf *WorkflowBuilder, params map[string]any, opts ...SubmitOption) (types.ExecutionID, error) {
	cfg := &submitConfig{}
	for _, o := range opts {
		o(cfg)
	}

	def, err := wf.build()
	if err != nil {
		return "", err
	}

	if err := e.registerWorkflowHandlers(wf); err != nil {
		return "", err
	}

	// Register direct handlers if the registry supports it.
	if len(wf.directHandlers()) > 0 {
		lr, ok := e.registry.(*execution.Registry)
		if !cfgAllowsDirectHandlers(e) || !ok {
			names := make([]string, 0, len(wf.directHandlers()))
			for n := range wf.directHandlers() {
				names = append(names, n)
			}
			return "", fmt.Errorf("nodes %v use direct action handlers (local mode only); with cluster, define custom nodes with node.Define and register consumer capabilities with xflow.WithNodes", names)
		}
		for nodeName, h := range wf.directHandlers() {
			lr.RegisterNodeHandler(nodeName, h)
		}
	}

	g, err := graph.Compile(def)
	if err != nil {
		return "", err
	}

	if cfg.execTTL > 0 {
		ctx = engine.WithExecutionTTL(ctx, cfg.execTTL)
	}
	ctx = engine.WithWorkflowDef(ctx, def)

	return e.eng.Submit(ctx, g, params)
}

func (e *Engine) registerWorkflowHandlers(wf *WorkflowBuilder) error {
	if wf == nil || len(wf.workflowHandlers()) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	if !ok {
		return fmt.Errorf("registry does not support workflow handler registration")
	}
	for nodeType, h := range wf.workflowHandlers() {
		lr.RegisterGlobal(nodeType, h)
	}
	return nil
}

// Wait blocks until the execution reaches a terminal state or ctx is canceled.
func (e *Engine) Wait(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	if e.waiter != nil {
		return e.waiter.WaitDone(ctx, id)
	}
	// Fallback: poll StateStore.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return types.Result{}, ctx.Err()
		case <-ticker.C:
			snap, err := e.eng.State().GetExecution(ctx, id)
			if err != nil || snap == nil {
				continue
			}
			if isTerminalStatus(snap.Status) {
				return types.Result{ExecutionID: id, Status: snap.Status}, nil
			}
		}
	}
}

// Signal delivers a named signal to a suspended node within the execution.
func (e *Engine) Signal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	return e.eng.DeliverSignal(ctx, id, name, data)
}

// RevokeSignal revokes a pre-delivered signal that has not yet been consumed.
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) error {
	return e.eng.RevokeSignal(ctx, id, name)
}

// Cancel cancels a running execution and releases suspended nodes.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	return e.eng.Cancel(ctx, id)
}

// Inspect returns execution and node status details for audit and UI flows.
func (e *Engine) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error) {
	return e.eng.Inspect(ctx, id, nodeNames...)
}

func isTerminalStatus(s types.ExecutionStatus) bool { return types.IsTerminalExecutionStatus(s) }

func cfgAllowsDirectHandlers(e *Engine) bool {
	if e == nil {
		return false
	}
	return e.allowDirectHandlers
}
