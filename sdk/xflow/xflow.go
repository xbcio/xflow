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
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// Engine is the user-facing workflow engine.
type Engine struct {
	eng                 *engine.Engine
	registry            engine.HandlerRegistry
	workflowRegistry    backend.WorkflowRegistry
	triggerRuntime      *triggerRuntime
	waiter              backend.Waiter
	stopFns             []func()
	allowDirectHandlers bool
}

// NewLocal creates an in-process engine backed by in-memory state and a
// goroutine worker pool.
//
// Use NewLocal for unit tests, examples, local development, and single-process
// embedded usage. It supports LocalNode and typed nodes, but all state is
// process memory: stopping the process loses executions, signals, outputs, and
// queued work. For distributed workers, durable Redis state, or API-only pods,
// use NewCluster instead.
func NewLocal(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 4, allowDirectHandlers: true}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}
	pool := resolveResourcePool(cfg)
	memOpts := []backendmemory.Option{backendmemory.WithConcurrency(cfg.concurrency)}
	if pool != nil {
		memOpts = append(memOpts, backendmemory.WithResourcePool(pool))
	}
	provider := backendmemory.New(memOpts...)
	return newFromConfig(cfg, provider)
}

// NewCluster creates a distributed engine backed by Redis/Asynq and an optional
// persistent Store.
//
// Use NewCluster when executions may outlive a process, multiple instances need
// to share the queue/state, or API pods and worker pods are separated. Cluster
// workflows must use portable typed nodes (node.Define, built-ins); LocalNode
// submissions are rejected because Go function values cannot be serialized or
// executed by another process.
//
// Consumer-capable worker processes should pass xflow.WithNodes for every
// custom node type they may execute. API-only processes can set
// ClusterConfig.DisableConsumer=true to submit, signal, cancel, and inspect
// without consuming tasks.
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

	asynqOpts := []backendasynq.Option{
		backendasynq.WithConcurrency(cfg.concurrency),
		backendasynq.WithConsumer(!clusterCfg.DisableConsumer),
	}
	if pool := resolveResourcePool(cfg); pool != nil && !clusterCfg.DisableConsumer {
		// Only worker pods need a pool; API-only pods don't dispatch handlers.
		asynqOpts = append(asynqOpts, backendasynq.WithResourcePool(pool))
	}
	a, err := backendasynq.New(clusterCfg.RedisAddr, clusterCfg.Store, asynqOpts...)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	return newFromConfig(cfg, a)
}

// resolveResourcePool returns the configured pool, the default pool built from
// resourcePoolConfig, or nil when the caller explicitly opted out via
// WithResourcePool(nil).
func resolveResourcePool(cfg *engineConfig) node.ResourcePool {
	if cfg.resourcePoolSet {
		return cfg.resourcePool // may be nil — explicit opt-out
	}
	poolCfg := node.DefaultResourcePoolConfig()
	if cfg.resourcePoolConfig != nil {
		poolCfg = *cfg.resourcePoolConfig
	}
	return node.NewDefaultResourcePool(poolCfg)
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

func (e *Engine) registerNodeDefinitions(defs []node.Handler) error {
	if len(defs) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	for _, def := range defs {
		if def == nil {
			continue
		}
		switch h := def.(type) {
		case types.ActionHandler:
			if !ok {
				return fmt.Errorf("registry does not support node definition registration")
			}
			lr.RegisterGlobal(h.Descriptor().Type, h)
			if th, isTrigger := def.(types.TriggerHandler); isTrigger {
				node.RegisterTrigger(th)
			}
		case types.TriggerHandler:
			node.RegisterTrigger(h)
		default:
			return fmt.Errorf("node definition %q is not an action or trigger handler", def.Descriptor().Type)
		}
	}
	return nil
}

func (e *Engine) registerDirectHandlers(wf *WorkflowBuilder) error {
	if len(wf.directHandlers()) == 0 {
		return nil
	}
	lr, ok := e.registry.(*execution.Registry)
	if !cfgAllowsDirectHandlers(e) || !ok {
		names := make([]string, 0, len(wf.directHandlers()))
		for n := range wf.directHandlers() {
			names = append(names, n)
		}
		return fmt.Errorf("nodes %v use direct action handlers (local mode only); with cluster, define custom nodes with node.Define and register consumer capabilities with xflow.WithNodes", names)
	}
	for nodeName, h := range wf.directHandlers() {
		lr.RegisterNodeHandler(nodeName, h)
	}
	return nil
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
	for _, h := range wf.workflowTriggerHandlers() {
		node.RegisterTrigger(h)
	}
	return nil
}

// Wait blocks until the execution reaches a terminal state or ctx is canceled.
//
// Backends that implement event watching wake promptly; otherwise Wait polls.
// The returned Result contains the final execution status and latest node
// outputs. In cyclic mode, repeated nodes expose only their latest output.
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
				detail, err := e.eng.Inspect(ctx, id)
				if err != nil {
					return types.Result{}, err
				}
				return resultFromDetail(detail), nil
			}
		}
	}
}

func resultFromDetail(detail engine.ExecutionDetail) types.Result {
	out := make(map[string]any, len(detail.Nodes))
	for _, n := range detail.Nodes {
		if n.Output != nil {
			out[n.Name] = n.Output
		}
	}
	if len(out) == 0 {
		out = nil
	}
	return types.Result{
		ExecutionID: detail.ExecutionID,
		Status:      detail.Status,
		Output:      out,
		Error:       detail.Error,
	}
}

// Signal delivers a named signal to a suspended node within the execution.
//
// Signal names are defined by suspending nodes. For built-in approval nodes the
// per-approver form is "NodeName/approval/approver". If a signal arrives before
// the node suspends, the backend stores it and consumes it when the node reaches
// the matching wait point.
func (e *Engine) Signal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	return e.eng.DeliverSignal(ctx, id, name, data)
}

// RevokeSignal revokes a pre-delivered signal that has not yet been consumed.
//
// It cannot revoke a signal that already resumed a node. Use it for UI flows
// where a user retracts an early signal before the workflow reaches the wait
// point.
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) error {
	return e.eng.RevokeSignal(ctx, id, name)
}

// Cancel cancels a running execution and releases suspended nodes.
//
// Cancel is best-effort for work already leased to a runner: the execution is
// marked canceled and suspended waits are released, while stale task commits are
// fenced by the engine/state store.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	return e.eng.Cancel(ctx, id)
}

// Inspect returns execution and node status details for audit and UI flows.
//
// When nodeNames are omitted, Inspect loads the stored graph and returns every
// node's current status and latest output. In cyclic mode this is still a
// latest-state view, not a per-activation history.
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
