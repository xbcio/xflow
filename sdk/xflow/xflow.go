package xflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/internal/adapter/cluster"
	"github.com/xbcio/xflow/sdk/internal/adapter/local"
	"github.com/xbcio/xflow/types"
)

// Waiter provides channel-based blocking for execution completion.
// Implementations that support event-driven notification (e.g. local mode)
// should implement this interface. If not provided, Wait() polls StateBackend.
type Waiter interface {
	WaitDone(ctx context.Context, id types.ExecutionID) (types.Result, error)
}

// Engine is the user-facing workflow engine.
type Engine struct {
	eng      *engine.Engine
	registry engine.HandlerRegistry
	waiter   Waiter
	stopFns  []func()
}

// New creates an Engine with explicitly injected backends.
// Use WithBackend for standard setups, or WithState+WithQueue for custom combinations.
//
// Example:
//
//	eng, err := xflow.New(xflow.WithBackend(local.New(local.WithConcurrency(8))))
//	defer eng.Stop()
//
// For common setups, use NewLocal or NewCluster instead.
func New(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.backend != nil {
		return newFromConfig(cfg, cfg.backend)
	}

	if cfg.state == nil {
		return nil, errors.New("xflow.New: WithBackend or WithState is required")
	}
	if cfg.queue == nil {
		return nil, errors.New("xflow.New: WithBackend or WithQueue is required")
	}

	var engOpts []engine.Option
	if cfg.registry != nil {
		engOpts = append(engOpts, engine.WithRegistry(cfg.registry))
	}
	if cfg.hooks != nil {
		engOpts = append(engOpts, engine.WithHooks(cfg.hooks))
	}
	if cfg.logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.logger))
	}

	eng := engine.New(cfg.state, cfg.queue, engOpts...)

	return &Engine{
		eng:      eng,
		registry: cfg.registry,
		waiter:   cfg.waiter,
		stopFns:  cfg.stopFns,
	}, nil
}

// NewLocal creates an in-process engine backed by in-memory state and a goroutine pool.
// Zero external dependencies — suitable for development, testing, and single-process use.
func NewLocal(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 4}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 4
	}

	backend := local.New(local.WithConcurrency(cfg.concurrency))
	return newFromConfig(cfg, backend)
}

// NewCluster creates a distributed engine backed by Redis (Asynq) and an
// optional persistent Store (any store.Store implementation).
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

	a, err := cluster.New(clusterCfg.RedisAddr, clusterCfg.Store,
		cluster.WithConcurrency(cfg.concurrency),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	return newFromConfig(cfg, a)
}

// newFromConfig assembles an Engine from a resolved engineConfig and a Backend.
func newFromConfig(cfg *engineConfig, backend Backend) (*Engine, error) {
	if cfg.state == nil {
		cfg.state = backend.State()
	}
	if cfg.queue == nil {
		cfg.queue = backend.Queue()
	}
	if cfg.registry == nil {
		cfg.registry = backend.Registry()
	}
	if cfg.waiter == nil {
		if w, ok := backend.(Waiter); ok {
			cfg.waiter = w
		}
	}

	var engOpts []engine.Option
	if cfg.registry != nil {
		engOpts = append(engOpts, engine.WithRegistry(cfg.registry))
	}
	if cfg.hooks != nil {
		engOpts = append(engOpts, engine.WithHooks(cfg.hooks))
	}
	if cfg.logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.logger))
	}

	eng := engine.New(cfg.state, cfg.queue, engOpts...)

	e := &Engine{
		eng:      eng,
		registry: cfg.registry,
		waiter:   cfg.waiter,
		stopFns:  cfg.stopFns,
	}

	stop := backend.Bind(eng)
	e.stopFns = append(e.stopFns, stop)

	return e, nil
}

// Stop shuts down background workers and releases resources.
// Stop functions are called in LIFO order.
func (e *Engine) Stop() {
	for i := len(e.stopFns) - 1; i >= 0; i-- {
		e.stopFns[i]()
	}
}

// Submit builds the workflow definition and starts an asynchronous execution.
func (e *Engine) Submit(ctx context.Context, wf *WorkflowBuilder, params map[string]any, opts ...SubmitOption) (types.ExecutionID, error) {
	cfg := &submitConfig{}
	for _, o := range opts {
		o(cfg)
	}

	def, err := wf.Build()
	if err != nil {
		return "", err
	}

	// Register direct handlers if the registry supports it.
	if len(wf.directHandlers()) > 0 {
		lr, ok := e.registry.(*local.LocalRegistry)
		if !ok {
			names := make([]string, 0, len(wf.directHandlers()))
			for n := range wf.directHandlers() {
				names = append(names, n)
			}
			return "", fmt.Errorf("nodes %v use direct task handlers (local mode only); with cluster, register via node.Register and use node.New instead", names)
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
		ctx = cluster.WithExecTTLCtx(ctx, cfg.execTTL)
	}

	return e.eng.Submit(ctx, g, params)
}

// Wait blocks until the execution reaches a terminal state or ctx is canceled.
func (e *Engine) Wait(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	if e.waiter != nil {
		return e.waiter.WaitDone(ctx, id)
	}
	// Fallback: poll StateBackend.
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

// Cancel marks the execution as canceled.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	return e.eng.Cancel(ctx, id)
}

// SubmitDef compiles a raw WorkflowDef and starts execution.
func (e *Engine) SubmitDef(ctx context.Context, def *types.WorkflowDef, params map[string]any) (types.ExecutionID, error) {
	g, err := graph.Compile(def)
	if err != nil {
		return "", err
	}
	return e.eng.Submit(ctx, g, params)
}

// Status returns the current status of an execution.
func (e *Engine) Status(ctx context.Context, id types.ExecutionID) (types.Status, error) {
	snap, err := e.eng.State().GetExecution(ctx, id)
	if err != nil {
		return "", err
	}
	if snap == nil {
		return "", fmt.Errorf("execution %q not found", id)
	}
	return snap.Status, nil
}

// RegisterHandler registers a node.TaskHandler for a given type in local mode.
// Returns an error if the engine was not created with a local registry.
func (e *Engine) RegisterHandler(nodeType string, h node.TaskHandler) error {
	lr, ok := e.registry.(*local.LocalRegistry)
	if !ok {
		return errors.New("xflow: RegisterHandler is only supported in local mode")
	}
	lr.RegisterGlobal(nodeType, h)
	return nil
}

func isTerminalStatus(s types.Status) bool {
	switch s {
	case types.StatusSuccess, types.StatusFailed, types.StatusCanceled, types.StatusTimeout:
		return true
	}
	return false
}
