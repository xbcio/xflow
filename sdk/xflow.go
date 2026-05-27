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
	engstore "github.com/xbcio/xflow/store"
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
// At minimum, WithState and WithQueue must be provided.
func New(opts ...Option) (*Engine, error) {
	cfg := &engineConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.state == nil {
		return nil, errors.New("xflow.New: WithState is required")
	}
	if cfg.queue == nil {
		return nil, errors.New("xflow.New: WithQueue is required")
	}

	var engOpts []engine.EngineOption
	if cfg.registry != nil {
		engOpts = append(engOpts, engine.WithRegistry(cfg.registry))
	}
	if cfg.hooks != nil {
		engOpts = append(engOpts, engine.WithHooks(cfg.hooks))
	}
	if cfg.logger != nil {
		engOpts = append(engOpts, engine.WithLogger(cfg.logger))
	}

	eng := engine.NewEngine(cfg.state, cfg.queue, engOpts...)

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

	a := local.New(local.WithConcurrency(cfg.concurrency))

	e, err := New(
		WithState(a.State()),
		WithQueue(a.Queue()),
		WithRegistry(a.Registry()),
		WithWaiter(a),
	)
	if err != nil {
		return nil, err
	}

	stop := a.Bind(e.eng)
	e.stopFns = append(e.stopFns, stop)
	return e, nil
}

// NewCluster creates a distributed engine backed by Redis (Asynq) and optionally MySQL.
// db may be nil for pure-Redis mode (no durable persistence).
func NewCluster(redisAddr string, db engstore.ClusterStore, opts ...Option) (*Engine, error) {
	cfg := &engineConfig{concurrency: 10}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = 10
	}

	a, err := cluster.New(redisAddr, db,
		cluster.WithConcurrency(cfg.concurrency),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	e, err := New(
		WithState(a.State()),
		WithQueue(a.Queue()),
		WithRegistry(a.Registry()),
	)
	if err != nil {
		return nil, err
	}

	stop := a.Bind(e.eng)
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
// Panics if the engine was not created with a local registry.
func (e *Engine) RegisterHandler(nodeType string, h node.TaskHandler) {
	lr, ok := e.registry.(*local.LocalRegistry)
	if !ok {
		panic("RegisterHandler is only supported with a local registry")
	}
	lr.RegisterGlobal(nodeType, h)
}

func isTerminalStatus(s types.Status) bool {
	switch s {
	case types.StatusSuccess, types.StatusFailed, types.StatusCanceled, types.StatusTimeout:
		return true
	}
	return false
}
