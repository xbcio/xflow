package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// ErrSignalConsumed is returned when a signal revocation fails because the
// signal was already consumed by a suspended node or was never delivered.
var ErrSignalConsumed = errors.New("signal already consumed or not found")

// ErrResuspendDepthExceeded is returned when a node exceeds the maximum
// number of consecutive resuspend cycles within a single execution task.
var ErrResuspendDepthExceeded = errors.New("resuspend depth exceeded maximum")

// maxResuspendDepth limits recursive resuspend cycles to prevent infinite loops.
const maxResuspendDepth = 10

// resuspendDepthKey is the context key for tracking resuspend depth.
type resuspendDepthKey struct{}

func resuspendDepthFromCtx(ctx context.Context) int {
	if v, ok := ctx.Value(resuspendDepthKey{}).(int); ok {
		return v
	}
	return 0
}

func withResuspendDepth(ctx context.Context, depth int) context.Context {
	return context.WithValue(ctx, resuspendDepthKey{}, depth)
}

// EngineOption configures an Engine at construction time.
type EngineOption func(*Engine)

// WithRegistry sets the handler registry.
func WithRegistry(r HandlerRegistry) EngineOption {
	return func(e *Engine) { e.registry = r }
}

// WithHooks sets the lifecycle hook receiver.
func WithHooks(h Hooks) EngineOption {
	return func(e *Engine) { e.hooks = h }
}

// WithLogger sets the logger.
func WithLogger(l Logger) EngineOption {
	return func(e *Engine) { e.logger = l }
}

// Engine is the pure-algorithm workflow execution engine.
// It has zero IO dependencies — all persistence and queuing are injected via interfaces.
type Engine struct {
	state    StateBackend
	queue    TaskQueue
	hooks    Hooks
	registry HandlerRegistry
	logger   Logger

	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph
}

// NewEngine creates an Engine wired to the given state backend and task queue.
func NewEngine(state StateBackend, queue TaskQueue, opts ...EngineOption) *Engine {
	e := &Engine{
		state:  state,
		queue:  queue,
		graphs: make(map[types.ExecutionID]*graph.Graph),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Submit starts a new execution of the given graph with the provided params.
// It persists the execution snapshot, caches the graph, and enqueues all
// root nodes (in-degree == 0).
func (e *Engine) Submit(ctx context.Context, g *graph.Graph, params map[string]any) (types.ExecutionID, error) {
	id := types.ExecutionID("exec-" + uuid.New().String())

	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.StatusRunning,
		Params: params,
	}
	if err := e.state.CreateExecution(ctx, snap); err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}

	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()

	for i, nd := range g.Nodes {
		if g.InDegree[i] == 0 {
			task := &Task{
				ExecutionID: id,
				NodeName:    nd.Name,
				NodeIdx:     i,
				Type:        TaskTypeNodeExec,
			}
			if err := e.queue.Enqueue(ctx, task); err != nil {
				// Rollback: mark execution as failed and remove from cache.
				_ = e.state.UpdateExecutionStatus(ctx, id, types.StatusFailed, fmt.Sprintf("enqueue root node %s: %v", nd.Name, err))
				e.mu.Lock()
				delete(e.graphs, id)
				e.mu.Unlock()
				return "", fmt.Errorf("enqueue root node %s: %w", nd.Name, err)
			}
		}
	}
	return id, nil
}

// ExecuteNode runs a single node task. It is called by the queue consumer
// (goroutine pool in local mode, Asynq worker in cluster mode).
func (e *Engine) ExecuteNode(ctx context.Context, t *Task) error {
	e.mu.RLock()
	g, ok := e.graphs[t.ExecutionID]
	e.mu.RUnlock()
	if !ok {
		// Attempt to recover graph from persistent state (cluster worker restart).
		var err error
		g, err = e.state.LoadGraph(ctx, t.ExecutionID)
		if err != nil {
			return fmt.Errorf("load graph for %q: %w", t.ExecutionID, err)
		}
		if g == nil {
			// Execution was already completed/canceled and cleaned up — ignore stale task.
			return nil
		}
		// Verify execution is still active.
		snap, err := e.state.GetExecution(ctx, t.ExecutionID)
		if err != nil || snap == nil || isTerminal(string(snap.Status)) {
			return nil
		}
		e.mu.Lock()
		e.graphs[t.ExecutionID] = g
		e.mu.Unlock()
	}

	// Idempotency: skip if node already reached a terminal state.
	ns, err := e.state.GetNode(ctx, t.ExecutionID, t.NodeName)
	if err == nil && ns != nil && isTerminal(ns.Status) {
		return nil
	}

	if err := e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "running",
	}); err != nil {
		return err
	}

	if e.hooks != nil {
		e.hooks.OnNodeStart(ctx, t.ExecutionID, t.NodeName)
	}

	meta := g.Nodes[t.NodeIdx]
	handler, err := e.registry.Get(t.ExecutionID, t.NodeName, meta.Type)
	if err != nil {
		return e.handleNodeError(ctx, t, g, fmt.Errorf("handler not found: %w", err), nil, nil)
	}

	input := e.buildInput(ctx, t, g)

	var output *node.Output
	var sysErr error

	if sh, isSuspending := handler.(node.SuspendingHandler); isSuspending {
		output, sysErr = e.executeSuspending(ctx, t, sh, input)
		if output == nil && sysErr == nil {
			// Node is now parked — execution continues when signal arrives.
			return nil
		}
	} else {
		output, sysErr = handler.Execute(ctx, input)
	}

	return e.finalizeNode(ctx, t, g, meta, output, sysErr)
}

// executeSuspending handles the suspend/resume protocol for SuspendingHandler nodes.
func (e *Engine) executeSuspending(ctx context.Context, t *Task, sh node.SuspendingHandler, input *node.Input) (*node.Output, error) {
	// Resume path: signal was already delivered and we have the payload.
	if t.Type == TaskTypeNodeResume {
		output, err := sh.OnResume(ctx, input, t.Payload)
		if err != nil {
			return output, err
		}
		if output != nil && output.Resuspend {
			return e.doResuspend(ctx, t, sh, input, output, t.Payload.Name)
		}
		return output, nil
	}

	// First execution: prepare the suspension spec.
	spec, err := sh.PrepareSuspend(ctx, input)
	if err != nil {
		return nil, err
	}

	// Atomic check: consume a pre-delivered signal or park the node.
	payload, err := e.state.SuspendOrConsume(ctx, t.ExecutionID, t.NodeName, spec)
	if err != nil {
		return nil, err
	}

	if payload != nil {
		// Signal was already waiting — resume immediately.
		output, err := sh.OnResume(ctx, input, payload)
		if err != nil {
			return output, err
		}
		if output != nil && output.Resuspend {
			return e.doResuspend(ctx, t, sh, input, output, payload.Name)
		}
		return output, nil
	}

	// Node is now suspended.
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "suspended",
	})
	if e.hooks != nil {
		e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
	}
	return nil, nil
}

// doResuspend handles the resuspend cycle: persists intermediate output, prepares
// a new suspend spec, and atomically transitions to the new signal. If the new
// signal is already available, it recursively resumes.
func (e *Engine) doResuspend(ctx context.Context, t *Task, sh node.SuspendingHandler, input *node.Input, resuspendOutput *node.Output, oldSignalName string) (*node.Output, error) {
	depth := resuspendDepthFromCtx(ctx) + 1
	if depth > maxResuspendDepth {
		return nil, ErrResuspendDepthExceeded
	}
	ctx = withResuspendDepth(ctx, depth)

	// Persist intermediate state and update input.Data so PrepareSuspend sees it.
	if resuspendOutput != nil && resuspendOutput.Data != nil {
		_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, resuspendOutput.Data)
		input = &node.Input{
			Params:      input.Params,
			Vars:        input.Vars,
			Config:      input.Config,
			ExecutionID: input.ExecutionID,
			NodeName:    input.NodeName,
			Data:        resuspendOutput.Data,
			Inputs:      input.Inputs,
		}
	}

	// Re-prepare the suspend spec for the new signal.
	spec, err := sh.PrepareSuspend(ctx, input)
	if err != nil {
		return nil, err
	}

	if len(spec.Signals) == 0 {
		return nil, fmt.Errorf("resuspend: PrepareSuspend returned empty signals")
	}
	newSignalName := spec.Signals[0]

	// Atomically transition: release old lock, remove old waiter, check new signal.
	payload, err := e.state.ResuspendAtomic(ctx, t.ExecutionID, t.NodeName, oldSignalName, newSignalName, spec)
	if err != nil {
		return nil, err
	}

	if payload != nil {
		// New signal was already waiting — resume immediately.
		output, err := sh.OnResume(ctx, input, payload)
		if err != nil {
			return output, err
		}
		if output != nil && output.Resuspend {
			return e.doResuspend(ctx, t, sh, input, output, payload.Name)
		}
		return output, nil
	}

	// Node is now suspended on the new signal.
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "suspended",
	})
	if e.hooks != nil {
		e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
	}
	return nil, nil
}

// finalizeNode persists the success result and triggers downstream scheduling.
func (e *Engine) finalizeNode(ctx context.Context, t *Task, g *graph.Graph, meta graph.NodeMeta, output *node.Output, sysErr error) error {
	if sysErr != nil || (output != nil && output.Error != nil) {
		var bizErr *node.Error
		if output != nil {
			bizErr = output.Error
		}
		return e.handleNodeError(ctx, t, g, sysErr, output, bizErr)
	}

	data := map[string]any{}
	if output != nil && output.Data != nil {
		data = output.Data
	}

	port := "main"
	if output != nil && output.Port != "" {
		port = output.Port
	}

	_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, data)
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      "success",
		Output:      data,
		Port:        port,
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, "success")
	}

	return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, port, data)
}

// handleNodeError applies the node's OnError strategy and either aborts the
// execution or routes to the appropriate output port.
func (e *Engine) handleNodeError(ctx context.Context, t *Task, g *graph.Graph, sysErr error, output *node.Output, bizErr *node.Error) error {
	meta := g.Nodes[t.NodeIdx]
	outcome := ApplyOnError(meta.OnError, sysErr, bizErr, output)

	_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, outcome.Output)
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: t.ExecutionID,
		Name:        t.NodeName,
		NodeIdx:     t.NodeIdx,
		Status:      outcome.NodeStatus,
		Output:      outcome.Output,
		Port:        outcome.RoutePort,
		Error:       outcome.ErrorMessage,
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, outcome.NodeStatus)
	}

	if outcome.ExecFatal {
		_ = e.state.UpdateExecutionStatus(ctx, t.ExecutionID, types.StatusFailed, outcome.ErrorMessage)
		if e.hooks != nil {
			e.hooks.OnExecutionComplete(ctx, t.ExecutionID, types.StatusFailed)
		}
		e.mu.Lock()
		delete(e.graphs, t.ExecutionID)
		e.mu.Unlock()
		return nil
	}

	return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, outcome.RoutePort, outcome.Output)
}

// buildInput assembles the node.Input from graph metadata and upstream outputs.
func (e *Engine) buildInput(ctx context.Context, t *Task, g *graph.Graph) *node.Input {
	input := &node.Input{
		Params:      g.Nodes[t.NodeIdx].Parameters,
		Vars:        g.Vars,
		Config:      g.Config,
		ExecutionID: string(t.ExecutionID),
		NodeName:    t.NodeName,
	}

	inEdges := g.InEdges[t.NodeIdx]
	switch len(inEdges) {
	case 0:
		// Root node — inject workflow-level submission params as input.Data so
		// that source handlers can read them (mirrors ClusterRunner behaviour).
		if snap, _ := e.state.GetExecution(ctx, t.ExecutionID); snap != nil {
			input.Data = snap.Params
		}
	case 1:
		data, _ := e.state.GetOutput(ctx, t.ExecutionID, g.Nodes[inEdges[0].SrcIdx].Name)
		input.Data = data
	default:
		// Fan-in: expose all upstream outputs keyed by node name.
		inputs := make(map[string]any, len(inEdges))
		for _, edge := range inEdges {
			data, _ := e.state.GetOutput(ctx, t.ExecutionID, g.Nodes[edge.SrcIdx].Name)
			inputs[g.Nodes[edge.SrcIdx].Name] = data
		}
		input.Inputs = inputs
	}
	return input
}

// DeliverSignal routes an external signal to the appropriate suspended node
// and enqueues a resume task if the node is ready.
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	resumeNode, err := e.state.DeliverSignal(ctx, id, name, data)
	if err != nil {
		return err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(ctx context.Context) {
			e.hooks.OnSignalDelivered(ctx, id, name, data)
		})
	}

	if resumeNode == "" {
		// Signal stored; node not yet suspended.
		return nil
	}

	// Check graph existence before acquiring the resume lock to avoid
	// holding a lock that can never be released if the graph is gone.
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()
	if g == nil {
		var err error
		g, err = e.state.LoadGraph(ctx, id)
		if err != nil || g == nil {
			return err
		}
		// Verify execution is still active before caching and proceeding.
		snap, err := e.state.GetExecution(ctx, id)
		if err != nil || snap == nil || isTerminal(string(snap.Status)) {
			return err
		}
		e.mu.Lock()
		e.graphs[id] = g
		e.mu.Unlock()
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, resumeNode)
	if err != nil || !acquired {
		return err
	}

	nodeIdx := g.Index[resumeNode]
	return e.queue.Enqueue(ctx, &Task{
		ExecutionID: id,
		NodeName:    resumeNode,
		NodeIdx:     nodeIdx,
		Type:        TaskTypeNodeResume,
		Payload: &node.SignalPayload{
			Triggered: node.SignalReceived,
			Name:      name,
			Data:      data,
		},
	})
}

// TimeoutNode directly enqueues a resume task with TimeoutFired trigger for a
// suspended node. Unlike DeliverSignal, this bypasses signal name matching —
// used by the Timeout Monitor when a node's deadline expires.
func (e *Engine) TimeoutNode(ctx context.Context, id types.ExecutionID, nodeName string) error {
	// Check graph existence before acquiring the resume lock.
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()
	if g == nil {
		var err error
		g, err = e.state.LoadGraph(ctx, id)
		if err != nil || g == nil {
			return err
		}
		// Verify execution is still active before caching and proceeding.
		snap, err := e.state.GetExecution(ctx, id)
		if err != nil || snap == nil || isTerminal(string(snap.Status)) {
			return err
		}
		e.mu.Lock()
		e.graphs[id] = g
		e.mu.Unlock()
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, nodeName)
	if err != nil || !acquired {
		return err
	}

	nodeIdx := g.Index[nodeName]
	return e.queue.Enqueue(ctx, &Task{
		ExecutionID: id,
		NodeName:    nodeName,
		NodeIdx:     nodeIdx,
		Type:        TaskTypeNodeResume,
		Payload: &node.SignalPayload{
			Triggered: node.TimeoutFired,
			Name:      "_timeout",
		},
	})
}

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()

	if err := e.state.UpdateExecutionStatus(ctx, id, types.StatusCanceling, ""); err != nil {
		return err
	}

	if g != nil {
		suspendedNodes, _ := e.state.ListSuspendedNodes(ctx, id)
		for _, nodeName := range suspendedNodes {
			_ = e.state.UpsertNode(ctx, &NodeSnapshot{
				ExecutionID: id,
				Name:        nodeName,
				NodeIdx:     g.Index[nodeName],
				Status:      "canceled",
			})
		}
	}

	_ = e.state.UpdateExecutionStatus(ctx, id, types.StatusCanceled, "")
	if e.hooks != nil {
		safeHook(ctx, e.logger, func(ctx context.Context) {
			e.hooks.OnExecutionComplete(ctx, id, types.StatusCanceled)
		})
	}

	e.mu.Lock()
	delete(e.graphs, id)
	e.mu.Unlock()
	return nil
}

// RevokeSignal atomically revokes a previously delivered signal that has not
// yet been consumed by a suspended node. Returns ErrSignalConsumed if the
// signal was already consumed or does not exist.
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error {
	revoked, err := e.state.RevokeSignal(ctx, id, signalName)
	if err != nil {
		return err
	}
	if !revoked {
		return ErrSignalConsumed
	}
	if e.hooks != nil {
		safeHook(ctx, e.logger, func(ctx context.Context) {
			e.hooks.OnSignalRevoked(ctx, id, signalName)
		})
	}
	return nil
}

// State returns the StateBackend used by this engine.
// Useful for callers that need to poll execution status (e.g. cluster Wait).
func (e *Engine) State() StateBackend { return e.state }
func isTerminal(status string) bool {
	switch status {
	case "success", "failed", "skipped", "canceled", "continued":
		return true
	}
	return false
}
