package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// ErrSignalConsumed is returned when a signal revocation fails because the
// signal was already consumed by a suspended node or was never delivered.
var ErrSignalConsumed = errors.New("signal already consumed or not found")

// ErrExecutionInactive is returned when a runner-facing operation targets an
// execution that has already completed, was canceled, or has been cleaned up.
var ErrExecutionInactive = errors.New("execution inactive")

// ErrInvalidLeaseToken is returned when a runner commits with a stale or
// unknown lease token.
var ErrInvalidLeaseToken = errors.New("invalid lease token")

// Option configures an Engine at construction time.
type Option func(*Engine)

// WithHooks sets the lifecycle hook receiver.
func WithHooks(h Hooks) Option {
	return func(e *Engine) { e.hooks = h }
}

// WithLogger sets the logger.
func WithLogger(l Logger) Option {
	return func(e *Engine) { e.logger = l }
}

// Engine is the pure-algorithm workflow execution engine.
// It has zero IO dependencies — all persistence and queuing are injected via interfaces.
type Engine struct {
	state  StateStore
	queue  TaskQueue
	hooks  Hooks
	logger Logger

	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph
}

// New creates an Engine wired to the given state store and task queue.
func New(state StateStore, queue TaskQueue, opts ...Option) *Engine {
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
func (e *Engine) Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	id := types.ExecutionID("exec-" + uuid.New().String())

	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: params,
	}
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	if err := e.state.CreateExecution(ctx, snap); err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}

	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()

	if g.AllowCycles {
		nd := g.Nodes[g.StartIdx]
		task := &Task{
			ExecutionID:  id,
			NodeName:     nd.Name,
			NodeIdx:      g.StartIdx,
			Type:         TaskTypeNodeExec,
			ActivationID: 1,
		}
		if err := e.queue.Enqueue(ctx, task); err != nil {
			_ = e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, fmt.Sprintf("enqueue start node %s: %v", nd.Name, err))
			e.mu.Lock()
			delete(e.graphs, id)
			e.mu.Unlock()
			return "", fmt.Errorf("enqueue start node %s: %w", nd.Name, err)
		}
		return id, nil
	}

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
				_ = e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, fmt.Sprintf("enqueue root node %s: %v", nd.Name, err))
				e.mu.Lock()
				delete(e.graphs, id)
				e.mu.Unlock()
				return "", fmt.Errorf("enqueue root node %s: %w", nd.Name, err)
			}
		}
	}
	return id, nil
}

// Invoke starts a new execution from one explicit entry node.
func (e *Engine) Invoke(ctx context.Context, g *graph.Graph, entryName string, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	entryIdx, ok := g.EntryIndexes[entryName]
	if !ok {
		return "", fmt.Errorf("entry node %q not found", entryName)
	}

	id := types.ExecutionID("exec-" + uuid.New().String())
	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: params,
	}
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	if err := e.state.CreateExecution(ctx, snap); err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}

	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()

	nd := g.Nodes[entryIdx]
	task := &Task{
		ExecutionID:  id,
		NodeName:     nd.Name,
		NodeIdx:      entryIdx,
		Type:         TaskTypeNodeExec,
		ActivationID: 1,
	}
	if err := e.queue.Enqueue(ctx, task); err != nil {
		_ = e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, fmt.Sprintf("enqueue entry node %s: %v", nd.Name, err))
		e.mu.Lock()
		delete(e.graphs, id)
		e.mu.Unlock()
		return "", fmt.Errorf("enqueue entry node %s: %w", nd.Name, err)
	}
	return id, nil
}

// BuildTaskLease assembles a runner-facing task lease from a queued task.
// The lease includes both handler routing metadata and the concrete input, so
// runner-side code does not need to access graph or state internals.
func (e *Engine) BuildTaskLease(ctx context.Context, t *Task) (*TaskLease, error) {
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrExecutionInactive
	}

	leaseID := LeaseID("lease-" + uuid.New().String())
	leaseToken := LeaseToken("token-" + uuid.New().String())
	attempt, started, err := e.acquireNodeLease(ctx, g, t, leaseID, leaseToken)
	if err != nil {
		return nil, err
	}
	if started && e.hooks != nil {
		e.hooks.OnNodeStart(ctx, t.ExecutionID, t.NodeName)
	}

	meta := g.Nodes[t.NodeIdx]
	return &TaskLease{
		LeaseID:     leaseID,
		LeaseToken:  leaseToken,
		Attempt:     attempt,
		Task:        *t,
		Input:       e.buildInput(ctx, t, g),
		NodeType:    meta.Type,
		NodeVersion: meta.Version,
	}, nil
}

// CommitTaskResult validates a runner lease token, persists the task result,
// and advances scheduling. Stale tokens are rejected so an older assignment
// cannot overwrite or advance state after a newer lease has been issued.
func (e *Engine) CommitTaskResult(ctx context.Context, lease *TaskLease, result TaskResult) error {
	if lease == nil {
		return ErrInvalidLeaseToken
	}
	t := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}

	ns, valid, err := e.state.ClaimTaskLease(ctx, lease)
	if err != nil {
		return err
	}
	if !valid {
		return ErrInvalidLeaseToken
	}
	if types.IsTerminalNodeStatus(ns.Status) {
		return nil
	}

	if result.Suspend != nil {
		return e.commitSuspendResult(ctx, t, result)
	}

	meta := g.Nodes[t.NodeIdx]
	return e.finalizeNode(ctx, t, g, meta, result.Output, result.Error)
}

func (e *Engine) loadActiveGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, bool, error) {
	e.mu.RLock()
	g, ok := e.graphs[id]
	e.mu.RUnlock()
	if ok {
		return g, true, nil
	}

	g, err := e.state.LoadGraph(ctx, id)
	if err != nil {
		return nil, false, fmt.Errorf("load graph for %q: %w", id, err)
	}
	if g == nil {
		return nil, false, nil
	}

	snap, err := e.state.GetExecution(ctx, id)
	if err != nil || snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
		return nil, false, err
	}

	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()
	return g, true, nil
}

func (e *Engine) acquireNodeLease(ctx context.Context, g *graph.Graph, t *Task, leaseID LeaseID, leaseToken LeaseToken) (int, bool, error) {
	ns, err := e.state.GetNode(ctx, t.ExecutionID, t.NodeName)
	if err != nil {
		return 0, false, err
	}
	if g.AllowCycles && t.ActivationID <= 0 {
		return 0, false, ErrExecutionInactive
	}
	if ns != nil && g.AllowCycles && ns.ActivationID > t.ActivationID {
		return 0, false, ErrExecutionInactive
	}
	if ns != nil && types.IsTerminalNodeStatus(ns.Status) && (!g.AllowCycles || ns.ActivationID >= t.ActivationID) {
		return 0, false, ErrExecutionInactive
	}
	if ns != nil && ns.Status == types.NodeStatusCommitting {
		return 0, false, ErrExecutionInactive
	}

	started := ns == nil || ns.Status != types.NodeStatusRunning
	attempt := 1
	if ns != nil {
		attempt = ns.Attempt + 1
	}

	if err := e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID:  t.ExecutionID,
		Name:         t.NodeName,
		NodeIdx:      t.NodeIdx,
		Status:       types.NodeStatusRunning,
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
		Attempt:      attempt,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
	}); err != nil {
		return 0, false, err
	}
	return attempt, started, nil
}

func (e *Engine) commitSuspendResult(ctx context.Context, t *Task, result TaskResult) error {
	if result.Output != nil && result.Output.Resuspend {
		return e.commitResuspendResult(ctx, t, result)
	}

	payload, err := e.state.SuspendOrConsume(ctx, t.ExecutionID, t.NodeName, result.Suspend)
	if err != nil {
		return err
	}
	if payload != nil {
		_ = e.state.UpsertNode(ctx, &NodeSnapshot{
			ExecutionID:  t.ExecutionID,
			Name:         t.NodeName,
			NodeIdx:      t.NodeIdx,
			Status:       types.NodeStatusSuspended,
			ActivationID: t.ActivationID,
			AutoDepth:    t.AutoDepth,
		})
		return e.enqueueResume(ctx, t.ExecutionID, t.NodeName, t.NodeIdx, payload)
	}

	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID:  t.ExecutionID,
		Name:         t.NodeName,
		NodeIdx:      t.NodeIdx,
		Status:       types.NodeStatusSuspended,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
	})
	if err := e.scheduleSuspendResumes(ctx, t, result.Suspend); err != nil {
		return err
	}
	if e.hooks != nil {
		e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
	}
	return nil
}

func (e *Engine) commitResuspendResult(ctx context.Context, t *Task, result TaskResult) error {
	if result.Output != nil && result.Output.Data != nil {
		_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, result.Output.Data)
	}
	if t.Payload == nil {
		return fmt.Errorf("resuspend result for %s without resume payload", t.NodeName)
	}
	if len(result.Suspend.Signals) == 0 {
		return fmt.Errorf("resuspend: empty signals")
	}

	payload, err := e.state.ResuspendAtomic(ctx, t.ExecutionID, t.NodeName, t.Payload.Name, result.Suspend.Signals[0], result.Suspend)
	if err != nil {
		return err
	}
	if payload != nil {
		_ = e.state.UpsertNode(ctx, &NodeSnapshot{
			ExecutionID:  t.ExecutionID,
			Name:         t.NodeName,
			NodeIdx:      t.NodeIdx,
			Status:       types.NodeStatusSuspended,
			ActivationID: t.ActivationID,
			AutoDepth:    t.AutoDepth,
		})
		return e.enqueueResume(ctx, t.ExecutionID, t.NodeName, t.NodeIdx, payload)
	}

	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID:  t.ExecutionID,
		Name:         t.NodeName,
		NodeIdx:      t.NodeIdx,
		Status:       types.NodeStatusSuspended,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
	})
	if err := e.scheduleSuspendResumes(ctx, t, result.Suspend); err != nil {
		return err
	}
	if e.hooks != nil {
		e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
	}
	return nil
}

func (e *Engine) scheduleSuspendResumes(ctx context.Context, t *Task, spec *types.SuspendSpec) error {
	if spec == nil {
		return nil
	}
	if spec.Mode == types.ModeTimer {
		if err := e.enqueueResumeAfter(ctx, t, spec.Timer, &types.SignalPayload{
			Triggered: types.TimerFired,
			Name:      "_timer",
		}); err != nil {
			return err
		}
	}
	if spec.Timeout > 0 {
		if err := e.enqueueResumeAfter(ctx, t, spec.Timeout, &types.SignalPayload{
			Triggered: types.TimeoutFired,
			Name:      "_timeout",
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) enqueueResumeAfter(ctx context.Context, t *Task, delay time.Duration, payload *types.SignalPayload) error {
	task := &Task{
		ExecutionID:  t.ExecutionID,
		NodeName:     t.NodeName,
		NodeIdx:      t.NodeIdx,
		Type:         TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: t.ActivationID,
		AutoDepth:    0,
	}
	if delay <= 0 {
		return e.queue.Enqueue(ctx, task)
	}
	return e.queue.EnqueueDelayed(ctx, task, delay)
}

func (e *Engine) enqueueResume(ctx context.Context, id types.ExecutionID, nodeName string, nodeIdx int, payload *types.SignalPayload) error {
	return e.queue.Enqueue(ctx, &Task{
		ExecutionID:  id,
		NodeName:     nodeName,
		NodeIdx:      nodeIdx,
		Type:         TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: currentActivationID(ctx, e.state, id, nodeName),
		AutoDepth:    0,
	})
}

// finalizeNode persists the success result and triggers downstream scheduling.
func (e *Engine) finalizeNode(ctx context.Context, t *Task, g *graph.Graph, meta graph.NodeMeta, output *types.Output, sysErr error) error {
	if sysErr != nil || (output != nil && output.Error != nil) {
		var bizErr *types.Error
		if output != nil {
			bizErr = output.Error
		}
		return e.handleNodeError(ctx, t, g, sysErr, output, bizErr)
	}

	data := map[string]any{}
	if output != nil && output.Data != nil {
		data = output.Data
	}

	// Loop/Split expansion: intercept and spawn sub-executions.
	if isLoopSplitOutput(data) {
		return e.expandLoopSplit(ctx, t, g, data)
	}

	port := "main"
	if output != nil && output.Port != "" {
		port = output.Port
	}

	_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, data)
	leaseID, leaseToken, attempt := e.currentLease(ctx, t.ExecutionID, t.NodeName)
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID:  t.ExecutionID,
		Name:         t.NodeName,
		NodeIdx:      t.NodeIdx,
		Status:       types.NodeStatusSuccess,
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
		Attempt:      attempt,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
		Output:       data,
		Port:         port,
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, types.NodeStatusSuccess)
	}

	return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, port, data)
}

// handleNodeError applies the node's OnError strategy and either aborts the
// execution or routes to the appropriate output port. Retryable failures are
// re-enqueued via tryRetry first; only once the retry budget is exhausted
// (or the error is marked permanent) does control fall through to OnError.
func (e *Engine) handleNodeError(ctx context.Context, t *Task, g *graph.Graph, sysErr error, output *types.Output, bizErr *types.Error) error {
	meta := g.Nodes[t.NodeIdx]
	if retried, err := e.tryRetry(ctx, t, meta, sysErr); err != nil {
		if e.logger != nil {
			e.logger.Error("retry enqueue failed", "err", err, "exec", string(t.ExecutionID), "node", t.NodeName)
		}
		// fall through to OnError so the workflow doesn't hang.
	} else if retried {
		return nil
	}
	outcome := ApplyOnError(meta.OnError, sysErr, bizErr, output)

	_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, outcome.Output)
	leaseID, leaseToken, attempt := e.currentLease(ctx, t.ExecutionID, t.NodeName)
	_ = e.state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID:  t.ExecutionID,
		Name:         t.NodeName,
		NodeIdx:      t.NodeIdx,
		Status:       outcome.NodeStatus,
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
		Attempt:      attempt,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
		Output:       outcome.Output,
		Port:         outcome.RoutePort,
		Error:        outcome.ErrorMessage,
	})

	if e.hooks != nil {
		e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, outcome.NodeStatus)
	}

	if outcome.ExecFatal {
		_ = e.state.UpdateExecutionStatus(ctx, t.ExecutionID, types.ExecutionStatusFailed, outcome.ErrorMessage)
		if e.hooks != nil {
			e.hooks.OnExecutionComplete(ctx, t.ExecutionID, types.ExecutionStatusFailed)
		}
		e.mu.Lock()
		delete(e.graphs, t.ExecutionID)
		e.mu.Unlock()
		return nil
	}

	return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, outcome.RoutePort, outcome.Output)
}

// tryRetry decides whether the failed task should be re-enqueued after a
// backoff delay. Returns (retried=true, nil) when the task was re-enqueued;
// (false, nil) means the caller should proceed with the OnError strategy.
func (e *Engine) tryRetry(ctx context.Context, t *Task, meta graph.NodeMeta, sysErr error) (bool, error) {
	if sysErr == nil {
		return false, nil // business errors are routed via OnError, never retried here
	}
	settings := retryFor(meta)
	if settings == nil {
		return false, nil
	}
	if types.IsPermanent(sysErr) {
		return false, nil
	}
	_, _, currentAttempt := e.currentLease(ctx, t.ExecutionID, t.NodeName)
	if currentAttempt >= settings.MaxAttempts {
		return false, nil
	}
	if err := e.state.ResetNodeForRetry(ctx, t.ExecutionID, t.NodeName); err != nil {
		return false, err
	}
	delay := retryBackoff(currentAttempt, settings, t.ExecutionID, t.NodeName)
	retryTask := &Task{
		ExecutionID:  t.ExecutionID,
		NodeName:     t.NodeName,
		NodeIdx:      t.NodeIdx,
		Type:         TaskTypeNodeExec,
		ActivationID: t.ActivationID,
		AutoDepth:    t.AutoDepth,
	}
	if err := e.queue.EnqueueDelayed(ctx, retryTask, delay); err != nil {
		return false, err
	}
	if e.hooks != nil {
		safeHook(ctx, e.logger, func(c context.Context) {
			e.hooks.OnNodeRetry(c, t.ExecutionID, t.NodeName, currentAttempt, delay)
		})
	}
	return true, nil
}

func (e *Engine) currentLease(ctx context.Context, id types.ExecutionID, nodeName string) (LeaseID, LeaseToken, int) {
	ns, err := e.state.GetNode(ctx, id, nodeName)
	if err != nil || ns == nil {
		return "", "", 0
	}
	return ns.LeaseID, ns.LeaseToken, ns.Attempt
}

func currentActivationID(ctx context.Context, state StateStore, id types.ExecutionID, nodeName string) int {
	ns, err := state.GetNode(ctx, id, nodeName)
	if err != nil || ns == nil {
		return 0
	}
	return ns.ActivationID
}

// buildInput assembles the types.Input from graph metadata and upstream outputs.
func (e *Engine) buildInput(ctx context.Context, t *Task, g *graph.Graph) *types.Input {
	var runtime *types.Runtime
	if snap, _ := e.state.GetExecution(ctx, t.ExecutionID); snap != nil && snap.Runtime != nil {
		runtime = snap.Runtime
	}
	input := &types.Input{
		Params:      g.Nodes[t.NodeIdx].Parameters,
		Vars:        mergeVars(g.Vars, runtimeVars(runtime)),
		Config:      g.Config,
		Runtime:     cloneRuntime(runtime),
		ExecutionID: string(t.ExecutionID),
		NodeName:    t.NodeName,
	}

	if t.Type == TaskTypeNodeResume {
		data, _ := e.state.GetOutput(ctx, t.ExecutionID, t.NodeName)
		if data != nil {
			input.Data = data
			return input
		}
	}

	inEdges := g.InEdges[t.NodeIdx]
	if g.AllowCycles && t.NodeIdx == g.StartIdx && t.ActivationID == 1 {
		if snap, _ := e.state.GetExecution(ctx, t.ExecutionID); snap != nil {
			input.Data = snap.Params
		}
		return input
	}
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

func runtimeVars(runtime *types.Runtime) map[string]any {
	if runtime == nil {
		return nil
	}
	return runtime.Vars
}

func mergeVars(staticVars map[string]any, runtimeVars map[string]any) map[string]any {
	if staticVars == nil && runtimeVars == nil {
		return nil
	}
	merged := cloneMap(staticVars)
	if merged == nil {
		merged = make(map[string]any, len(runtimeVars))
	}
	for k, v := range runtimeVars {
		merged[k] = v
	}
	return merged
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

// DeliverSignal routes an external signal to the appropriate suspended node
// and enqueues a resume task if the node is ready.
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	resumeNode, payload, err := e.state.DeliverSignal(ctx, id, name, data)
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
		if err != nil || snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
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
	if payload == nil {
		payload = &types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      name,
			Data:      data,
		}
	}
	return e.queue.Enqueue(ctx, &Task{
		ExecutionID:  id,
		NodeName:     resumeNode,
		NodeIdx:      nodeIdx,
		Type:         TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: currentActivationID(ctx, e.state, id, resumeNode),
		AutoDepth:    0,
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
		if err != nil || snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
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
		Payload: &types.SignalPayload{
			Triggered: types.TimeoutFired,
			Name:      "_timeout",
		},
		ActivationID: currentActivationID(ctx, e.state, id, nodeName),
		AutoDepth:    0,
	})
}

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		return err
	}

	if g != nil {
		suspendedNodes, _ := e.state.ListSuspendedNodes(ctx, id)
		for _, nodeName := range suspendedNodes {
			_ = e.state.UpsertNode(ctx, &NodeSnapshot{
				ExecutionID: id,
				Name:        nodeName,
				NodeIdx:     g.Index[nodeName],
				Status:      types.NodeStatusCanceled,
			})
		}
	}

	_ = e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "")
	if e.hooks != nil {
		safeHook(ctx, e.logger, func(ctx context.Context) {
			e.hooks.OnExecutionComplete(ctx, id, types.ExecutionStatusCanceled)
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

// State returns the StateStore used by this engine.
// Useful for callers that need to poll execution status (e.g. cluster Wait).
func (e *Engine) State() StateStore { return e.state }
