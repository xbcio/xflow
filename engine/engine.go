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

// ErrSuspendUnsupported is returned when a runtime mode disables suspend nodes.
var ErrSuspendUnsupported = errors.New("xflow: suspend nodes are unsupported in transient execution mode")

// ErrLeaseAlreadyActive is returned when a task already has an unexpired
// running lease. Callers should retry after the lease is committed or reclaimed.
var ErrLeaseAlreadyActive = errors.New("lease already active")

// ErrLeaseNotRecoverable reports that no current, replay-safe lease exists for
// a task. It is distinct from ErrExecutionInactive so control-plane recovery
// can requeue a still-running assignment without treating the workflow as
// terminal.
var ErrLeaseNotRecoverable = errors.New("lease is not recoverable")

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

// WithDefaultLeaseTTL overrides the default deadline applied to every issued
// task lease. Runners must finish (and call CommitTaskResult or
// ReportResult) before IssuedAt+TTL or the lease sweeper will reclaim the
// task. A non-positive value disables the feature (no deadline, no sweep).
func WithDefaultLeaseTTL(ttl time.Duration) Option {
	return func(e *Engine) { e.defaultLeaseTTL = ttl }
}

// WithSuspendDisabled makes runtime suspend requests fail the leased task
// instead of parking it. If err is nil, ErrSuspendUnsupported is used.
func WithSuspendDisabled(err error) Option {
	return func(e *Engine) {
		e.suspendDisabled = true
		if err == nil {
			err = ErrSuspendUnsupported
		}
		e.suspendDisabledErr = err
	}
}

// DefaultLeaseTTL is the lease deadline applied when an Engine is constructed
// without an explicit override. It is intentionally short enough that crashed
// runners are reclaimed within a minute while leaving slow handlers (e.g.
// HTTP timeouts) headroom.
const DefaultLeaseTTL = 60 * time.Second

// Engine is the pure-algorithm workflow execution engine.
// It has zero IO dependencies — all persistence and queuing are injected via interfaces.
type Engine struct {
	state                    StateStore
	queue                    TaskQueue
	hooks                    Hooks
	logger                   Logger
	commitObserver           CommitObserver
	outboxObserver           OutboxObserver
	outboxMaxDeliveryAttempt int
	defaultLeaseTTL          time.Duration
	suspendDisabled          bool
	suspendDisabledErr       error

	mu     sync.RWMutex
	graphs map[types.ExecutionID]*graph.Graph
}

// New creates an Engine wired to the given state store and task queue.
func New(state StateStore, queue TaskQueue, opts ...Option) *Engine {
	e := &Engine{
		state:                    state,
		queue:                    queue,
		graphs:                   make(map[types.ExecutionID]*graph.Graph),
		outboxMaxDeliveryAttempt: DefaultOutboxMaxDeliveryAttempts,
		defaultLeaseTTL:          DefaultLeaseTTL,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// LeaseTTL returns the deadline applied to leases issued by this engine.
// Exposed so sweepers running on the same process can pick the same value.
func (e *Engine) LeaseTTL() time.Duration { return e.defaultLeaseTTL }

// EvictExecution removes an execution's in-process graph cache. Backends that
// expire runtime state independently can call this so late runner callbacks
// observe the execution as inactive instead of using stale cached graph data.
func (e *Engine) EvictExecution(id types.ExecutionID) {
	e.mu.Lock()
	delete(e.graphs, id)
	e.mu.Unlock()
}

// Submit starts a new execution of the given graph with the provided params.
// It persists the execution snapshot, caches the graph, and schedules all
// root nodes (in-degree == 0) through a durable outbox when the StateStore
// implements AtomicStateStore.
func (e *Engine) Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	id := types.ExecutionID("exec-" + uuid.New().String())
	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: params,
	}
	attachTraceMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	return e.startExecution(ctx, snap, submitInitialTasks(id, g))
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
	attachTraceMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	entry := g.Nodes[entryIdx]
	return e.startExecution(ctx, snap, []initialTask{{
		task: Task{
			ExecutionID:  id,
			NodeName:     entry.Name,
			NodeIdx:      entryIdx,
			Type:         TaskTypeNodeExec,
			ActivationID: 1,
		},
		operation: fmt.Sprintf("enqueue entry node %q", entry.Name),
	}})
}

// failInitialExecution marks an execution failed after its initial task could
// not be queued. Cache eviction is intentionally deferred until the
// authoritative terminal status was persisted.
func (e *Engine) failInitialExecution(ctx context.Context, id types.ExecutionID, operation string, cause error) error {
	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusFailed, fmt.Sprintf("%s: %v", operation, cause)); err != nil {
		return errors.Join(
			fmt.Errorf("%s: %w", operation, cause),
			fmt.Errorf("mark execution %q failed: %w", id, err),
		)
	}
	e.EvictExecution(id)
	return fmt.Errorf("%s: %w", operation, cause)
}

// BuildTaskLease assembles a runner-facing task lease from a queued task.
// The lease includes both handler routing metadata and the concrete input, so
// runner-side code does not need to access graph or state internals.
func (e *Engine) BuildTaskLease(ctx context.Context, t *Task) (*TaskLease, error) {
	if t == nil {
		return nil, fmt.Errorf("build task lease: nil task")
	}
	if handled, err := e.HandleSystemTask(ctx, t); err != nil {
		return nil, err
	} else if handled {
		return nil, ErrSystemTaskHandled
	}

	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrExecutionInactive
	}
	if _, err := e.checkTaskRouteActive(ctx, g, t); err != nil {
		return nil, err
	}

	leaseID := LeaseID("lease-" + uuid.New().String())
	leaseToken := LeaseToken("token-" + uuid.New().String())
	issuedAt := time.Now().UTC()
	ttl := e.defaultLeaseTTL
	lease := &TaskLease{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Task:       *t,
		IssuedAt:   issuedAt,
		TTL:        ttl,
	}
	prev, acquired, err := e.state.AcquireTaskLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, e.classifyLeaseAcquireFailure(g, t, prev, issuedAt)
	}

	lease.Attempt = 1
	if prev != nil {
		lease.Attempt = prev.Attempt + 1
	}

	meta := g.Nodes[t.NodeIdx]
	input, err := e.buildInput(ctx, t, g)
	if err != nil {
		released, releaseErr := e.ReleaseTaskLease(ctx, lease)
		if releaseErr != nil {
			return nil, fmt.Errorf("build input for %q/%q: %w (release lease: %v)", t.ExecutionID, t.NodeName, err, releaseErr)
		}
		if !released {
			return nil, fmt.Errorf("build input for %q/%q: %w (lease was no longer active)", t.ExecutionID, t.NodeName, err)
		}
		return nil, fmt.Errorf("build input for %q/%q: %w", t.ExecutionID, t.NodeName, err)
	}
	lease.Input = input
	lease.NodeType = meta.Type
	lease.NodeVersion = meta.Version

	started := prev == nil || prev.Status != types.NodeStatusRunning
	if started && e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnNodeStart(hookCtx, t.ExecutionID, t.NodeName)
		})
	}
	return lease, nil
}

// RecoverTaskLease rebuilds the runner-facing representation of an already
// issued lease without mutating node state. It closes the durable handoff gap
// where a control-plane process crashes after BuildTaskLease has committed the
// engine lease but before the RunnerDirectory records or delivers it.
//
// The returned lease preserves the existing ID, token, attempt, issue time,
// and TTL. Callers must only use it to replay the same task; issuing a fresh
// lease remains the responsibility of BuildTaskLease after fenced revocation.
func (e *Engine) RecoverTaskLease(ctx context.Context, task *Task) (*TaskLease, error) {
	if task == nil {
		return nil, fmt.Errorf("recover task lease: nil task")
	}
	if task.Type == TaskTypeNodeAdvance || task.Type == TaskTypeNodeSkip || task.Type == TaskTypeNodeBatch {
		return nil, ErrLeaseNotRecoverable
	}
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return nil, err
	}
	if !active || task.NodeIdx < 0 || task.NodeIdx >= len(g.Nodes) || g.Nodes[task.NodeIdx].Name != task.NodeName {
		return nil, ErrExecutionInactive
	}

	node, err := e.state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil {
		return nil, fmt.Errorf("recover task lease %q/%q: get node: %w", task.ExecutionID, task.NodeName, err)
	}
	if node == nil || node.Status != types.NodeStatusRunning || node.LeaseID == "" || node.LeaseToken == "" {
		return nil, ErrLeaseNotRecoverable
	}
	if task.ActivationID > 0 && node.ActivationID != task.ActivationID {
		return nil, ErrExecutionInactive
	}
	if !node.LeaseIssuedAt.IsZero() && node.LeaseTTL > 0 && !time.Now().Before(node.LeaseIssuedAt.Add(node.LeaseTTL)) {
		return nil, ErrLeaseNotRecoverable
	}

	input, err := e.buildInput(ctx, task, g)
	if err != nil {
		return nil, fmt.Errorf("recover task lease %q/%q: build input: %w", task.ExecutionID, task.NodeName, err)
	}
	meta := g.Nodes[task.NodeIdx]
	return &TaskLease{
		LeaseID:     node.LeaseID,
		LeaseToken:  node.LeaseToken,
		Attempt:     node.Attempt,
		Task:        *task,
		Input:       input,
		NodeType:    meta.Type,
		NodeVersion: meta.Version,
		IssuedAt:    node.LeaseIssuedAt,
		TTL:         node.LeaseTTL,
	}, nil
}

// TaskRouting returns runner placement metadata for a queued task without
// issuing a lease or mutating node attempt state.
func (e *Engine) TaskRouting(ctx context.Context, t *Task) (TaskRouting, error) {
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return TaskRouting{}, err
	}
	if !active {
		return TaskRouting{}, ErrExecutionInactive
	}
	if _, err := e.checkTaskRouteActive(ctx, g, t); err != nil {
		return TaskRouting{}, err
	}
	meta := g.Nodes[t.NodeIdx]
	return TaskRouting{
		NodeType:       meta.Type,
		NodeVersion:    meta.Version,
		RunnerSelector: cloneRunnerSelector(meta.RunnerSelector),
	}, nil
}

func cloneRunnerSelector(selector *types.RunnerSelector) *types.RunnerSelector {
	if selector == nil {
		return nil
	}
	out := &types.RunnerSelector{
		Mode: selector.Mode,
	}
	if len(selector.MatchLabels) > 0 {
		out.MatchLabels = make(map[string]string, len(selector.MatchLabels))
		for key, value := range selector.MatchLabels {
			out.MatchLabels[key] = value
		}
	}
	return out
}

// CommitTaskResult validates a runner lease token, persists the task result,
// and advances scheduling. Stale tokens are rejected so an older assignment
// cannot overwrite or advance state after a newer lease has been issued.
func (e *Engine) CommitTaskResult(ctx context.Context, lease *TaskLease, result TaskResult) error {
	_, err := e.CommitTaskResultWithOutcome(ctx, lease, result)
	return err
}

// CommitTaskResultWithOutcome validates a runner lease token, persists the
// task result, advances scheduling, and returns a structured outcome for
// control-plane cleanup.
func (e *Engine) CommitTaskResultWithOutcome(ctx context.Context, lease *TaskLease, result TaskResult) (outcome CommitOutcome, err error) {
	defer func() {
		e.notifyCommitOutcome(ctx, outcome)
	}()

	if lease == nil {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	t := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}

	if result.Suspend != nil && e.suspendDisabled {
		if err := e.CommitTaskFailure(ctx, lease, e.suspendDisabledErr); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	}

	if !g.AllowCycles && result.Suspend == nil && (result.Output == nil || !isLoopSplitOutput(result.Output.Data)) {
		return e.commitAcyclicTaskResult(ctx, lease, g, result)
	}
	if result.Suspend != nil {
		return e.commitSuspendedTaskResult(ctx, lease, result)
	}
	return e.commitLegacyTaskResult(ctx, lease, g, result)
}

// CommitTaskFailure forces a leased task to fail the whole execution without
// applying the node's OnError strategy. Backend adapters use this for runtime
// failures that make the execution mode itself incompatible with the task.
func (e *Engine) CommitTaskFailure(ctx context.Context, lease *TaskLease, failure error) error {
	if lease == nil {
		return ErrInvalidLeaseToken
	}
	if failure == nil {
		failure = errors.New("task failed")
	}
	t := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if !g.AllowCycles {
		return e.commitAcyclicFailure(ctx, lease, failure)
	}
	outcome, err := e.commitLegacyNode(ctx, lease, types.NodeStatusFailed, nil, "", failure.Error(), true)
	if err != nil {
		return err
	}
	if outcome == CommitOutcomeStaleToken {
		return ErrInvalidLeaseToken
	}
	return nil
}

// commitLegacyTaskResult handles cyclic and experimental loop/split outputs.
// Ordinary terminal transitions use a backend-owned token fence directly,
// avoiding a long-lived committing state. Suspend and expansion retain the
// explicit claim protocol because they have additional coordination state.
func (e *Engine) commitLegacyTaskResult(ctx context.Context, lease *TaskLease, g *graph.Graph, result TaskResult) (CommitOutcome, error) {
	task := &lease.Task
	meta := g.Nodes[task.NodeIdx]
	if result.Error != nil || (result.Output != nil && result.Output.Error != nil) {
		var businessErr *types.Error
		if result.Output != nil {
			businessErr = result.Output.Error
		}
		return e.commitLegacyNodeError(ctx, lease, meta, result.Error, result.Output, businessErr)
	}
	if retryErr := outputPortRetryError(result.Output); retryErr != nil {
		retried, err := e.tryRetryWithAttempt(ctx, task, meta, retryErr, lease.Attempt, lease.LeaseToken)
		if err != nil {
			return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if retried {
			return CommitOutcomeAccepted, nil
		}
	}

	data := map[string]any{}
	if result.Output != nil && result.Output.Data != nil {
		data = result.Output.Data
	}
	if isLoopSplitOutput(data) {
		node, claimed, err := e.state.ClaimTaskLease(ctx, lease)
		if err != nil {
			return CommitOutcomeTransientError, err
		}
		if !claimed {
			return CommitOutcomeStaleToken, ErrInvalidLeaseToken
		}
		if types.IsTerminalNodeStatus(node.Status) {
			return CommitOutcomeDuplicateTerminal, nil
		}
		if err := e.expandLoopSplit(ctx, lease, g, data); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	}

	port := "main"
	if result.Output != nil && result.Output.Port != "" {
		port = result.Output.Port
	}
	return e.commitLegacyNode(ctx, lease, types.NodeStatusSuccess, data, port, "", false)
}

func (e *Engine) commitLegacyNodeError(ctx context.Context, lease *TaskLease, meta graph.NodeMeta, systemErr error, output *types.Output, businessErr *types.Error) (CommitOutcome, error) {
	if retried, err := e.tryRetryWithAttempt(ctx, &lease.Task, meta, systemErr, lease.Attempt, lease.LeaseToken); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("retry node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	} else if retried {
		return CommitOutcomeAccepted, nil
	}
	outcome := ApplyOnError(meta.OnError, systemErr, businessErr, output)
	return e.commitLegacyNode(ctx, lease, outcome.NodeStatus, outcome.Output, outcome.RoutePort, outcome.ErrorMessage, outcome.ExecFatal)
}

func (e *Engine) commitLegacyNode(ctx context.Context, lease *TaskLease, status types.NodeStatus, output map[string]any, port, errMsg string, fatal bool) (CommitOutcome, error) {
	task := &lease.Task
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}

	// Experimental Loop/Split workflows may be acyclic even though their parent
	// reaches an intermediate waiting state. Once the fenced child generation
	// completes, use the normal atomic commit/outbox path so downstream work is
	// not left to the legacy direct scheduler.
	if !g.AllowCycles {
		return e.commitAcyclicNode(ctx, lease, status, output, port, errMsg, fatal)
	}

	committer, ok := e.state.(LegacyNodeCommitter)
	if !ok {
		return CommitOutcomeTransientError, ErrAtomicCommitUnsupported
	}
	result, err := committer.CommitLeasedNode(ctx, CommitNodeRequest{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		AutoDepth:    task.AutoDepth,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       status,
		Output:       output,
		StoreOutput:  true,
		Port:         port,
		Error:        errMsg,
		Fatal:        fatal,
	})
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	switch result.Outcome {
	case CommitOutcomeAccepted:
		e.notifyNodeComplete(ctx, task.ExecutionID, task.NodeName, status)
		if fatal {
			if err := e.state.UpdateExecutionStatus(ctx, task.ExecutionID, types.ExecutionStatusFailed, errMsg); err != nil {
				return CommitOutcomeTransientError, fmt.Errorf("mark execution %q failed: %w", task.ExecutionID, err)
			}
			e.notifyExecutionComplete(ctx, task.ExecutionID, types.ExecutionStatusFailed)
			e.EvictExecution(task.ExecutionID)
			return CommitOutcomeAccepted, nil
		}
		if err := e.OnNodeComplete(ctx, task.ExecutionID, g, task.NodeIdx, port); err != nil {
			return CommitOutcomeTransientError, err
		}
		return CommitOutcomeAccepted, nil
	case CommitOutcomeDuplicateTerminal:
		return CommitOutcomeDuplicateTerminal, nil
	case CommitOutcomeStaleToken:
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	case CommitOutcomeExecutionInactive:
		return CommitOutcomeExecutionInactive, nil
	default:
		return CommitOutcomeTransientError, fmt.Errorf("commit legacy node %q/%q returned %q", task.ExecutionID, task.NodeName, result.Outcome)
	}
}

// commitSuspendedTaskResult claims the active lease, then delegates the full
// output/signal/status transition to a token-fenced state-store primitive.
func (e *Engine) commitSuspendedTaskResult(ctx context.Context, lease *TaskLease, result TaskResult) (CommitOutcome, error) {
	node, claimed, err := e.state.ClaimTaskLease(ctx, lease)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !claimed {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	if types.IsTerminalNodeStatus(node.Status) {
		return CommitOutcomeDuplicateTerminal, nil
	}

	storeOutput := true
	var output map[string]any
	oldSignalName := ""
	if result.Output != nil && result.Output.Resuspend {
		storeOutput = result.Output.Data != nil
		output = result.Output.Data
		if lease.Task.Payload == nil {
			return CommitOutcomeTransientError, fmt.Errorf("resuspend result for %s without resume payload", lease.Task.NodeName)
		}
		oldSignalName = lease.Task.Payload.Name
	} else if lease.Input != nil {
		output = cloneMap(lease.Input.Data)
	}

	suspender, ok := e.state.(DurableLeaseSuspender)
	if !ok {
		return CommitOutcomeTransientError, ErrAtomicCommitUnsupported
	}
	committed, err := suspender.SuspendTaskLeaseWithOutbox(ctx, lease, output, storeOutput, result.Suspend, oldSignalName)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !committed {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	if err := e.FlushOutbox(ctx, lease.Task.ExecutionID); err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("deliver suspend continuation for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	e.notifyNodeSuspended(ctx, &lease.Task)
	return CommitOutcomeAccepted, nil
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

func (e *Engine) checkTaskRouteActive(ctx context.Context, g *graph.Graph, t *Task) (*NodeSnapshot, error) {
	ns, err := e.state.GetNode(ctx, t.ExecutionID, t.NodeName)
	if err != nil {
		return nil, err
	}
	if g.AllowCycles && t.ActivationID <= 0 {
		return nil, ErrExecutionInactive
	}
	if ns != nil && g.AllowCycles && ns.ActivationID > t.ActivationID {
		return nil, ErrExecutionInactive
	}
	if ns != nil && types.IsTerminalNodeStatus(ns.Status) && (!g.AllowCycles || ns.ActivationID >= t.ActivationID) {
		return nil, ErrExecutionInactive
	}
	if ns != nil && ns.Status == types.NodeStatusCommitting {
		return nil, ErrExecutionInactive
	}
	return ns, nil
}

func (e *Engine) classifyLeaseAcquireFailure(g *graph.Graph, t *Task, ns *NodeSnapshot, now time.Time) error {
	if g.AllowCycles && t.ActivationID <= 0 {
		return ErrExecutionInactive
	}
	if ns != nil && g.AllowCycles && ns.ActivationID > t.ActivationID {
		return ErrExecutionInactive
	}
	if ns != nil && types.IsTerminalNodeStatus(ns.Status) && (!g.AllowCycles || ns.ActivationID >= t.ActivationID) {
		return ErrExecutionInactive
	}
	if ns != nil && ns.Status == types.NodeStatusCommitting {
		return ErrExecutionInactive
	}
	if ns != nil && ns.Status == types.NodeStatusRunning && ns.LeaseToken != "" {
		deadline := ns.LeaseIssuedAt.Add(ns.LeaseTTL)
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 || now.Before(deadline) {
			return ErrLeaseAlreadyActive
		}
	}
	return ErrExecutionInactive
}

func (e *Engine) notifyNodeSuspended(ctx context.Context, t *Task) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeSuspended(hookCtx, t.ExecutionID, t.NodeName)
	})
}

func (e *Engine) notifyNodeComplete(ctx context.Context, id types.ExecutionID, nodeName string, status types.NodeStatus) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeComplete(hookCtx, id, nodeName, status)
	})
}

func (e *Engine) notifyExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnExecutionComplete(hookCtx, id, status)
	})
}

func outputPortRetryError(output *types.Output) error {
	if output == nil || output.Port != "error" || output.Error != nil {
		return nil
	}
	if output.Data != nil {
		if msg, ok := output.Data["error"].(string); ok && msg != "" {
			return errors.New(msg)
		}
	}
	return errors.New("node returned error port")
}

// scheduleRetry resets a current attempt and records the next task. Atomic
// StateStores make the reset and durable delayed intent one transition; legacy
// stores retain the historical direct queue fallback.
func (e *Engine) scheduleRetry(ctx context.Context, task *Task, attempt int, settings *types.RetrySettings, token LeaseToken) (bool, error) {
	delay := retryBackoff(attempt, settings, task.ExecutionID, task.NodeName)
	retryTask := Task{
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		Type:         TaskTypeNodeExec,
		ActivationID: task.ActivationID,
		AutoDepth:    task.AutoDepth,
	}

	if state, ok := e.state.(AtomicStateStore); ok {
		availableAt := time.Now().UTC().Add(delay)
		scheduled, err := state.ResetNodeForRetryWithOutbox(ctx, task.ExecutionID, task.NodeName, token, OutboxEntry{
			ID:          retryOutboxID(task.ExecutionID, task.NodeName, task.ActivationID, attempt),
			Task:        retryTask,
			AvailableAt: availableAt,
		})
		if err != nil {
			return false, fmt.Errorf("reset retry state for %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if !scheduled {
			return false, fmt.Errorf("%w: retry state for %q/%q is no longer active", ErrInvalidLeaseToken, task.ExecutionID, task.NodeName)
		}
		e.notifyNodeRetry(ctx, task.ExecutionID, task.NodeName, attempt, delay)
		if err := e.FlushOutbox(ctx, task.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver retry outbox for %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		return true, nil
	}

	released, err := e.state.RevokeLease(ctx, task.ExecutionID, task.NodeName, token)
	if err != nil {
		return false, fmt.Errorf("reset retry lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	if !released {
		return false, fmt.Errorf("%w: retry lease for %q/%q is no longer active", ErrInvalidLeaseToken, task.ExecutionID, task.NodeName)
	}
	if err := e.queue.EnqueueDelayed(ctx, &retryTask, delay); err != nil {
		return false, err
	}
	e.notifyNodeRetry(ctx, task.ExecutionID, task.NodeName, attempt, delay)
	return true, nil
}

func (e *Engine) notifyNodeRetry(ctx context.Context, id types.ExecutionID, nodeName string, attempt int, delay time.Duration) {
	if e.hooks == nil {
		return
	}
	safeHook(ctx, e.logger, func(hookCtx context.Context) {
		e.hooks.OnNodeRetry(hookCtx, id, nodeName, attempt, delay)
	})
}

func (e *Engine) currentActivationID(ctx context.Context, id types.ExecutionID, nodeName string) (int, error) {
	ns, err := e.state.GetNode(ctx, id, nodeName)
	if err != nil {
		return 0, fmt.Errorf("get node %q/%q: %w", id, nodeName, err)
	}
	if ns == nil {
		return 0, fmt.Errorf("%w: node %q/%q not found", ErrExecutionInactive, id, nodeName)
	}
	return ns.ActivationID, nil
}

// buildInput assembles the types.Input from graph metadata and upstream outputs.
// Backend read failures are authoritative failures: they must never be treated
// as an empty execution or absent upstream business data.
func (e *Engine) buildInput(ctx context.Context, t *Task, g *graph.Graph) (*types.Input, error) {
	snap, err := e.state.GetExecution(ctx, t.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("get execution %q: %w", t.ExecutionID, err)
	}
	if snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
		return nil, ErrExecutionInactive
	}

	runtime := snap.Runtime
	input := &types.Input{
		Params:      cloneMap(g.Nodes[t.NodeIdx].Parameters),
		Vars:        mergeVars(g.Vars, runtimeVars(runtime)),
		Config:      cloneMap(g.Config),
		Runtime:     cloneRuntime(runtime),
		ExecutionID: string(t.ExecutionID),
		NodeName:    t.NodeName,
		TraceID:     snap.TraceID,
		SpanID:      snap.SpanID,
	}

	if t.Type == TaskTypeNodeResume {
		data, err := e.state.GetOutput(ctx, t.ExecutionID, t.NodeName)
		if err != nil {
			return nil, fmt.Errorf("get resumed node output %q/%q: %w", t.ExecutionID, t.NodeName, err)
		}
		input.Data = cloneMap(data)
		return input, nil
	}

	inEdges := g.InEdges[t.NodeIdx]
	if g.AllowCycles && t.NodeIdx == g.StartIdx && t.ActivationID == 1 {
		input.Data = cloneMap(snap.Params)
		return input, nil
	}
	switch len(inEdges) {
	case 0:
		// Root node — inject workflow-level submission params as input.Data so
		// source handlers can read them (mirrors ClusterRunner behaviour).
		input.Data = cloneMap(snap.Params)
	case 1:
		data, err := e.state.GetOutput(ctx, t.ExecutionID, g.Nodes[inEdges[0].SrcIdx].Name)
		if err != nil {
			return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, g.Nodes[inEdges[0].SrcIdx].Name, err)
		}
		input.Data = cloneMap(data)
	default:
		// Fan-in: expose all upstream outputs keyed by node name.
		inputs := make(map[string]any, len(inEdges))
		for _, edge := range inEdges {
			name := g.Nodes[edge.SrcIdx].Name
			data, err := e.state.GetOutput(ctx, t.ExecutionID, name)
			if err != nil {
				return nil, fmt.Errorf("get upstream output %q/%q: %w", t.ExecutionID, name, err)
			}
			inputs[name] = cloneMap(data)
		}
		input.Inputs = inputs
	}
	return input, nil
}

func attachTraceMetadata(ctx context.Context, snap *ExecutionSnapshot) {
	if traceID, ok := TraceIDFromContext(ctx); ok {
		snap.TraceID = traceID
	}
	if spanID, ok := SpanIDFromContext(ctx); ok {
		snap.SpanID = spanID
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

func cloneSignalPayload(payload *types.SignalPayload) *types.SignalPayload {
	if payload == nil {
		return nil
	}
	cp := *payload
	cp.Data = cloneMap(payload.Data)
	if payload.All != nil {
		cp.All = make(map[string]map[string]any, len(payload.All))
		for name, data := range payload.All {
			cp.All[name] = cloneMap(data)
		}
	}
	return &cp
}

// DeliverSignal routes an external signal to the appropriate suspended node
// and enqueues a resume task if the node is ready. When the backend supports
// DurableSignalDeliverer, the signal consumption and resume-task persistence
// happen in one atomic transition so a crash between them cannot lose the
// signal; otherwise the legacy two-step path is used.
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	if durable, ok := e.state.(DurableSignalDeliverer); ok {
		return e.deliverSignalDurable(ctx, id, name, data, durable)
	}
	return e.deliverSignalLegacy(ctx, id, name, data)
}

// deliverSignalDurable peeks the resume target, resolves graph metadata, then
// atomically consumes the signal and persists the resume task in one outbox
// transition. A crash after consumption but before FlushOutbox leaves the
// resume intent durable; the outbox dispatcher redelivers it.
func (e *Engine) deliverSignalDurable(ctx context.Context, id types.ExecutionID, name string, data map[string]any, durable DurableSignalDeliverer) error {
	resumeNode, err := durable.PeekResumeTarget(ctx, id, name)
	if err != nil {
		return err
	}

	var intent ResumeIntent
	if resumeNode != "" {
		g, active, err := e.loadActiveGraph(ctx, id)
		if err != nil {
			return fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
		}
		if !active {
			// The execution completed, was canceled, or was cleaned up while
			// the signal was in flight. Signal delivery is a control/user-facing
			// operation (unlike runner-facing lease/commit paths), so a terminal
			// target is a benign no-op — the signal simply has nowhere to resume.
			// Returning ErrExecutionInactive here would surface as an HTTP 500 on
			// the control plane's signal endpoint for an already-finished workflow.
			return nil
		}
		nodeIdx, ok := g.Index[resumeNode]
		if !ok {
			return fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
		}
		activationID, err := e.currentActivationID(ctx, id, resumeNode)
		if err != nil {
			return fmt.Errorf("read signal target %q/%q: %w", id, resumeNode, err)
		}
		intent = ResumeIntent{NodeName: resumeNode, NodeIdx: nodeIdx, ActivationID: activationID}
	}

	node, _, committed, err := durable.DeliverSignalWithOutbox(ctx, id, name, data, intent)
	if err != nil {
		return err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if !committed || node == "" {
		// Signal stored or multi-signal quorum not yet reached; no resume to deliver.
		return nil
	}
	// Resume task is durably persisted in the outbox; flush delivers it now.
	return e.FlushOutbox(ctx, id)
}

// deliverSignalLegacy is the non-atomic two-step path used by backends that do
// not implement DurableSignalDeliverer (e.g. transient). A crash between signal
// consumption and enqueue can lose the resume — acceptable for non-durable
// backends whose state is already lost on crash.
func (e *Engine) deliverSignalLegacy(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	resumeNode, payload, err := e.state.DeliverSignal(ctx, id, name, data)
	if err != nil {
		return err
	}

	if e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnSignalDelivered(hookCtx, id, name, data)
		})
	}

	if resumeNode == "" {
		// Signal stored; node not yet suspended.
		return nil
	}

	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return fmt.Errorf("load graph for signal %q on %q: %w", name, id, err)
	}
	if !active {
		// Terminal/cleaned-up target: benign no-op for the control/user-facing
		// signal path (see deliverSignalDurable for rationale). The signal was
		// already consumed above; the node is terminal so there is no resume to
		// enqueue.
		return nil
	}
	nodeIdx, ok := g.Index[resumeNode]
	if !ok {
		return fmt.Errorf("signal %q targeted unknown node %q", name, resumeNode)
	}
	activationID, err := e.currentActivationID(ctx, id, resumeNode)
	if err != nil {
		return fmt.Errorf("read signal target %q/%q: %w", id, resumeNode, err)
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, resumeNode)
	if err != nil || !acquired {
		return err
	}

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
		ActivationID: activationID,
		AutoDepth:    0,
	})
}

// TimeoutNode directly enqueues a resume task with TimeoutFired trigger for a
// suspended node. Unlike DeliverSignal, this bypasses signal name matching —
// used by the Timeout Monitor when a node's deadline expires.
func (e *Engine) TimeoutNode(ctx context.Context, id types.ExecutionID, nodeName string) error {
	g, active, err := e.loadActiveGraph(ctx, id)
	if err != nil {
		return fmt.Errorf("load graph for timeout on %q: %w", id, err)
	}
	if !active {
		return ErrExecutionInactive
	}
	nodeIdx, ok := g.Index[nodeName]
	if !ok {
		return fmt.Errorf("timeout targeted unknown node %q", nodeName)
	}
	activationID, err := e.currentActivationID(ctx, id, nodeName)
	if err != nil {
		return fmt.Errorf("read timeout target %q/%q: %w", id, nodeName, err)
	}

	acquired, err := e.state.AcquireResumeLock(ctx, id, nodeName)
	if err != nil || !acquired {
		return err
	}

	return e.queue.Enqueue(ctx, &Task{
		ExecutionID: id,
		NodeName:    nodeName,
		NodeIdx:     nodeIdx,
		Type:        TaskTypeNodeResume,
		Payload: &types.SignalPayload{
			Triggered: types.TimeoutFired,
			Name:      "_timeout",
		},
		ActivationID: activationID,
		AutoDepth:    0,
	})
}

// Cancel marks an execution as canceled, transitions all suspended nodes to
// canceled status, and removes the execution from the in-memory cache.
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
	e.mu.RLock()
	g := e.graphs[id]
	e.mu.RUnlock()
	if g == nil {
		var err error
		g, err = e.state.LoadGraph(ctx, id)
		if err != nil {
			return fmt.Errorf("load graph for canceled execution %q: %w", id, err)
		}
		if g == nil {
			return ErrExecutionInactive
		}
	}

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		return fmt.Errorf("mark execution %q canceling: %w", id, err)
	}

	suspendedNodes, err := e.state.ListSuspendedNodes(ctx, id)
	if err != nil {
		return fmt.Errorf("list suspended nodes for %q: %w", id, err)
	}
	for _, nodeName := range suspendedNodes {
		nodeIdx, ok := g.Index[nodeName]
		if !ok {
			return fmt.Errorf("suspended node %q is not in execution graph", nodeName)
		}
		if err := e.state.UpsertNode(ctx, &NodeSnapshot{
			ExecutionID: id,
			Name:        nodeName,
			NodeIdx:     nodeIdx,
			Status:      types.NodeStatusCanceled,
		}); err != nil {
			return fmt.Errorf("mark suspended node %q/%q canceled: %w", id, nodeName, err)
		}
	}

	if err := e.state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		return fmt.Errorf("mark execution %q canceled: %w", id, err)
	}
	e.notifyExecutionComplete(ctx, id, types.ExecutionStatusCanceled)
	e.EvictExecution(id)
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

// ReleaseTaskLease immediately releases a lease that an execution boundary
// knows was never handed to a handler or remote runner. It verifies the lease
// token before resetting the node and records the exact original task in the
// durable outbox when supported, so a stale delivery cannot revoke a newer
// owner or lose resume payload metadata.
//
// Unknown execution outcomes must not use this method: leave those leases in
// place for their normal expiry and recovery path to avoid duplicate effects.
func (e *Engine) ReleaseTaskLease(ctx context.Context, lease *TaskLease) (bool, error) {
	if lease == nil || lease.LeaseToken == "" {
		return false, ErrInvalidLeaseToken
	}

	task := lease.Task
	if state, ok := e.state.(AtomicStateStore); ok {
		released, err := state.RevokeLeaseWithOutbox(ctx, task.ExecutionID, task.NodeName, lease.LeaseToken, OutboxEntry{
			ID:   requeueOutboxID(task.ExecutionID, task.NodeName, task.ActivationID, lease.LeaseID),
			Task: task,
		})
		if err != nil {
			return false, fmt.Errorf("release task lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if !released {
			return false, nil
		}
		if err := e.FlushOutbox(ctx, task.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver released task outbox %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		return true, nil
	}

	released, err := e.state.RevokeLease(ctx, task.ExecutionID, task.NodeName, lease.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("release task lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	if !released {
		return false, nil
	}
	if err := e.queue.Enqueue(ctx, &task); err != nil {
		return true, fmt.Errorf("re-enqueue released task %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	return true, nil
}

// ReclaimLease revokes an expired task lease and re-enqueues the exact queued
// task so a healthy runner can pick it up. The persisted task type and resume
// payload are required for committing-state recovery: replaying a resume as a
// normal execution would lose the external signal that selected its path.
// Atomic StateStores persist the token-fenced revocation and redelivery intent
// together.
func (e *Engine) ReclaimLease(ctx context.Context, lease ExpiredLease) (bool, error) {
	if lease.LeaseToken == "" {
		return false, nil
	}
	task := Task{
		ExecutionID:  lease.ExecutionID,
		NodeName:     lease.NodeName,
		NodeIdx:      lease.NodeIdx,
		Type:         lease.TaskType,
		Payload:      cloneSignalPayload(lease.Payload),
		ActivationID: lease.ActivationID,
		AutoDepth:    lease.AutoDepth,
	}
	if state, ok := e.state.(AtomicStateStore); ok {
		revoked, err := state.RevokeLeaseWithOutbox(ctx, lease.ExecutionID, lease.NodeName, lease.LeaseToken, OutboxEntry{
			ID:   requeueOutboxID(lease.ExecutionID, lease.NodeName, lease.ActivationID, lease.LeaseID),
			Task: task,
		})
		if err != nil {
			return false, fmt.Errorf("revoke lease %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
		}
		if !revoked {
			return false, nil
		}
		if err := e.FlushOutbox(ctx, lease.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver reclaimed task outbox %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
		}
		return true, nil
	}

	revoked, err := e.state.RevokeLease(ctx, lease.ExecutionID, lease.NodeName, lease.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("revoke lease %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
	}
	if !revoked {
		return false, nil
	}
	if err := e.queue.Enqueue(ctx, &task); err != nil {
		return true, fmt.Errorf("re-enqueue reclaimed task %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
	}
	return true, nil
}
