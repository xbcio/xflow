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

	evidenceBuffer *RuntimeEvidenceBuffer
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

// State returns the StateStore used by this engine.
// Useful for callers that need to poll execution status (e.g. cluster Wait).
func (e *Engine) State() StateStore { return e.state }

// EvictExecution removes an execution's in-process graph cache. Backends that
// expire runtime state independently can call this so late runner callbacks
// observe the execution as inactive instead of using stale cached graph data.
func (e *Engine) EvictExecution(id types.ExecutionID) {
	e.mu.Lock()
	delete(e.graphs, id)
	e.mu.Unlock()
}

// NewExecutionID mints a fresh execution id. Extracted so the apiserver authz
// wrapper can pre-allocate the id before the mutation handler runs (R3.1),
// letting the admission audit row carry the same id the engine will persist.
func NewExecutionID() types.ExecutionID {
	return types.ExecutionID("exec-" + uuid.New().String())
}

// preallocOrNewExecutionID returns the caller-pre-allocated id from the
// submission context when present, otherwise mints a fresh one. Used by
// Submit/Invoke so a server-side pre-allocated id flows into the persisted
// snapshot and the audit row simultaneously.
func preallocOrNewExecutionID(ctx context.Context) types.ExecutionID {
	if id, ok := ExecutionIDFromContext(ctx); ok {
		return id
	}
	return NewExecutionID()
}

// Submit starts a new execution of the given graph with the provided params.
// It persists the execution snapshot, caches the graph, and schedules all
// root nodes (in-degree == 0) through a durable outbox when the StateStore
// implements AtomicStateStore.
func (e *Engine) Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	id := preallocOrNewExecutionID(ctx)
	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: cloneMap(params),
	}
	attachTraceMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	return e.startExecution(ctx, snap, submitInitialTasks(id, g))
}

// Invoke starts a new execution from one explicit entry node.
func (e *Engine) Invoke(ctx context.Context, g *graph.Graph, entryName string, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	entryIdx, ok := g.EntryIndex(entryName)
	if !ok {
		return "", fmt.Errorf("entry node %q not found", entryName)
	}

	id := preallocOrNewExecutionID(ctx)
	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: cloneMap(params),
	}
	attachTraceMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	entry := g.NodeAt(entryIdx)
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

func (e *Engine) loadActiveGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, bool, error) {
	e.mu.RLock()
	g, ok := e.graphs[id]
	e.mu.RUnlock()
	if ok {
		// A cache hit is not sufficient to treat the execution as active: a
		// concurrent Cancel or a delayed EvictExecution can leave a terminal
		// execution's graph in the cache. Confirm the execution is still
		// non-terminal before handing the graph to a lease/schedule path,
		// otherwise a lease could be issued against an already-finished
		// execution. Evict the stale entry so we do not re-check it forever.
		snap, err := e.state.GetExecution(ctx, id)
		if err != nil {
			return nil, false, err
		}
		if snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
			e.EvictExecution(id)
			return nil, false, nil
		}
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

func attachTraceMetadata(ctx context.Context, snap *ExecutionSnapshot) {
	if traceID, ok := TraceIDFromContext(ctx); ok {
		snap.TraceID = traceID
	}
	if spanID, ok := SpanIDFromContext(ctx); ok {
		snap.SpanID = spanID
	}
	if carrier := TraceCarrierFromContext(ctx); len(carrier) > 0 {
		snap.TraceCarrier = carrier
	}
}
