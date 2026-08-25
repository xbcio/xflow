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

// ErrExecutionNotFound is returned when a user-facing operation targets an
// execution that does not exist in the caller's state namespace.
var ErrExecutionNotFound = errors.New("execution not found")

// ErrEntryNotFound is returned when Invoke targets a graph entry that does not
// exist in the compiled workflow.
var ErrEntryNotFound = errors.New("entry not found")

// ErrInvalidLeaseToken is returned when a runner commits with a stale or
// unknown lease token.
var ErrInvalidLeaseToken = errors.New("invalid lease token")

// ErrGroupLeaseNotSupported is returned when a node-only operation is handed a
// group lease. A group unit leases through its own state
// (group:<unitIdx>:status/meta), not through the entry node's; sending it down
// the node path fences against a node that never entered "running". Today this
// is a deliberate refusal, not the accidental stale-token rejection the node
// path would produce — the difference matters because a stale-token error
// tells the caller to retry, while a group lease must not be retried on the
// node path.
var ErrGroupLeaseNotSupported = errors.New("operation does not support group leases")

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

// WithDefaultNodeTimeout overrides the bound applied to a node that declares no
// Timeout of its own.
//
// d == 0 DISABLES the default entirely: unconfigured nodes become unbounded,
// which is exactly the pre-timeout behaviour this feature exists to end. That
// inverts NodeDef.Timeout's own zero ("not configured") on purpose -- an option
// caller is stating an intent, not leaving a field blank. It exists so a
// deployment that cannot accept the behaviour change can roll back without
// first rewriting workflow data; reach for a larger d before reaching for 0.
func WithDefaultNodeTimeout(d time.Duration) Option {
	return func(e *Engine) { e.defaultNodeTimeout = d }
}

// WithOuterDeadline sets an absolute upper bound on every lease this engine
// issues. BuildTaskLease and RecoverTaskLease clamp ExecutionDeadline to the
// earlier of the member's own stamped deadline and this bound. Used by the
// sub-graph executor to propagate the group's deadline to its members: the
// member handler's context starts from context.Background() (memory_queue.go
// worker goroutines must not inherit the submitter's cancellation), so the
// deadline cannot travel as context lineage -- it travels as data through the
// lease.
//
// A zero value (the default) means no outer bound; member deadlines apply
// unchanged.
func WithOuterDeadline(t time.Time) Option {
	return func(e *Engine) { e.outerDeadline = t }
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

// DefaultNodeTimeout bounds one execution of a node whose NodeDef.Timeout is
// unset. It is not "how long a node should take" -- each node type keeps its
// own default for that (xflow.http's 30s, DefaultScriptTimeout). This is the
// point past which a running node is presumed stuck rather than slow, which is
// an order of magnitude coarser: 30 seconds on an HTTP call may still be a slow
// upstream; 30 minutes has no innocent explanation.
//
// DefaultLeaseTTL is deliberately NOT a reference point here. That is the
// renewal clock, and the whole purpose of renewal is to let a node run longer
// than it.
const DefaultNodeTimeout = 30 * time.Minute

// Engine is the pure-algorithm workflow execution engine.
// It has zero IO dependencies — all persistence and queuing are injected via interfaces.
type Engine struct {
	state                    StateStore
	queue                    TaskQueue
	hooks                    Hooks
	logger                   Logger
	commitObserver           CommitObserver
	outboxObserver           OutboxObserver
	nodeFailureObserver      NodeFailureObserver
	itemFailureObserver      ItemFailureObserver
	outboxMaxDeliveryAttempt int
	defaultLeaseTTL          time.Duration
	defaultNodeTimeout       time.Duration
	suspendDisabled          bool
	suspendDisabledErr       error
	groupExecutor            GroupExecutor
	batchBodyExecutor        BatchBodyExecutor
	remoteBatchExecution     bool
	outerDeadline            time.Time

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
		defaultNodeTimeout:       DefaultNodeTimeout,
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
	attachSubmissionMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	ctx = attachTransientHint(ctx, g)
	return e.startExecution(ctx, snap, submitInitialTasks(id, g))
}

// Invoke starts a new execution from one explicit entry node.
func (e *Engine) Invoke(ctx context.Context, g *graph.Graph, entryName string, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error) {
	entryIdx, ok := g.EntryIndex(entryName)
	if !ok {
		return "", fmt.Errorf("entry node %q: %w", entryName, ErrEntryNotFound)
	}

	id := preallocOrNewExecutionID(ctx)
	snap := &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: cloneMap(params),
	}
	attachSubmissionMetadata(ctx, snap)
	if len(runtime) > 0 {
		snap.Runtime = cloneRuntime(runtime[0])
	}
	ctx = attachTransientHint(ctx, g)
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
		active, err := e.executionActive(ctx, id)
		if err != nil {
			return nil, false, err
		}
		if !active {
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

	active, err := e.executionActive(ctx, id)
	if err != nil || !active {
		return nil, false, err
	}

	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()
	return g, true, nil
}

// executionActive answers loadActiveGraph's only question: does this execution
// exist and is it still non-terminal?
//
// It prefers ExecutionStatusReader because that is the whole question. The
// GetExecution fallback assembles params, runtime, scope and three tracing
// fields to hand back one enum, which on the Redis backend costs eight
// sequential GETs and four JSON decodes instead of one GET. Every commit, lease,
// advance, signal, group and subgraph transition pays that, so the narrow read
// is the common case and the fallback is for backends that predate it.
func (e *Engine) executionActive(ctx context.Context, id types.ExecutionID) (bool, error) {
	if reader, ok := e.state.(ExecutionStatusReader); ok {
		status, found, err := reader.GetExecutionStatus(ctx, id)
		if err != nil {
			return false, err
		}
		return found && !types.IsTerminalExecutionStatus(status), nil
	}
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return false, err
	}
	return snap != nil && !types.IsTerminalExecutionStatus(snap.Status), nil
}

func attachSubmissionMetadata(ctx context.Context, snap *ExecutionSnapshot) {
	if traceID, ok := TraceIDFromContext(ctx); ok {
		snap.TraceID = traceID
	}
	if spanID, ok := SpanIDFromContext(ctx); ok {
		snap.SpanID = spanID
	}
	if carrier := TraceCarrierFromContext(ctx); len(carrier) > 0 {
		snap.TraceCarrier = carrier
	}
	// Cloned rather than aliased: the caller's map (a map body's per-item roots)
	// is rebuilt for the next item, and the snapshot outlives this call.
	if scope := ExecutionScopeFromContext(ctx); len(scope) > 0 {
		snap.Scope = cloneMap(scope)
	}
}

// attachTransientHint enriches the submission context with the per-workflow
// transient hint when the compiled graph declares Transient=true. This lets
// the StateStore apply per-execution transient behavior without coupling to
// the graph package.
func attachTransientHint(ctx context.Context, g *graph.Graph) context.Context {
	if g == nil || !g.Transient() {
		return ctx
	}
	return WithExecutionTransient(ctx, TransientHint{
		TTL:           g.TransientTTL(),
		CompletionTTL: g.TransientCompletionTTL(),
	})
}
