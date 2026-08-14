package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ErrAtomicCommitUnsupported reports a StateStore that has not implemented
// the atomic scheduling contract required by Engine result commits.
var ErrAtomicCommitUnsupported = errors.New("state store does not support atomic node commits")

// ErrSystemTaskHandled signals that an internal scheduler task completed
// without producing a runner-facing lease.
var ErrSystemTaskHandled = errors.New("system task handled")

// OutboxEntry is a durable task-delivery intent created together with a
// scheduling state transition. Delivery is at-least-once: callers may observe
// the same task again when enqueue succeeds but acknowledgment is lost.
type OutboxEntry struct {
	ID          string
	Task        Task
	AvailableAt time.Time
	CreatedAt   time.Time
	Attempts    int
}

// CommitNodeRequest describes one fenced terminal node transition. A normal
// request must match the active lease; system requests are used only by the
// internal skip cascade after its scheduling marker was persisted.
type CommitNodeRequest struct {
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeIdx      int
	ActivationID int
	AutoDepth    int
	LeaseID      LeaseID
	LeaseToken   LeaseToken
	Attempt      int
	Status       types.NodeStatus
	Output       map[string]any
	StoreOutput  bool
	Port         string
	Error        string
	System       bool
	// Fatal short-circuits the ACYCLIC completion protocol: the backend finalizes
	// the execution immediately instead of waiting for the remaining-unit counter
	// to reach zero. It is meaningless on a cyclic graph, which has no such
	// counter — a cyclic node that must end the execution says so via
	// CyclicComplete instead. Setting both is rejected (see Validate).
	Fatal bool
	// AdvanceTask is the acyclic downstream scheduling task. Cyclic downstream
	// travels in CyclicOutbox; setting this on a cyclic commit is rejected.
	AdvanceTask *Task
	// AllowCycles is the graph type of the execution being committed. It selects
	// which of the two mutually exclusive completion protocols the backend runs,
	// so it must describe the compiled graph — never a backend guess.
	//
	// The backends used to re-derive it by loading the graph at commit time, and
	// an unavailable graph (in-memory cache lost to a restart, Redis key aged out
	// under a long-lived execution) read as "acyclic". That silently dropped the
	// CyclicOutbox intents, whose persistence is gated on this flag, and ran the
	// acyclic branch against a completion counter that is only ever seeded for
	// acyclic graphs. Both losses are unrecoverable and produce no error.
	//
	// The engine knows the type from the graph it is already holding, so it states
	// it here. Validate cross-checks it against the cyclic-only payload fields to
	// keep a forgotten field from re-defaulting to "acyclic".
	AllowCycles bool
	// CyclicOutbox carries downstream delivery intents for a cyclic-graph node
	// commit. Cyclic downstream is dynamic and is not static in-degree counted
	// like the acyclic AdvanceTask, so the engine computes it deterministically
	// before the commit and the backend persists these entries in the SAME
	// fenced transition as the terminal node write. This closes the window
	// where a crash (or enqueue failure) between the terminal commit and a
	// separate Enqueue permanently lost downstream cyclic tasks. Empty for
	// acyclic commits.
	CyclicOutbox []OutboxEntry
	// CyclicComplete marks a cyclic node whose active branch has no downstream
	// (or exceeded MaxAutoDepth): the backend finalizes the execution status
	// (CyclicFinalStatus, with CyclicFinalError recorded when failed) atomically
	// with the terminal node write. Ignored for acyclic commits and when
	// CyclicOutbox is non-empty.
	CyclicComplete    bool
	CyclicFinalStatus types.ExecutionStatus
	CyclicFinalError  string
}

// Validate rejects a request whose graph type contradicts its payload.
//
// AllowCycles selects between two completion protocols that share no state, and
// a bool defaults to the acyclic one. Without this cross-check a caller that
// forgot the field would reintroduce the exact silent loss the field exists to
// prevent: cyclic intents dropped, an unseeded counter decremented. Every field
// below belongs to exactly one protocol, so a mismatch is a programming error
// and is worth failing the commit over rather than guessing which half is right.
func (r CommitNodeRequest) Validate() error {
	if r.AllowCycles {
		if r.Fatal {
			return fmt.Errorf("commit %s/%s: Fatal is the acyclic finalization signal and is "+
				"meaningless on a cyclic graph; use CyclicComplete", r.ExecutionID, r.NodeName)
		}
		if r.AdvanceTask != nil {
			return fmt.Errorf("commit %s/%s: AdvanceTask is acyclic downstream scheduling; "+
				"cyclic downstream travels in CyclicOutbox", r.ExecutionID, r.NodeName)
		}
		return nil
	}
	if len(r.CyclicOutbox) > 0 {
		return fmt.Errorf("commit %s/%s: CyclicOutbox set on an acyclic commit; the backend "+
			"would drop these delivery intents", r.ExecutionID, r.NodeName)
	}
	if r.CyclicComplete {
		return fmt.Errorf("commit %s/%s: CyclicComplete set on an acyclic commit; acyclic "+
			"finalization is driven by the remaining-unit counter", r.ExecutionID, r.NodeName)
	}
	return nil
}

// CommitNodeResult is the stable result of an atomic node commit.
type CommitNodeResult struct {
	Outcome         CommitOutcome
	Applied         bool
	ExecutionDone   bool
	ExecutionStatus types.ExecutionStatus
	OutboxIDs       []string
}

// DownstreamArrival aggregates all edges from one source completion to a
// destination. ArrivalCount and ActiveCount preserve fan-in semantics while
// allowing the backend to update each destination once atomically.
type DownstreamArrival struct {
	NodeName     string
	NodeIdx      int // 图目标：下游 unit 的代表 node 下标（诊断 / 任务 NodeName 解析）
	UnitIdx      int // 计数键：下游 durable unit 下标（in-degree / active / schedule 三键均按此）
	ArrivalCount int
	ActiveCount  int
	MergeMode    string
	ExecTaskType TaskType // 执行意图任务类型：node 下游 TaskTypeNodeExec；group 下游 TaskTypeGroupExec；0 回退 NodeExec
}

// AdvanceNodeRequest progresses the already-committed source node through its
// downstream scheduling counters. It is invoked from a durable internal task,
// not from the result commit call stack.
type AdvanceNodeRequest struct {
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeIdx      int
	ActivationID int
	AutoDepth    int
	Arrivals     []DownstreamArrival
}

// AdvanceNodeResult reports whether an internal advance task made a new
// scheduling transition. A duplicate task returns Applied=false.
type AdvanceNodeResult struct {
	Applied   bool
	OutboxIDs []string
}

// AtomicStateStore is the durable scheduling extension of StateStore. It is
// intentionally a StateStore-owned capability so Engine continues to depend
// only on StateStore and TaskQueue while each backend owns its transaction
// implementation.
type AtomicStateStore interface {
	// CreateExecutionWithOutbox atomically persists a new execution and its
	// initial delivery intents. A successful call guarantees that every root
	// task remains discoverable by the outbox dispatcher even if the caller
	// crashes before synchronous queue delivery.
	CreateExecutionWithOutbox(ctx context.Context, execution *ExecutionSnapshot, entries []OutboxEntry) error
	// ResetNodeForRetryWithOutbox atomically rolls the matching active lease
	// back to pending and records its delayed retry delivery intent. scheduled
	// is false when recovery or a newer lease already won the token fence.
	ResetNodeForRetryWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token LeaseToken, entry OutboxEntry) (scheduled bool, err error)
	// RevokeLeaseWithOutbox token-fences lease revocation and records the exact
	// task that must be redelivered in the same state transition. revoked is
	// false when the lease was already committed or replaced.
	RevokeLeaseWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token LeaseToken, entry OutboxEntry) (revoked bool, err error)
	CommitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error)
	AdvanceNode(ctx context.Context, req AdvanceNodeRequest) (AdvanceNodeResult, error)
	ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error)
	AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error
	ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error)
}

func initialOutboxID(id types.ExecutionID, nodeName string, activationID int) string {
	return fmt.Sprintf("root/%s/%s/%d", id, nodeName, activationID)
}

func retryOutboxID(id types.ExecutionID, nodeName string, activationID, attempt int) string {
	return fmt.Sprintf("retry/%s/%s/%d/%d", id, nodeName, activationID, attempt)
}

func requeueOutboxID(id types.ExecutionID, nodeName string, activationID int, leaseID LeaseID) string {
	return fmt.Sprintf("requeue/%s/%s/%d/%s", id, nodeName, activationID, leaseID)
}

// SuspendResumeOutboxEntry builds one deterministic resume delivery intent.
// kind distinguishes a consumed signal from timer and timeout wakeups for the
// same suspended lease generation.
func SuspendResumeOutboxEntry(lease *TaskLease, kind string, payload *types.SignalPayload, availableAt time.Time) OutboxEntry {
	return OutboxEntry{
		ID: fmt.Sprintf("resume/%s/%s/%d/%s/%s", lease.Task.ExecutionID, lease.Task.NodeName, lease.Task.ActivationID, lease.LeaseID, kind),
		Task: Task{
			ExecutionID:  lease.Task.ExecutionID,
			NodeName:     lease.Task.NodeName,
			NodeIdx:      lease.Task.NodeIdx,
			UnitIdx:      lease.Task.UnitIdx,
			Type:         TaskTypeNodeResume,
			Payload:      cloneSignalPayload(payload),
			ActivationID: lease.Task.ActivationID,
		},
		AvailableAt: availableAt,
	}
}

// SuspendOutboxEntries creates deterministic resume delivery intents for one
// fenced suspend transition. Backends persist these entries while they still
// hold the same state transaction that clears the lease and consumes signals.
func SuspendOutboxEntries(lease *TaskLease, spec *types.SuspendSpec, payload *types.SignalPayload, now time.Time) []OutboxEntry {
	if lease == nil || spec == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if payload != nil {
		return []OutboxEntry{SuspendResumeOutboxEntry(lease, "signal", payload, now)}
	}
	entries := make([]OutboxEntry, 0, 2)
	if spec.Mode == types.ModeTimer {
		entries = append(entries, SuspendResumeOutboxEntry(lease, "timer", &types.SignalPayload{Triggered: types.TimerFired, Name: "_timer"}, now.Add(spec.Timer)))
	}
	if spec.Timeout > 0 {
		entries = append(entries, SuspendResumeOutboxEntry(lease, "timeout", &types.SignalPayload{Triggered: types.TimeoutFired, Name: "_timeout"}, now.Add(spec.Timeout)))
	}
	return entries
}

func (e *Engine) atomicState() (AtomicStateStore, error) {
	state, ok := e.state.(AtomicStateStore)
	if !ok {
		return nil, ErrAtomicCommitUnsupported
	}
	return state, nil
}

func (e *Engine) commitNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	state, err := e.atomicState()
	if err != nil {
		return CommitNodeResult{Outcome: CommitOutcomeTransientError}, err
	}
	return state.CommitNode(ctx, req)
}

// FlushOutbox delivers ready task intents for one execution. An enqueue that
// succeeds before AckOutbox fails is deliberately retried later; lease fencing
// makes that duplicate delivery safe.
//
// A nil return does not mean the outbox is empty. When the queue reports
// ErrQueueFull the flush stops early and returns nil, leaving the remaining
// intents durable and un-attempted; the OutboxDispatcher's next tick and any
// subsequent commit both re-enter here to finish the delivery.
func (e *Engine) FlushOutbox(ctx context.Context, id types.ExecutionID) error {
	ctx, span := outboxTracer().Start(ctx, "xflow.outbox.flush", "execution_id", string(id))
	defer span.End()
	state, err := e.atomicState()
	if err != nil {
		return err
	}

	const batchSize = 256
	for {
		entries, err := state.ListOutbox(ctx, id, time.Now().UTC(), batchSize)
		if err != nil {
			e.notifyOutboxError(ctx, "list", err)
			return fmt.Errorf("list outbox for %q: %w", id, err)
		}
		if len(entries) == 0 {
			return nil
		}
		var firstErr error
		for _, entry := range entries {
			if entry.Task.Type == TaskTypeNodeAdvance || entry.Task.Type == TaskTypeNodeSkip {
				handled, err := e.handleSystemTask(ctx, &entry.Task, false)
				if err != nil {
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, err)
					if firstErr == nil {
						firstErr = fmt.Errorf("handle outbox system task %q for %q: %w", entry.ID, id, err)
					}
					continue
				}
				if !handled {
					err := fmt.Errorf("outbox task %q for %q was not handled", entry.ID, id)
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
			} else {
				var enqueueErr error
				if entry.AvailableAt.After(time.Now()) {
					enqueueErr = e.queue.EnqueueDelayed(ctx, &entry.Task, time.Until(entry.AvailableAt))
				} else if nb, ok := e.queue.(NonBlockingTaskQueue); ok {
					enqueueErr = nb.TryEnqueue(ctx, &entry.Task)
				} else {
					enqueueErr = e.queue.Enqueue(ctx, &entry.Task)
				}
				if errors.Is(enqueueErr, ErrQueueFull) {
					// Backpressure, not failure. Leave this entry and every
					// entry behind it unacknowledged, and do not touch the
					// delivery-attempt counter — recording an attempt here
					// would dead-letter a perfectly good intent after enough
					// full queues. The OutboxDispatcher and the next commit
					// both re-enter FlushOutbox, so delivery resumes once the
					// workers have drained the queue. An outbox that stops
					// draining still surfaces: the dispatcher's OnOutboxPending
					// reports the backlog and its oldest entry's age.
					return nil
				}
				if enqueueErr != nil {
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, enqueueErr)
					if firstErr == nil {
						firstErr = fmt.Errorf("enqueue outbox %q for %q: %w", entry.ID, id, enqueueErr)
					}
					continue
				}
			}
			if err := state.AckOutbox(ctx, id, entry.ID); err != nil {
				e.notifyOutboxError(ctx, "ack", err)
				return fmt.Errorf("ack outbox %q for %q: %w", entry.ID, id, err)
			}
		}
		if firstErr != nil {
			return firstErr
		}
		// Continue even after a short batch: handling an internal advance/skip
		// intent can have appended the next durable intent during this batch.
	}
}

func (e *Engine) afterAtomicCommit(ctx context.Context, req CommitNodeRequest, result CommitNodeResult) error {
	return e.afterAtomicCommitWithFlush(ctx, req, result, true)
}

func (e *Engine) afterAtomicCommitWithFlush(ctx context.Context, req CommitNodeRequest, result CommitNodeResult, flush bool) error {
	if result.Applied && e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnNodeComplete(hookCtx, req.ExecutionID, req.NodeName, req.Status)
		})
	}
	if result.ExecutionDone {
		if e.hooks != nil {
			safeHook(ctx, e.logger, func(hookCtx context.Context) {
				e.hooks.OnExecutionComplete(hookCtx, req.ExecutionID, result.ExecutionStatus)
			})
		}
		e.EvictExecution(req.ExecutionID)
	}
	if flush && (result.Outcome == CommitOutcomeAccepted || result.Outcome == CommitOutcomeDuplicateTerminal) {
		if err := e.FlushOutbox(ctx, req.ExecutionID); err != nil {
			return err
		}
	}
	return nil
}

// HandleSystemTask consumes internal advance and skip tasks locally. It never
// creates a runner lease, so control-plane and embedded dispatchers must call
// it before routing a task to a handler.
func (e *Engine) HandleSystemTask(ctx context.Context, task *Task) (bool, error) {
	return e.handleSystemTask(ctx, task, true)
}

// handleSystemTask applies one internal task. flush is false when FlushOutbox
// is already draining the same execution; the outer loop will observe and
// deliver newly created intents without recursive skip propagation.
func (e *Engine) handleSystemTask(ctx context.Context, task *Task, flush bool) (bool, error) {
	if task == nil {
		return false, nil
	}
	switch task.Type {
	case TaskTypeNodeAdvance:
		g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
		if err != nil {
			return true, err
		}
		if !active {
			return true, nil
		}
		node, err := e.state.GetNode(ctx, task.ExecutionID, task.NodeName)
		if err != nil {
			return true, fmt.Errorf("read advance source %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if node == nil || !types.IsTerminalNodeStatus(node.Status) {
			return true, nil
		}
		if node.ActivationID != task.ActivationID {
			if e.logger != nil {
				e.logger.Warn("dropped stale advance task",
					"execution_id", string(task.ExecutionID),
					"node_name", task.NodeName,
					"task_activation", task.ActivationID,
					"node_activation", node.ActivationID)
			}
			return true, nil
		}
		arrivals := downstreamArrivals(g, task.NodeIdx, node.Port)
		state, err := e.atomicState()
		if err != nil {
			return true, err
		}
		result, err := state.AdvanceNode(ctx, AdvanceNodeRequest{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
			Arrivals:     arrivals,
		})
		if err != nil {
			return true, fmt.Errorf("advance node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		e.publishAdvanceReceipt(ctx, task, result)
		if !flush {
			return true, nil
		}
		return true, e.FlushOutbox(ctx, task.ExecutionID)

	case TaskTypeNodeBatch:
		if e.remoteBatchExecution {
			// Let the Dispatcher route this to a remote runner, the same escape
			// TaskTypeGroupExec uses below. Consuming the batch here would keep
			// the map node's runnerSelector from ever reaching the directory
			// that matches labels, so body work would silently run wherever the
			// engine runs. Only a deployment that HAS a directory opts in (see
			// WithRemoteBatchExecution): an embedded engine has nothing to
			// escape to, and a batch routed nowhere hangs the map node forever.
			return false, nil
		}
		return true, e.ExecuteBatch(ctx, task)

	case TaskTypeNodeSkip:
		g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
		if err != nil {
			return true, err
		}
		if !active {
			return true, nil
		}
		if task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
			return true, fmt.Errorf("skip node index %d is out of range", task.NodeIdx)
		}
		advance := &Task{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			UnitIdx:      task.UnitIdx,
			Type:         TaskTypeNodeAdvance,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
		}
		result, err := e.commitNode(ctx, CommitNodeRequest{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
			Status:       types.NodeStatusSkipped,
			System:       true,
			AdvanceTask:  advance,
		})
		if err != nil {
			return true, fmt.Errorf("commit skipped node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		return true, e.afterAtomicCommitWithFlush(ctx, CommitNodeRequest{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
			Status:       types.NodeStatusSkipped,
		}, result, flush)
	case TaskTypeGroupExec:
		if e.groupExecutor == nil {
			// No local executor — let the Dispatcher route this to a remote runner.
			return false, nil
		}
		return true, e.executeGroup(ctx, task, flush)
	default:
		return false, nil
	}
}

func downstreamArrivals(g *graph.Graph, sourceIdx int, activePort string) []DownstreamArrival {
	if g == nil || sourceIdx < 0 || sourceIdx >= g.NodeCount() {
		return nil
	}
	byDestination := make(map[int]DownstreamArrival)
	for _, edge := range g.NodeOutEdges(sourceIdx) {
		arrival := byDestination[edge.DstIdx]
		if arrival.ArrivalCount == 0 {
			meta := g.NodeAt(edge.DstIdx)
			dstUnit := g.UnitIndexForNode(edge.DstIdx)
			execType := TaskTypeNodeExec
			if g.UnitKindAt(dstUnit) == graph.UnitGroup {
				execType = TaskTypeGroupExec
			}
			arrival = DownstreamArrival{
				NodeName:     meta.Name,
				NodeIdx:      edge.DstIdx,
				UnitIdx:      dstUnit,
				MergeMode:    meta.MergeMode,
				ExecTaskType: execType,
			}
		}
		arrival.ArrivalCount++
		if edge.SrcPort == activePort {
			arrival.ActiveCount++
		}
		byDestination[edge.DstIdx] = arrival
	}
	indexes := make([]int, 0, len(byDestination))
	for index := range byDestination {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	arrivals := make([]DownstreamArrival, 0, len(indexes))
	for _, index := range indexes {
		arrivals = append(arrivals, byDestination[index])
	}
	return arrivals
}

// OutboxDispatcher periodically retries durable delivery intents left behind
// by queue outages, response loss, or process crashes.
type OutboxDispatcher struct {
	engine   *Engine
	interval time.Duration
}

// NewOutboxDispatcher creates a retry loop for durable scheduling intents.
func NewOutboxDispatcher(eng *Engine, interval time.Duration) *OutboxDispatcher {
	if interval <= 0 {
		interval = time.Second
	}
	return &OutboxDispatcher{engine: eng, interval: interval}
}

// Run drains ready outboxes until ctx is canceled.
func (d *OutboxDispatcher) Run(ctx context.Context) {
	if d == nil || d.engine == nil {
		return
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		d.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *OutboxDispatcher) drain(ctx context.Context) {
	state, err := d.engine.atomicState()
	if err != nil {
		d.engine.notifyOutboxError(ctx, "state", err)
		return
	}
	ids, err := state.ListOutboxExecutions(ctx, 256)
	if err != nil {
		d.engine.notifyOutboxError(ctx, "list_executions", err)
		if d.engine.logger != nil {
			d.engine.logger.Error("list outbox executions failed", "err", err)
		}
		return
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := d.engine.FlushOutbox(ctx, id); err != nil && d.engine.logger != nil {
			d.engine.logger.Error("flush durable outbox failed", "execution_id", string(id), "err", err)
		}
	}
	d.engine.observeOutboxMetrics(ctx, state)
}
