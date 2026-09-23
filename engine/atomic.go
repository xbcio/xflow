package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
// the same task again when enqueue succeeds but acknowledgment is lost, or when
// a deliverer dies while holding the entry's delivery lease.
type OutboxEntry struct {
	ID          string    `json:"id"`
	Task        Task      `json:"task"`
	AvailableAt time.Time `json:"available_at"`
	CreatedAt   time.Time `json:"created_at"`
	Attempts    int       `json:"attempts"`
	// LeasedFromScoreMs carries the entry's availability time as it stood when
	// LeaseOutbox leased it, so ReleaseOutbox can restore it exactly. It is set
	// by the store on the way out and is meaningless on an entry the caller
	// constructs.
	LeasedFromScoreMs int64 `json:"leased_from_score_ms,omitempty"`
	// LeaseDeadlineMs is when the lease this caller holds expires, and doubles
	// as the fencing token that identifies the holder.
	//
	// The lease is otherwise anonymous — there is no process identity to hang it
	// on — so RenewOutbox proves ownership by presenting the deadline it expects
	// to still be in force. A deliverer that stalled past its own deadline finds
	// the entry has moved on, and its renewal is refused rather than yanking the
	// entry back from whoever legitimately claimed it. Without that check
	// renewal would reintroduce the duplicate delivery the lease exists to
	// prevent, in the one case the lease is there for: a deliverer that stops
	// responding and then comes back.
	//
	// Set by the store on the way out of LeaseOutbox and RenewOutbox.
	LeaseDeadlineMs int64 `json:"lease_deadline_ms,omitempty"`
}

// CommitNodeRequest describes one fenced terminal node transition. A normal
// request must match the active lease; system requests are used only by the
// internal skip cascade after its scheduling marker was persisted.
type CommitNodeRequest struct {
	ExecutionID types.ExecutionID
	NodeName    string
	NodeIdx     int
	// UnitIdx is the durable scheduling unit this node belongs to, and the key
	// the backend stores its scheduling marker, in-degree and active-input
	// counters under. It is not NodeIdx: a unit collapses every node of a group
	// body, and a declaration-only node (supply) takes a node index while having
	// no unit at all, so the two diverge for any definition that declares one.
	//
	// A system commit must resolve the marker by this field. Reading it by
	// NodeIdx resolves a neighbouring unit instead — and because the marker
	// resolves to "execute" rather than "skip", the backend refuses the commit,
	// the skip cascade drops it, and the branch's units never terminalize, so the
	// execution stays running with every node it did run reporting success.
	UnitIdx      int
	ActivationID int
	AutoDepth    int
	LeaseID      LeaseID
	LeaseToken   LeaseToken
	Attempt      int
	Status       types.NodeStatus
	Output       map[string]any
	StoreOutput  bool
	// ReclaimOutputNames names transient runtime outputs that the engine proved
	// are no longer reachable once this fenced successful terminal transition is
	// accepted.
	// Backends must apply the deletion in the same atomic commit as this node; a
	// stale, duplicate, or inactive request must leave every named output intact.
	//
	// The field is engine-derived, never runner-controlled. It is intentionally
	// conservative: it is populated only for the supported transient
	// group-boundary-to-single-normal-consumer topology.
	ReclaimOutputNames []string
	// PrivateOutput keeps the runtime output available to downstream execution
	// while excluding it from public node snapshots and result projections. The
	// engine derives it from the compiled graph; backends must never treat it as
	// a runner-controlled visibility claim.
	PrivateOutput bool
	Port          string
	Error         string
	// ErrorDetails is the structured companion to Error, taken from the
	// EffectiveClassification bound to this commit and persisted alongside the
	// node's terminal state so it survives a restart and a second replica. The
	// backends store it verbatim: it is already bounded by
	// engine.boundedErrorDetails, and applying a projection per backend would
	// let the two disagree about what a failure said. Visibility policy for
	// this field lives at the read surface (engine/inspect.go).
	ErrorDetails map[string]any
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
	if len(r.ReclaimOutputNames) > 0 && (r.AllowCycles || r.Fatal || r.System || r.Status != types.NodeStatusSuccess) {
		return fmt.Errorf("commit %s/%s: ReclaimOutputNames is only valid for a non-system, non-fatal successful acyclic commit", r.ExecutionID, r.NodeName)
	}
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

// SkippedUnit names one downstream unit a scheduling transition resolved as
// skip rather than execute, and how many units that name covers.
//
// A skip is the graph deciding that a unit's inbound routes carried no active
// port: the unit is recorded as scheduled, a skip intent is queued
// (engine.TaskTypeNodeSkip), and it terminalizes without ever running — so the
// branch the upstream data never reached is consumed instead of executed. That
// is not a silent drop (the intent is durable), but it is invisible from the
// outside: no error is raised, and the scheduling position does advance.
//
// It replaces an aggregate count because a single transition can skip several
// different nodes at once, and an operator's question is "which node is being
// skipped", which a total cannot answer.
type SkippedUnit struct {
	NodeName string
	Count    int
}

// AdvanceNodeResult reports whether an internal advance task made a new
// scheduling transition. A duplicate task returns Applied=false.
//
// With one exception, which is deliberate and worth knowing before treating
// Applied as a dedup signal: when Arrivals is empty the distributed backend
// short-circuits in Go and reports Applied=true without consulting the advance
// marker, so a redelivered advance for a node with no downstream work claims
// Applied twice. The memory backend runs its guards either way and reports
// Applied=false on the second.
//
// Empty Arrivals is not an edge case — every acyclic graph has at least one
// node with no outgoing edge, and each of them produces one such advance per
// execution. Honoring the dedup guarantee there would cost a Redis round trip
// per execution to correct a field whose only consumer is the optional evidence
// buffer (publishAdvanceReceipt). That trade was not worth making, so the
// guarantee is stated as it actually holds rather than enforced.
type AdvanceNodeResult struct {
	Applied   bool
	OutboxIDs []string
	// Skipped reports the downstream units this transition resolved as skip.
	//
	// The backend reports it because only the backend knows which of the two
	// candidate intents per arrival it actually wrote. OutboxIDs cannot
	// substitute: deriving it from an ID prefix would make a naming convention
	// load-bearing for an operator-visible series, and that field's other
	// consumer (the evidence buffer) is defined over every intent, not just the
	// skipped ones.
	//
	// Nil on an all-execute transition and on a duplicate delivery that applied
	// nothing.
	Skipped []SkippedUnit
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
	// ListOutbox reports ready delivery intents without changing any state.
	//
	// It is deliberately a pure read. The delivery lease lives on LeaseOutbox
	// instead, because a read that also claims is a trap for every observer:
	// a probe, a metric, or an admin query that merely wants to see the backlog
	// would hide those entries from the flush path that has to deliver them.
	//
	// before is the availability cutoff — entries scheduled later than it are
	// not yet due. An entry another caller currently holds a delivery lease on
	// is not ready, so it is not returned.
	ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error)
	AckOutbox(ctx context.Context, id types.ExecutionID, entryID string) error
	ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error)
}

// OutboxDeliveryLeaseTTL is how long a delivery lease survives without renewal.
//
// It is deliberately short. A live deliverer renews its leases every
// OutboxLeaseRenewInterval for as long as it holds them, so the TTL is not a
// budget for how long delivery may take — it is how long a DEAD deliverer's
// entries stay stranded. Making it long only delays crash recovery; making it
// shorter than a couple of renewal intervals lets a slow network reclaim a
// lease from a deliverer that is still alive and about to enqueue.
const OutboxDeliveryLeaseTTL = 5 * time.Second

// OutboxLeaseRenewInterval is how often a deliverer renews the leases it holds.
//
// It must stay well under OutboxDeliveryLeaseTTL so a renewal that is merely
// slow does not read as a death.
const OutboxLeaseRenewInterval = OutboxDeliveryLeaseTTL / 3

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
		entries, err := e.claimOutbox(ctx, state, id, batchSize)
		if err != nil {
			e.notifyOutboxError(ctx, "list", err)
			return fmt.Errorf("list outbox for %q: %w", id, err)
		}
		if len(entries) == 0 {
			return nil
		}
		// Only handling an advance/skip intent can append to this outbox from
		// inside the loop, so track whether this batch did. See the exit below.
		appended := false
		// Prove this deliverer is alive for as long as it holds the batch, so
		// OutboxDeliveryLeaseTTL can stay short enough to recover from a crash
		// promptly without stealing work from a slow-but-live flush.
		keeper := e.startOutboxLeaseKeeper(ctx, state, id, entries)
		var firstErr error
		for i, entry := range entries {
			if entry.Task.Type == TaskTypeNodeAdvance || entry.Task.Type == TaskTypeNodeSkip {
				handled, err := e.handleSystemTask(ctx, &entry.Task, false)
				if err != nil {
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, err)
					keeper.forget(entry.ID)
					if firstErr == nil {
						firstErr = fmt.Errorf("handle outbox system task %q for %q: %w", entry.ID, id, err)
					}
					continue
				}
				if !handled {
					err := fmt.Errorf("outbox task %q for %q was not handled", entry.ID, id)
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, err)
					keeper.forget(entry.ID)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				appended = true
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
					//
					// Give the delivery leases back first. A full queue drains
					// in milliseconds while a lease hides its entry for
					// OutboxDeliveryLeaseTTL, so holding them would convert
					// ordinary backpressure into a stall — on a fan-out wide
					// enough to fill the queue, one that outlives the flush
					// itself. Every entry from this batch that has not been
					// acked is still leased, not just the one that hit the
					// full queue, so release from here to the end.
					for _, held := range entries[i:] {
						e.releaseOutboxLease(ctx, state, id, held)
						keeper.forget(held.ID)
					}
					keeper.stop()
					return nil
				}
				if enqueueErr != nil {
					e.recordOutboxDeliveryFailure(ctx, state, id, entry, enqueueErr)
					keeper.forget(entry.ID)
					if firstErr == nil {
						firstErr = fmt.Errorf("enqueue outbox %q for %q: %w", entry.ID, id, enqueueErr)
					}
					continue
				}
			}
			if err := state.AckOutbox(ctx, id, entry.ID); err != nil {
				e.notifyOutboxError(ctx, "ack", err)
				keeper.stop()
				return fmt.Errorf("ack outbox %q for %q: %w", entry.ID, id, err)
			}
			keeper.forget(entry.ID)
		}
		keeper.stop()
		if firstErr != nil {
			return firstErr
		}
		// Handling an internal advance/skip intent can have appended the next
		// durable intent during this batch, so a batch that handled one must
		// re-poll even if it was short. A batch that did not, and that came back
		// short, cannot have anything behind it: nothing else in this loop writes
		// to the outbox, and a short batch means the store had no more ready
		// entries to hand out. Re-polling there costs a full round trip per commit
		// to be told what the short batch already said.
		//
		// Entries another actor appends after this point are left for that actor's
		// own flush and the dispatcher's next tick — which is where they were left
		// before too, since the empty poll this replaces raced the same way.
		if !appended && len(entries) < batchSize {
			return nil
		}
		// Otherwise continue: a full batch may have more behind it, and a batch
		// that handled an advance/skip has queued its successor.
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
// warnStaleAdvance reports an advance task fenced out by a newer activation of
// its source node.
func (e *Engine) warnStaleAdvance(task *Task, nodeActivation int) {
	if e.logger == nil {
		return
	}
	e.logger.Warn("dropped stale advance task",
		"execution_id", string(task.ExecutionID),
		"node_name", task.NodeName,
		"task_activation", task.ActivationID,
		"node_activation", nodeActivation)
}

// explainUnappliedAdvance recovers the stale-advance diagnostic on the path
// that no longer reads the source node up front.
//
// Carrying Task.Port removed the only reason to read the node before advancing,
// but it also removed the place the "dropped stale advance task" warning came
// from. AdvanceNodeResult.Applied cannot substitute: the store returns
// Applied=false for a stale activation, a non-terminal source, a terminated
// execution and an ordinary duplicate delivery alike, and only the first is
// worth a warning — the last is at-least-once working as designed.
//
// So the read moves here, from every advance to only the ones that did no work.
// Duplicates are a subset of advances, so this is strictly less traffic than
// before, and nothing at all when no logger is configured. Failures are
// swallowed on purpose: this runs after the authoritative transition has
// already been decided and must not turn a completed advance into an error.
func (e *Engine) explainUnappliedAdvance(ctx context.Context, task *Task) {
	if e.logger == nil {
		return
	}
	node, err := e.state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil || node == nil {
		return
	}
	if node.ActivationID != task.ActivationID {
		e.warnStaleAdvance(task, node.ActivationID)
	}
}

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
		var port string
		if task.Port != nil {
			port = *task.Port
		} else {
			// Legacy path: an advance entry written to a durable outbox before
			// Task.Port existed, or a backend whose queue does not carry it.
			// Read the node for the port; the terminal and activation checks
			// come along for free on this path.
			node, err := e.state.GetNode(ctx, task.ExecutionID, task.NodeName)
			if err != nil {
				return true, fmt.Errorf("read advance source %q/%q: %w", task.ExecutionID, task.NodeName, err)
			}
			if node == nil || !types.IsTerminalNodeStatus(node.Status) {
				return true, nil
			}
			if node.ActivationID != task.ActivationID {
				e.warnStaleAdvance(task, node.ActivationID)
				return true, nil
			}
			port = node.Port
		}
		arrivals := downstreamArrivals(g, task.NodeIdx, port)
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
		if !result.Applied {
			e.explainUnappliedAdvance(ctx, task)
		}
		e.publishAdvanceReceipt(ctx, task, result)
		e.notifySkip(ctx, flowAdvance, result.Skipped)
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
		// The commit below resolves its scheduling marker by UnitIdx, so an
		// unset one is not merely unvalidated: it addresses a key nothing ever
		// wrote, the guard sees no "skip", and the refusal is reported as
		// handled — the intent is acked and the branch silently drops instead of
		// cascading. UnitIdxUnknown (-1) is exactly that value, and a lease or
		// queue payload that lost the field defaults to it. Reject it here,
		// where the failure is still loud.
		if task.UnitIdx < 0 || task.UnitIdx >= g.UnitCount() {
			return true, fmt.Errorf("skip node unit index %d is out of range [0,%d)", task.UnitIdx, g.UnitCount())
		}
		// A skipped node commits with no active port (the CommitNodeRequest
		// below sets none), and that empty port is what makes downstreamArrivals
		// give every out-edge ActiveCount 0 and propagate the skip. Carry it
		// explicitly rather than leaving Port nil: nil would send this advance
		// down the legacy read-the-node path, which is the whole per-hop round
		// trip the field exists to remove, and the skip cascade is the path that
		// pays it most often.
		skippedPort := ""
		advance := &Task{
			ExecutionID:  task.ExecutionID,
			NodeName:     task.NodeName,
			NodeIdx:      task.NodeIdx,
			UnitIdx:      task.UnitIdx,
			Type:         TaskTypeNodeAdvance,
			ActivationID: task.ActivationID,
			AutoDepth:    task.AutoDepth,
			Port:         &skippedPort,
		}
		privateOutput := privateOutputForTask(g, task)
		result, err := e.commitNode(ctx, CommitNodeRequest{
			ExecutionID:   task.ExecutionID,
			NodeName:      task.NodeName,
			NodeIdx:       task.NodeIdx,
			UnitIdx:       task.UnitIdx,
			ActivationID:  task.ActivationID,
			AutoDepth:     task.AutoDepth,
			Status:        types.NodeStatusSkipped,
			PrivateOutput: privateOutput,
			System:        true,
			AdvanceTask:   advance,
		})
		if err != nil {
			return true, fmt.Errorf("commit skipped node %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		return true, e.afterAtomicCommitWithFlush(ctx, CommitNodeRequest{
			ExecutionID:   task.ExecutionID,
			NodeName:      task.NodeName,
			NodeIdx:       task.NodeIdx,
			ActivationID:  task.ActivationID,
			AutoDepth:     task.AutoDepth,
			Status:        types.NodeStatusSkipped,
			PrivateOutput: privateOutput,
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

// DefaultOutboxMetricsInterval bounds how often the dispatcher runs the
// outbox backlog scan. The scan walks every execution-scoped ready and
// dead-letter index in the keyspace, so it is orders of magnitude more
// expensive than one drain; it reports a gauge, so it does not need the
// dispatcher's delivery cadence.
const DefaultOutboxMetricsInterval = 30 * time.Second

// DefaultOutboxDiscoveryPage bounds one tick's outbox discovery page, sharded
// across namespaces by the state store.
//
// The scan used to be exhaustive, so a larger page bought nothing but a larger
// keyspace walk; it is now cursor-resumed, which makes the cost of raising this
// linear in the page rather than quadratic in the backlog. A drain still
// delivers everything a page yields, so this stays well under the point where
// one tick's flush would dominate the interval.
//
// The page is also the discovery CEILING of the keyspace SWEEP, which is what
// makes it a throughput parameter rather than a tuning nicety. SCAN's COUNT
// counts keys EXAMINED, not keys matched, so at a keyspace of N keys one sweep
// reaches at most about page/N of the ready backlog and a cursor needs N/page
// drains to come all the way around.
//
// That ceiling is the discovery half of the deficit this default was sized
// against: a host deployed on a shared Redis Cluster with a five-figure key
// count reported 68 outbox dispatches per minute against 364 executions
// created per minute, with the backlog stable instead of draining. At 256, a
// cursor needs some eighty drains to see that keyspace once, so the ready
// backlog is rediscovered far more slowly than it is produced no matter how
// healthy delivery itself is.
//
// 2048 rather than 256: at a 20k-key keyspace it covers the cursor in ~10
// drains instead of ~80, while the flush work a full page implies (~2048
// executions) still fits an interval on the deployments this bounds.
//
// The keyspace dependency itself is now removed rather than merely bounded: a
// store may keep a readiness index and answer discovery from it in time
// proportional to the ready backlog, and this page then bounds only the
// throttled SWEEP that backstops a registration the index missed. A backend
// without one keeps discovering by sweep, so this stays the knob that bounds
// that path — a keyspace far larger than the default anticipates needs the host
// to raise it further, see WithOutboxDiscoveryPage, because no fixed default
// can track an unbounded keyspace.
const DefaultOutboxDiscoveryPage = 2048

// OutboxDispatcherOption configures an OutboxDispatcher.
type OutboxDispatcherOption func(*OutboxDispatcher)

// WithOutboxDiscoveryPage sizes one drain's discovery page. Zero or negative
// leaves DefaultOutboxDiscoveryPage in place.
//
// The right value is a deployment property, not a library constant: it trades
// keyspace scanned per tick against how long the cursor takes to come around,
// and only the operator knows the keyspace size and how much scan load the
// shared Redis will take. Raising it is the supported answer to "the outbox
// backlog grows but xflow_outbox_drain_discovered stays flat" — see
// DefaultOutboxDiscoveryPage for why the page bounds discovery at all.
func WithOutboxDiscoveryPage(page int) OutboxDispatcherOption {
	return func(d *OutboxDispatcher) {
		if page > 0 {
			d.discoveryPage = page
		}
	}
}

// WithOutboxDrainBudget caps how long one drain may run before it yields to the
// next tick. Zero or negative removes the cap, which is the pre-existing
// behaviour: one drain walks the whole discovery page however long that takes.
//
// The cap is a latency bound, not a throughput one. Work already flushed leaves
// the due page, so a drain that stops early is resumed by the next one rather
// than repeated; what changes is how long outbox metrics and shutdown can be
// held up by a slow store.
func WithOutboxDrainBudget(budget time.Duration) OutboxDispatcherOption {
	return func(d *OutboxDispatcher) {
		d.budget = budget
	}
}

// DefaultOutboxDrainBudget bounds one drain well below the ~10 minutes a page
// costs on a store whose round trip is ~80ms, while staying far above the cost
// of a page on a co-located one (a 2048-entry page at ~0.5ms per round trip is
// single-digit seconds). It is deliberately generous: the budget exists to stop
// a pathological pass, not to shape the steady state.
const DefaultOutboxDrainBudget = 30 * time.Second

// WithOutboxFlushConcurrency sets how many executions one drain flushes at the
// same time. Values below one take the default.
//
// This is the dispatcher's throughput knob, and it is the only one that changes
// throughput at all: one flush costs a fixed number of round trips to the store,
// so a serial loop delivers 1/(that cost) executions per second no matter how
// the page or the budget are sized. Measured against a store ~80ms away, a flush
// costs ~310ms, which caps a serial drain at ~3 executions/second — below the
// rate a collection pipeline produces them, so the backlog grows without bound
// and the sink deliveries it holds fall behind the output TTL they depend on.
//
// Concurrency is safe here because the loop's unit is one execution and distinct
// executions own distinct keys. FlushOutbox is already called concurrently for
// distinct executions by every runner commit, lease and group path, so a
// concurrent drain adds no concurrency class the engine does not already run.
func WithOutboxFlushConcurrency(n int) OutboxDispatcherOption {
	return func(d *OutboxDispatcher) {
		if n > 0 {
			d.flushConcurrency = n
		}
	}
}

// DefaultOutboxFlushConcurrency covers a store whose round trip is tens of
// milliseconds while staying far below the point where one dispatcher's parallel
// flushes would contend with the runners for the shared store. Against a
// co-located store (sub-millisecond round trip) a serial drain already keeps up;
// this only makes it keep up with margin.
const DefaultOutboxFlushConcurrency = 8

// OutboxDispatcher periodically retries durable delivery intents left behind
// by queue outages, response loss, or process crashes.
type OutboxDispatcher struct {
	engine   *Engine
	interval time.Duration
	// discoveryPage is the per-drain discovery limit; metricsInterval is how
	// often drain may run the backlog scan, and lastMetrics is when it last
	// did. drain runs on a single goroutine (Run), so neither needs a lock;
	// metricsRunning is the one field the metrics goroutine shares with it.
	discoveryPage   int
	metricsInterval time.Duration
	lastMetrics     time.Time
	metricsRunning  atomic.Bool
	// budget caps one drain's wall clock so a slow store cannot make a single
	// pass outlive its own tick by minutes. Zero disables the cap.
	budget time.Duration
	// flushConcurrency is how many executions one drain flushes at once. It is
	// the throughput knob; see WithOutboxFlushConcurrency.
	flushConcurrency int
}

// NewOutboxDispatcher creates a retry loop for durable scheduling intents.
func NewOutboxDispatcher(eng *Engine, interval time.Duration, opts ...OutboxDispatcherOption) *OutboxDispatcher {
	if interval <= 0 {
		interval = time.Second
	}
	d := &OutboxDispatcher{
		engine:           eng,
		interval:         interval,
		discoveryPage:    DefaultOutboxDiscoveryPage,
		metricsInterval:  DefaultOutboxMetricsInterval,
		budget:           DefaultOutboxDrainBudget,
		flushConcurrency: DefaultOutboxFlushConcurrency,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

// Run drains ready outboxes until ctx is canceled.
//
// The loop never idles while there is work to find: a drain that outran the
// interval has already spent the wait the ticker would have imposed, so it goes
// straight back to draining instead of blocking on a tick it has effectively
// paid for. That matters because drain time is bounded by discovery and flush,
// not by the interval — on a large keyspace one drain routinely takes longer
// than the tick, and a loop that waited anyway would run one drain per (drain +
// interval) instead of one per drain.
func (d *OutboxDispatcher) Run(ctx context.Context) {
	if d == nil || d.engine == nil {
		return
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		start := time.Now()
		d.drain(ctx)
		if time.Since(start) >= d.interval {
			// Drop the tick that elapsed while this drain ran. It is already
			// spent, and letting it stand would make the NEXT iteration return
			// immediately for no reason — an extra drain on a backlog that this
			// one may have just emptied.
			select {
			case <-ticker.C:
			default:
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// drain runs one discovery-and-flush pass and reports what it cost.
//
// The duration covers the whole pass — discovery, the flush of every execution
// the page yielded, and the throttled backlog scan — because that is the
// interval the loop actually experiences. A drain longer than the configured
// interval is the signal that dispatch is bounded by its own work rather than
// by the tick; see xflow_outbox_drain_duration_seconds.
func (d *OutboxDispatcher) drain(ctx context.Context) {
	start := time.Now()
	discovered := 0
	defer func() {
		d.engine.notifyOutboxDrain(ctx, discovered, time.Since(start))
	}()
	state, err := d.engine.atomicState()
	if err != nil {
		d.engine.notifyOutboxError(ctx, "state", err)
		return
	}
	page := d.discoveryPage
	if page <= 0 {
		page = DefaultOutboxDiscoveryPage
	}
	ids, err := state.ListOutboxExecutions(ctx, page)
	if err != nil {
		d.engine.notifyOutboxError(ctx, "list_executions", err)
		if d.engine.logger != nil {
			d.engine.logger.Error("list outbox executions failed", "err", err)
		}
		return
	}
	discovered = len(ids)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	d.flushPage(ctx, ids, start)
	d.observeMetrics(ctx, state)
}

// flushPage flushes one discovery page, at most flushConcurrency executions at a
// time, and stops submitting once the drain's budget is spent.
//
// A serial loop is what this replaces, and the reason it had to go is that a
// serial loop's throughput is fixed by the cost of one flush rather than by
// anything the loop controls: page size and budget both change how the work is
// shaped, never how fast it completes. One flush is a handful of round trips to
// the store, so a store 80ms away caps a serial drain at ~3 executions/second
// while the collection pipeline produces ~16 — the backlog then grows without
// bound, and because a sink delivery is what reclaims the collection output, the
// outputs it cannot reach in time sit out their whole TTL. Concurrency is the
// only knob that moves that number.
//
// A page entry this pass does not reach is not skipped work: FlushOutbox drives
// each execution to a fixed point (it loops until a claim comes back empty), so
// a flushed execution has either leased all its ready work or been pruned from
// the due page. Whatever is left is picked up by the next drain.
func (d *OutboxDispatcher) flushPage(ctx context.Context, ids []types.ExecutionID, start time.Time) {
	workers := d.flushConcurrency
	if workers <= 0 {
		workers = DefaultOutboxFlushConcurrency
	}
	if workers > len(ids) {
		workers = len(ids)
	}
	if workers <= 1 {
		for i, id := range ids {
			if i > 0 && d.budgetSpent(start) {
				return
			}
			d.flushOne(ctx, id)
		}
		return
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	submit := func(id types.ExecutionID) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d.flushOne(ctx, id)
		}()
	}
	for i, id := range ids {
		// One flush is allowed to finish past the budget — it is in flight by the
		// time the budget is checked again — so the bound is the budget plus at
		// most one flush.
		if i > 0 && d.budgetSpent(start) {
			break
		}
		if ctx.Err() != nil {
			break
		}
		submit(id)
	}
	wg.Wait()
}

// budgetSpent reports whether this drain has used up its wall-clock budget. A
// zero budget means unbounded, which is the pre-existing behaviour.
func (d *OutboxDispatcher) budgetSpent(start time.Time) bool {
	return d.budget > 0 && time.Since(start) >= d.budget
}

// flushOne flushes a single execution's outbox, reporting a delivery failure
// without aborting the rest of the page: one execution's store error is not
// evidence about any other execution's.
func (d *OutboxDispatcher) flushOne(ctx context.Context, id types.ExecutionID) {
	if err := d.engine.FlushOutbox(ctx, id); err != nil && d.engine.logger != nil {
		d.engine.logger.Error("flush durable outbox failed", "execution_id", string(id), "err", err)
	}
}

// observeMetrics runs the outbox backlog scan at most once per metricsInterval.
// The scan walks the whole keyspace, so running it on the delivery tick costs
// far more than the gauge it reports, and the gauge does not need second-level
// freshness. A zero lastMetrics means the first drain is always observed.
//
// It runs OFF the delivery goroutine, and that is not an optimisation. The scan
// is unbounded and its cost grows with the backlog it measures, so inline it
// made delivery wait on the measurement of its own backlog: while one scan ran,
// no outbox intent was delivered at all, which is precisely the state that keeps
// the backlog large. Observed on a keyspace of a few thousand pending
// executions, at 1s ticks, as every sink dispatch stalling for minutes — the
// backlog then never draining, and the executions behind it never completing.
//
// At most one scan is in flight. A tick that finds one running is dropped rather
// than queued: a stale gauge is harmless, a queue of expensive scans is not.
func (d *OutboxDispatcher) observeMetrics(ctx context.Context, state AtomicStateStore) {
	now := time.Now()
	if !d.lastMetrics.IsZero() && now.Sub(d.lastMetrics) < d.metricsInterval {
		return
	}
	if !d.metricsRunning.CompareAndSwap(false, true) {
		return
	}
	d.lastMetrics = now
	go func() {
		defer d.metricsRunning.Store(false)
		d.engine.observeOutboxMetrics(ctx, state)
	}()
}

// requeueGroupOutboxID is the group-unit analogue of requeueOutboxID. It keys
// on the unit index rather than the node name because a group's entry node name
// is shared by every unit the group expands into.
func requeueGroupOutboxID(id types.ExecutionID, unitIdx int, leaseID LeaseID) string {
	return fmt.Sprintf("requeue-group/%s/%d/%s", id, unitIdx, leaseID)
}
