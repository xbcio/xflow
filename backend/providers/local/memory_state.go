package local

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// queuedOutboxHold is the availability stamped on the initial intents of a
// QUEUE-mode execution: far enough in the future that no read path will ever
// consider one due, which is how "persisted but not deliverable" is expressed
// without a second outbox structure.
//
// It is a fixed instant rather than a deadline the store re-evaluates. A queued
// execution's intents become deliverable when it is promoted, not when some
// time passes, so anything that made them due by clock alone would start a
// queued execution that holds no slot.
var queuedOutboxHold = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// memoryState implements engine.StateStore using in-memory maps protected by a mutex.
// It is suitable for single-process (local) execution only.
type memoryState struct {
	mu         sync.Mutex
	executions map[types.ExecutionID]*execEntry
	nodes      map[string]*engine.NodeSnapshot // key: execID+"/"+name
	inDegrees  map[string]int                  // key: execID+"/"+unitIdx
	activeIns  map[string]int                  // key: execID+"/"+unitIdx
	remaining  map[types.ExecutionID]int
	failed     map[types.ExecutionID]int
	advanced   map[string]bool
	scheduled  map[string]string
	outbox     map[types.ExecutionID]map[string]memoryOutboxEntry
	deadOutbox map[types.ExecutionID]map[string]memoryOutboxEntry
	// replayReceipts is the authoritative in-memory receipt keyed by
	// execution→requestID; replayEntryIdx maps execution→entryID→requestID so a
	// retried replay with a different RequestID returns already_replayed.
	replayReceipts map[types.ExecutionID]map[string]*memoryReplayReceipt
	replayEntryIdx map[types.ExecutionID]map[string]string
	outputs        map[string]map[string]any // key: execID+"/"+name
	// privateOutputs is an execution-local, monotonic privacy marker for runtime
	// output keys. It is separate from nodes because group and entry exits have
	// runtime outputs without necessarily materializing a NodeSnapshot.
	privateOutputs map[string]bool
	suspended      map[string]*types.SuspendSpec // key: execID+"/"+nodeName
	signals        map[string]map[string]any     // pre-delivered: key: execID+"/"+signalName
	signalSets     map[string]map[string]map[string]any
	resumed        map[string]bool                   // resume lock: key: execID+"/"+nodeName
	subExecs       map[string][]*engine.SubExecution // key: execID+"/"+nodeName
	// subWorkflowLinks is the parent-side record of each parked child
	// execution, keyed like nodes. It is deliberately NOT cleared by
	// RevokeLease/ResetNodeForRetry: the link outlives the wait so a reclaimed
	// redelivery resumes the same child, and a retry overwrites it when the
	// next generation parks.
	subWorkflowLinks map[string]*engine.SubWorkflowLink      // key: execID+"/"+nodeName
	groupUnits       map[string]*groupUnitState              // key: "<execID>/<unitIdx>"
	admissions       map[engine.AdmissionKey]*admissionEntry // trigger admission keys
	// idempotency holds the claims of keyed submissions, keyed by the
	// deterministic execution ID the key derives (engine.IdempotentStateStore).
	idempotency map[types.ExecutionID]*idempotencyClaim
	// workflowSlots is the workflow-scoped concurrency slot set, keyed by
	// graph.ConcurrencyKey(ns, workflow name), and slotHolders records which set
	// each admitted execution was charged to so a terminal transition can
	// release it without consulting a graph. queued is the mirror image for
	// QUEUE-mode executions that hold no slot yet: their set, so a promotion or
	// a cancellation can find the line they are standing in.
	workflowSlots map[string]*workflowSlot
	slotHolders   map[types.ExecutionID]*workflowSlot
	queued        map[types.ExecutionID]*queuedExecution

	// done channels allow Wait() callers to block until execution completes.
	doneCh        map[types.ExecutionID]chan struct{}
	eventWatchers map[types.ExecutionID][]chan engine.ExecutionEvent
}

type execEntry struct {
	snap   engine.ExecutionSnapshot
	closed bool // true once the done channel has been closed
}

func newMemoryState() *memoryState {
	return &memoryState{
		executions:       make(map[types.ExecutionID]*execEntry),
		nodes:            make(map[string]*engine.NodeSnapshot),
		inDegrees:        make(map[string]int),
		activeIns:        make(map[string]int),
		remaining:        make(map[types.ExecutionID]int),
		failed:           make(map[types.ExecutionID]int),
		advanced:         make(map[string]bool),
		scheduled:        make(map[string]string),
		outbox:           make(map[types.ExecutionID]map[string]memoryOutboxEntry),
		deadOutbox:       make(map[types.ExecutionID]map[string]memoryOutboxEntry),
		replayReceipts:   make(map[types.ExecutionID]map[string]*memoryReplayReceipt),
		replayEntryIdx:   make(map[types.ExecutionID]map[string]string),
		outputs:          make(map[string]map[string]any),
		privateOutputs:   make(map[string]bool),
		suspended:        make(map[string]*types.SuspendSpec),
		signals:          make(map[string]map[string]any),
		signalSets:       make(map[string]map[string]map[string]any),
		resumed:          make(map[string]bool),
		subExecs:         make(map[string][]*engine.SubExecution),
		subWorkflowLinks: make(map[string]*engine.SubWorkflowLink),
		groupUnits:       make(map[string]*groupUnitState),
		doneCh:           make(map[types.ExecutionID]chan struct{}),
		eventWatchers:    make(map[types.ExecutionID][]chan engine.ExecutionEvent),
	}
}

// ---------------------------------------------------------------------------
// ExecutionStore
// ---------------------------------------------------------------------------

func (s *memoryState) CreateExecution(_ context.Context, e *engine.ExecutionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createExecutionLocked(e)
	return nil
}

// CreateExecutionWithOutbox atomically records an execution and its initial
// durable task intents under the memory reference model's single mutex.
func (s *memoryState) CreateExecutionWithOutbox(_ context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createExecutionLocked(e)
	for _, entry := range entries {
		// A root skip intent is fenced on its unit's "skip" scheduling marker,
		// which must exist before the intent can be delivered (see
		// engine.UnselectedRootSkips).
		if entry.Task.Type == engine.TaskTypeNodeSkip {
			s.scheduled[memoryCounterKey(e.ID, entry.Task.UnitIdx)] = "skip"
		}
		s.putOutboxEntryLocked(e.ID, entry)
	}
	return nil
}

var _ engine.ConcurrencyLimitedStateStore = (*memoryState)(nil)
var _ engine.ConcurrencyQueueExpirer = (*memoryState)(nil)
var _ engine.ConcurrencyQueueInspector = (*memoryState)(nil)

// workflowSlot names the slot set one admitted execution was charged to. It is
// recorded at admission because a release must free the SAME set, and by
// terminal-write time the graph the admission read may no longer be around.
type workflowSlot struct {
	key     string
	holders map[types.ExecutionID]bool
	// limit is the capacity the most recently admitted submission declared.
	// Promotions read it at release time, when the submitting graph is long
	// gone, so it has to be remembered here. A lowered limit therefore lets the
	// running holders finish and stops the line at the new capacity; raising it
	// frees capacity the next release, or the next admission, fills.
	limit int
	// queued is the FIFO line of executions waiting for a holder slot, head
	// first. Order is the whole point of the structure: the execution that has
	// waited longest is the one promoted next.
	queued []types.ExecutionID
}

// queuedExecution is one execution standing in a workflow's line: the set it
// waits for, and the instant its queue timeout expires (zero when the workflow
// declared none).
type queuedExecution struct {
	slot     *workflowSlot
	deadline time.Time
}

// CreateExecutionWithConcurrency admits one submission against its workflow's
// slot set, then writes the execution and its initial intents under the same
// mutex acquisition. Doing the three in one critical section is what makes the
// limit sound: a slot charged for an execution that failed to persist would
// leak until restart, and an execution persisted without its slot would
// over-admit.
//
// A refused submission writes nothing. The engine relies on that to report a
// 429 for a rejection rather than leaving a zero-node execution behind.
func (s *memoryState) CreateExecutionWithConcurrency(_ context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry, admission engine.ConcurrencyAdmission) (engine.ConcurrencyAdmissionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, err := s.chargeOrParkLocked(e.ID, admission)
	if err != nil {
		return engine.ConcurrencyAdmissionResult{}, err
	}
	status := types.ExecutionStatusRunning
	hold := time.Time{}
	if pending {
		status, hold = types.ExecutionStatusQueued, queuedOutboxHold
	}
	admitted := *e
	admitted.Status = status
	s.createExecutionLocked(&admitted)
	s.writeInitialIntentsLocked(e.ID, entries, hold)
	if !pending {
		return engine.ConcurrencyAdmissionResult{}, nil
	}
	return engine.ConcurrencyAdmissionResult{Queued: true, QueuePosition: s.queuePositionLocked(e.ID)}, nil
}

// chargeOrParkLocked decides where one admission lands and records the
// decision: a charged slot, or a place at the end of the workflow's line.
//
// It is the decision alone, with no execution written under it, because the two
// paths that call it write different things — a submission writes its initial
// intents, an entry seed writes an admission — and the decision is the part
// that must not diverge between them. Everything a caller writes afterwards
// happens in the same critical section, so the slot (or the place) is durable
// before anything depends on it.
func (s *memoryState) chargeOrParkLocked(id types.ExecutionID, admission engine.ConcurrencyAdmission) (bool, error) {
	slot := s.slotForLocked(graph.ConcurrencyKey(admission.Namespace, admission.Workflow))
	slot.limit = admission.Limit

	if admission.Mode == types.ConcurrencyModeQueue {
		return s.parkOrChargeLocked(slot, id, admission)
	}
	if len(slot.holders) >= admission.Limit {
		return false, engine.ErrConcurrencyLimit
	}
	s.chargeSlotLocked(slot, id)
	return false, nil
}

// chargeSlotLocked records that one execution holds a slot in the set.
func (s *memoryState) chargeSlotLocked(slot *workflowSlot, id types.ExecutionID) {
	slot.holders[id] = true
	if s.slotHolders == nil {
		s.slotHolders = make(map[types.ExecutionID]*workflowSlot)
	}
	s.slotHolders[id] = slot
}

// slotForLocked returns the workflow's slot state, creating it on first use.
func (s *memoryState) slotForLocked(key string) *workflowSlot {
	slot := s.workflowSlots[key]
	if slot == nil {
		slot = &workflowSlot{key: key, holders: make(map[types.ExecutionID]bool)}
		if s.workflowSlots == nil {
			s.workflowSlots = make(map[string]*workflowSlot)
		}
		s.workflowSlots[key] = slot
	}
	return slot
}

// parkOrChargeLocked places an over-limit submission in the workflow's FIFO
// line, or charges it a slot when the workflow has room and nobody is waiting.
//
// The line is what decides: a free slot belongs to the execution that has been
// waiting longest, so a fresh submission is admitted directly only when the
// line is empty. Admitting it ahead of the line would be exactly the
// starvation FIFO exists to prevent, and it is reachable — capacity frees up
// between a release and the next admission without the head having been
// promoted only in the crash window the reconciler repairs.
func (s *memoryState) parkOrChargeLocked(slot *workflowSlot, id types.ExecutionID, admission engine.ConcurrencyAdmission) (bool, error) {
	if len(slot.holders) < slot.limit && len(slot.queued) == 0 {
		s.chargeSlotLocked(slot, id)
		return false, nil
	}
	if admission.MaxQueued > 0 && len(slot.queued) >= admission.MaxQueued {
		// Refused before anything is written, like a reject-mode refusal: a
		// full queue is a 429 the caller can retry, not a persisted execution
		// waiting nowhere.
		return false, engine.ErrConcurrencyQueueFull
	}

	slot.queued = append(slot.queued, id)
	if s.queued == nil {
		s.queued = make(map[types.ExecutionID]*queuedExecution)
	}
	var deadline time.Time
	if admission.QueueTimeout > 0 {
		deadline = time.Now().UTC().Add(admission.QueueTimeout)
	}
	s.queued[id] = &queuedExecution{slot: slot, deadline: deadline}
	return true, nil
}

// queuePositionLocked reports where one queued execution stands in its line,
// 1-based, or 0 when it is not waiting.
func (s *memoryState) queuePositionLocked(id types.ExecutionID) int {
	waiting, ok := s.queued[id]
	if !ok {
		return 0
	}
	for i, queued := range waiting.slot.queued {
		if queued == id {
			return i + 1
		}
	}
	return 0
}

// holdOutboxLocked parks every deliverable intent the execution holds.
//
// The seed path writes its intents through the same helpers a running
// execution uses, so the parked case re-stamps them here rather than threading
// a hold parameter through those helpers: the decision to hold belongs to the
// admission, not to whichever function happened to write the entry. An intent
// that is already held or already leased is left alone.
func (s *memoryState) holdOutboxLocked(id types.ExecutionID) {
	for entryID, held := range s.outbox[id] {
		if !held.entry.AvailableAt.IsZero() {
			continue
		}
		held.entry.AvailableAt = queuedOutboxHold
		s.outbox[id][entryID] = held
	}
}

// writeInitialIntentsLocked persists an execution's root intents, stamped with
// the availability they start out at.
//
// A queued execution's intents are stamped queuedOutboxHold: they exist in full
// — a crash cannot lose them, and promotion does not have to reconstruct them —
// but nothing considers them due until the execution holds a slot.
func (s *memoryState) writeInitialIntentsLocked(id types.ExecutionID, entries []engine.OutboxEntry, availableAt time.Time) {
	for _, entry := range entries {
		if entry.Task.Type == engine.TaskTypeNodeSkip {
			s.scheduled[memoryCounterKey(id, entry.Task.UnitIdx)] = "skip"
		}
		if !availableAt.IsZero() {
			entry.AvailableAt = availableAt
		}
		s.putOutboxEntryLocked(id, entry)
	}
}

// releaseWorkflowSlotLocked frees the concurrency state an execution held. It is
// called from every path that moves an execution into a terminal status, so no
// engine call site can forget it; a non-terminal transition (canceling) keeps
// the slot, because the execution still holds the resources the limit protects.
//
// A queued execution holds no slot but does hold a place in line, and its
// intents were never deliverable, so leaving the line is the whole release and
// the parked intents are dropped with it — a canceled or timed-out execution
// that never ran has nothing left to deliver.
func (s *memoryState) releaseWorkflowSlotLocked(id types.ExecutionID) {
	slot, held := s.slotHolders[id]
	if held {
		delete(s.slotHolders, id)
		delete(slot.holders, id)
	} else if waiting, ok := s.queued[id]; ok {
		delete(s.queued, id)
		waiting.slot.removeQueued(id)
		s.dropHeldOutboxLocked(id)
		slot = waiting.slot
	} else {
		return
	}
	// Whatever capacity the departure freed goes to the head of the line. A
	// queued execution's own departure frees none, but the loop is the same
	// either way: it fills capacity when there is any and does nothing when
	// there is not.
	s.promoteLocked(slot)
	if len(slot.holders) == 0 && len(slot.queued) == 0 {
		delete(s.workflowSlots, slot.key)
	}
}

// removeQueued takes one execution out of the line, keeping the order of the
// rest. The line is short (bounded by max_queued) and this runs once per
// promotion or cancellation, so the copy is cheaper than a linked structure
// would be to reason about.
func (slot *workflowSlot) removeQueued(id types.ExecutionID) {
	for i, queued := range slot.queued {
		if queued == id {
			slot.queued = append(slot.queued[:i], slot.queued[i+1:]...)
			return
		}
	}
}

// promoteLocked fills the workflow's free capacity from the head of its line.
// It loops because a single promotion can free the capacity another promotion
// needs — a workflow admitted below its limit after a limit raise, or a
// reconciler-repaired set, promotes as many as fit rather than one per event.
func (s *memoryState) promoteLocked(slot *workflowSlot) {
	for slot.limit > 0 && len(slot.holders) < slot.limit && len(slot.queued) > 0 {
		id := slot.queued[0]
		slot.queued = slot.queued[1:]
		delete(s.queued, id)
		slot.holders[id] = true
		if s.slotHolders == nil {
			s.slotHolders = make(map[types.ExecutionID]*workflowSlot)
		}
		s.slotHolders[id] = slot
		s.completePromotionLocked(id)
	}
}

// completePromotionLocked starts a promoted execution.
//
// The two halves move together: an execution that is running while its intents
// stay held never starts at all, and one whose intents became deliverable while
// it still reads queued would run while holding no slot — the failure the limit
// exists to prevent. The status is what a caller polls, the availability is
// what the dispatcher reads, and neither is meaningful without the other.
func (s *memoryState) completePromotionLocked(id types.ExecutionID) {
	if entry, ok := s.executions[id]; ok && entry.snap.Status == types.ExecutionStatusQueued {
		entry.snap.Status = types.ExecutionStatusRunning
		s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: types.ExecutionStatusRunning})
	}
	for entryID, held := range s.outbox[id] {
		if !held.entry.AvailableAt.Equal(queuedOutboxHold) {
			continue
		}
		held.entry.AvailableAt = time.Time{}
		s.outbox[id][entryID] = held
	}
}

// dropHeldOutboxLocked discards the intents a queued execution parked. Only
// held entries go: an entry that became deliverable was promoted work, and
// discarding it would drop a task the execution still owes.
func (s *memoryState) dropHeldOutboxLocked(id types.ExecutionID) {
	for entryID, held := range s.outbox[id] {
		if held.entry.AvailableAt.Equal(queuedOutboxHold) {
			delete(s.outbox[id], entryID)
		}
	}
	if len(s.outbox[id]) == 0 {
		delete(s.outbox, id)
	}
}

// ExpireQueuedExecutions drives every queued execution whose queue timeout has
// passed into the timeout terminal state, and reports how many it expired.
//
// The clock starts when the execution is parked, which is the only instant that
// makes queue_timeout mean what it says: a bound on how long a submission may
// wait for a slot. Measured from promotion it could never fire at all — a
// promoted execution is not waiting for anything — and a workflow whose limit
// never frees would hold submissions forever.
func (s *memoryState) ExpireQueuedExecutions(_ context.Context, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queued) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	expired := make([]types.ExecutionID, 0, len(s.queued))
	for id, waiting := range s.queued {
		if limit > 0 && len(expired) >= limit {
			break
		}
		if waiting.deadline.IsZero() || waiting.deadline.After(now) {
			continue
		}
		expired = append(expired, id)
	}
	for _, id := range expired {
		// Through the same terminal write every other path uses, so the queue
		// departure, the event and the error read-back cannot diverge from a
		// cancellation.
		_ = s.updateExecutionStatusLocked(id, types.ExecutionStatusTimeout, engine.ConcurrencyQueueTimeoutError)
	}
	return len(expired), nil
}

// ConcurrencyQueuePosition reports where one queued execution stands in its
// workflow's line, 1-based, or 0 when it is not queued.
//
// The line is a slice, so the position is its index: the head is the execution
// the next release promotes, which is exactly what a waiting submitter wants to
// know. An execution that was never queued, or whose wait already ended in a
// promotion, an expiry or a cancellation, is not in the map and answers 0.
func (s *memoryState) ConcurrencyQueuePosition(_ context.Context, id types.ExecutionID) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting, ok := s.queued[id]
	if !ok {
		return 0, nil
	}
	for i, queued := range waiting.slot.queued {
		if queued == id {
			return i + 1, nil
		}
	}
	// The map and the line disagree, which can only mean a half-finished
	// departure. Reporting 0 is the honest answer to "where does it stand":
	// nowhere this store can point at.
	return 0, nil
}

func (s *memoryState) createExecutionLocked(e *engine.ExecutionSnapshot) {
	cp := *e
	s.executions[e.ID] = &execEntry{snap: cp}
	// Reuse a pre-registered done channel from an early waitDone call (race with
	// CreateExecution), or create a fresh one.
	if _, ok := s.doneCh[e.ID]; !ok {
		s.doneCh[e.ID] = make(chan struct{})
	}
	// Seed in-degree counters and O(1) completion counters from the compiled graph.
	if e.Graph != nil {
		for i := 0; i < e.Graph.UnitCount(); i++ {
			d := e.Graph.UnitInDegreeAt(i)
			key := fmt.Sprintf("%s/%d", e.ID, i)
			s.inDegrees[key] = d
		}
		if !e.Graph.AllowCycles() {
			s.remaining[e.ID] = e.Graph.UnitCount()
			s.failed[e.ID] = 0
		}
	}
}

func (s *memoryState) UpdateExecutionStatus(_ context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateExecutionStatusLocked(id, status, errMsg)
}

// updateExecutionStatusLocked is the terminal-write body shared by the public
// method and the store's own background transitions (the queue timeout). They
// have to agree on everything — the CAS fence, the reason read-back, the slot
// release, the event — and the only way to guarantee that is to have one.
func (s *memoryState) updateExecutionStatusLocked(id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	entry, ok := s.executions[id]
	if !ok {
		return nil
	}
	// CAS guard — mirrors the distributed backend's Lua fencing:
	//   - A terminal status is never overwritten.
	//   - canceling blocks any non-canceled write (cancel owns the terminal transition).
	current := entry.snap.Status
	if isTerminalStatus(current) {
		return nil
	}
	if current == types.ExecutionStatusCanceling && status != types.ExecutionStatusCanceled {
		return nil
	}
	entry.snap.Status = status
	// Record the execution-level reason alongside the status, using the
	// distributed backend's exact rule: updateExecutionStatusLua writes
	// exec:<id>:error only when the reason argument is non-empty, and
	// never deletes it. Gating on the status instead would diverge from Redis —
	// local would clear a reason that Redis keeps.
	if errMsg != "" {
		entry.snap.Error = errMsg
	}
	// Close the done channel on terminal status so Wait() unblocks.
	if isTerminalStatus(status) {
		s.releaseWorkflowSlotLocked(id)
		if !entry.closed {
			entry.closed = true
			if ch, ok := s.doneCh[id]; ok {
				close(ch)
			}
		}
	}
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
	return nil
}

func (s *memoryState) GetExecution(_ context.Context, id types.ExecutionID) (*engine.ExecutionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	cp := entry.snap
	return &cp, nil
}

// GetExecutionStatus reads back only the lifecycle status
// (engine.ExecutionStatusReader). Here it saves a snapshot copy rather than
// round trips — the win is on key-value backends — but it is implemented anyway
// so both backends answer loadActiveGraph's activeness question through the same
// method. If only one implemented it, the engine would take a different path per
// backend and the contract suite could not pin that the two agree.
func (s *memoryState) GetExecutionStatus(_ context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return "", false, nil
	}
	return entry.snap.Status, true, nil
}

// GetExecutionStatuses is the batch form of GetExecutionStatus. It exists for the
// same reason the interface does: the callers ask about a whole page at once, and
// a backend that answered the batch differently from the single read would make
// activeness depend on which one the caller happened to use.
func (s *memoryState) GetExecutionStatuses(_ context.Context, ids []types.ExecutionID) ([]types.ExecutionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]types.ExecutionStatus, len(ids))
	for i, id := range ids {
		if entry, ok := s.executions[id]; ok {
			out[i] = entry.snap.Status
		}
	}
	return out, nil
}

func (s *memoryState) LoadGraph(_ context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.executions[id]
	if !ok {
		return nil, nil
	}
	return entry.snap.Graph, nil
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

func (s *memoryState) UpsertNode(_ context.Context, n *engine.NodeSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memoryNodeKey(n.ExecutionID, n.Name)
	// Record a privacy decision before checking the write fence. A stale update
	// that carries the marker must still fail closed; a later zero-value update
	// is never authority to declassify this runtime output.
	privateOutput := s.preserveOutputPrivacyLocked(key, n.PrivateOutput)
	if existing, ok := s.nodes[key]; ok && isTerminalNode(existing.Status) && n.ActivationID <= existing.ActivationID {
		return nil // CAS: don't overwrite terminal state
	}
	if existing, ok := s.nodes[key]; ok && (existing.Status == types.NodeStatusCommitting || existing.Status == types.NodeStatusWaiting) && n.Status == types.NodeStatusRunning {
		return nil
	}
	cp := *n
	cp.PrivateOutput = privateOutput
	if cp.PrivateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	return nil
}

func (s *memoryState) GetNode(_ context.Context, id types.ExecutionID, name string) (*engine.NodeSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memoryNodeKey(id, name)
	return s.publicNodeSnapshotLocked(key, s.nodes[key]), nil
}

// publicNodeSnapshotLocked returns an isolated, externally readable node
// snapshot. A group or entry exit can set a marker without materializing a
// node, so the key-level marker is authoritative for every public read.
// Callers must hold s.mu.
func (s *memoryState) publicNodeSnapshotLocked(key string, ns *engine.NodeSnapshot) *engine.NodeSnapshot {
	cp := cloneNodeSnapshot(ns)
	if cp != nil && s.outputIsPrivateLocked(key) {
		cp.PrivateOutput = true
		cp.Output = nil
	}
	return cp
}

// preserveOutputPrivacyLocked records an output privacy decision without ever
// clearing an earlier one. Callers must hold s.mu. Output privacy is graph
// policy, so a false value in an ordinary retry/resume update is not authority
// to declassify a previously private output.
func (s *memoryState) preserveOutputPrivacyLocked(key string, requested bool) bool {
	privateOutput := requested || s.outputIsPrivateLocked(key)
	if !privateOutput {
		return false
	}
	s.privateOutputs[key] = true
	if node := s.nodes[key]; node != nil && (!node.PrivateOutput || node.Output != nil) {
		cp := *node
		cp.PrivateOutput = true
		cp.Output = nil
		s.nodes[key] = &cp
	}
	return true
}

// outputIsPrivateLocked reports the monotonic marker for one runtime output
// key. The snapshot fallback keeps states created before the marker map in
// sync; new writes always populate the map through preserveOutputPrivacyLocked.
func (s *memoryState) outputIsPrivateLocked(key string) bool {
	if s.privateOutputs[key] {
		return true
	}
	node := s.nodes[key]
	return node != nil && node.PrivateOutput
}

// CancelSuspendedNode atomically flips a node from Suspended to Canceled under
// the state lock, preserving all other snapshot fields. It returns
// canceled=false without mutating anything when the node is missing or no
// longer Suspended, so a concurrent resume's live lease is never clobbered
// (engine.SuspendedNodeCanceler).
//
// Retiring the waiter is part of the transition, not bookkeeping that can be
// deferred. s.suspended is two things at once here: the set ListSuspendedNodes
// reports, and the index DeliverSignal/PeekResumeTarget scan to find who is
// waiting on a signal name. Leaving the entry behind therefore did two things —
// ListSuspendedNodes kept naming a node that is Canceled, and a later signal
// still matched it and consumed itself against a node that can never run.
//
// The distributed backend does this in the same atomic step
// (cancelSuspendedNodeLua SREMs the suspended set) and wipes the rest of the
// waiter state in cleanupOnCancel when the execution reaches Canceled. There is
// no cleanupOnCancel here, so this deletion is the only place it can happen.
func (s *memoryState) CancelSuspendedNode(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil || ns.Status != types.NodeStatusSuspended {
		return false, nil
	}
	cp := *ns
	cp.Status = types.NodeStatusCanceled
	cp.PrivateOutput = s.preserveOutputPrivacyLocked(key, cp.PrivateOutput)
	if cp.PrivateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	delete(s.suspended, key)
	// Multi-signal waiters accumulate partial arrivals under the same key; the
	// consume paths drop both together and so must the cancel path.
	delete(s.signalSets, key)
	return true, nil
}

func cloneNodeSnapshot(ns *engine.NodeSnapshot) *engine.NodeSnapshot {
	if ns == nil {
		return nil
	}
	cp := *ns
	cp.Output = cloneData(ns.Output)
	// ErrorDetails is returned to read callers, so it must not alias the stored
	// snapshot either — a caller that mutates the map it was handed would
	// otherwise edit the node's persisted failure detail.
	cp.ErrorDetails = cloneData(ns.ErrorDetails)
	cp.LeasePayload = cloneLeasePayload(ns.LeasePayload)
	return &cp
}

func cloneLeasePayload(payload *types.SignalPayload) *types.SignalPayload {
	if payload == nil {
		return nil
	}
	cp := *payload
	cp.Data = cloneData(payload.Data)
	if payload.All != nil {
		cp.All = make(map[string]map[string]any, len(payload.All))
		for name, data := range payload.All {
			cp.All[name] = cloneData(data)
		}
	}
	return &cp
}

func (s *memoryState) AcquireTaskLease(_ context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)
	current := s.nodes[key]
	if current != nil {
		if lease.Task.ActivationID > 0 && current.ActivationID > lease.Task.ActivationID {
			return s.publicNodeSnapshotLocked(key, current), false, nil
		}
		if isTerminalNode(current.Status) && (lease.Task.ActivationID <= 0 || current.ActivationID >= lease.Task.ActivationID) {
			return s.publicNodeSnapshotLocked(key, current), false, nil
		}
		if current.Status == types.NodeStatusCommitting || current.Status == types.NodeStatusWaiting {
			return s.publicNodeSnapshotLocked(key, current), false, nil
		}
		if current.Status == types.NodeStatusRunning && current.LeaseToken != "" {
			deadline := current.LeaseIssuedAt.Add(current.LeaseTTL)
			if current.LeaseIssuedAt.IsZero() || current.LeaseTTL <= 0 || lease.IssuedAt.Before(deadline) {
				return s.publicNodeSnapshotLocked(key, current), false, nil
			}
		}
	}

	// Mirrors acquireTaskLeaseLua: attempt counts retries within one activation
	// and restarts at 1 on a cyclic re-entry (a higher ActivationID), so a
	// looping node cannot exhaust MaxAttempts across activations.
	attempt := 1
	if current != nil && current.ActivationID == lease.Task.ActivationID {
		attempt = current.Attempt + 1
	}
	privateOutput := s.preserveOutputPrivacyLocked(key, false)
	node := &engine.NodeSnapshot{
		ExecutionID:     lease.Task.ExecutionID,
		Name:            lease.Task.NodeName,
		NodeIdx:         lease.Task.NodeIdx,
		UnitIdx:         lease.Task.UnitIdx,
		Status:          types.NodeStatusRunning,
		LeaseID:         lease.LeaseID,
		LeaseToken:      lease.LeaseToken,
		Attempt:         attempt,
		ActivationID:    lease.Task.ActivationID,
		AutoDepth:       lease.Task.AutoDepth,
		LeaseIssuedAt:   lease.IssuedAt,
		LeaseTTL:        lease.TTL,
		LeaseTaskType:   lease.Task.Type,
		LeasePayload:    cloneLeasePayload(lease.Task.Payload),
		LeaseGeneration: lease.Task.Generation,
		PrivateOutput:   privateOutput,
	}
	// The re-claim rebuilds the record, so anything not carried over is dropped.
	// The child a sub-workflow node waited on outlives the lease exactly as it
	// outlives the commit (see CommitNode): the redelivered wait resumes that
	// child, and the drill-down reads it from here, so a re-claim that cleared
	// it would lose both. The rstate acquire works on the existing hash and
	// never touches the field, which is the behavior this mirrors.
	if current != nil {
		node.ChildExecutionID = current.ChildExecutionID
	}
	s.nodes[key] = node
	return s.publicNodeSnapshotLocked(key, current), true, nil
}

// ResetNodeForRetryWithOutbox atomically creates the retry delivery intent
// when the current node is eligible to move back to pending.
func (s *memoryState) ResetNodeForRetryWithOutbox(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetNodeForRetryLocked(id, name, token) {
		return false, nil
	}
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

func (s *memoryState) resetNodeForRetryLocked(id types.ExecutionID, name string, token ...engine.LeaseToken) bool {
	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil {
		return false
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
		return false
	}
	if len(token) > 0 && (token[0] == "" || ns.LeaseToken != token[0]) {
		return false
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	cp.LeaseGeneration = 0
	cp.PrivateOutput = s.preserveOutputPrivacyLocked(key, cp.PrivateOutput)
	if cp.PrivateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	return true
}

func (s *memoryState) ListExpiredLeases(_ context.Context, before time.Time) ([]engine.ExpiredLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.ExpiredLease
	for _, ns := range s.nodes {
		if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
			continue
		}
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 {
			continue
		}
		if ns.LeaseIssuedAt.Add(ns.LeaseTTL).After(before) {
			continue
		}
		out = append(out, engine.ExpiredLease{
			ExecutionID:  ns.ExecutionID,
			NodeName:     ns.Name,
			NodeIdx:      ns.NodeIdx,
			UnitIdx:      ns.UnitIdx,
			LeaseID:      ns.LeaseID,
			LeaseToken:   ns.LeaseToken,
			IssuedAt:     ns.LeaseIssuedAt,
			TTL:          ns.LeaseTTL,
			ActivationID: ns.ActivationID,
			AutoDepth:    ns.AutoDepth,
			TaskType:     ns.LeaseTaskType,
			Payload:      cloneLeasePayload(ns.LeasePayload),
			Generation:   ns.LeaseGeneration,
		})
	}
	// Group units lease through their own state map, not s.nodes. A sweeper
	// that only walked s.nodes would leave a runner that died holding a group
	// lease stranding its unit in "running" forever — AcquireGroupLease
	// refuses to re-acquire a running unit, so nothing else revives it.
	for _, gs := range s.groupUnits {
		if gs.status != groupUnitRunning || gs.leaseToken == "" {
			continue
		}
		if gs.deadline.IsZero() || gs.deadline.After(before) {
			continue
		}
		out = append(out, engine.ExpiredLease{
			ExecutionID:  gs.execID,
			NodeName:     gs.groupName,
			NodeIdx:      gs.entryNodeIdx,
			UnitIdx:      gs.unitIdx,
			LeaseID:      gs.leaseID,
			LeaseToken:   gs.leaseToken,
			IssuedAt:     gs.issuedAt,
			TTL:          gs.ttl,
			ActivationID: gs.activationID,
			TaskType:     engine.TaskTypeGroupExec,
		})
	}
	return out, nil
}

func (s *memoryState) RevokeLease(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revokeLeaseLocked(id, name, token), nil
}

// RevokeLeaseWithOutbox atomically fences lease revocation with the durable
// task intent needed to retry the exact task.
func (s *memoryState) RevokeLeaseWithOutbox(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.revokeLeaseLocked(id, name, token) {
		return false, nil
	}
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

func (s *memoryState) revokeLeaseLocked(id types.ExecutionID, name string, token engine.LeaseToken) bool {
	key := string(id) + "/" + name
	ns := s.nodes[key]
	if ns == nil {
		return false
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusCommitting && ns.Status != types.NodeStatusWaiting {
		return false
	}
	if token == "" || ns.LeaseToken != token {
		return false
	}
	cp := *ns
	cp.Status = types.NodeStatusPending
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	cp.LeaseGeneration = 0
	cp.PrivateOutput = s.preserveOutputPrivacyLocked(key, cp.PrivateOutput)
	if cp.PrivateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	return true
}

func (s *memoryState) ClaimTaskLease(_ context.Context, lease *engine.TaskLease) (*engine.NodeSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := memoryNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)
	ns := s.nodes[key]
	if ns == nil {
		return nil, false, nil
	}
	if isTerminalNode(ns.Status) {
		if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
			return s.publicNodeSnapshotLocked(key, ns), false, nil
		}
		return s.publicNodeSnapshotLocked(key, ns), true, nil
	}
	if lease.Task.ActivationID > 0 && ns.ActivationID != lease.Task.ActivationID {
		return s.publicNodeSnapshotLocked(key, ns), false, nil
	}
	if ns.Status != types.NodeStatusRunning || ns.LeaseToken == "" || ns.LeaseToken != lease.LeaseToken {
		return s.publicNodeSnapshotLocked(key, ns), false, nil
	}
	if lease.LeaseID != "" && ns.LeaseID != lease.LeaseID {
		return s.publicNodeSnapshotLocked(key, ns), false, nil
	}
	if lease.Attempt != 0 && ns.Attempt != lease.Attempt {
		return s.publicNodeSnapshotLocked(key, ns), false, nil
	}

	cp := *ns
	cp.Status = types.NodeStatusCommitting
	cp.PrivateOutput = s.preserveOutputPrivacyLocked(key, cp.PrivateOutput)
	if cp.PrivateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	return s.publicNodeSnapshotLocked(key, &cp), true, nil
}

// SuspendTaskLease atomically parks a claimed lease after verifying the same
// lease that entered committing still owns the node. A sweeper that reset the
// lease, or a newer runner, therefore cannot be overwritten by a stale
// suspend result.
func (s *memoryState) SuspendTaskLease(_ context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, privateOutput bool, spec *types.SuspendSpec, oldSignalName string) (*types.SignalPayload, bool, error) {
	if lease == nil || spec == nil {
		return nil, false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	node := s.nodes[key]
	if node == nil || node.Status != types.NodeStatusCommitting || node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken || node.Attempt != lease.Attempt || node.ActivationID != lease.Task.ActivationID {
		return nil, false, nil
	}
	privateOutput = s.preserveOutputPrivacyLocked(key, privateOutput)
	if storeOutput {
		s.outputs[key] = cloneData(output)
	}

	delete(s.resumed, key)
	if oldSignalName != "" {
		delete(s.suspended, key)
	}

	var payload *types.SignalPayload
	if spec.Mode == types.ModeMultiSignal {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			data, found := s.signals[signalKey]
			if !found {
				continue
			}
			delete(s.signals, signalKey)
			if ready := s.addMultiSignalLocked(lease.Task.ExecutionID, lease.Task.NodeName, signalName, data, spec); ready != nil {
				payload = ready
				delete(s.signalSets, key)
				break
			}
		}
	} else {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			if data, found := s.signals[signalKey]; found {
				delete(s.signals, signalKey)
				payload = &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: cloneData(data)}
				break
			}
		}
	}
	if payload == nil {
		s.suspended[key] = spec
	} else {
		delete(s.suspended, key)
	}

	cp := *node
	cp.Status = types.NodeStatusSuspended
	cp.LeaseID = ""
	cp.LeaseToken = ""
	cp.LeaseIssuedAt = time.Time{}
	cp.LeaseTTL = 0
	cp.LeaseTaskType = engine.TaskTypeNodeExec
	cp.LeasePayload = nil
	cp.LeaseGeneration = 0
	cp.PrivateOutput = privateOutput
	if privateOutput {
		cp.Output = nil
	}
	s.nodes[key] = &cp
	for _, entry := range engine.SuspendOutboxEntries(lease, spec, payload, time.Now().UTC()) {
		s.putOutboxLocked(lease.Task.ExecutionID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return cloneLeasePayload(payload), true, nil
}

// SuspendTaskLeaseWithOutbox shares the fenced suspend transition above. The
// transition writes continuation intents while the memory-state mutex is held,
// so callers never observe a consumed pre-signal without a recoverable task.
func (s *memoryState) SuspendTaskLeaseWithOutbox(ctx context.Context, lease *engine.TaskLease, output map[string]any, storeOutput bool, privateOutput bool, spec *types.SuspendSpec, oldSignalName string) (bool, error) {
	_, committed, err := s.SuspendTaskLease(ctx, lease, output, storeOutput, privateOutput, spec, oldSignalName)
	return committed, err
}

// ---------------------------------------------------------------------------
// Scheduling counters
// ---------------------------------------------------------------------------

func (s *memoryState) DecrementInDegree(_ context.Context, id types.ExecutionID, unitIdx int, portActive bool) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s/%d", id, unitIdx)
	s.inDegrees[key]--
	if portActive {
		s.activeIns[key]++
	}
	return s.inDegrees[key], s.activeIns[key], nil
}

func (s *memoryState) CheckCompletion(_ context.Context, id types.ExecutionID, totalNodes int) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Guard: if the execution is already terminal or canceling, report
	// not-complete so the engine does not try to overwrite the status. Matches
	// the distributed checkCompletionLua guard.
	if entry, ok := s.executions[id]; ok {
		st := entry.snap.Status
		if isTerminalStatus(st) || st == types.ExecutionStatusCanceling {
			return false, false, nil
		}
	}
	prefix := string(id) + "/"
	done := 0
	hasFailed := false
	for key, ns := range s.nodes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if isTerminalNode(ns.Status) {
			done++
		}
		if ns.Status == types.NodeStatusFailed {
			hasFailed = true
		}
	}
	return done >= totalNodes, hasFailed, nil
}

// ---------------------------------------------------------------------------
// Suspend / signal
// ---------------------------------------------------------------------------

func (s *memoryState) SuspendOrConsume(_ context.Context, id types.ExecutionID, name string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if spec != nil && spec.Mode == types.ModeMultiSignal {
		return s.suspendOrConsumeMultiLocked(id, name, spec), nil
	}
	// Check if any awaited signal was pre-delivered (keyed by signal name).
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := s.signals[sigKey]; ok {
			delete(s.signals, sigKey)
			// Return a clone to prevent caller from aliasing internal state.
			return &types.SignalPayload{Triggered: types.SignalReceived, Name: sigName, Data: cloneData(sig)}, nil
		}
	}
	// Park the node.
	key := string(id) + "/" + name
	delete(s.resumed, key)
	s.suspended[key] = spec
	return nil, nil
}

func (s *memoryState) DeliverSignal(_ context.Context, id types.ExecutionID, signalName string, data map[string]any) (string, *types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	// Check if any suspended node is waiting for this signal.
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, sig := range spec.Signals {
			if sig == signalName {
				nodeName := strings.TrimPrefix(key, prefix)
				if spec.Mode == types.ModeMultiSignal {
					payload := s.addMultiSignalLocked(id, nodeName, signalName, data, spec)
					if payload == nil {
						return "", nil, nil
					}
					delete(s.suspended, key)
					delete(s.signalSets, key)
					return nodeName, payload, nil
				}
				delete(s.suspended, key)
				return nodeName, nil, nil
			}
		}
	}
	// No suspended node yet — store for later consumption. Clone to decouple
	// from caller's map (matches distributed serialize semantics).
	s.signals[string(id)+"/"+signalName] = cloneData(data)
	return "", nil, nil
}

var _ engine.DurableSignalDeliverer = (*memoryState)(nil)

// PeekResumeTarget returns the node name suspended and waiting for signalName,
// or "" when no waiter exists. Does not consume.
func (s *memoryState) PeekResumeTarget(_ context.Context, id types.ExecutionID, signalName string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, sig := range spec.Signals {
			if sig == signalName {
				return strings.TrimPrefix(key, prefix), nil
			}
		}
	}
	return "", nil
}

// DeliverSignalWithOutbox consumes a signal and records the resume delivery
// intent in the outbox within the same locked transition, mirroring the distributed
// durable path. A crash after this call still leaves the resume recoverable.
func (s *memoryState) DeliverSignalWithOutbox(_ context.Context, id types.ExecutionID, signalName string, data map[string]any, intent engine.ResumeIntent) (string, *types.SignalPayload, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Guard first, before touching any waiter state. An empty NodeName means the
	// caller's PeekResumeTarget found no waiter and built a zero ResumeIntent; a
	// waiter can appear in the window between those two calls. Consuming it here
	// would delete the suspend entry and enqueue a resume whose NodeIdx and
	// UnitIdx are 0 — not absent, but addressing unit 0 of the graph while the
	// task names a different node. Nothing could then wake the node: the waiter
	// is gone, so no later signal matches it, and only the execution TTL clears
	// it. Store the signal instead and leave the suspend intact, so the next
	// delivery (whose peek succeeds) or the suspend timeout drives the resume.
	// deliverSignalWithOutboxLua does the same, and pins it with a named test.
	if intent.NodeName == "" {
		s.signals[string(id)+"/"+signalName] = cloneData(data)
		return "", nil, false, nil
	}
	prefix := string(id) + "/"
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, sig := range spec.Signals {
			if sig != signalName {
				continue
			}
			nodeName := strings.TrimPrefix(key, prefix)
			if spec.Mode == types.ModeMultiSignal {
				payload := s.addMultiSignalLocked(id, nodeName, signalName, data, spec)
				if payload == nil {
					return "", nil, false, nil
				}
				delete(s.suspended, key)
				delete(s.signalSets, key)
				s.scheduleResumeOutbox(id, nodeName, intent, payload)
				return nodeName, payload, true, nil
			}
			delete(s.suspended, key)
			payload := &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: cloneData(data)}
			s.scheduleResumeOutbox(id, nodeName, intent, payload)
			return nodeName, payload, true, nil
		}
	}
	// No suspended node yet — store for later consumption.
	s.signals[string(id)+"/"+signalName] = cloneData(data)
	return "", nil, false, nil
}

// scheduleResumeOutbox records a resume delivery intent for a woken node.
// It reads the LIVE activation_id from the node state (already under the lock)
// rather than relying on the caller's snapshot, mirroring the Redis Lua path's
// authoritative read that closes the TOCTOU window.
func (s *memoryState) scheduleResumeOutbox(id types.ExecutionID, nodeName string, intent engine.ResumeIntent, payload *types.SignalPayload) {
	activationID := intent.ActivationID
	if node, ok := s.nodes[string(id)+"/"+nodeName]; ok {
		activationID = node.ActivationID
	}
	task := engine.Task{
		ExecutionID:  id,
		NodeName:     nodeName,
		NodeIdx:      intent.NodeIdx,
		UnitIdx:      intent.UnitIdx,
		Type:         engine.TaskTypeNodeResume,
		Payload:      payload,
		ActivationID: activationID,
		AutoDepth:    intent.AutoDepth,
	}
	entryID := fmt.Sprintf("resume/%s/%s/%d/signal", id, nodeName, activationID)
	s.putOutboxLocked(id, entryID, task, time.Now().UTC())
}

func (s *memoryState) AcquireResumeLock(_ context.Context, id types.ExecutionID, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(id) + "/" + name
	if s.resumed[key] {
		return false, nil
	}
	s.resumed[key] = true
	return true, nil
}

func (s *memoryState) RevokeSignal(_ context.Context, id types.ExecutionID, signalName string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sigKey := string(id) + "/" + signalName
	if _, ok := s.signals[sigKey]; !ok {
		return false, nil
	}
	// Check if any node that was waiting for this signal has already been resumed.
	// The suspended map keys are execID/nodeName → *SuspendSpec, and resumed keys are execID/nodeName.
	// We need to find the node waiting for this signal by checking suspended entries.
	for key, spec := range s.suspended {
		if !strings.HasPrefix(key, string(id)+"/") {
			continue
		}
		for _, sig := range spec.Signals {
			if sig == signalName {
				// Found the node waiting for this signal — check its resume lock.
				if s.resumed[key] {
					return false, nil
				}
				break
			}
		}
	}
	delete(s.signals, sigKey)
	return true, nil
}

func (s *memoryState) ResuspendAtomic(_ context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *types.SuspendSpec) (*types.SignalPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execID := string(id)

	// 1. Release the resume lock for this node.
	delete(s.resumed, execID+"/"+nodeName)

	// 2. Delete old waiter (the suspended entry keyed by node name).
	delete(s.suspended, execID+"/"+nodeName)

	// 3. Check if the new signal was pre-delivered.
	newSigKey := execID + "/" + newSignalName
	if sig, ok := s.signals[newSigKey]; ok {
		delete(s.signals, newSigKey)
		return &types.SignalPayload{Triggered: types.SignalReceived, Name: newSignalName, Data: cloneData(sig)}, nil
	}

	// 4. No signal available — register new waiter.
	s.suspended[execID+"/"+nodeName] = spec
	return nil, nil
}

func (s *memoryState) suspendOrConsumeMultiLocked(id types.ExecutionID, nodeName string, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	for _, sigName := range spec.Signals {
		sigKey := string(id) + "/" + sigName
		if sig, ok := s.signals[sigKey]; ok {
			delete(s.signals, sigKey)
			if payload := s.addMultiSignalLocked(id, nodeName, sigName, sig, spec); payload != nil {
				delete(s.signalSets, key)
				return payload
			}
		}
	}
	delete(s.resumed, key)
	s.suspended[key] = spec
	return nil
}

func (s *memoryState) addMultiSignalLocked(id types.ExecutionID, nodeName string, signalName string, data map[string]any, spec *types.SuspendSpec) *types.SignalPayload {
	key := string(id) + "/" + nodeName
	all := s.signalSets[key]
	if all == nil {
		all = make(map[string]map[string]any)
		s.signalSets[key] = all
	}
	all[signalName] = data
	if len(all) < signalQuorum(spec) {
		return nil
	}
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      signalName,
		Data:      data,
		All:       cloneSignalSet(all),
	}
}

func signalQuorum(spec *types.SuspendSpec) int {
	if spec == nil {
		return 1
	}
	if spec.Quorum > 0 {
		return spec.Quorum
	}
	if len(spec.Signals) > 0 {
		return len(spec.Signals)
	}
	return 1
}

func cloneSignalSet(in map[string]map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(in))
	for name, payload := range in {
		cp := make(map[string]any, len(payload))
		for k, v := range payload {
			cp[k] = v
		}
		out[name] = cp
	}
	return out
}

// ---------------------------------------------------------------------------
// Cancel support
// ---------------------------------------------------------------------------

func (s *memoryState) ListSuspendedNodes(_ context.Context, id types.ExecutionID) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var nodes []string
	prefix := string(id) + "/"
	for key := range s.suspended {
		if strings.HasPrefix(key, prefix) {
			nodes = append(nodes, strings.TrimPrefix(key, prefix))
		}
	}
	return nodes, nil
}

// ---------------------------------------------------------------------------
// Output store
// ---------------------------------------------------------------------------

func (s *memoryState) PutOutput(_ context.Context, id types.ExecutionID, name string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(id) + "/" + name
	// PutOutput has no privacy parameter, so it must never declassify a key
	// marked private by a node, group, or entry transition.
	s.preserveOutputPrivacyLocked(key, false)
	s.outputs[key] = data
	return nil
}

func (s *memoryState) GetOutput(_ context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs[string(id)+"/"+name], nil
}

// ---------------------------------------------------------------------------
// Wait support
// ---------------------------------------------------------------------------

// GetAllOutputs returns all node outputs for an execution, keyed by node name.
func (s *memoryState) GetAllOutputs(id types.ExecutionID) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(id) + "/"
	result := make(map[string]any)
	for key, data := range s.outputs {
		if strings.HasPrefix(key, prefix) && !s.outputIsPrivateLocked(key) {
			nodeName := strings.TrimPrefix(key, prefix)
			result[nodeName] = data
		}
	}
	return result
}
func (s *memoryState) waitDone(id types.ExecutionID) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.doneCh[id]
	if !ok {
		// Unknown execution — register a pending channel that will be used when
		// the execution is created (race with CreateExecution) or closed when it
		// completes. Callers rely on ctx timeout to avoid blocking forever on
		// executions that never appear.
		ch = make(chan struct{})
		s.doneCh[id] = ch
		return ch
	}
	return ch
}

func (s *memoryState) PublishExecutionEvent(_ context.Context, event engine.ExecutionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishLocked(event)
	return nil
}

func (s *memoryState) WatchExecution(ctx context.Context, id types.ExecutionID) (<-chan engine.ExecutionEvent, error) {
	ch := make(chan engine.ExecutionEvent, 8)
	s.mu.Lock()
	s.eventWatchers[id] = append(s.eventWatchers[id], ch)
	s.mu.Unlock()

	// Clean up when the caller's context is canceled (if cancellable).
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			s.mu.Lock()
			defer s.mu.Unlock()
			s.removeWatcherLocked(id, ch)
		}()
	}

	return ch, nil
}

// removeWatcherLocked removes a specific watcher channel from eventWatchers and
// closes it. Caller must hold s.mu.
func (s *memoryState) removeWatcherLocked(id types.ExecutionID, ch chan engine.ExecutionEvent) {
	watchers := s.eventWatchers[id]
	for i, watcher := range watchers {
		if watcher == ch {
			s.eventWatchers[id] = append(watchers[:i], watchers[i+1:]...)
			close(ch)
			return
		}
	}
}

func (s *memoryState) publishLocked(event engine.ExecutionEvent) {
	for _, watcher := range s.eventWatchers[event.ExecutionID] {
		select {
		case watcher <- event:
		default:
		}
	}
	// On terminal status, close and remove all watchers for this execution to
	// prevent memory leaks when ctx is non-cancellable (e.g. context.Background).
	if isTerminalStatus(event.Status) {
		for _, watcher := range s.eventWatchers[event.ExecutionID] {
			close(watcher)
		}
		delete(s.eventWatchers, event.ExecutionID)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func isTerminalStatus(s types.ExecutionStatus) bool { return types.IsTerminalExecutionStatus(s) }

func isTerminalNode(status types.NodeStatus) bool { return types.IsTerminalNodeStatus(status) }

// ---------------------------------------------------------------------------
// Sub-execution support
// ---------------------------------------------------------------------------

func (s *memoryState) CreateSubExecution(_ context.Context, sub *engine.SubExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(sub.ParentExecID) + "/" + sub.ParentNode
	s.subExecs[key] = append(s.subExecs[key], sub)
	return nil
}

func (s *memoryState) CompleteSubExecution(_ context.Context, parentExecID types.ExecutionID, parentNode string, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := s.subExecs[key]
	allDone := true
	for _, sub := range subs {
		if sub.ChildExecID == childExecID {
			sub.Status = status
			sub.Result = result
		}
		// The same rule the expansion path applies: only a terminal child is
		// done. "Not running" also matches a child that is parked in a
		// non-terminal state, and treating that as finished would hand the
		// parent a batch with a hole in it.
		if !types.IsTerminalExecutionStatus(sub.Status) {
			allDone = false
		}
	}
	return allDone, nil
}

func (s *memoryState) GetSubExecutionResults(_ context.Context, parentExecID types.ExecutionID, parentNode string) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(parentExecID) + "/" + parentNode
	subs := s.subExecs[key]
	results := make([]map[string]any, 0, len(subs))
	for _, sub := range subs {
		if sub.Result != nil {
			results = append(results, sub.Result)
		}
	}
	return results, nil
}

// RenewTaskLease implements engine.NodeLeaseRenewer.
//
// This backend derives lease expiry from LeaseIssuedAt+LeaseTTL rather than
// from a separate index, so extending the deadline means widening the TTL and
// leaving IssuedAt alone — moving IssuedAt instead would keep the same
// deadline. The token fence and the running check mirror the Redis store: a
// revoked, committed, or suspended lease has had its work requeued or
// finalized, and reviving its deadline would fence out the current owner.
//
// Waiting is accepted alongside Running for the same reason the Redis store
// accepts it: an expansion parent sits in Waiting while its batches execute on
// remote runners, and ListExpiredLeases reclaims Waiting nodes just like
// Running ones, so that is precisely when renewal has to work.
func (s *memoryState) RenewTaskLease(_ context.Context, id types.ExecutionID, name string, token engine.LeaseToken, deadline time.Time) (bool, error) {
	if token == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.nodes[string(id)+"/"+name]
	if ns == nil || ns.LeaseToken != token {
		return false, nil
	}
	if ns.Status != types.NodeStatusRunning && ns.Status != types.NodeStatusWaiting {
		return false, nil
	}
	issuedAt := ns.LeaseIssuedAt
	if issuedAt.IsZero() {
		issuedAt = time.Now().UTC()
		ns.LeaseIssuedAt = issuedAt
	}
	ttl := deadline.Sub(issuedAt)
	if ttl < 0 {
		ttl = 0
	}
	ns.LeaseTTL = ttl
	return true, nil
}

var _ engine.NodeLeaseRenewer = (*memoryState)(nil)
