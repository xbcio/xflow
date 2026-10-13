package local

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type memoryOutboxEntry struct {
	entry engine.OutboxEntry
	// leasedUntil mirrors the Redis store's ready-ZSET score bump: while it is
	// in the future ListOutbox skips the entry, so two concurrent flushes do
	// not both deliver it. Ack stays the only removal path, so a deliverer that
	// dies leaves the entry to become listable again on its own.
	leasedUntil time.Time
	// deadMeta mirrors the Redis per-entry dead-letter meta hash: the node,
	// activation, and intent captured from the failing delivery when the entry
	// was dead-lettered. Nil while the entry is ready; replay drops it.
	deadMeta *memoryDeadLetterMeta
}

// memoryDeadLetterMeta is the immutable guard metadata ReplayDeadLetter
// evaluates. activation is kept in its stored string form so an absent value
// is distinguishable from activation 0, exactly as in the Redis meta hash.
type memoryDeadLetterMeta struct {
	node       string
	activation string
	intent     string
}

// memoryReplayReceipt is the authoritative in-memory record of one replay
// attempt, keyed by execution→requestID. It is written for a replay and for
// every decidable rejection, and survives the dead entry's removal so a retry
// with the same RequestID recovers the original outcome and AuditID.
type memoryReplayReceipt struct {
	auditID      string
	node         string
	activation   string
	entryID      string
	operator     string
	reason       string
	outcome      engine.DeadLetterReplayOutcome
	occurredAtMs int64
}

var _ engine.AtomicStateStore = (*memoryState)(nil)
var _ engine.LegacyNodeCommitter = (*memoryState)(nil)
var _ engine.LeaseSuspender = (*memoryState)(nil)
var _ engine.DurableLeaseSuspender = (*memoryState)(nil)
var _ engine.OutboxFailureRecorder = (*memoryState)(nil)
var _ engine.OutboxLeaser = (*memoryState)(nil)
var _ engine.OutboxReleaser = (*memoryState)(nil)
var _ engine.OutboxMetricsReader = (*memoryState)(nil)
var _ engine.DeadLetterStore = (*memoryState)(nil)
var _ engine.ReplayReceiptReader = (*memoryState)(nil)

// CommitLeasedNode shares the storage fence used by CommitNode while callers
// retain legacy cycle/expansion scheduling outside the static-DAG counter.
func (s *memoryState) CommitLeasedNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	return s.CommitNode(ctx, req)
}

// CommitNode atomically applies a fenced terminal node transition, updates
// execution completion counters, and records the follow-up advance task.
func (s *memoryState) CommitNode(_ context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	if err := req.Validate(); err != nil {
		return engine.CommitNodeResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.executions[req.ExecutionID]
	if entry == nil {
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeExecutionInactive}, nil
	}
	key := memoryNodeKey(req.ExecutionID, req.NodeName)
	current := s.nodes[key]
	if current != nil && isTerminalNode(current.Status) {
		if current.ActivationID == req.ActivationID && ((!req.System && current.CommittedLeaseToken == req.LeaseToken && req.LeaseToken != "") || (req.System && current.Status == req.Status)) {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeDuplicateTerminal}, nil
		}
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
	}
	if types.IsTerminalExecutionStatus(entry.snap.Status) {
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeExecutionInactive}, nil
	}

	if req.System {
		// UnitIdx, not NodeIdx: the scheduling marker lives in the unit-indexed
		// counter space, which is what AdvanceNode writes and what a supply node
		// shifts NodeIdx away from. The two coincided for every definition without
		// a declaration-only node, so reading NodeIdx here silently refused the
		// cascade — and a refused system commit is reported as handled, so the
		// intent was acked and dropped instead of the branch being consumed.
		//
		// The legal system-commit shapes for a unit, keyed by its "skip"
		// scheduling marker:
		//   - pinned  + no marker: a pinned node served from pin_data on a unit
		//     the graph decided to EXECUTE (or a root, which has no marker);
		//   - skipped + no marker: a definition-disabled node, scheduled to
		//     execute but never leased (its "main" port stays active);
		//   - skipped + "skip" marker: a skip-cascade resolution.
		// A pinned commit on a skip-marked unit, and any other status without
		// the marker, are stale or rogue and are refused. Every other
		// combination keeps its previous verdict.
		marker := s.scheduled[memoryCounterKey(req.ExecutionID, req.UnitIdx)]
		skipMarked := marker == "skip"
		pinned := req.Status == types.NodeStatusPinned
		skipped := req.Status == types.NodeStatusSkipped
		if (pinned && skipMarked) || (!pinned && !skipped && !skipMarked) {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
		}
		if current != nil && current.Status != types.NodeStatusPending {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
		}
	} else {
		if current == nil || (current.Status != types.NodeStatusRunning && current.Status != types.NodeStatusCommitting && current.Status != types.NodeStatusWaiting) || current.LeaseID != req.LeaseID || current.LeaseToken != req.LeaseToken || current.Attempt != req.Attempt || current.ActivationID != req.ActivationID {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
		}
	}

	privateOutput := s.preserveOutputPrivacyLocked(key, req.PrivateOutput)
	// ErrorDetails is cloned like Output: the caller's map must not stay
	// aliased into stored state, where a later mutation would rewrite history.
	node := &engine.NodeSnapshot{
		ExecutionID:         req.ExecutionID,
		Name:                req.NodeName,
		NodeIdx:             req.NodeIdx,
		Status:              req.Status,
		Attempt:             req.Attempt,
		ActivationID:        req.ActivationID,
		AutoDepth:           req.AutoDepth,
		PrivateOutput:       privateOutput,
		Port:                req.Port,
		Error:               req.Error,
		ErrorDetails:        cloneData(req.ErrorDetails),
		CommittedLeaseToken: req.LeaseToken,
		CommittedAttempt:    req.Attempt,
	}
	if !privateOutput {
		node.Output = cloneData(req.Output)
	}
	if req.System && current != nil {
		node.Attempt = current.Attempt
		node.AutoDepth = current.AutoDepth
	}
	// The child a sub-workflow node waited on outlives its commit, like the
	// privacy marker above: the record is rebuilt here, so without this the
	// drill-down would exist only while the node was parked, and the exact
	// child a finished node ran would be unreadable.
	if current != nil {
		node.ChildExecutionID = current.ChildExecutionID
	}
	s.nodes[key] = node
	if req.StoreOutput {
		s.outputs[key] = cloneData(req.Output)
	}

	result := engine.CommitNodeResult{Outcome: engine.CommitOutcomeAccepted, Applied: true}
	for _, sourceName := range req.ReclaimOutputNames {
		delete(s.outputs, memoryNodeKey(req.ExecutionID, sourceName))
	}
	if !req.AllowCycles {
		s.remaining[req.ExecutionID]--
		if req.Status == types.NodeStatusFailed {
			s.failed[req.ExecutionID]++
		}
		if req.Fatal || s.remaining[req.ExecutionID] == 0 {
			status := types.ExecutionStatusSuccess
			if req.Fatal || s.failed[req.ExecutionID] > 0 {
				status = types.ExecutionStatusFailed
			}
			s.finishExecutionLocked(req.ExecutionID, entry, status,
				engine.TerminalExecutionError(status, req.Error, ""))
			result.ExecutionDone = true
			result.ExecutionStatus = status
		}
	}

	if req.AdvanceTask != nil && !req.Fatal && !result.ExecutionDone {
		outboxID := advanceOutboxID(req.ExecutionID, req.NodeName, req.ActivationID)
		if s.putOutboxLocked(req.ExecutionID, outboxID, *req.AdvanceTask, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, outboxID)
		}
	}

	// Cyclic graphs are not static in-degree counted (the acyclic block above is
	// skipped for AllowCycles). Persist the engine-computed downstream intents,
	// or finalize the execution when the branch terminated, in this same locked
	// transition so a crash cannot lose them (#7).
	if req.AllowCycles && !req.Fatal && !result.ExecutionDone {
		if len(req.CyclicOutbox) > 0 {
			for _, oe := range req.CyclicOutbox {
				if s.putOutboxEntryLocked(req.ExecutionID, oe) {
					result.OutboxIDs = append(result.OutboxIDs, oe.ID)
				}
			}
		} else if req.CyclicComplete {
			s.finishExecutionLocked(req.ExecutionID, entry, req.CyclicFinalStatus,
				engine.TerminalExecutionError(req.CyclicFinalStatus, req.Error, req.CyclicFinalError))
			result.ExecutionDone = true
			result.ExecutionStatus = req.CyclicFinalStatus
		}
	}
	return result, nil
}

// AdvanceNode atomically converts the committed source's outbound edge
// arrivals into execution or skip outbox tasks. The persisted advance marker
// makes replay idempotent and removes recursive skip propagation.
func (s *memoryState) AdvanceNode(_ context.Context, req engine.AdvanceNodeRequest) (engine.AdvanceNodeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.executions[req.ExecutionID]
	if entry == nil || types.IsTerminalExecutionStatus(entry.snap.Status) {
		return engine.AdvanceNodeResult{}, nil
	}
	key := memoryNodeKey(req.ExecutionID, req.NodeName)
	node := s.nodes[key]
	if node == nil || !isTerminalNode(node.Status) || node.ActivationID != req.ActivationID {
		return engine.AdvanceNodeResult{}, nil
	}
	advanceKey := memoryAdvanceKey(req.ExecutionID, req.NodeName, req.ActivationID)
	if s.advanced[advanceKey] {
		return engine.AdvanceNodeResult{}, nil
	}
	s.advanced[advanceKey] = true

	result := engine.AdvanceNodeResult{Applied: true}
	for _, arrival := range req.Arrivals {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := memoryCounterKey(req.ExecutionID, arrival.UnitIdx)
		previousActive := s.activeIns[counterKey]
		s.inDegrees[counterKey] -= arrival.ArrivalCount
		if arrival.ActiveCount > 0 {
			s.activeIns[counterKey] += arrival.ActiveCount
		}
		if s.scheduled[counterKey] != "" {
			continue
		}

		schedule := ""
		if arrival.MergeMode == "wait_any" && arrival.ActiveCount > 0 && previousActive == 0 {
			schedule = "execute"
		} else if s.inDegrees[counterKey] <= 0 {
			if s.activeIns[counterKey] > 0 {
				schedule = "execute"
			} else {
				schedule = "skip"
			}
		}
		if schedule == "" {
			continue
		}
		s.scheduled[counterKey] = schedule
		taskType := arrival.ExecTaskType
		if taskType == 0 {
			taskType = engine.TaskTypeNodeExec
		}
		outboxID := executeOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		isSkip := schedule == "skip"
		if isSkip {
			taskType = engine.TaskTypeNodeSkip
			outboxID = skipOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		}
		if s.putOutboxLocked(req.ExecutionID, outboxID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			UnitIdx:      arrival.UnitIdx,
			Type:         taskType,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, outboxID)
			// Only when the intent was actually written: a redelivered advance
			// whose skip intent already exists applies nothing new, and
			// reporting it would double-count one skip.
			if isSkip {
				result.Skipped = append(result.Skipped, engine.SkippedUnit{NodeName: arrival.NodeName, Count: 1})
			}
		}
	}
	return result, nil
}

// ListOutbox reports ready delivery intents without changing any state. An
// entry another deliverer currently holds a lease on is not ready, so it is
// omitted. Claiming for delivery is LeaseOutbox's job.
func (s *memoryState) ListOutbox(_ context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.readyOutboxIDsLocked(id, before, limit)
	entries := s.outbox[id]
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		out = append(out, cloneOutboxEntry(entries[entryID].entry))
	}
	return out, nil
}

// LeaseOutbox claims ready delivery intents for exclusive delivery, so a
// concurrent caller does not redeliver work that is already in flight. See
// engine.OutboxDeliveryLeaseTTL.
func (s *memoryState) LeaseOutbox(_ context.Context, id types.ExecutionID, now time.Time, limit int) ([]engine.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.readyOutboxIDsLocked(id, now, limit)
	entries := s.outbox[id]
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		held := entries[entryID]
		held.leasedUntil = now.Add(engine.OutboxDeliveryLeaseTTL)
		entries[entryID] = held
		leased := cloneOutboxEntry(held.entry)
		leased.LeaseDeadlineMs = held.leasedUntil.UnixMilli()
		out = append(out, leased)
	}
	return out, nil
}

// RenewOutbox extends the leases this deliverer still holds, proving it is
// alive so engine.OutboxDeliveryLeaseTTL can stay short. It returns the entries
// whose renewal was granted, each carrying the new deadline the next renewal
// must present.
//
// The caller proves ownership by presenting the deadline it believes it holds,
// because the lease has no other identity. An entry whose lease already lapsed
// may have been legitimately claimed by another deliverer, and extending it
// again would yank it back mid-flight and deliver it twice — the exact
// duplication the lease exists to prevent, in precisely the case it exists for.
func (s *memoryState) RenewOutbox(_ context.Context, id types.ExecutionID, renew []engine.OutboxEntry, now time.Time) ([]engine.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.outbox[id]
	deadline := now.Add(engine.OutboxDeliveryLeaseTTL)
	out := make([]engine.OutboxEntry, 0, len(renew))
	for _, entry := range renew {
		held, ok := entries[entry.ID]
		if !ok || !held.leasedUntil.After(now) {
			continue
		}
		if held.leasedUntil.UnixMilli() != entry.LeaseDeadlineMs {
			continue
		}
		held.leasedUntil = deadline
		entries[entry.ID] = held
		renewed := cloneOutboxEntry(held.entry)
		renewed.LeasedFromScoreMs = entry.LeasedFromScoreMs
		renewed.LeaseDeadlineMs = deadline.UnixMilli()
		out = append(out, renewed)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// readyOutboxIDsLocked returns the sorted IDs of entries that are due and not
// currently leased. Shared by the read and claim paths so the two can never
// disagree about what "ready" means.
//
// A paused execution has no ready entries at any horizon, which is how a pause
// suspends dispatch without moving anything: see dispatchHeldLocked. The
// refusal is placed before the deadline test rather than expressed as a
// deadline, because the hold is a property of the execution and not of the
// instant asked about — a caller asking about next year must not be able to
// deliver work this year's pause stopped.
func (s *memoryState) readyOutboxIDsLocked(id types.ExecutionID, before time.Time, limit int) []string {
	entries := s.outbox[id]
	if len(entries) == 0 || limit == 0 || s.dispatchHeldLocked(id) {
		return nil
	}
	ids := make([]string, 0, len(entries))
	for entryID, entry := range entries {
		if entry.leasedUntil.After(before) {
			continue
		}
		if entry.entry.AvailableAt.IsZero() || !entry.entry.AvailableAt.After(before) {
			ids = append(ids, entryID)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

// AckOutbox removes one successfully handed-off delivery intent. It is
// intentionally idempotent so response loss after a prior acknowledgement is
// harmless.
func (s *memoryState) AckOutbox(_ context.Context, id types.ExecutionID, entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entries := s.outbox[id]; entries != nil {
		delete(entries, entryID)
		if len(entries) == 0 {
			delete(s.outbox, id)
		}
	}
	return nil
}

// ReleaseOutbox hands a leased entry back for immediate redelivery instead of
// making the next flush wait out the visibility timeout. Like AckOutbox it is
// idempotent, and it must not resurrect an entry that was acked or
// dead-lettered in the meantime — hence the presence check.
func (s *memoryState) ReleaseOutbox(_ context.Context, id types.ExecutionID, entry engine.OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.outbox[id]
	held, ok := entries[entry.ID]
	if !ok {
		return nil
	}
	held.leasedUntil = time.Time{}
	entries[entry.ID] = held
	return nil
}

// RecordOutboxFailure increments one durable delivery attempt and moves an
// entry to in-memory dead-letter storage once the configured threshold is met.
func (s *memoryState) RecordOutboxFailure(_ context.Context, id types.ExecutionID, entry engine.OutboxEntry, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entryID := entry.ID
	entries := s.outbox[id]
	stored, ok := entries[entryID]
	if !ok {
		return engine.OutboxDeliveryFailure{}, nil
	}
	if maxAttempts <= 0 {
		maxAttempts = engine.DefaultOutboxMaxDeliveryAttempts
	}
	stored.entry.Attempts++
	result := engine.OutboxDeliveryFailure{Attempts: stored.entry.Attempts}
	if stored.entry.Attempts >= maxAttempts {
		delete(entries, entryID)
		if len(entries) == 0 {
			delete(s.outbox, id)
		}
		deadEntries := s.deadOutbox[id]
		if deadEntries == nil {
			deadEntries = make(map[string]memoryOutboxEntry)
			s.deadOutbox[id] = deadEntries
		}
		// Capture the replay guard metadata from the failing delivery, as
		// recordOutboxFailureLua writes the per-entry dead meta hash.
		stored.deadMeta = &memoryDeadLetterMeta{
			node:       entry.Task.NodeName,
			activation: strconv.Itoa(entry.Task.ActivationID),
			intent:     deadLetterIntent(entryID),
		}
		deadEntries[entryID] = stored
		// Clear a stale replay index mapping so a second dead-letter cycle does
		// not permanently block re-replay under a new RequestID.
		delete(s.replayEntryIdx[id], entryID)
		result.DeadLettered = true
		return result, nil
	}
	entries[entryID] = stored
	return result, nil
}

// OutboxMetrics returns aggregate pending, due-now, and dead-letter counts for
// the in-memory durable-outbox reference implementation.
func (s *memoryState) OutboxMetrics(_ context.Context) (engine.OutboxMetricsSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var snapshot engine.OutboxMetricsSnapshot
	for _, entries := range s.outbox {
		for _, stored := range entries {
			snapshot.Pending++
			// The same split the Redis store makes with a ZCOUNT to now: an
			// entry held behind a retry backoff is pending but not deliverable
			// yet. The memory store takes no delivery lease, so availability is
			// the only thing standing between pending and ready here.
			if availableAt := stored.entry.AvailableAt; availableAt.IsZero() || !availableAt.After(now) {
				snapshot.Ready++
			}
			createdAt := stored.entry.CreatedAt
			if createdAt.IsZero() {
				createdAt = stored.entry.AvailableAt
			}
			if !createdAt.IsZero() && (snapshot.OldestPendingAt.IsZero() || createdAt.Before(snapshot.OldestPendingAt)) {
				snapshot.OldestPendingAt = createdAt
			}
		}
	}
	for _, entries := range s.deadOutbox {
		snapshot.DeadLettered += len(entries)
	}
	return snapshot, nil
}

// ListDeadLetters returns one page of dead-lettered entries for an execution,
// oldest-first, using a stable opaque cursor (the last entry ID). Entries are
// not removed. An empty NextCursor marks the final page.
func (s *memoryState) ListDeadLetters(_ context.Context, id types.ExecutionID, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	const defaultLimit, maxLimit = 100, 500
	if page.Limit <= 0 {
		page.Limit = defaultLimit
	}
	if page.Limit > maxLimit {
		page.Limit = maxLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deadEntries := s.deadOutbox[id]
	out := make([]engine.OutboxEntry, 0, len(deadEntries))
	for _, stored := range deadEntries {
		out = append(out, stored.entry)
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].CreatedAt, out[j].CreatedAt
		if ai.IsZero() {
			ai = out[i].AvailableAt
		}
		if aj.IsZero() {
			aj = out[j].AvailableAt
		}
		return ai.Before(aj)
	})
	// Stable cursor: skip past the cursor entry ID, then bound to Limit+1 to
	// detect a next page.
	start := 0
	if page.Cursor != "" {
		for start < len(out) && out[start].ID != page.Cursor {
			start++
		}
		if start < len(out) {
			start++ // skip the cursor itself
		}
	}
	page2 := out[start:]
	var nextCursor string
	if len(page2) > page.Limit {
		nextCursor = page2[page.Limit-1].ID
		page2 = page2[:page.Limit]
	}
	return engine.DeadLetterList{Entries: page2, NextCursor: nextCursor}, nil
}

// ReplayDeadLetter moves a dead-lettered entry back to the ready set,
// mirroring replayDeadLetterLua in the Redis backend step for step so both
// backends return the same outcome for the same state. The memory mutex
// serializes replays so concurrent calls collapse: exactly one returns
// ReplayReplayed, the rest return ReplayAlreadyReplayed with the original
// AuditID.
//
// Order of evaluation: a receipt under the same RequestID (or a successful
// replay of the entry under another RequestID) recovers the stored outcome;
// then a missing entry is a stable not_found with no receipt; then the
// fail-closed metadata guard, the execution status guard (only "running" may
// replay), the intent-branched node guard, and the activation guard. Every
// decidable rejection writes a receipt so a retry recovers the same rejection.
func (s *memoryState) ReplayDeadLetter(_ context.Context, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	if req.EntryID == "" {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayNotFound, ExecutionID: req.ExecutionID}, nil
	}
	requestID := req.RequestID
	if requestID == "" {
		requestID = req.EntryID
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Idempotency: a receipt under this RequestID recovers its outcome; a
	//    successful replay of this entry under another RequestID collapses to
	//    that receipt. The entry index is only written on success.
	if r, ok := s.replayReceipts[req.ExecutionID][requestID]; ok {
		return r.result(req.ExecutionID), nil
	}
	if priorReqID := s.replayEntryIdx[req.ExecutionID][req.EntryID]; priorReqID != "" {
		if r, ok := s.replayReceipts[req.ExecutionID][priorReqID]; ok {
			return r.result(req.ExecutionID), nil
		}
	}

	// 2. A missing entry is a stable not_found with no receipt.
	deadEntries := s.deadOutbox[req.ExecutionID]
	stored, ok := deadEntries[req.EntryID]
	if !ok {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayNotFound, ExecutionID: req.ExecutionID}, nil
	}
	var meta memoryDeadLetterMeta
	if stored.deadMeta != nil {
		meta = *stored.deadMeta
	}
	auditID := requestID + ":" + strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
	reject := func(outcome engine.DeadLetterReplayOutcome) (engine.ReplayDeadLetterResult, error) {
		return s.writeReplayReceiptLocked(req, requestID, auditID, meta, outcome).result(req.ExecutionID), nil
	}

	// 3. Fail-closed metadata guard: without node and activation the
	//    activation-safe guards cannot be evaluated.
	if meta.node == "" || meta.activation == "" {
		return reject(engine.ReplayRejectedMetadataMissing)
	}

	// 4. Execution status guard. Only an explicitly running execution may
	//    replay; canceling must not be bypassed.
	exec := s.executions[req.ExecutionID]
	if exec == nil {
		return reject(engine.ReplayRejectedInactive)
	}
	if exec.snap.Status != types.ExecutionStatusRunning {
		return reject(engine.ReplayRejectedTerminal)
	}

	// 5. Intent-branched node guard. advance requires a terminal source; every
	//    other intent rejects a terminal node and must find a node status its
	//    intent allows, otherwise the guard state is unrecognised.
	node := s.nodes[memoryNodeKey(req.ExecutionID, meta.node)]
	nodeTerminal := node != nil && isTerminalNode(node.Status)
	if meta.intent == "advance" {
		if !nodeTerminal {
			return reject(engine.ReplayRejectedMetadataMissing)
		}
	} else {
		if nodeTerminal {
			return reject(engine.ReplayRejectedNodeTerminal)
		}
		if !replayIntentAllowsNode(meta.intent, node) {
			return reject(engine.ReplayRejectedMetadataMissing)
		}
	}

	// 6. Activation guard. root/execute/skip with no node yet have no current
	//    activation to compare against.
	skipActivation := node == nil && (meta.intent == "root" || meta.intent == "execute" || meta.intent == "skip")
	if !skipActivation && strconv.Itoa(node.ActivationID) != meta.activation {
		return reject(engine.ReplayRejectedActivationMismatch)
	}

	// 7. Atomic dead->ready move: preserve body, reset attempts, drop meta.
	stored.entry.Attempts = 0
	stored.leasedUntil = time.Time{}
	stored.deadMeta = nil
	readyEntries := s.outbox[req.ExecutionID]
	if readyEntries == nil {
		readyEntries = make(map[string]memoryOutboxEntry)
		s.outbox[req.ExecutionID] = readyEntries
	}
	readyEntries[req.EntryID] = stored
	delete(deadEntries, req.EntryID)
	if len(deadEntries) == 0 {
		delete(s.deadOutbox, req.ExecutionID)
	}

	// 8. Authoritative receipt + entry index.
	receipt := s.writeReplayReceiptLocked(req, requestID, auditID, meta, engine.ReplayReplayed)
	if s.replayEntryIdx[req.ExecutionID] == nil {
		s.replayEntryIdx[req.ExecutionID] = make(map[string]string)
	}
	s.replayEntryIdx[req.ExecutionID][req.EntryID] = requestID
	return engine.ReplayDeadLetterResult{Outcome: engine.ReplayReplayed, AuditID: receipt.auditID, ExecutionID: req.ExecutionID, NodeID: receipt.node, ActivationID: receipt.activation}, nil
}

// replayIntentAllowsNode reports whether node's status is an eligible
// non-terminal value for a non-advance intent. A nil node is an absent status,
// which only the scheduling-stage intents accept. An unknown or empty intent
// (a legacy entry) is never eligible.
func replayIntentAllowsNode(intent string, node *engine.NodeSnapshot) bool {
	if node == nil {
		return intent == "root" || intent == "execute" || intent == "skip"
	}
	switch node.Status {
	case types.NodeStatusPending:
		return intent == "root" || intent == "retry" || intent == "requeue" || intent == "resume" || intent == "execute" || intent == "skip"
	case types.NodeStatusRunning, types.NodeStatusCommitting, types.NodeStatusWaiting:
		return intent == "root" || intent == "retry" || intent == "requeue" || intent == "execute" || intent == "skip"
	case types.NodeStatusSuspended:
		return intent == "resume"
	}
	return false
}

// writeReplayReceiptLocked stores the immutable receipt for one replay
// decision under requestID. It carries only operational metadata, never the
// task body.
func (s *memoryState) writeReplayReceiptLocked(req engine.ReplayDeadLetterRequest, requestID, auditID string, meta memoryDeadLetterMeta, outcome engine.DeadLetterReplayOutcome) *memoryReplayReceipt {
	receipt := &memoryReplayReceipt{
		auditID: auditID, node: meta.node, activation: meta.activation,
		entryID: req.EntryID, operator: req.Operator, reason: req.Reason,
		outcome: outcome, occurredAtMs: time.Now().UTC().UnixMilli(),
	}
	if s.replayReceipts[req.ExecutionID] == nil {
		s.replayReceipts[req.ExecutionID] = make(map[string]*memoryReplayReceipt)
	}
	s.replayReceipts[req.ExecutionID][requestID] = receipt
	return receipt
}

// result recovers a receipt as a replay result. A recovered replayed receipt
// surfaces as already_replayed; a rejection recovers as itself.
func (r *memoryReplayReceipt) result(id types.ExecutionID) engine.ReplayDeadLetterResult {
	outcome := r.outcome
	if outcome == engine.ReplayReplayed {
		outcome = engine.ReplayAlreadyReplayed
	}
	return engine.ReplayDeadLetterResult{Outcome: outcome, AuditID: r.auditID, ExecutionID: id, NodeID: r.node, ActivationID: r.activation}
}

// deadLetterIntent extracts the intent prefix (root/retry/requeue/resume/
// advance/execute/skip) from an outbox entry ID, as the Redis backend records
// it in the dead-letter meta. An ID without a prefix yields "".
func deadLetterIntent(entryID string) string {
	if i := strings.IndexByte(entryID, '/'); i >= 0 {
		return entryID[:i]
	}
	return ""
}

// ScanReplayReceipts iterates every authoritative in-memory replay receipt,
// invoking fn once per receipt. It is the local backend's half of the
// engine.ReplayReceiptReader capability (the Redis backend implements the other
// half): the receipt projector + reconcile tooling that can see this process's
// state — control.NewReceiptProjector used in-process — reads receipts through
// it. It is read-only: nothing here mutates a receipt or an execution.
//
// Namespace is the empty string. The local backend has no namespace dimension
// — ReplayDeadLetterRequest carries none and every execution is addressed by
// ID alone — so naming the default namespace here would attribute an operator's
// replay to a tenant that never made it, and a fabricated name would be written
// into the durable SQL projection as if it were real. A projector consuming a
// local scan must treat "" as "the local backend's single unnamed scope".
//
// Order: execution id, then request id, both lexicographic. The Redis reader
// visits receipts in namespace-then-execution (sorted-key) order; local's
// empty namespace collapses that to execution-then-request.
func (s *memoryState) ScanReplayReceipts(ctx context.Context, fn func(engine.ReplayReceipt) error) error {
	// Snapshot under the lock, emit outside it: fn is caller code that may
	// block (SQL projection) or call back into the store, and no receipt is
	// mutated in place after being written, so copies are stable.
	s.mu.Lock()
	snapshot := make([]engine.ReplayReceipt, 0, len(s.replayReceipts))
	for execID, receipts := range s.replayReceipts {
		for requestID, r := range receipts {
			if r == nil || r.auditID == "" {
				// No idempotency key means nothing is projectable (the SQL
				// projector keys on audit_id). Mirror the Redis reader, which
				// skips an audit-less hash rather than emitting it.
				continue
			}
			snapshot = append(snapshot, engine.ReplayReceipt{
				Namespace:    "", // local has no namespace dimension; see the doc comment
				ExecutionID:  string(execID),
				RequestID:    requestID,
				AuditID:      r.auditID,
				NodeID:       r.node,
				ActivationID: r.activation,
				Outcome:      r.outcome,
				Operator:     r.operator,
				Reason:       r.reason,
				EntryID:      r.entryID,
				TimestampMs:  r.occurredAtMs,
			})
		}
	}
	s.mu.Unlock()
	sort.Slice(snapshot, func(i, j int) bool {
		if snapshot[i].ExecutionID != snapshot[j].ExecutionID {
			return snapshot[i].ExecutionID < snapshot[j].ExecutionID
		}
		return snapshot[i].RequestID < snapshot[j].RequestID
	})

	for _, r := range snapshot {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// ListOutboxExecutions identifies executions with at least one durable intent
// so the background dispatcher can recover delivery after process restart.
//
// An execution parked by the workflow concurrency limit is deliberately not
// reported: every intent it holds is scored at queuedOutboxHold until promotion,
// so listing it would hand the dispatcher an execution that can produce nothing
// — one no-op flush per drain pass, per parked submission, competing for the
// same discovery page as the work that is actually due. Promotion releases the
// hold, and the next drain finds the execution as usual.
func (s *memoryState) ListOutboxExecutions(_ context.Context, limit int) ([]types.ExecutionID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]types.ExecutionID, 0, len(s.outbox))
	for id, entries := range s.outbox {
		if len(entries) == 0 || allOutboxEntriesHeld(entries) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

// allOutboxEntriesHeld reports whether every intent belongs to an execution
// that the concurrency limit has not promoted yet. A mixed set — the instant a
// promotion leaves behind — is reported, because something in it is
// deliverable and the dispatcher is the only reader that can say so.
func allOutboxEntriesHeld(entries map[string]memoryOutboxEntry) bool {
	for _, held := range entries {
		if !held.entry.AvailableAt.Equal(queuedOutboxHold) {
			return false
		}
	}
	return true
}

func (s *memoryState) putOutboxLocked(id types.ExecutionID, entryID string, task engine.Task, availableAt time.Time) bool {
	return s.putOutboxEntryLocked(id, engine.OutboxEntry{
		ID:          entryID,
		Task:        task,
		AvailableAt: availableAt,
	})
}

func (s *memoryState) putOutboxEntryLocked(id types.ExecutionID, entry engine.OutboxEntry) bool {
	if entry.ID == "" {
		return false
	}
	entries := s.outbox[id]
	if entries == nil {
		entries = make(map[string]memoryOutboxEntry)
		s.outbox[id] = entries
	}
	if _, exists := entries[entry.ID]; exists {
		return false
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}
	entry.Task = cloneTask(entry.Task)
	entries[entry.ID] = memoryOutboxEntry{entry: entry}
	return true
}

// finishExecutionLocked drives an execution to a terminal status, recording
// errMsg as the execution-level failure reason.
//
// The two guards mirror the distributed backend's commit Lua verbatim
// (`finalStatus == 'failed'` AND a non-empty reason argument): a non-failed
// status carries no reason, and an empty reason never overwrites a stored one.
// Diverging here would make the reason readable in one backend and not the
// other.
//
// The reason matters most where no node carries it: a cyclic execution that
// trips MaxAutoDepth finishes with every node at success, because the engine
// rejects the downstream activation instead of failing the node that tripped
// the limit.
func (s *memoryState) finishExecutionLocked(id types.ExecutionID, entry *execEntry, status types.ExecutionStatus, errMsg string) {
	if types.IsTerminalExecutionStatus(entry.snap.Status) {
		return
	}
	entry.snap.Status = status
	if status == types.ExecutionStatusFailed && errMsg != "" {
		entry.snap.Error = errMsg
	}
	s.releaseWorkflowSlotLocked(id)
	if !entry.closed {
		entry.closed = true
		if done := s.doneCh[id]; done != nil {
			close(done)
		}
	}
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status, Error: entry.snap.Error})
}

func memoryNodeKey(id types.ExecutionID, name string) string {
	return string(id) + "/" + name
}

// memoryCounterKey names the in-degree / active-input / schedule counter triplet
// for one durable unit. It is keyed by the UNIT index, not the node index: a
// supply node occupies a node index but no unit, so the two diverge for every
// node after one. Naming the parameter nodeIdx here is how the skip-cascade
// guard came to read the wrong key.
func memoryCounterKey(id types.ExecutionID, unitIdx int) string {
	return fmt.Sprintf("%s/%d", id, unitIdx)
}

func memoryAdvanceKey(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("%s/%s/%d", id, name, activationID)
}

func advanceOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("advance/%s/%s/%d", id, name, activationID)
}

func executeOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("execute/%s/%s/%d", id, name, activationID)
}

func skipOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("skip/%s/%s/%d", id, name, activationID)
}

func cloneData(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}
	out := make(map[string]any, len(data))
	for key, value := range data {
		out[key] = value
	}
	return out
}

func cloneOutboxEntry(entry engine.OutboxEntry) engine.OutboxEntry {
	entry.Task = cloneTask(entry.Task)
	return entry
}

func cloneTask(task engine.Task) engine.Task {
	clone := task
	if task.Payload != nil {
		payload := *task.Payload
		payload.Data = cloneData(task.Payload.Data)
		if task.Payload.All != nil {
			payload.All = make(map[string]map[string]any, len(task.Payload.All))
			for key, value := range task.Payload.All {
				payload.All[key] = cloneData(value)
			}
		}
		clone.Payload = &payload
	}
	return clone
}
