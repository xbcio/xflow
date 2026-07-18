package local

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type memoryOutboxEntry struct {
	entry engine.OutboxEntry
}

// memoryReplayReceipt is the authoritative in-memory record of one replay,
// keyed by execution→requestID. It survives the dead entry's removal so a
// retried replay with the same RequestID returns already_replayed.
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
var _ engine.DurableLeaseSuspender = (*memoryState)(nil)
var _ engine.OutboxFailureRecorder = (*memoryState)(nil)
var _ engine.OutboxMetricsReader = (*memoryState)(nil)
var _ engine.DeadLetterStore = (*memoryState)(nil)

// CommitLeasedNode shares the storage fence used by CommitNode while callers
// retain legacy cycle/expansion scheduling outside the static-DAG counter.
func (s *memoryState) CommitLeasedNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	return s.CommitNode(ctx, req)
}

// CommitNode atomically applies a fenced terminal node transition, updates
// execution completion counters, and records the follow-up advance task.
func (s *memoryState) CommitNode(_ context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
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
		if s.scheduled[memoryCounterKey(req.ExecutionID, req.NodeIdx)] != "skip" {
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

	node := &engine.NodeSnapshot{
		ExecutionID:         req.ExecutionID,
		Name:                req.NodeName,
		NodeIdx:             req.NodeIdx,
		Status:              req.Status,
		Attempt:             req.Attempt,
		ActivationID:        req.ActivationID,
		AutoDepth:           req.AutoDepth,
		Output:              cloneData(req.Output),
		Port:                req.Port,
		Error:               req.Error,
		CommittedLeaseToken: req.LeaseToken,
		CommittedAttempt:    req.Attempt,
	}
	if req.System && current != nil {
		node.Attempt = current.Attempt
		node.AutoDepth = current.AutoDepth
	}
	s.nodes[key] = node
	if req.StoreOutput {
		s.outputs[key] = cloneData(req.Output)
	}

	result := engine.CommitNodeResult{Outcome: engine.CommitOutcomeAccepted, Applied: true}
	if entry.snap.Graph != nil && !entry.snap.Graph.AllowCycles() {
		s.remaining[req.ExecutionID]--
		if req.Status == types.NodeStatusFailed {
			s.failed[req.ExecutionID]++
		}
		if req.Fatal || s.remaining[req.ExecutionID] == 0 {
			status := types.ExecutionStatusSuccess
			if req.Fatal || s.failed[req.ExecutionID] > 0 {
				status = types.ExecutionStatusFailed
			}
			s.finishExecutionLocked(req.ExecutionID, entry, status)
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
	if entry.snap.Graph != nil && entry.snap.Graph.AllowCycles() && !req.Fatal && !result.ExecutionDone {
		if len(req.CyclicOutbox) > 0 {
			for _, oe := range req.CyclicOutbox {
				if s.putOutboxEntryLocked(req.ExecutionID, oe) {
					result.OutboxIDs = append(result.OutboxIDs, oe.ID)
				}
			}
		} else if req.CyclicComplete {
			s.finishExecutionLocked(req.ExecutionID, entry, req.CyclicFinalStatus)
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
		counterKey := memoryCounterKey(req.ExecutionID, arrival.NodeIdx)
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
		taskType := engine.TaskTypeNodeExec
		outboxID := executeOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		if schedule == "skip" {
			taskType = engine.TaskTypeNodeSkip
			outboxID = skipOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		}
		if s.putOutboxLocked(req.ExecutionID, outboxID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			Type:         taskType,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, outboxID)
		}
	}
	return result, nil
}

// ListOutbox returns ready delivery intents for a single execution.
func (s *memoryState) ListOutbox(_ context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.outbox[id]
	if len(entries) == 0 || limit == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(entries))
	for entryID, entry := range entries {
		if entry.entry.AvailableAt.IsZero() || !entry.entry.AvailableAt.After(before) {
			ids = append(ids, entryID)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]engine.OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		out = append(out, cloneOutboxEntry(entries[entryID].entry))
	}
	return out, nil
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
		deadEntries[entryID] = stored
		result.DeadLettered = true
		return result, nil
	}
	entries[entryID] = stored
	return result, nil
}

// OutboxMetrics returns aggregate pending and dead-letter counts for the
// in-memory durable-outbox reference implementation.
func (s *memoryState) OutboxMetrics(_ context.Context) (engine.OutboxMetricsSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var snapshot engine.OutboxMetricsSnapshot
	for _, entries := range s.outbox {
		for _, stored := range entries {
			snapshot.Pending++
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

// ReplayDeadLetter moves a dead-lettered entry back to the ready set. The
// memory mutex serializes replays so concurrent calls collapse: exactly one
// returns ReplayReplayed, the rest return ReplayAlreadyReplayed with the
// original AuditID. Terminal/expired executions, terminal nodes, and stale
// activations are rejected without mutating state.
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

	// 1. Idempotency: same RequestID or same entry already replayed.
	if receipts, ok := s.replayReceipts[req.ExecutionID]; ok {
		if r, ok := receipts[requestID]; ok {
			return engine.ReplayDeadLetterResult{Outcome: engine.ReplayAlreadyReplayed, AuditID: r.auditID, ExecutionID: req.ExecutionID, NodeID: r.node, ActivationID: r.activation}, nil
		}
	}
	if idx, ok := s.replayEntryIdx[req.ExecutionID]; ok {
		if priorReqID, ok := idx[req.EntryID]; ok && priorReqID != "" {
			if r, ok := s.replayReceipts[req.ExecutionID][priorReqID]; ok {
				return engine.ReplayDeadLetterResult{Outcome: engine.ReplayAlreadyReplayed, AuditID: r.auditID, ExecutionID: req.ExecutionID, NodeID: r.node, ActivationID: r.activation}, nil
			}
		}
	}

	// 2. Execution status guard.
	exec, ok := s.executions[req.ExecutionID]
	if !ok || exec == nil {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayRejectedInactive, ExecutionID: req.ExecutionID}, nil
	}
	if types.IsTerminalExecutionStatus(exec.snap.Status) {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayRejectedTerminal, ExecutionID: req.ExecutionID}, nil
	}

	// 3. Read dead entry + node guard.
	deadEntries := s.deadOutbox[req.ExecutionID]
	stored, ok := deadEntries[req.EntryID]
	if !ok {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayNotFound, ExecutionID: req.ExecutionID}, nil
	}
	nodeName := stored.entry.Task.NodeName
	entryActivation := stored.entry.Task.ActivationID
	if nodeName != "" {
		node := s.nodes[memoryNodeKey(req.ExecutionID, nodeName)]
		if node != nil && isTerminalNode(node.Status) {
			return engine.ReplayDeadLetterResult{Outcome: engine.ReplayRejectedNodeTerminal, ExecutionID: req.ExecutionID, NodeID: nodeName, ActivationID: fmt.Sprintf("%d", entryActivation)}, nil
		}
		if entryActivation > 0 && node != nil && node.ActivationID != 0 && node.ActivationID != entryActivation {
			return engine.ReplayDeadLetterResult{Outcome: engine.ReplayRejectedActivationMismatch, ExecutionID: req.ExecutionID, NodeID: nodeName, ActivationID: fmt.Sprintf("%d", entryActivation)}, nil
		}
	}

	// 4. Atomic dead->ready move: preserve body, reset attempts.
	stored.entry.Attempts = 0
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

	// 5. Write authoritative receipt + entry index.
	auditID := requestID + ":" + fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	receipt := &memoryReplayReceipt{
		auditID: auditID, node: nodeName,
		activation:   fmt.Sprintf("%d", entryActivation),
		entryID:      req.EntryID, operator: req.Operator, reason: req.Reason,
		outcome: engine.ReplayReplayed, occurredAtMs: time.Now().UTC().UnixMilli(),
	}
	if s.replayReceipts[req.ExecutionID] == nil {
		s.replayReceipts[req.ExecutionID] = make(map[string]*memoryReplayReceipt)
	}
	s.replayReceipts[req.ExecutionID][requestID] = receipt
	if s.replayEntryIdx[req.ExecutionID] == nil {
		s.replayEntryIdx[req.ExecutionID] = make(map[string]string)
	}
	s.replayEntryIdx[req.ExecutionID][req.EntryID] = requestID

	return engine.ReplayDeadLetterResult{Outcome: engine.ReplayReplayed, AuditID: auditID, ExecutionID: req.ExecutionID, NodeID: nodeName, ActivationID: receipt.activation}, nil
}

// ListOutboxExecutions identifies executions with at least one durable intent
// so the background dispatcher can recover delivery after process restart.
func (s *memoryState) ListOutboxExecutions(_ context.Context, limit int) ([]types.ExecutionID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]types.ExecutionID, 0, len(s.outbox))
	for id, entries := range s.outbox {
		if len(entries) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
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

func (s *memoryState) finishExecutionLocked(id types.ExecutionID, entry *execEntry, status types.ExecutionStatus) {
	if types.IsTerminalExecutionStatus(entry.snap.Status) {
		return
	}
	entry.snap.Status = status
	if !entry.closed {
		entry.closed = true
		if done := s.doneCh[id]; done != nil {
			close(done)
		}
	}
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status})
}

func memoryNodeKey(id types.ExecutionID, name string) string {
	return string(id) + "/" + name
}

func memoryCounterKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("%s/%d", id, nodeIdx)
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
