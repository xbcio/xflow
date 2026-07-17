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
func (s *memoryState) RecordOutboxFailure(_ context.Context, id types.ExecutionID, entryID string, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// ListDeadLetters returns up to limit dead-lettered entries for an execution,
// oldest-first. Entries are not removed.
func (s *memoryState) ListDeadLetters(_ context.Context, id types.ExecutionID, limit int) ([]engine.OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
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
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ReplayDeadLetter moves a dead-lettered entry back to the ready set. The
// memory mutex serializes replays so concurrent calls collapse to exactly one
// ReplayReplayed. Terminal/expired executions are rejected.
func (s *memoryState) ReplayDeadLetter(_ context.Context, id types.ExecutionID, entryID string) (engine.DeadLetterReplayOutcome, error) {
	if entryID == "" {
		return engine.ReplayNotFound, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.executions[id]
	if !ok || exec == nil {
		return engine.ReplayRejectedInactive, nil
	}
	if types.IsTerminalExecutionStatus(exec.snap.Status) {
		return engine.ReplayRejectedTerminal, nil
	}
	deadEntries := s.deadOutbox[id]
	stored, ok := deadEntries[entryID]
	if !ok {
		return engine.ReplayNotFound, nil
	}
	stored.entry.Attempts = 0
	readyEntries := s.outbox[id]
	if readyEntries == nil {
		readyEntries = make(map[string]memoryOutboxEntry)
		s.outbox[id] = readyEntries
	}
	readyEntries[entryID] = stored
	delete(deadEntries, entryID)
	if len(deadEntries) == 0 {
		delete(s.deadOutbox, id)
	}
	return engine.ReplayReplayed, nil
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
