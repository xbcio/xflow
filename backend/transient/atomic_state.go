package transient

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type transientOutboxEntry struct {
	entry engine.OutboxEntry
}

var _ engine.AtomicStateStore = (*state)(nil)
var _ engine.LegacyNodeCommitter = (*state)(nil)
var _ engine.LeaseExpander = (*state)(nil)
var _ engine.DurableLeaseExpander = (*state)(nil)
var _ engine.OutboxFailureRecorder = (*state)(nil)
var _ engine.OutboxMetricsReader = (*state)(nil)

// CreateExecutionWithOutbox is an in-memory atomic reference for transient
// mode. The outbox is intentionally process-local, but state and scheduling
// remain one mutex-protected transition while the process is alive.
func (s *state) CreateExecutionWithOutbox(_ context.Context, execution *engine.ExecutionSnapshot, entries []engine.OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createExecutionLocked(execution)
	for _, entry := range entries {
		s.putOutboxEntryLocked(execution.ID, entry)
	}
	return nil
}

func (s *state) CommitLeasedNode(ctx context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	return s.CommitNode(ctx, req)
}

func (s *state) CommitNode(_ context.Context, req engine.CommitNodeRequest) (engine.CommitNodeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.executions[req.ExecutionID]
	if entry == nil || types.IsTerminalExecutionStatus(entry.snap.Status) {
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeExecutionInactive}, nil
	}
	key := transientNodeKey(req.ExecutionID, req.NodeName)
	current := s.nodes[key]
	if current != nil && types.IsTerminalNodeStatus(current.Status) {
		if current.ActivationID == req.ActivationID && ((!req.System && req.LeaseToken != "" && current.CommittedLeaseToken == req.LeaseToken) || (req.System && current.Status == req.Status)) {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeDuplicateTerminal}, nil
		}
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
	}
	if req.System {
		if s.scheduled[transientCounterKey(req.ExecutionID, req.NodeIdx)] != "skip" || (current != nil && current.Status != types.NodeStatusPending) {
			return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
		}
	} else if current == nil || (current.Status != types.NodeStatusRunning && current.Status != types.NodeStatusCommitting && current.Status != types.NodeStatusWaiting) || current.LeaseID != req.LeaseID || current.LeaseToken != req.LeaseToken || current.Attempt != req.Attempt || current.ActivationID != req.ActivationID {
		return engine.CommitNodeResult{Outcome: engine.CommitOutcomeStaleToken}, nil
	}

	node := &engine.NodeSnapshot{
		ExecutionID:         req.ExecutionID,
		Name:                req.NodeName,
		NodeIdx:             req.NodeIdx,
		Status:              req.Status,
		Attempt:             req.Attempt,
		ActivationID:        req.ActivationID,
		AutoDepth:           req.AutoDepth,
		Output:              cloneMap(req.Output),
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
		s.outputs[key] = cloneMap(req.Output)
	}

	result := engine.CommitNodeResult{Outcome: engine.CommitOutcomeAccepted, Applied: true}
	if entry.snap.Graph != nil && !entry.snap.Graph.AllowCycles {
		s.remaining[req.ExecutionID]--
		if req.Status == types.NodeStatusFailed {
			s.failed[req.ExecutionID]++
		}
		if req.Fatal || s.remaining[req.ExecutionID] == 0 {
			status := types.ExecutionStatusSuccess
			if req.Fatal || s.failed[req.ExecutionID] > 0 {
				status = types.ExecutionStatusFailed
			}
			s.finishExecutionLocked(req.ExecutionID, entry, status, req.Error)
			result.ExecutionDone = true
			result.ExecutionStatus = status
		}
	}
	if req.AdvanceTask != nil && !req.Fatal && !result.ExecutionDone {
		entryID := transientAdvanceOutboxID(req.ExecutionID, req.NodeName, req.ActivationID)
		if s.putOutboxLocked(req.ExecutionID, entryID, *req.AdvanceTask, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, entryID)
		}
	}
	if !result.ExecutionDone {
		s.touchActiveLocked(req.ExecutionID)
	}
	return result, nil
}

func (s *state) AdvanceNode(_ context.Context, req engine.AdvanceNodeRequest) (engine.AdvanceNodeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.executions[req.ExecutionID]
	if entry == nil || types.IsTerminalExecutionStatus(entry.snap.Status) {
		return engine.AdvanceNodeResult{}, nil
	}
	node := s.nodes[transientNodeKey(req.ExecutionID, req.NodeName)]
	if node == nil || !types.IsTerminalNodeStatus(node.Status) || node.ActivationID != req.ActivationID {
		return engine.AdvanceNodeResult{}, nil
	}
	advanceKey := fmt.Sprintf("%s/%s/%d", req.ExecutionID, req.NodeName, req.ActivationID)
	if s.advanced[advanceKey] {
		return engine.AdvanceNodeResult{}, nil
	}
	s.advanced[advanceKey] = true

	result := engine.AdvanceNodeResult{Applied: true}
	for _, arrival := range req.Arrivals {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := transientCounterKey(req.ExecutionID, arrival.NodeIdx)
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
		entryID := transientExecuteOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		if schedule == "skip" {
			taskType = engine.TaskTypeNodeSkip
			entryID = transientSkipOutboxID(req.ExecutionID, arrival.NodeName, req.ActivationID)
		}
		if s.putOutboxLocked(req.ExecutionID, entryID, engine.Task{
			ExecutionID:  req.ExecutionID,
			NodeName:     arrival.NodeName,
			NodeIdx:      arrival.NodeIdx,
			Type:         taskType,
			ActivationID: req.ActivationID,
			AutoDepth:    req.AutoDepth,
		}, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, entryID)
		}
	}
	s.touchActiveLocked(req.ExecutionID)
	return result, nil
}

func (s *state) ResetNodeForRetryWithOutbox(_ context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[transientNodeKey(id, nodeName)]
	if !isActiveLeaseNode(node) || token == "" || node.LeaseToken != token {
		return false, nil
	}
	s.resetLeaseLocked(node)
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	s.touchActiveLocked(id)
	return true, nil
}

func (s *state) RevokeLeaseWithOutbox(ctx context.Context, id types.ExecutionID, nodeName string, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	return s.ResetNodeForRetryWithOutbox(ctx, id, nodeName, token, entry)
}

func (s *state) ListOutbox(_ context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.outbox[id]
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
		out = append(out, cloneTransientOutboxEntry(entries[entryID].entry))
	}
	return out, nil
}

func (s *state) AckOutbox(_ context.Context, id types.ExecutionID, entryID string) error {
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

// RecordOutboxFailure increments one transient outbox handoff attempt and
// retains terminally failed intents in process-local dead-letter storage.
func (s *state) RecordOutboxFailure(_ context.Context, id types.ExecutionID, entryID string, maxAttempts int) (engine.OutboxDeliveryFailure, error) {
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
			deadEntries = make(map[string]transientOutboxEntry)
			s.deadOutbox[id] = deadEntries
		}
		deadEntries[entryID] = stored
		result.DeadLettered = true
		return result, nil
	}
	entries[entryID] = stored
	return result, nil
}

// OutboxMetrics reports aggregate process-local pending and dead-letter
// counts for transient execution mode.
func (s *state) OutboxMetrics(_ context.Context) (engine.OutboxMetricsSnapshot, error) {
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

func (s *state) ListOutboxExecutions(_ context.Context, limit int) ([]types.ExecutionID, error) {
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

func (s *state) BeginTaskExpansionWithOutbox(_ context.Context, lease *engine.TaskLease, children []engine.SubExecution, entries []engine.OutboxEntry) (bool, error) {
	if lease == nil || len(children) != len(entries) {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesTransientLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	for index, child := range children {
		if child.ChildExecID == "" || child.ParentExecID != lease.Task.ExecutionID || child.ParentNode != lease.Task.NodeName || entries[index].ID == "" {
			return false, engine.ErrInvalidLeaseToken
		}
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)] = &copy
	key := transientExpansionKey(lease)
	for index, child := range children {
		childCopy := child
		childCopy.Result = cloneMap(child.Result)
		s.subExecs[key] = append(s.subExecs[key], &childCopy)
		s.putOutboxLocked(lease.Task.ExecutionID, entries[index].ID, entries[index].Task, entries[index].AvailableAt)
	}
	s.touchActiveLocked(lease.Task.ExecutionID)
	return true, nil
}

func (s *state) BeginTaskExpansion(_ context.Context, lease *engine.TaskLease) (bool, error) {
	if lease == nil {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesTransientLease(node, lease, types.NodeStatusCommitting) {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusWaiting
	s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)] = &copy
	s.touchActiveLocked(lease.Task.ExecutionID)
	return true, nil
}

func (s *state) CreateExpandedSubExecution(_ context.Context, lease *engine.TaskLease, sub *engine.SubExecution) (bool, error) {
	if lease == nil || sub == nil || sub.ChildExecID == "" {
		return false, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesTransientLease(node, lease, types.NodeStatusWaiting) {
		return false, nil
	}
	key := transientExpansionKey(lease)
	for _, existing := range s.subExecs[key] {
		if existing.ChildExecID == sub.ChildExecID {
			return true, nil
		}
	}
	copy := *sub
	copy.Result = cloneMap(sub.Result)
	s.subExecs[key] = append(s.subExecs[key], &copy)
	s.touchActiveLocked(lease.Task.ExecutionID)
	return true, nil
}

func (s *state) CompleteExpandedSubExecution(_ context.Context, lease *engine.TaskLease, childExecID types.ExecutionID, status types.ExecutionStatus, result map[string]any) (bool, bool, []map[string]any, error) {
	if lease == nil || childExecID == "" {
		return false, false, nil, engine.ErrInvalidLeaseToken
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[transientNodeKey(lease.Task.ExecutionID, lease.Task.NodeName)]
	if !matchesTransientLease(node, lease, types.NodeStatusWaiting) {
		return false, false, nil, nil
	}
	subs := s.subExecs[transientExpansionKey(lease)]
	found := false
	for _, sub := range subs {
		if sub.ChildExecID != childExecID {
			continue
		}
		found = true
		if sub.Status == types.ExecutionStatusRunning {
			sub.Status = status
			sub.Result = cloneMap(result)
		}
		break
	}
	if !found {
		return false, false, nil, nil
	}
	for _, sub := range subs {
		if sub.Status == types.ExecutionStatusRunning {
			return false, true, nil, nil
		}
	}
	return true, true, transientExpansionResults(subs), nil
}

func (s *state) createExecutionLocked(e *engine.ExecutionSnapshot) {
	if timer := s.cleanupTimers[e.ID]; timer != nil {
		timer.Stop()
		delete(s.cleanupTimers, e.ID)
	}
	copy := *e
	s.executions[e.ID] = &execEntry{snap: copy}
	s.doneCh[e.ID] = make(chan struct{})
	if e.Graph != nil {
		for i, degree := range e.Graph.InDegree {
			s.inDegrees[transientCounterKey(e.ID, i)] = degree
		}
		if !e.Graph.AllowCycles {
			s.remaining[e.ID] = len(e.Graph.Nodes)
			s.failed[e.ID] = 0
		}
	}
	s.touchActiveLocked(e.ID)
}

func (s *state) finishExecutionLocked(id types.ExecutionID, entry *execEntry, status types.ExecutionStatus, errMsg string) {
	if types.IsTerminalExecutionStatus(entry.snap.Status) {
		return
	}
	entry.snap.Status = status
	entry.err = errMsg
	s.stopActiveTimerLocked(id)
	if !entry.closed {
		entry.closed = true
		if done := s.doneCh[id]; done != nil {
			close(done)
		}
	}
	s.scheduleCleanupLocked(id)
	s.publishLocked(engine.ExecutionEvent{ExecutionID: id, Status: status, Error: errMsg})
}

func (s *state) putOutboxLocked(id types.ExecutionID, entryID string, task engine.Task, availableAt time.Time) bool {
	return s.putOutboxEntryLocked(id, engine.OutboxEntry{
		ID:          entryID,
		Task:        task,
		AvailableAt: availableAt,
	})
}

func (s *state) putOutboxEntryLocked(id types.ExecutionID, entry engine.OutboxEntry) bool {
	if entry.ID == "" {
		return false
	}
	entries := s.outbox[id]
	if entries == nil {
		entries = make(map[string]transientOutboxEntry)
		s.outbox[id] = entries
	}
	if _, exists := entries[entry.ID]; exists {
		return false
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}
	entry.Task = cloneTransientTask(entry.Task)
	entries[entry.ID] = transientOutboxEntry{entry: entry}
	return true
}

func isActiveLeaseNode(node *engine.NodeSnapshot) bool {
	return node != nil && (node.Status == types.NodeStatusRunning || node.Status == types.NodeStatusCommitting || node.Status == types.NodeStatusWaiting)
}

func (s *state) resetLeaseLocked(node *engine.NodeSnapshot) {
	node.Status = types.NodeStatusPending
	node.LeaseID = ""
	node.LeaseToken = ""
	node.LeaseIssuedAt = time.Time{}
	node.LeaseTTL = 0
	node.LeaseTaskType = engine.TaskTypeNodeExec
	node.LeasePayload = nil
}

func transientNodeKey(id types.ExecutionID, name string) string { return string(id) + "/" + name }
func transientCounterKey(id types.ExecutionID, idx int) string  { return fmt.Sprintf("%s/%d", id, idx) }
func transientExpansionKey(lease *engine.TaskLease) string {
	return fmt.Sprintf("%s/%s/%s", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID)
}
func transientAdvanceOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("advance/%s/%s/%d", id, name, activationID)
}
func transientExecuteOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("execute/%s/%s/%d", id, name, activationID)
}
func transientSkipOutboxID(id types.ExecutionID, name string, activationID int) string {
	return fmt.Sprintf("skip/%s/%s/%d", id, name, activationID)
}

func matchesTransientLease(node *engine.NodeSnapshot, lease *engine.TaskLease, status types.NodeStatus) bool {
	return node != nil && node.Status == status && node.LeaseID == lease.LeaseID && node.LeaseToken == lease.LeaseToken && node.Attempt == lease.Attempt && node.ActivationID == lease.Task.ActivationID
}

func transientExpansionResults(subs []*engine.SubExecution) []map[string]any {
	sorted := append([]*engine.SubExecution(nil), subs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].BatchIndex < sorted[j].BatchIndex })
	results := make([]map[string]any, 0, len(sorted))
	for _, sub := range sorted {
		if sub.Result != nil {
			results = append(results, cloneMap(sub.Result))
		}
	}
	return results
}

func cloneTransientTask(task engine.Task) engine.Task {
	copy := task
	copy.Payload = cloneLeasePayload(task.Payload)
	return copy
}

func cloneTransientOutboxEntry(entry engine.OutboxEntry) engine.OutboxEntry {
	entry.Task = cloneTransientTask(entry.Task)
	return entry
}
