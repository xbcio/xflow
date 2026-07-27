package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/types"
)

var _ AtomicStateStore = (*fakeState)(nil)
var _ LegacyNodeCommitter = (*fakeState)(nil)
var _ DurableLeaseSuspender = (*fakeState)(nil)

func (f *fakeState) CommitLeasedNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	return f.CommitNode(ctx, req)
}

var _ LeaseSuspender = (*fakeState)(nil)

func (f *fakeState) SuspendTaskLease(_ context.Context, lease *TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (*types.SignalPayload, bool, error) {
	if lease == nil || spec == nil {
		return nil, false, ErrInvalidLeaseToken
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	key := string(lease.Task.ExecutionID) + "/" + lease.Task.NodeName
	node := f.nodes[key]
	if node == nil || node.Status != types.NodeStatusCommitting || node.LeaseID != lease.LeaseID || node.LeaseToken != lease.LeaseToken || node.Attempt != lease.Attempt || node.ActivationID != lease.Task.ActivationID {
		return nil, false, nil
	}
	if storeOutput {
		f.outputs[key] = cloneMap(output)
	}
	delete(f.resumed, key)
	if oldSignalName != "" {
		delete(f.suspended, key)
	}

	var payload *types.SignalPayload
	if spec.Mode == types.ModeMultiSignal {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			data, found := f.signals[signalKey]
			if !found {
				continue
			}
			delete(f.signals, signalKey)
			if ready := f.addMultiSignalLocked(lease.Task.ExecutionID, lease.Task.NodeName, signalName, data, spec); ready != nil {
				payload = ready
				delete(f.signalSets, key)
				break
			}
		}
	} else {
		for _, signalName := range spec.Signals {
			signalKey := string(lease.Task.ExecutionID) + "/" + signalName
			if data, found := f.signals[signalKey]; found {
				delete(f.signals, signalKey)
				payload = &types.SignalPayload{Triggered: types.SignalReceived, Name: signalName, Data: cloneMap(data)}
				break
			}
		}
	}
	if payload == nil {
		f.suspended[key] = spec
	} else {
		delete(f.suspended, key)
	}
	copy := *node
	copy.Status = types.NodeStatusSuspended
	copy.LeaseID = ""
	copy.LeaseToken = ""
	copy.LeaseIssuedAt = time.Time{}
	copy.LeaseTTL = 0
	copy.LeaseTaskType = TaskTypeNodeExec
	copy.LeasePayload = nil
	f.nodes[key] = &copy
	for _, entry := range SuspendOutboxEntries(lease, spec, payload, time.Now().UTC()) {
		f.putAtomicOutboxLocked(lease.Task.ExecutionID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return cloneFakeLeasePayload(payload), true, nil
}

func (f *fakeState) SuspendTaskLeaseWithOutbox(ctx context.Context, lease *TaskLease, output map[string]any, storeOutput bool, spec *types.SuspendSpec, oldSignalName string) (bool, error) {
	_, committed, err := f.SuspendTaskLease(ctx, lease, output, storeOutput, spec, oldSignalName)
	return committed, err
}

func (f *fakeState) CreateExecutionWithOutbox(_ context.Context, execution *ExecutionSnapshot, entries []OutboxEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createExecutionLocked(execution)
	for _, entry := range entries {
		f.putAtomicOutboxLocked(execution.ID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return nil
}

func (f *fakeState) ResetNodeForRetryWithOutbox(_ context.Context, id types.ExecutionID, nodeName string, token LeaseToken, entry OutboxEntry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + nodeName
	node := f.nodes[key]
	if node == nil || (node.Status != types.NodeStatusRunning && node.Status != types.NodeStatusCommitting && node.Status != types.NodeStatusWaiting) || token == "" || node.LeaseToken != token {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusPending
	copy.LeaseID = ""
	copy.LeaseToken = ""
	copy.LeaseIssuedAt = time.Time{}
	copy.LeaseTTL = 0
	copy.LeaseTaskType = TaskTypeNodeExec
	copy.LeasePayload = nil
	f.nodes[key] = &copy
	f.putAtomicOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

func (f *fakeState) RevokeLeaseWithOutbox(_ context.Context, id types.ExecutionID, nodeName string, token LeaseToken, entry OutboxEntry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(id) + "/" + nodeName
	node := f.nodes[key]
	if node == nil || (node.Status != types.NodeStatusRunning && node.Status != types.NodeStatusCommitting && node.Status != types.NodeStatusWaiting) || token == "" || node.LeaseToken != token {
		return false, nil
	}
	copy := *node
	copy.Status = types.NodeStatusPending
	copy.LeaseID = ""
	copy.LeaseToken = ""
	copy.LeaseIssuedAt = time.Time{}
	copy.LeaseTTL = 0
	copy.LeaseTaskType = TaskTypeNodeExec
	copy.LeasePayload = nil
	f.nodes[key] = &copy
	f.putAtomicOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

// CommitNode is the test reference implementation of the atomic commit
// contract. It keeps the same fence, duplicate, terminal-count, and outbox
// semantics as production backends while using fakeState's single mutex as
// its transaction boundary.
func (f *fakeState) CommitNode(_ context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	exec := f.executions[req.ExecutionID]
	if exec == nil {
		return CommitNodeResult{Outcome: CommitOutcomeExecutionInactive}, nil
	}
	key := string(req.ExecutionID) + "/" + req.NodeName
	current := f.nodes[key]
	if current != nil && types.IsTerminalNodeStatus(current.Status) {
		if current.ActivationID == req.ActivationID {
			if req.System && current.Status == req.Status {
				return CommitNodeResult{Outcome: CommitOutcomeDuplicateTerminal}, nil
			}
			if !req.System && req.LeaseToken != "" && current.CommittedLeaseToken == req.LeaseToken {
				return CommitNodeResult{Outcome: CommitOutcomeDuplicateTerminal}, nil
			}
		}
		return CommitNodeResult{Outcome: CommitOutcomeStaleToken}, nil
	}
	if types.IsTerminalExecutionStatus(exec.Status) {
		return CommitNodeResult{Outcome: CommitOutcomeExecutionInactive}, nil
	}

	if req.System {
		if f.atomicSchedule[fakeAtomicCounterKey(req.ExecutionID, req.NodeIdx)] != "skip" {
			return CommitNodeResult{Outcome: CommitOutcomeStaleToken}, nil
		}
		if current != nil && current.Status != types.NodeStatusPending {
			return CommitNodeResult{Outcome: CommitOutcomeStaleToken}, nil
		}
	} else if current == nil || (current.Status != types.NodeStatusRunning && current.Status != types.NodeStatusCommitting && current.Status != types.NodeStatusWaiting) || current.LeaseID != req.LeaseID || current.LeaseToken != req.LeaseToken || current.Attempt != req.Attempt || current.ActivationID != req.ActivationID {
		return CommitNodeResult{Outcome: CommitOutcomeStaleToken}, nil
	}

	node := &NodeSnapshot{
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
	f.nodes[key] = node
	if req.StoreOutput {
		f.outputs[key] = cloneMap(req.Output)
	}

	result := CommitNodeResult{Outcome: CommitOutcomeAccepted, Applied: true}
	if exec.Graph != nil && !exec.Graph.AllowCycles() {
		remaining := 0
		hasFailed := false
		for i := 0; i < exec.Graph.NodeCount(); i++ {
			meta := exec.Graph.NodeAt(i)
			snapshot := f.nodes[string(req.ExecutionID)+"/"+meta.Name]
			if snapshot == nil || !types.IsTerminalNodeStatus(snapshot.Status) {
				remaining++
				continue
			}
			if snapshot.Status == types.NodeStatusFailed {
				hasFailed = true
			}
		}
		if req.Fatal || remaining == 0 {
			status := types.ExecutionStatusSuccess
			if req.Fatal || hasFailed {
				status = types.ExecutionStatusFailed
			}
			exec.Status = status
			f.publishLocked(ExecutionEvent{ExecutionID: req.ExecutionID, Status: status, Error: req.Error})
			result.ExecutionDone = true
			result.ExecutionStatus = status
		}
	}

	if req.AdvanceTask != nil && !req.Fatal && !result.ExecutionDone {
		entryID := fmt.Sprintf("advance/%s/%s/%d", req.ExecutionID, req.NodeName, req.ActivationID)
		if f.putAtomicOutboxLocked(req.ExecutionID, entryID, *req.AdvanceTask, time.Time{}) {
			result.OutboxIDs = append(result.OutboxIDs, entryID)
		}
	}

	// Reference mirror of the production cyclic path (#7): persist the
	// engine-computed downstream intents, or finalize the execution when the
	// active branch terminated, in the same mutex-guarded transition.
	if exec.Graph != nil && exec.Graph.AllowCycles() && !req.Fatal && !result.ExecutionDone {
		if len(req.CyclicOutbox) > 0 {
			for _, oe := range req.CyclicOutbox {
				if f.putAtomicOutboxLocked(req.ExecutionID, oe.ID, oe.Task, oe.AvailableAt) {
					result.OutboxIDs = append(result.OutboxIDs, oe.ID)
				}
			}
		} else if req.CyclicComplete {
			// Cancel-aware finalization fence, mirroring commitNodeLua: a
			// concurrent Cancel may have moved the execution to canceling/
			// canceled (or another terminal) while this cyclic node was
			// completing. The terminal node write above still lands, but the
			// execution finalization must NOT clobber that cancel/terminal.
			if !isCancelOrTerminalExecStatus(exec.Status) {
				exec.Status = req.CyclicFinalStatus
				f.publishLocked(ExecutionEvent{ExecutionID: req.ExecutionID, Status: req.CyclicFinalStatus, Error: req.CyclicFinalError})
				result.ExecutionDone = true
				result.ExecutionStatus = req.CyclicFinalStatus
			}
		}
	}
	return result, nil
}

// isCancelOrTerminalExecStatus reports whether an execution status is in the
// canceling/terminal set that a fenced finalization must not overwrite. It
// mirrors the guard in commitNodeLua (canceling/canceled/timeout/success/
// failed).
func isCancelOrTerminalExecStatus(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusCanceling, types.ExecutionStatusCanceled,
		types.ExecutionStatusTimeout, types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed:
		return true
	}
	return false
}

func (f *fakeState) AdvanceNode(_ context.Context, req AdvanceNodeRequest) (AdvanceNodeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	exec := f.executions[req.ExecutionID]
	if exec == nil || types.IsTerminalExecutionStatus(exec.Status) {
		return AdvanceNodeResult{}, nil
	}
	source := f.nodes[string(req.ExecutionID)+"/"+req.NodeName]
	if source == nil || !types.IsTerminalNodeStatus(source.Status) || source.ActivationID != req.ActivationID {
		return AdvanceNodeResult{}, nil
	}
	advanceKey := fmt.Sprintf("advance/%s/%s/%d", req.ExecutionID, req.NodeName, req.ActivationID)
	if f.atomicAdvanced[advanceKey] {
		return AdvanceNodeResult{}, nil
	}
	f.atomicAdvanced[advanceKey] = true

	result := AdvanceNodeResult{Applied: true}
	for _, arrival := range req.Arrivals {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := fakeAtomicCounterKey(req.ExecutionID, arrival.NodeIdx)
		activeBefore := f.activeIns[counterKey]
		f.inDegrees[counterKey] -= arrival.ArrivalCount
		if arrival.ActiveCount > 0 {
			f.activeIns[counterKey] += arrival.ActiveCount
		}
		if f.atomicSchedule[counterKey] != "" {
			continue
		}

		action := ""
		if arrival.MergeMode == "wait_any" && arrival.ActiveCount > 0 && activeBefore == 0 {
			action = "execute"
		} else if f.inDegrees[counterKey] <= 0 {
			if f.activeIns[counterKey] > 0 {
				action = "execute"
			} else {
				action = "skip"
			}
		}
		if action == "" {
			continue
		}
		f.atomicSchedule[counterKey] = action
		taskType := TaskTypeNodeExec
		entryID := fmt.Sprintf("execute/%s/%s/%d", req.ExecutionID, arrival.NodeName, req.ActivationID)
		if action == "skip" {
			taskType = TaskTypeNodeSkip
			entryID = fmt.Sprintf("skip/%s/%s/%d", req.ExecutionID, arrival.NodeName, req.ActivationID)
		}
		if f.putAtomicOutboxLocked(req.ExecutionID, entryID, Task{
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
	return result, nil
}

func (f *fakeState) ListOutbox(_ context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		return nil, nil
	}
	entries := f.atomicOutbox[id]
	ids := make([]string, 0, len(entries))
	for entryID, entry := range entries {
		if entry.AvailableAt.IsZero() || !entry.AvailableAt.After(before) {
			ids = append(ids, entryID)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]OutboxEntry, 0, len(ids))
	for _, entryID := range ids {
		entry := entries[entryID]
		entry.Task = cloneAtomicTask(entry.Task)
		out = append(out, entry)
	}
	return out, nil
}

func (f *fakeState) AckOutbox(_ context.Context, id types.ExecutionID, entryID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entries := f.atomicOutbox[id]; entries != nil {
		delete(entries, entryID)
		if len(entries) == 0 {
			delete(f.atomicOutbox, id)
		}
	}
	return nil
}

func (f *fakeState) ListOutboxExecutions(_ context.Context, limit int) ([]types.ExecutionID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]types.ExecutionID, 0, len(f.atomicOutbox))
	for id, entries := range f.atomicOutbox {
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

func (f *fakeState) putAtomicOutboxLocked(id types.ExecutionID, entryID string, task Task, availableAt time.Time) bool {
	entries := f.atomicOutbox[id]
	if entries == nil {
		entries = make(map[string]OutboxEntry)
		f.atomicOutbox[id] = entries
	}
	if _, exists := entries[entryID]; exists {
		return false
	}
	entries[entryID] = OutboxEntry{ID: entryID, Task: cloneAtomicTask(task), AvailableAt: availableAt}
	return true
}

func fakeAtomicCounterKey(id types.ExecutionID, nodeIdx int) string {
	return fmt.Sprintf("%s/%d", id, nodeIdx)
}

func cloneAtomicTask(task Task) Task {
	copy := task
	if task.Payload == nil {
		return copy
	}
	payload := *task.Payload
	payload.Data = cloneMap(task.Payload.Data)
	if task.Payload.All != nil {
		payload.All = make(map[string]map[string]any, len(task.Payload.All))
		for name, data := range task.Payload.All {
			payload.All[name] = cloneMap(data)
		}
	}
	copy.Payload = &payload
	return copy
}
