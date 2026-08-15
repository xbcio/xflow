package local

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

const (
	groupUnitPending = iota
	groupUnitRunning
	groupUnitDone
	groupUnitSuspended
)

type groupUnitState struct {
	status         int
	leaseID        engine.LeaseID
	leaseToken     engine.LeaseToken
	attempt        int
	deadline       time.Time
	committedToken engine.LeaseToken

	// Identity of the queued group task this unit was leased for. Recorded so
	// ListExpiredLeases can rebuild the task without a compiled graph, exactly
	// as the Redis store does from its group meta hash.
	execID       types.ExecutionID
	unitIdx      int
	groupName    string
	entryNodeIdx int
	activationID int
	issuedAt     time.Time
	ttl          time.Duration
}

var _ engine.GroupStateStore = (*memoryState)(nil)
var _ engine.GroupLeaseReader = (*memoryState)(nil)
var _ engine.GroupLeaseExpirer = (*memoryState)(nil)
var _ engine.GroupLeaseReclaimer = (*memoryState)(nil)

func groupKey(id types.ExecutionID, unitIdx int) string {
	return fmt.Sprintf("%s/%d", id, unitIdx)
}

func (s *memoryState) AcquireGroupLease(_ context.Context, lease *engine.GroupLease) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := groupKey(lease.ExecutionID, lease.GroupUnitIdx)
	st := s.groupUnits[key]
	if st == nil {
		st = &groupUnitState{status: groupUnitPending}
		s.groupUnits[key] = st
	}
	if st.status == groupUnitRunning || st.status == groupUnitDone {
		return false, nil
	}
	st.status = groupUnitRunning
	attempt := lease.Attempt
	if st.attempt >= attempt {
		attempt = st.attempt + 1
	}
	st.leaseID, st.leaseToken, st.attempt = lease.LeaseID, lease.LeaseToken, attempt
	st.deadline = lease.IssuedAt.Add(lease.TTL)
	st.execID, st.unitIdx = lease.ExecutionID, lease.GroupUnitIdx
	st.groupName, st.entryNodeIdx, st.activationID = lease.GroupName, lease.EntryNodeIdx, lease.ActivationID
	st.issuedAt, st.ttl = lease.IssuedAt, lease.TTL
	lease.Attempt = attempt
	return true, nil
}

func (s *memoryState) RenewGroupLease(_ context.Context, id types.ExecutionID, unitIdx int, token engine.LeaseToken, deadline time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.groupUnits[groupKey(id, unitIdx)]
	if st == nil || st.status != groupUnitRunning || st.leaseToken != token {
		return false, nil
	}
	st.deadline = deadline
	return true, nil
}

func (s *memoryState) GetGroupLease(_ context.Context, id types.ExecutionID, unitIdx int) (*engine.GroupLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.groupUnits[groupKey(id, unitIdx)]
	if st == nil || st.status != groupUnitRunning {
		return nil, nil
	}
	return &engine.GroupLease{
		LeaseID:      st.leaseID,
		LeaseToken:   st.leaseToken,
		Attempt:      st.attempt,
		ExecutionID:  id,
		GroupUnitIdx: unitIdx,
		GroupName:    st.groupName,
		EntryNodeIdx: st.entryNodeIdx,
		ActivationID: st.activationID,
		IssuedAt:     st.issuedAt,
		TTL:          st.ttl,
	}, nil
}

func (s *memoryState) ExpireGroupLease(_ context.Context, id types.ExecutionID, unitIdx int, token engine.LeaseToken) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expireGroupLeaseLocked(id, unitIdx, token), nil
}

// RevokeGroupLeaseWithOutbox pairs the fenced expiry with the redelivery task
// under one lock, so no observer can see a unit that is pending, unleased, and
// unqueued — the state the sweeper cannot rediscover.
func (s *memoryState) RevokeGroupLeaseWithOutbox(_ context.Context, id types.ExecutionID, unitIdx int, token engine.LeaseToken, entry engine.OutboxEntry) (bool, error) {
	if token == "" {
		return false, nil
	}
	if entry.ID == "" {
		return false, fmt.Errorf("requeue outbox for %q/#%d has empty ID", id, unitIdx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.expireGroupLeaseLocked(id, unitIdx, token) {
		return false, nil
	}
	s.putOutboxLocked(id, entry.ID, entry.Task, entry.AvailableAt)
	return true, nil
}

// expireGroupLeaseLocked transitions a running group unit back to retry-ready
// under a token fence. Caller must hold s.mu.
func (s *memoryState) expireGroupLeaseLocked(id types.ExecutionID, unitIdx int, token engine.LeaseToken) bool {
	st := s.groupUnits[groupKey(id, unitIdx)]
	if st == nil || st.status != groupUnitRunning || st.leaseToken != token {
		return false
	}
	st.status = groupUnitPending
	st.leaseID = ""
	st.leaseToken = ""
	st.attempt++
	st.deadline = time.Time{}
	return true
}

func (s *memoryState) CommitGroup(_ context.Context, req engine.GroupCommitRequest) (engine.GroupCommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.groupUnits[groupKey(req.ExecutionID, req.GroupUnitIdx)]
	// No lease / already terminal / fence mismatch => not applied.
	if st == nil || st.leaseToken != req.LeaseToken || st.attempt != req.Attempt || st.status != groupUnitRunning {
		outcome := engine.CommitOutcomeStaleToken
		if st != nil && st.status == groupUnitDone {
			outcome = engine.CommitOutcomeDuplicateTerminal
		}
		return engine.GroupCommitResult{Applied: false, Outcome: outcome}, nil
	}
	entry := s.executions[req.ExecutionID]
	if entry == nil || isTerminalStatus(entry.snap.Status) {
		return engine.GroupCommitResult{Applied: false, Outcome: engine.CommitOutcomeExecutionInactive}, nil
	}
	// 1. Persist boundary outputs (same path as CommitNode: s.outputs keyed by execID+"/"+name).
	for _, ex := range req.Exits {
		s.outputs[string(req.ExecutionID)+"/"+ex.NodeName] = cloneData(ex.Data)
	}
	// 2. Terminalize the group unit.
	st.status = groupUnitDone
	st.committedToken = req.LeaseToken
	st.leaseID, st.leaseToken = "", ""
	// 3. Decrement remaining by unit (same completion logic as CommitNode).
	// A group unit is never cyclic — graph.validateGroupsAllowCyclesExclusion
	// rejects AllowCycles at compile time — so the acyclic completion protocol
	// always applies. This mirrors the Redis store, which passes a hardcoded
	// allowCycles=0 for the same reason; reading it off the snapshot's graph
	// instead would silently skip the counting for a graph-less snapshot.
	result := engine.GroupCommitResult{Applied: true, Outcome: engine.CommitOutcomeAccepted}
	s.remaining[req.ExecutionID]--
	if req.Outcome == engine.GroupOutcomeFailed {
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
	// 4. Downstream unit arrival counting (same unit-keyed counting as AdvanceNode).
	result.OutboxIDs = append(result.OutboxIDs, s.applyGroupDownstreamLocked(req.ExecutionID, req.Downstream)...)
	return result, nil
}

// applyGroupDownstreamLocked converts group commit downstream arrivals into
// execute/skip outbox intents, using unit-keyed counting with the same semantics
// as AdvanceNode (DECR in-degree, accumulate active, wait_any/wait_all threshold).
// Caller must hold s.mu.
func (s *memoryState) applyGroupDownstreamLocked(id types.ExecutionID, arrivals []engine.DownstreamArrival) []string {
	var ids []string
	for _, arrival := range arrivals {
		if arrival.ArrivalCount <= 0 {
			continue
		}
		counterKey := memoryCounterKey(id, arrival.UnitIdx)
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
		outboxID := executeOutboxID(id, arrival.NodeName, 0)
		if schedule == "skip" {
			taskType = engine.TaskTypeNodeSkip
			outboxID = skipOutboxID(id, arrival.NodeName, 0)
		}
		if s.putOutboxLocked(id, outboxID, engine.Task{
			ExecutionID: id,
			NodeName:    arrival.NodeName,
			NodeIdx:     arrival.NodeIdx,
			UnitIdx:     arrival.UnitIdx,
			Type:        taskType,
		}, time.Time{}) {
			ids = append(ids, outboxID)
		}
	}
	return ids
}
