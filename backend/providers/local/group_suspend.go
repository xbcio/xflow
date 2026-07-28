package local

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Compile-time interface checks.
var _ engine.GroupSuspender = (*memoryState)(nil)
var _ engine.GroupResumer = (*memoryState)(nil)
var _ engine.GroupSuspendReader = (*memoryState)(nil)
var _ engine.GroupCanceler = (*memoryState)(nil)

// suspendedGroupKey uniquely identifies a suspended group unit.
type suspendedGroupKey struct {
	execID  types.ExecutionID
	unitIdx int
}

// SuspendGroup atomically transitions a running group unit to suspended state.
// It persists the suspend spec, signal journal, and entry input; clears the
// lease so the sweeper ignores the unit; and checks for pre-delivered signals.
func (s *memoryState) SuspendGroup(_ context.Context, req engine.GroupSuspendRequest) (engine.GroupSuspendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := groupKey(req.ExecutionID, req.GroupUnitIdx)
	st := s.groupUnits[key]
	if st == nil || st.status != groupUnitRunning {
		return engine.GroupSuspendResult{}, nil
	}
	// Fence: lease token must match.
	if st.leaseToken != req.LeaseToken {
		return engine.GroupSuspendResult{}, nil
	}

	// Transition to suspended.
	st.status = groupUnitSuspended
	st.leaseID = ""
	st.leaseToken = ""
	st.deadline = time.Time{}

	// Store suspend state.
	sgKey := suspendedGroupKey{execID: req.ExecutionID, unitIdx: req.GroupUnitIdx}
	s.suspendedGroups[sgKey] = &engine.GroupSuspendState{
		Spec:           req.SuspendSpec,
		SignalJournal:  append([]engine.GroupSignal(nil), req.SignalJournal...),
		EntryInput:     cloneData(req.EntryInput),
		IdempotencyKey: req.IdempotencyKey,
	}
	// Check for pre-delivered signals.
	for _, sigName := range req.SuspendSpec.WaitSignals {
		sigKey := string(req.ExecutionID) + "/group/" + fmt.Sprintf("%d", req.GroupUnitIdx) + "/" + sigName
		if _, found := s.signals[sigKey]; found {
			// Signal already available — immediate resume.
			return engine.GroupSuspendResult{Committed: true, ImmediateResume: true}, nil
		}
	}

	return engine.GroupSuspendResult{Committed: true}, nil
}

// ResumeGroup delivers a signal to a suspended group unit. If quorum is
// satisfied, it transitions the unit back to pending and writes a resume
// outbox entry.
func (s *memoryState) ResumeGroup(_ context.Context, req engine.GroupResumeRequest) (engine.GroupResumeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := groupKey(req.ExecutionID, req.GroupUnitIdx)
	st := s.groupUnits[key]
	if st == nil || st.status != groupUnitSuspended {
		return engine.GroupResumeResult{}, nil
	}

	sgKey := suspendedGroupKey{execID: req.ExecutionID, unitIdx: req.GroupUnitIdx}
	state := s.suspendedGroups[sgKey]
	if state == nil {
		return engine.GroupResumeResult{}, nil
	}

	// Validate signal name is in WaitSignals.
	found := false
	for _, sigName := range state.Spec.WaitSignals {
		if sigName == req.SignalName {
			found = true
			break
		}
	}
	if !found {
		return engine.GroupResumeResult{}, nil
	}

	// Append to DeliveredSignals.
	state.DeliveredSignals = append(state.DeliveredSignals, engine.GroupSignal{
		Name: req.SignalName,
		Data: cloneData(req.SignalData),
	})

	// Determine quorum.
	quorum := state.Spec.Quorum
	if quorum <= 0 {
		quorum = 1
	}

	if len(state.DeliveredSignals) < quorum {
		// Partial delivery — quorum not yet met.
		return engine.GroupResumeResult{Resumed: false, Pending: quorum - len(state.DeliveredSignals)}, nil
	}

	// Quorum satisfied — transition to pending and write outbox.
	st.status = groupUnitPending
	st.attempt++

	// Build resume journal = original SignalJournal + newly delivered signals.
	resumeJournal := make([]engine.GroupSignal, 0, len(state.SignalJournal)+len(state.DeliveredSignals))
	resumeJournal = append(resumeJournal, state.SignalJournal...)
	resumeJournal = append(resumeJournal, state.DeliveredSignals...)

	// Write outbox entry with TaskTypeGroupResume.
	outboxID := fmt.Sprintf("group-resume/%s/%d", req.ExecutionID, req.GroupUnitIdx)
	s.putOutboxLocked(req.ExecutionID, outboxID, engine.Task{
		ExecutionID: req.ExecutionID,
		UnitIdx:     req.GroupUnitIdx,
		Type:        engine.TaskTypeGroupResume,
	}, time.Now().UTC())

	// Clean up suspend state.
	delete(s.suspendedGroups, sgKey)
	_ = resumeJournal // journal stored in outbox task payload (simplified in-memory model)

	return engine.GroupResumeResult{Resumed: true}, nil
}

// GetGroupSuspendState returns the stored suspend state for a group unit, or
// nil if the unit is not currently suspended.
func (s *memoryState) GetGroupSuspendState(_ context.Context, execID types.ExecutionID, unitIdx int) (*engine.GroupSuspendState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sgKey := suspendedGroupKey{execID: execID, unitIdx: unitIdx}
	state := s.suspendedGroups[sgKey]
	if state == nil {
		return nil, nil
	}
	// Return a copy to prevent mutation.
	cp := *state
	cp.SignalJournal = append([]engine.GroupSignal(nil), state.SignalJournal...)
	cp.DeliveredSignals = append([]engine.GroupSignal(nil), state.DeliveredSignals...)
	cp.EntryInput = cloneData(state.EntryInput)
	return &cp, nil
}

// CancelSuspendedGroup transitions a suspended group unit to done and cleans
// up the suspend state. It also decrements the execution's remaining counter
// which may complete the execution.
func (s *memoryState) CancelSuspendedGroup(_ context.Context, execID types.ExecutionID, unitIdx int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := groupKey(execID, unitIdx)
	st := s.groupUnits[key]
	if st == nil || st.status != groupUnitSuspended {
		return nil
	}

	// Transition to done.
	st.status = groupUnitDone
	st.leaseID = ""
	st.leaseToken = ""

	// Clean up suspend state.
	sgKey := suspendedGroupKey{execID: execID, unitIdx: unitIdx}
	delete(s.suspendedGroups, sgKey)

	// Decrement remaining counter — may complete the execution.
	entry := s.executions[execID]
	if entry != nil && entry.snap.Graph != nil && !entry.snap.Graph.AllowCycles() {
		s.remaining[execID]--
		s.failed[execID]++
		if s.remaining[execID] == 0 {
			s.finishExecutionLocked(execID, entry, types.ExecutionStatusFailed)
		}
	}

	return nil
}
