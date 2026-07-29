package local

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.EntryAdmissionStore = (*memoryState)(nil)

// admissionEntry tracks a previously admitted entry-unit result.
type admissionEntry struct {
	executionID types.ExecutionID
	resultHash  engine.ResultHash
}

// SeedExecutionFromEntry implements engine.EntryAdmissionStore. It
// atomically admits an entry-unit result under the single-process mutex,
// performing all 7 steps of the admission contract in one transition.
func (s *memoryState) SeedExecutionFromEntry(_ context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execID := engine.DeterministicExecutionID(req.AdmissionKey)

	// Step 1: Check admission key occupancy.
	if s.admissions == nil {
		s.admissions = make(map[engine.AdmissionKey]*admissionEntry)
	}
	if existing, ok := s.admissions[req.AdmissionKey]; ok {
		if existing.resultHash == req.ResultHash {
			// Duplicate accepted — same key, same hash.
			return engine.SeedExecutionFromEntryResponse{
				State:       engine.AdmissionStateAccepted,
				ExecutionID: existing.executionID,
				Duplicate:   true,
			}, nil
		}
		// Conflict — same key, different hash.
		return engine.SeedExecutionFromEntryResponse{
			State:       engine.AdmissionStateConflict,
			ExecutionID: existing.executionID,
		}, nil
	}

	// Step 2: Create execution.
	snap := &engine.ExecutionSnapshot{
		ID:     execID,
		Graph:  req.Graph,
		Status: types.ExecutionStatusRunning,
		Params: req.Params,
	}
	s.createExecutionLocked(snap)

	// Step 3: Unit counters were initialized by createExecutionLocked (remaining,
	// failed, in-degree from the graph).

	// Step 4: Mark entry unit as done.
	gKey := groupKey(execID, req.EntryUnitIdx)
	s.groupUnits[gKey] = &groupUnitState{
		status:         groupUnitDone,
		committedToken: "seed-triggered",
	}

	// Step 5: Write boundary outputs.
	for _, ex := range req.Exits {
		s.outputs[string(execID)+"/"+ex.NodeName] = cloneData(ex.Data)
	}

	// Step 6: Completion counting + downstream outbox.
	var result engine.SeedExecutionFromEntryResponse
	result.State = engine.AdmissionStateAccepted
	result.ExecutionID = execID

	if req.Graph != nil && !req.Graph.AllowCycles() {
		s.remaining[execID]--
		if req.Outcome == engine.GroupOutcomeFailed {
			s.failed[execID]++
		}
		fatal := req.Outcome == engine.GroupOutcomeFailed && len(req.Downstream) == 0
		if fatal || s.remaining[execID] == 0 {
			status := types.ExecutionStatusSuccess
			if fatal || s.failed[execID] > 0 {
				status = types.ExecutionStatusFailed
			}
			entry := s.executions[execID]
			s.finishExecutionLocked(execID, entry, status)
		}
	}

	// Step 6b: Write downstream outbox entries.
	if req.Outcome == engine.GroupOutcomeSuccess || len(req.Downstream) > 0 {
		s.applyGroupDownstreamLocked(execID, req.Downstream)
	}

	// Step 7: Store admission entry.
	s.admissions[req.AdmissionKey] = &admissionEntry{
		executionID: execID,
		resultHash:  req.ResultHash,
	}

	return result, nil
}

// admissionOutboxID builds a deterministic outbox ID for an admission's
// downstream intent.
func admissionOutboxID(id types.ExecutionID, name string) string {
	return fmt.Sprintf("admission/%s/%s", id, name)
}

// entrySeedExecOutboxID builds a deterministic ID for the initial execute
// task written by entry-seed admission.
func entrySeedExecOutboxID(id types.ExecutionID, name string) string {
	return fmt.Sprintf("tg-exec/%s/%s", id, name)
}

// putTriggerAdmissionOutbox writes an outbox entry for the entry-seed admission
// downstream. Currently unused — applyGroupDownstreamLocked handles it.
func (s *memoryState) putTriggerAdmissionOutbox(id types.ExecutionID, entry engine.OutboxEntry) {
	s.putOutboxLocked(id, entry.ID, entry.Task, time.Time{})
}
