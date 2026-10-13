package local

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.ExecutionSeeder = (*memoryState)(nil)

// seedScheduledMarker is what the scheduling marker says for a unit a seed
// resolved. It occupies the same slot an execute/skip decision does, because
// that is what it is: the decision for this unit is made and nothing may make
// it again. A third value rather than an invented one — "skip" would claim the
// unit was skipped and "execute" that it is due.
const seedScheduledMarker = "seed"

// SeedExecution implements engine.ExecutionSeeder: an execution created around
// units whose outcome is already decided, admitted and scheduled in one
// critical section.
//
// It is the submission path plus a recorded prefix. The execution is created
// with the same helpers a submission uses (chargeOrParkLocked,
// createExecutionLocked, putOutboxEntryLocked) so a seeded execution's
// concurrency accounting, counters and intents are not a parallel
// implementation that can drift from them.
//
// Every resolved unit is written as terminal AND marked resolved before any
// arrival is applied. The order is load-bearing: an arrival from one resolved
// unit reaches another resolved unit's counters, and the marker is what stops
// that arrival from scheduling work whose result the new execution was built
// on. Marking as we go would let the first unit's arrivals schedule the second.
func (s *memoryState) SeedExecution(_ context.Context, req engine.SeedExecutionRequest) (engine.SeedExecutionResponse, error) {
	if req.Execution == nil {
		return engine.SeedExecutionResponse{}, errors.New("seed execution: no execution snapshot")
	}
	for _, unit := range req.Units {
		if unit.Status != types.NodeStatusSuccess && unit.Status != types.NodeStatusSkipped {
			return engine.SeedExecutionResponse{}, fmt.Errorf(
				"seed execution %q: unit %q has status %q, want %q or %q",
				req.Execution.ID, unit.NodeName, unit.Status,
				types.NodeStatusSuccess, types.NodeStatusSkipped)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := req.Execution.ID

	// Step 1: admission. A refusal leaves nothing behind — no execution, no
	// counters, no intents — so a caller whose seed was refused still owns the
	// work it was going to seed.
	pending := false
	if req.Admission != nil {
		var err error
		pending, err = s.chargeOrParkLocked(id, *req.Admission)
		if err != nil {
			return engine.SeedExecutionResponse{}, err
		}
	}

	// Step 2: create. A parked admission is created queued in the same critical
	// section that took its place in line, so it is never observable running
	// without a slot.
	admitted := *req.Execution
	admitted.Status = types.ExecutionStatusRunning
	if pending {
		admitted.Status = types.ExecutionStatusQueued
	}
	s.createExecutionLocked(&admitted)

	// Step 3: mark every resolved unit, before applying a single arrival.
	for _, unit := range req.Units {
		key := memoryNodeKey(id, unit.NodeName)
		privateOutput := s.preserveOutputPrivacyLocked(key, unit.PrivateOutput)
		node := &engine.NodeSnapshot{
			ExecutionID:   id,
			Name:          unit.NodeName,
			NodeIdx:       unit.NodeIdx,
			UnitIdx:       unit.UnitIdx,
			Status:        unit.Status,
			Port:          unit.Port,
			PrivateOutput: privateOutput,
		}
		if !privateOutput {
			node.Output = cloneData(unit.Output)
		}
		s.nodes[key] = node
		// The stored output is written even for a private unit: privacy gates what
		// the public projections show (the marker does that), not what a
		// downstream expression resolves. A seed is not the place to re-decide
		// that — it copies what the parent execution had.
		s.outputs[key] = cloneData(unit.Output)
		s.scheduled[memoryCounterKey(id, unit.UnitIdx)] = seedScheduledMarker
	}

	// Step 4: count the resolutions. Every seeded unit is resolved, which is what
	// makes "remaining" the number of units this execution still has to run.
	if req.Execution.Graph != nil && !req.Execution.Graph.AllowCycles() {
		s.remaining[id] -= len(req.Units)
	}

	// Step 5: apply the arrivals, which schedules whatever the resolutions made
	// ready. This is the same transition a commit's fan-in runs, and it is what
	// decides execute vs skip for each successor.
	var result engine.SeedExecutionResponse
	for _, unit := range req.Units {
		_, skipped := s.applyGroupDownstreamLocked(id, unit.Downstream)
		result.Skipped = append(result.Skipped, skipped...)
	}

	// Step 6: the caller's own entries, for units no arrival can reach.
	s.writeInitialIntentsLocked(id, req.Entries, time.Time{})

	// Step 7: an execution with nothing left to run completes here rather than
	// sitting at zero remaining with nothing queued.
	if req.Execution.Graph != nil && !req.Execution.Graph.AllowCycles() {
		if entry := s.executions[id]; entry != nil && s.remaining[id] <= 0 {
			status := types.ExecutionStatusSuccess
			if s.failed[id] > 0 {
				status = types.ExecutionStatusFailed
			}
			s.finishExecutionLocked(id, entry, status, "")
		}
	}

	// Step 8: hold the ready work of a parked admission. The intents were written
	// the way a running execution's are, so holding them is one pass rather than a
	// flag threaded through every writer.
	if entry := s.executions[id]; entry != nil && entry.snap.Status == types.ExecutionStatusQueued {
		result.QueuePosition = s.queuePositionLocked(id)
		result.Queued = result.QueuePosition > 0
		if result.Queued {
			s.holdOutboxLocked(id)
		}
	}

	return result, nil
}
