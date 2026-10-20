package local

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var _ engine.EntryAdmissionStore = (*memoryState)(nil)
var _ engine.ConcurrencyLimitedEntryAdmissionStore = (*memoryState)(nil)

// admissionEntry tracks a previously admitted entry-unit result.
type admissionEntry struct {
	executionID types.ExecutionID
	resultHash  engine.ResultHash
}

// SeedExecutionFromEntry implements engine.EntryAdmissionStore. It
// atomically admits an entry-unit result under the single-process mutex,
// performing all 7 steps of the admission contract in one transition.
func (s *memoryState) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	return s.seedExecutionFromEntry(ctx, req, nil)
}

// SeedExecutionFromEntryWithConcurrency implements
// engine.ConcurrencyLimitedEntryAdmissionStore: the same admission, judged
// against the seeded graph's concurrency policy before anything is written.
func (s *memoryState) SeedExecutionFromEntryWithConcurrency(ctx context.Context, req engine.SeedExecutionFromEntryRequest, admission engine.ConcurrencyAdmission) (engine.SeedExecutionFromEntryResponse, error) {
	return s.seedExecutionFromEntry(ctx, req, &admission)
}

// seedExecutionFromEntry is the admission body both entry points share. Going
// through one body is what keeps a seeded execution indistinguishable from a
// submitted one: the seed writes its execution, its counters and its intents
// with the same helpers a submission does, and an admission that lands in the
// queue leaves the same state behind — status queued, intents held, a place in
// line — because it is the same decision (chargeOrParkLocked) that put it
// there.
//
// admission is nil when the seeded graph declares no concurrency limit, which
// is the plain admission: no slot is consulted and the execution starts
// running.
func (s *memoryState) seedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest, admission *engine.ConcurrencyAdmission) (engine.SeedExecutionFromEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	// The admission's namespace is the registration namespace dispatch resolves
	// sub-workflows with: the request's when supplied, the ctx otherwise — never
	// the queue ctx dispatch will later run under.
	registrationNamespace := req.Namespace
	if registrationNamespace == "" {
		registrationNamespace = namespace.FromContext(ctx)
	}

	// Step 1: Check admission key occupancy. This runs before the slot decision
	// because it must be the first thing a retry meets: a redelivered batch
	// neither charges a second slot nor loses the place in line its first
	// attempt took.
	if s.admissions == nil {
		s.admissions = make(map[engine.AdmissionKey]*admissionEntry)
	}
	if existing, ok := s.admissions[req.AdmissionKey]; ok {
		position := s.queuePositionLocked(existing.executionID)
		if existing.resultHash == req.ResultHash {
			// Duplicate accepted — same key, same hash.
			return engine.SeedExecutionFromEntryResponse{
				State:         engine.AdmissionStateAccepted,
				ExecutionID:   existing.executionID,
				Duplicate:     true,
				Queued:        position > 0,
				QueuePosition: position,
			}, nil
		}
		// Conflict — same key, different hash.
		return engine.SeedExecutionFromEntryResponse{
			State:         engine.AdmissionStateConflict,
			ExecutionID:   existing.executionID,
			Queued:        position > 0,
			QueuePosition: position,
		}, nil
	}

	// Step 2a: Slot decision. A refusal writes nothing at all — not the
	// execution, not the admission — so the caller still owns the entry-unit
	// result and can retry it once a slot frees or the backlog drains. That the
	// refusal is a response state rather than an error is exactly what the
	// caller needs: it is a decision the caller acts on, not a failed call.
	pending := false
	if admission != nil {
		var err error
		pending, err = s.chargeOrParkLocked(execID, *admission)
		if err != nil {
			return engine.SeedAdmissionRefusal(err)
		}
	}

	// Step 2b: Create execution. A parked one is created queued in the same
	// transition that took its place in line, so it is never observable running
	// without a slot.
	status := types.ExecutionStatusRunning
	if pending {
		status = types.ExecutionStatusQueued
	}
	snap := &engine.ExecutionSnapshot{
		ID:     execID,
		Graph:  req.Graph,
		Status: status,
		Params: req.Params,
		// The admitted entry unit carries the workflow identity the control
		// plane derived the admission key from. Persisting it here is what lets
		// Inspect/GetExecution name the workflow a trigger-created execution
		// ran, exactly as the submission path does.
		WorkflowID:      req.WorkflowID,
		WorkflowVersion: req.WorkflowVersion,
		Namespace:       string(registrationNamespace),
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

	// Step 5: Write boundary outputs. Private exits remain in runtime output
	// storage for downstream execution, but their monotonic marker excludes
	// them from public aggregation and WaitDone.
	for _, ex := range req.Exits {
		key := memoryNodeKey(execID, ex.NodeName)
		s.preserveOutputPrivacyLocked(key, ex.PrivateOutput)
		s.outputs[key] = cloneData(ex.Data)
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
			s.finishExecutionLocked(execID, entry, status,
				engine.TerminalExecutionError(status, req.Error, ""))
		}
	}

	// Step 6b: Write downstream outbox entries.
	if req.Outcome == engine.GroupOutcomeSuccess || len(req.Downstream) > 0 {
		_, skipped := s.applyGroupDownstreamLocked(execID, req.Downstream)
		// Reported only on this path: the duplicate and conflict returns above
		// applied no transition, so they skipped nothing.
		result.Skipped = skipped
	}

	// Step 6c: Resolve the roots this admission did not seed, so they neither
	// hold remaining open nor block a fan-in shared with the seeded entry.
	if entry := s.executions[execID]; entry != nil && !types.IsTerminalExecutionStatus(entry.snap.Status) {
		for _, skip := range engine.UnselectedRootSkips(execID, req.Graph, req.EntryUnitIdx) {
			counterKey := memoryCounterKey(execID, skip.UnitIdx)
			if s.scheduled[counterKey] != "" {
				continue
			}
			s.scheduled[counterKey] = "skip"
			s.putOutboxLocked(execID, skipOutboxID(execID, skip.NodeName, 0), skip, time.Time{})
		}
	}

	// Step 6d: Park what a queued admission just wrote. The intents above were
	// written the way a running execution's are — they are the same intents,
	// from the same helpers — so holding them is one pass over them rather than
	// a hold parameter threaded through every writer.
	//
	// The position is read here, after the completion counting, because that is
	// what decides whether the execution is still waiting: an admission that
	// went straight to a terminal status (a fatal entry outcome with nothing
	// downstream) left the line in step 6, and holding its intents at that point
	// would leave them parked on an execution no release will ever mention.
	result.QueuePosition = s.queuePositionLocked(execID)
	result.Queued = result.QueuePosition > 0
	if result.Queued {
		s.holdOutboxLocked(execID)
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
