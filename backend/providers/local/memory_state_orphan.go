package local

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

var _ engine.ConcurrencyOrphanReclaimer = (*memoryState)(nil)

// orphanSweepBatch bounds one maintenance pass when the caller names no limit.
const orphanSweepBatch = 256

// ReclaimConcurrencyOrphans is the in-memory counterpart of rstate's
// implementation. The judgement is the same, and it is atomic for the same
// reason: every fact it reads and every write it makes is guarded by s.mu, so
// no graph write, lease acquisition, or suspension can interleave with it.
//
// The shapes match the Redis backend exactly:
//
//   - a live status (running/queued) whose graph is gone, with no active node or
//     group lease, no suspended node, and an age — the newer of the status
//     transition clock and the node activity clock — older than the grace;
//   - a concurrency member whose execution no longer exists, older than the
//     grace, or when force mode names it explicitly.
//
// Force mode ignores the status transition clock (an explicit Cancel or reset
// has already established that the execution cannot be routed) but still
// refuses an execution whose node activity is inside the grace: a lease or an
// accepted commit is direct evidence that work is running.
func (s *memoryState) ReclaimConcurrencyOrphans(ctx context.Context, req engine.OrphanReclaimRequest) (engine.OrphanReclaimResult, error) {
	mode := req.Mode
	if mode == "" {
		mode = engine.OrphanReclaimSweep
	}
	grace := req.Grace
	if grace <= 0 {
		grace = s.concurrencyOrphanGrace
	}
	if grace <= 0 {
		grace = engine.DefaultOrphanReclaimGrace
	}
	limit := req.Limit
	if limit <= 0 {
		limit = orphanSweepBatch
	}

	var (
		out        engine.OrphanReclaimResult
		projects   []types.ExecutionID
		missing    []types.ExecutionID
		missingIDs []types.ExecutionID
	)
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		now := time.Now().UTC()
		reason := req.Trigger()
		ids := s.orphanCandidatesLocked(req, limit)
		for _, id := range ids {
			if out.Inspected >= limit {
				break
			}
			out.Inspected++
			candidate, reclaimed, project, gone := s.evaluateOrphanLocked(id, req.Namespace, mode, grace, req.DryRun, req.IncludeLive, reason, now)
			if candidate != nil {
				out.Candidates = append(out.Candidates, *candidate)
			}
			if reclaimed {
				out.Reclaimed++
			}
			if project {
				projects = append(projects, id)
			}
			if gone {
				missingIDs = append(missingIDs, id)
			}
		}
	}()

	for _, id := range projects {
		s.mirrorExecutionStatus(ctx, id)
	}
	for _, id := range missingIDs {
		if s.mirrorSkipped(id) {
			continue
		}
		missing = append(missing, id)
	}
	for _, id := range missing {
		s.projectMissingExecutionOrphan(ctx, id)
	}
	s.flushOrphanProjections(ctx)
	return out, nil
}

// orphanCandidatesLocked returns the executions a sweep must judge, in a stable
// order so a bounded pass does not starve the same tail on every run.
func (s *memoryState) orphanCandidatesLocked(req engine.OrphanReclaimRequest, limit int) []types.ExecutionID {
	if req.ExecutionID != "" {
		return []types.ExecutionID{req.ExecutionID}
	}
	seen := make(map[types.ExecutionID]struct{}, len(s.slotHolders)+len(s.queued))
	if req.Workflow != "" {
		// A workflow-scoped pass addresses one slot set directly, exactly as
		// the Redis backend addresses one tag: an execution is a candidate
		// only through the membership that names it.
		key := graph.ConcurrencyKey(string(orphanNamespace(req.Namespace, "")), req.Workflow)
		if slot := s.workflowSlots[key]; slot != nil {
			for id := range slot.holders {
				seen[id] = struct{}{}
			}
			for _, id := range slot.queued {
				seen[id] = struct{}{}
			}
		}
	} else {
		for id := range s.slotHolders {
			seen[id] = struct{}{}
		}
		for id := range s.queued {
			seen[id] = struct{}{}
		}
	}
	ids := make([]types.ExecutionID, 0, len(seen))
	for id := range seen {
		if req.Namespace != "" {
			entry := s.executions[id]
			// A member with no execution left has no recorded namespace; it belongs
			// to whichever pass discovers it, and skipping it would hide exactly the
			// dangling shape this sweep exists to clean. An execution created without
			// a recorded namespace belongs to the default one, matching the
			// namespace the admission keyed its slot with.
			if entry != nil {
				ns := entry.snap.Namespace
				if ns == "" {
					ns = string(namespace.Default)
				}
				if ns != string(req.Namespace) {
					continue
				}
			}
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

// evaluateOrphanLocked is the single judgement. It reports the candidate, the
// applied transition, whether the durable mirror must project the new status,
// and whether the execution key itself was gone (so the projection is the
// direct missing-row write).
func (s *memoryState) evaluateOrphanLocked(id types.ExecutionID, reqNS namespace.Namespace, mode engine.OrphanReclaimMode, grace time.Duration, dryRun, includeLive bool, reason string, now time.Time) (*engine.OrphanReclaimCandidate, bool, bool, bool) {
	entry := s.executions[id]
	_, inSlot := s.slotHolders[id]
	_, inQueue := s.queued[id]
	if entry == nil {
		return s.evaluateMissingOrphanLocked(id, reqNS, inSlot, inQueue, mode, grace, dryRun, reason, now)
	}
	if isTerminalStatus(entry.snap.Status) {
		if inSlot || inQueue {
			s.releaseWorkflowSlotLocked(id)
		}
		return nil, false, false, false
	}
	if entry.snap.Status != types.ExecutionStatusRunning && entry.snap.Status != types.ExecutionStatusQueued {
		return nil, false, false, false
	}
	if entry.snap.Graph != nil && !includeLive {
		return nil, false, false, false
	}
	if !includeLive {
		if s.hasActiveLeaseLocked(id, now) {
			return nil, false, false, false
		}
		if s.hasSuspendedLocked(id) {
			return nil, false, false, false
		}
	}
	if mode != engine.OrphanReclaimForce {
		// The age anchor is the newer of the status transition clock and the node
		// activity clock: a long execution between two leases is not idle just
		// because its status has not moved. An unknown age is not proof of age.
		anchor, known := s.orphanAgeAnchorLocked(id, entry.snap.UpdatedAt)
		if !known || now.Sub(anchor) < grace {
			return nil, false, false, false
		}
	} else if !includeLive {
		// Force skips the status clock, never direct evidence of work: Cancel's
		// self-heal must not converge an execution whose node was leased,
		// renewed, or committed inside the grace. The operator reset lifts this
		// guard with include_live.
		if activity, ok := s.activityAt[id]; ok && now.Sub(activity) < grace {
			return nil, false, false, false
		}
	}
	anchor, _ := s.orphanAgeAnchorLocked(id, entry.snap.UpdatedAt)
	age := time.Duration(0)
	if !anchor.IsZero() {
		age = now.Sub(anchor)
	}
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   orphanNamespace(reqNS, entry.snap.Namespace),
		Workflow:    string(entry.snap.WorkflowID),
		Status:      entry.snap.Status,
		Age:         age,
		Evidence:    engine.OrphanEvidenceGraphMissing,
	}
	if dryRun {
		return candidate, false, false, false
	}
	// An include-live convergence terminates work the operator explicitly
	// named, so its durable reason is the audit label operator_reset rather
	// than the automatic-repair label.
	terminalReason := engine.OrphanReclaimReason
	if includeLive {
		terminalReason = engine.OrphanReclaimReasonOperatorReset
	}
	if !s.updateExecutionStatusLocked(id, types.ExecutionStatusCanceled, terminalReason) {
		return nil, false, false, false
	}
	s.observeOrphanReclaimed(candidate, reason)
	s.logOrphanReclaimed(candidate, reason)
	return candidate, true, true, false
}

// orphanAgeAnchorLocked returns the newest clock the orphan age guard may use:
// the status transition clock or the node activity clock, whichever is later.
// known is false when neither exists, which never qualifies as "old enough".
// Callers must hold s.mu.
func (s *memoryState) orphanAgeAnchorLocked(id types.ExecutionID, updatedAt time.Time) (time.Time, bool) {
	anchor := updatedAt
	if activity := s.activityAt[id]; activity.After(anchor) {
		anchor = activity
	}
	return anchor, !anchor.IsZero()
}

// evaluateMissingOrphanLocked handles a concurrency member whose execution is
// entirely gone. Redis cannot invent a status key for it, and neither can this
// backend invent a snapshot: the membership is removed and the durable mirror
// is written directly.
func (s *memoryState) evaluateMissingOrphanLocked(id types.ExecutionID, reqNS namespace.Namespace, inSlot, inQueue bool, mode engine.OrphanReclaimMode, grace time.Duration, dryRun bool, reason string, now time.Time) (*engine.OrphanReclaimCandidate, bool, bool, bool) {
	if !inSlot && !inQueue {
		return nil, false, false, false
	}
	var (
		age      time.Duration
		ageKnown bool
	)
	if inSlot {
		if at, ok := s.slotChargedAt[id]; ok {
			age = now.Sub(at)
			ageKnown = true
		}
	} else if at, ok := s.queuedAt[id]; ok {
		age = now.Sub(at)
		ageKnown = true
	}
	if mode != engine.OrphanReclaimForce && (!ageKnown || age < grace) {
		return nil, false, false, false
	}
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   orphanNamespace(reqNS, ""),
		Age:         age,
		Evidence:    engine.OrphanEvidenceExecutionMissing,
	}
	if dryRun {
		return candidate, false, false, false
	}
	s.removeMissingMemberLocked(id, inSlot, inQueue)
	s.observeOrphanReclaimed(candidate, reason)
	s.logOrphanReclaimed(candidate, reason)
	return candidate, true, false, true
}

// removeMissingMemberLocked drops one execution from the concurrency state and
// hands any freed slot to the head of the line.
func (s *memoryState) removeMissingMemberLocked(id types.ExecutionID, inSlot, inQueue bool) {
	if inSlot {
		slot := s.slotHolders[id]
		if slot != nil {
			s.unchargeSlotLocked(slot, id)
			s.promoteLocked(slot)
			s.dropIdleSlotLocked(slot)
		}
	}
	if inQueue {
		if waiting, ok := s.queued[id]; ok {
			delete(s.queued, id)
			delete(s.queuedAt, id)
			waiting.slot.removeQueued(id)
			s.dropIdleSlotLocked(waiting.slot)
		}
	}
	s.dropHeldOutboxLocked(id)
}

// dropIdleSlotLocked forgets a workflow whose holders and line are both empty.
func (s *memoryState) dropIdleSlotLocked(slot *workflowSlot) {
	if slot == nil {
		return
	}
	if len(slot.holders) == 0 && len(slot.queued) == 0 {
		delete(s.workflowSlots, slot.key)
	}
}

// hasActiveLeaseLocked reports an unexpired node or group lease for the
// execution. Such work is visibly running and must never be reclaimed, whatever
// the routing state says.
func (s *memoryState) hasActiveLeaseLocked(id types.ExecutionID, now time.Time) bool {
	for _, ns := range s.nodes {
		if ns.ExecutionID != id {
			continue
		}
		switch ns.Status {
		case types.NodeStatusRunning, types.NodeStatusCommitting, types.NodeStatusWaiting:
		default:
			continue
		}
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 {
			continue
		}
		if ns.LeaseIssuedAt.Add(ns.LeaseTTL).After(now) {
			return true
		}
	}
	for _, gs := range s.groupUnits {
		if gs.execID != id || gs.status != groupUnitRunning || gs.leaseToken == "" {
			continue
		}
		if !gs.deadline.IsZero() && gs.deadline.After(now) {
			return true
		}
	}
	return false
}

// hasSuspendedLocked reports a parked node. A suspension is a deliberate wait,
// so a missing graph there is a routing fault to investigate, not work to
// terminate behind the owner's back.
func (s *memoryState) hasSuspendedLocked(id types.ExecutionID) bool {
	prefix := string(id) + "/"
	for key := range s.suspended {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// convergePromotedOrphanLocked terminalizes an execution that promotion found
// graph-lost, without charging it the slot the line had offered. It runs with
// s.mu held, so the mirror projection is deferred to the public entry point
// that triggered the promotion (see flushOrphanProjections).
func (s *memoryState) convergePromotedOrphanLocked(id types.ExecutionID, entry *execEntry) {
	now := time.Now().UTC()
	previous := entry.snap.Status
	anchor, _ := s.orphanAgeAnchorLocked(id, entry.snap.UpdatedAt)
	age := time.Duration(0)
	if !anchor.IsZero() {
		age = now.Sub(anchor)
	}
	entry.snap.Status = types.ExecutionStatusCanceled
	entry.snap.Error = engine.OrphanReclaimReason
	entry.snap.UpdatedAt = now
	delete(s.cancelingSince, id)
	s.withdrawDeadlineLocked(id, types.ExecutionStatusCanceled)
	s.dropHeldOutboxLocked(id)
	if !entry.closed {
		entry.closed = true
		if ch, ok := s.doneCh[id]; ok {
			close(ch)
		}
	}
	if s.promotionOrphans == nil {
		s.promotionOrphans = make(map[types.ExecutionID]struct{})
	}
	s.promotionOrphans[id] = struct{}{}
	s.publishLocked(engine.ExecutionEvent{
		ExecutionID: id,
		Status:      types.ExecutionStatusCanceled,
		Error:       engine.OrphanReclaimReason,
	})
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   orphanNamespace("", entry.snap.Namespace),
		Workflow:    string(entry.snap.WorkflowID),
		Status:      previous,
		Age:         age,
		Evidence:    engine.OrphanEvidenceGraphMissing,
	}
	s.observeOrphanReclaimed(candidate, engine.OrphanTriggerPromotion)
	s.logOrphanReclaimed(candidate, engine.OrphanTriggerPromotion)
}

// reportPromotedMissingOrphanLocked reports a dangling queue member that
// promotion removed because its execution record was already gone. It runs
// with s.mu held, like convergePromotedOrphanLocked, and reports the same
// reason the Redis backend's promoteOrReap does.
func (s *memoryState) reportPromotedMissingOrphanLocked(id types.ExecutionID) {
	candidate := &engine.OrphanReclaimCandidate{
		ExecutionID: id,
		Namespace:   namespace.Default,
		Evidence:    engine.OrphanEvidenceExecutionMissing,
	}
	s.observeOrphanReclaimed(candidate, engine.OrphanTriggerPromotion)
	s.logOrphanReclaimed(candidate, engine.OrphanTriggerPromotion)
}

// flushOrphanProjections mirrors the statuses promotion converged while s.mu was
// held. It must be called without the lock.
func (s *memoryState) flushOrphanProjections(ctx context.Context) {
	s.mu.Lock()
	if len(s.promotionOrphans) == 0 {
		s.mu.Unlock()
		return
	}
	ids := make([]types.ExecutionID, 0, len(s.promotionOrphans))
	for id := range s.promotionOrphans {
		ids = append(ids, id)
		delete(s.promotionOrphans, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.mirrorExecutionStatus(ctx, id)
	}
}

// mirrorSkipped reports whether the durable mirror must stay untouched for one
// execution (a transient workflow or a seed-ID collision).
func (s *memoryState) mirrorSkipped(id types.ExecutionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mirrorSkip[id] || s.rolledBack[id] || s.mirrorConflict[id]
}

// projectMissingExecutionOrphan writes the canceled status for an execution
// whose in-memory record is already gone. It is the direct counterpart of
// rstate.projectMissingExecutionOrphan and closes the same drift blind spot:
// the reconciler skips a missing row as not_found.
func (s *memoryState) projectMissingExecutionOrphan(ctx context.Context, id types.ExecutionID) {
	if s.db == nil {
		return
	}
	wctx, cancel := mirrorCtx(ctx)
	defer cancel()
	err := s.db.UpdateExecutionStatus(wctx, id, types.ExecutionStatusCanceled, engine.OrphanReclaimReason)
	if err != nil && !errors.Is(err, store.ErrNotFound) && s.logger != nil {
		s.logger.Error("orphan_execution_sql_projection_failed", "execution_id", string(id), "err", err)
	}
}

// logOrphanReclaimed emits the same structured line the Redis backend does:
// enough to tell a deliberate repair from a vanished execution.
func (s *memoryState) logOrphanReclaimed(candidate *engine.OrphanReclaimCandidate, reason string) {
	if s.logger == nil || candidate == nil {
		return
	}
	s.logger.Info("execution_orphan_reclaimed",
		"namespace", string(candidate.Namespace),
		"execution_id", string(candidate.ExecutionID),
		"workflow", candidate.Workflow,
		"original_status", string(candidate.Status),
		"age_ms", candidate.Age.Milliseconds(),
		"evidence", candidate.Evidence,
		"reason", reason,
	)
}

// observeOrphanReclaimed reports one applied convergence to the optional
// observer. A dry run reports candidates in the result and counts nothing.
func (s *memoryState) observeOrphanReclaimed(candidate *engine.OrphanReclaimCandidate, reason string) {
	if s.orphanObserver == nil || candidate == nil {
		return
	}
	defer func() { _ = recover() }()
	s.orphanObserver.OnConcurrencyOrphanReclaimed(
		namespace.WithNamespace(context.Background(), candidate.Namespace),
		candidate.Kind(), reason,
	)
}

// orphanNamespace prefers an explicit request namespace, then the execution's
// recorded one, then the platform default.
func orphanNamespace(request namespace.Namespace, recorded string) namespace.Namespace {
	if request != "" {
		return request
	}
	if recorded != "" {
		return namespace.Namespace(recorded)
	}
	return namespace.Default
}
