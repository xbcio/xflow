package control

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// defaultDeadQueuedAssignmentReapBatch bounds one reaper pass. It caps both how
// many assignments are removed and, through the scan and inspect caps below,
// how much of the shared Redis one call reads.
//
// Much larger than the stranded-lease reaper's batch on purpose: that one drains a
// finite residue left by crashed runners, while this one drains a continuous
// arrival — every execution that reaches its transient TTL with one of its tasks
// still queued leaves another entry behind. The pass has to exceed that arrival
// rate for the queue to converge rather than keep growing.
//
// The size is set from the measured arrival, not from the older estimate: one
// deployment this exists for measured ~42/min and 512 per pass was enough, while
// the deployment that forced this value grows its queue by hundreds per minute —
// the runner's own claim path could not keep up, so the queue reached 12k entries
// with a live tail a runner never reached. 4096 per pass at the sweeper's
// five-minute cadence is ~819/min, which clears that arrival with headroom.
//
// A batch this size is only affordable because the liveness probe below answers it
// in one round trip. Asked per candidate, 4096 candidates were 4096 sequential
// reads on a link whose round trip is tens of milliseconds — the pass would have
// spent minutes learning that the entries were already dead, which is the same
// cost that capped the older value.
const defaultDeadQueuedAssignmentReapBatch = 4096

// redisReapPipelineBatch bounds the removal transitions that one pipeline
// carries. A batch-sized pass hands the directory thousands of candidates,
// and every removal is still its own atomic Lua transition — the script below
// is unchanged — but issuing the transitions one round trip at a time made
// the pass cost thousands of link round trips with the server idle between
// them, which on a link of tens of milliseconds pinned a pass at tens of
// removals per second. Pipelining in batches of this size keeps the link busy,
// while one batch stays small enough that its server time does not become a
// stall on shared Redis.
const redisReapPipelineBatch = 128

// DeadQueuedAssignmentReaper is the durable-directory capability that reclaims
// assignments left in 'queued' state after the execution they belong to has
// gone away. Like ClaimReclaimer, StrandedLeaseReaper and the other optional
// capabilities it is discovered by type assertion, so the in-memory directory
// and any other RunnerDirectory implementation are unaffected.
type DeadQueuedAssignmentReaper interface {
	// ReapDeadQueuedAssignments removes up to limit assignments that can never
	// be claimed again, and reports how many candidates it inspected and how
	// many of them it removed. It is idempotent, safe to call concurrently from
	// several replicas, and safe to run against a live directory: it never
	// touches an assignment that is claimable or currently leased.
	//
	// Inspected counts the candidates of the two shapes this pass drains that
	// the pass read: assignment-state entries whose recorded state is 'queued',
	// and lane marker fields whose assignment payload is already gone. Both are
	// deliberately narrower than the structures they scan: an entry in any
	// other state, or a marker with a payload behind it, is not a candidate,
	// and counting it would make the ratio against the removed count a measure
	// of how full the directory is rather than of how much of its candidate set
	// the pass releases. Most inspected state entries are expected NOT to be
	// removed on a healthy fleet — the pass walks the head of a live queue and
	// correctly leaves claimable work alone — so a high inspected/released
	// ratio is only a defect signal when the queue is not converging.
	ReapDeadQueuedAssignments(ctx context.Context, limit int) (ReapResult, error)
}

var _ DeadQueuedAssignmentReaper = (*RedisRunnerDirectory)(nil)

// queuedReapCursor is the reaper's resume position within the assignment-state
// hash, so one call reads a bounded part of it and the next call continues
// where that one stopped. Like claimCursors it is process-local scheduling
// state rather than authority: losing it (a restart, an eviction, a leadership
// change) only means the next lap starts at the head of the hash, which is
// harmless because every pass is idempotent.
type queuedReapCursor struct {
	mu     sync.Mutex
	cursor uint64
}

func (c *queuedReapCursor) load() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

func (c *queuedReapCursor) store(cursor uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = cursor
}

// ReapDeadQueuedAssignments reclaims 'queued' assignments whose execution can
// no longer be leased, then removes the whole record atomically.
//
// # How this decides an execution is gone
//
// It asks the engine's own question, through the engine's own primitive:
// engine.ExecutionStatusReader.GetExecutionStatus, which answers "active" when
// the execution's status key exists and is not terminal. That is deliberately
// not a new signal and not a new key lookup: it is exactly the predicate
// BuildTaskLease applies before it will mint a lease — loadActiveGraph asks the
// same reader the same question (engine/engine.go executionActive) — so this
// reaper removes exactly the assignments the engine would refuse to lease. A
// reaper with its own notion of "dead" could remove an assignment the engine
// was still willing to run, and that loses work.
//
// Both halves of the predicate are load-bearing:
//
//   - Status absent: the execution's transient keys have expired. Redis state
//     is the system of record (docs/design/STORAGE-CONTRACT.md) and those keys
//     carry the transient TTL, so their absence is the deletion, not a cache
//     miss. This is the 90% shape measured on the shared deployment.
//   - Status terminal: success/failed/canceled/timeout. No lease can be built
//     on a terminal execution either, so the queued assignment is immortal
//     garbage even though its execution was never deleted.
//
// The probe is issued under the assignment's own namespace, which is the value
// the poll path already injects before BuildTaskLease (service/control/core.go
// pollTask). The lease write path and this reaper therefore resolve the same
// key for the same assignment; they cannot disagree about a live execution
// unless they disagree about something that already breaks claiming.
//
// # Why the existing reclaim paths cannot see it
//
//   - ReclaimExpiredClaims walks claims; it only ever sees 'claimed'.
//   - The LeaseSweeper enumerates the execution's own lease index, which lives
//     under the same transient keys that have already expired.
//   - StrandedLeaseReaper covers 'leased' assignments whose lease metadata is
//     gone: a capacity leak, not a queue leak.
//   - The poll path does resolve this shape, but only for an assignment a live
//     runner actually claims, one entry per poll iteration, and only while that
//     runner keeps polling. A queue that is 90% dead is exactly what that leaves
//     behind once runners stop draining.
//
// # Safety
//
// Two fences, one per side of the race. The candidate is only offered here when
// its recorded state is 'queued', and the removal itself re-checks that state
// inside the Lua transition (see redisReapDeadQueuedAssignmentLua). The claim
// transition requires the same state and flips it to 'claimed' atomically, so
// either the claim ran first and this script skips, or this script ran first and
// the claim fails its own state and payload checks without having removed
// anything. A leased assignment is never a candidate at all: it left 'queued'
// when it was claimed.
//
// # Cost
//
// Candidates come from an HScan over assignment:state, which is the
// authoritative index of every assignment and does not share the queue's
// positional fragility. One page's payloads are read with a single HMGet, and
// each candidate that survives the filter costs one status read plus one
// bounded transition.
//
// # The second candidate source: orphaned lane markers
//
// This pass runs two laps under one removal budget, because the residue a lane
// rollout leaves behind comes in two shapes and only one of them is a 'queued'
// record. The first lap is the state-hash walk described above. The second
// walks the lane marker hash (assignment:lane) for fields whose assignment
// payload is already gone: the record they mark no longer exists, so the marker
// and every queue copy of it are residue. That shape is what a completion by an
// older control plane leaves (it knows neither lanes nor markers, so it clears
// the legacy copy and the record, and leaves the lane copy and its marker), and
// it is also the safety net for any marker a removal path ever fails to drop.
//
// The second lap needs no liveness probe — a missing payload is the proof, and
// the guard transition re-checks it atomically — so a marker whose payload is
// present is never touched, however stale its placement looks. It runs even
// with no lanes configured, which is what lets a rollback drain the markers and
// lane copies written during a previous lane window.
//
// The two laps share one cursor family but not a position: each hash has its
// own resume point, and each lap spends what is left of the removal budget
// after the one before it.
func (d *RedisRunnerDirectory) ReapDeadQueuedAssignments(ctx context.Context, limit int) (ReapResult, error) {
	if limit <= 0 || d.executions == nil {
		// Without a status reader the directory cannot tell a live assignment
		// from a dead one, so it reclaims nothing rather than guessing from key
		// names or from age. The marker lap is gated with it: this is one pass
		// with one contract, and a deployment that cannot prove a queued
		// assignment dead does not get half of the reaper.
		return ReapResult{}, nil
	}
	result, err := d.reapDeadQueuedStateLap(ctx, limit)
	if err != nil {
		return result, err
	}
	if remaining := limit - result.Released; remaining > 0 {
		markerResult, markerErr := d.reapOrphanedLaneMarkersLap(ctx, remaining)
		result.Inspected += markerResult.Inspected
		result.Released += markerResult.Released
		if markerErr != nil {
			return result, markerErr
		}
	}
	return result, nil
}

// reapDeadQueuedStateLap is the state-hash lap of ReapDeadQueuedAssignments:
// one bounded walk over assignment:state, removing the assignments recorded
// 'queued' whose execution can no longer be leased.
func (d *RedisRunnerDirectory) reapDeadQueuedStateLap(ctx context.Context, limit int) (ReapResult, error) {
	batch := defaultDeadQueuedAssignmentReapBatch
	if limit < batch {
		batch = limit
	}
	// The removal limit says nothing about how many entries a caller must look
	// at to find that many dead ones — a mostly-live hash costs reads without
	// producing removals — so one call also bounds itself and resumes on the
	// next cadence. The page budget is the second half of that bound: a scan
	// cursor that kept returning empty pages would otherwise keep this pass
	// turning without ever adding to the scanned count.
	inspectCap := 4 * limit
	maxPages := inspectCap/batch + 1

	cursor := d.queuedReap.load()
	scanned := 0
	var result ReapResult
	for pages := 0; ; pages++ {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.assignmentState, cursor, "", int64(batch)).Result()
		if err != nil {
			d.queuedReap.store(cursor)
			return result, fmt.Errorf("reap dead queued assignments: scan assignment states: %w", err)
		}
		pageReclaimed, pageInspected, err := d.reapDeadQueuedAssignmentPage(ctx, pairs, limit-result.Released)
		result.Released += pageReclaimed
		result.Inspected += pageInspected
		scanned += len(pairs) / 2
		if err != nil {
			// Resume at this page: re-inspecting it is harmless because every
			// step of the pass is idempotent, and it is the only way a page that
			// failed part-way still gets fully covered.
			d.queuedReap.store(cursor)
			return result, err
		}
		if result.Released >= limit {
			// Stopped part-way through the page by the removal limit. Hold the
			// page head so the candidates this call did not reach are
			// re-inspected rather than skipped. Each call still removes up to
			// limit entries, so a page cannot pin the cursor forever.
			d.queuedReap.store(cursor)
			return result, nil
		}
		cursor = next
		if cursor == 0 || scanned >= inspectCap || pages+1 >= maxPages {
			// A completed lap restarts at the head. The hash has no meaningful
			// order to preserve the way the queue list does, so a lap boundary
			// is simply where coverage is known to have wrapped.
			d.queuedReap.store(cursor)
			return result, nil
		}
	}
}

// reapOrphanedLaneMarkersLap is the second lap of ReapDeadQueuedAssignments:
// one bounded walk over the lane marker hash, removing the marker fields whose
// assignment payload is gone and the queue copies they name.
//
// It answers a different question from the state lap, and needs a different
// source to answer it. An old control plane's completion path leaves no
// assignment:state field behind — it deletes the record's own hash fields, so
// the state hash cannot see the residue at all. The marker is the only index of
// "this assignment was placed on a lane", and therefore the only structure that
// can enumerate what the record's removal failed to collect.
//
// The lap is deliberately blind to every record whose payload is still present:
// a marker with a live record behind it is correct whatever it says, including
// the placement a relabeled lane configuration left behind, and reconciling it
// would be a guess about a record that is still someone's work. Only absence is
// proof, and the guard transition re-checks it atomically (see
// redisReconcileLaneMarkerLua).
//
// One page's payload presence is read in a single HMGet, and only the fields
// that fail it cost a transition — the same shape as the state lap, for the
// same reason: on a live deployment most markers are backed by a live record,
// and a per-candidate read would spend the pass's whole cost on the ones that
// are not candidates at all.
func (d *RedisRunnerDirectory) reapOrphanedLaneMarkersLap(ctx context.Context, limit int) (ReapResult, error) {
	batch := defaultDeadQueuedAssignmentReapBatch
	if limit < batch {
		batch = limit
	}
	// Bounded exactly like the state lap: a marker hash that keeps returning
	// live records must not keep this lap turning without ever reaching a
	// removal, and the resume point must survive a page that failed part-way.
	inspectCap := 4 * limit
	maxPages := inspectCap/batch + 1

	cursor := d.laneMarkerReap.load()
	scanned := 0
	var result ReapResult
	for pages := 0; ; pages++ {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.assignmentLane, cursor, "", int64(batch)).Result()
		if err != nil {
			d.laneMarkerReap.store(cursor)
			return result, fmt.Errorf("reap orphaned lane markers: scan lane markers: %w", err)
		}
		pageReclaimed, pageInspected, err := d.reapOrphanedLaneMarkerPage(ctx, pairs, limit-result.Released)
		result.Released += pageReclaimed
		result.Inspected += pageInspected
		scanned += len(pairs) / 2
		if err != nil {
			d.laneMarkerReap.store(cursor)
			return result, err
		}
		if result.Released >= limit {
			// Hold the page head so the markers this call did not reach are
			// re-inspected rather than skipped; each call still removes up to
			// limit, so a page cannot pin the cursor forever.
			d.laneMarkerReap.store(cursor)
			return result, nil
		}
		cursor = next
		if cursor == 0 || scanned >= inspectCap || pages+1 >= maxPages {
			d.laneMarkerReap.store(cursor)
			return result, nil
		}
	}
}

// reapOrphanedLaneMarkerPage reconciles the orphaned markers of one scan page.
// It reads the page's payloads in one call and hands the ids that have none to
// the guard transitions; inspected counts those ids, which is the shape this
// lap drains and the same rule the state lap's counter follows. A marker whose
// payload is present is not counted — it is not a candidate — which is what
// keeps the pass's inspected/released ratio a measure of residue rather than of
// directory size.
func (d *RedisRunnerDirectory) reapOrphanedLaneMarkerPage(ctx context.Context, pairs []string, limit int) (reclaimed, inspected int, err error) {
	if limit <= 0 {
		return 0, 0, nil
	}
	ids := make([]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		ids = append(ids, pairs[i])
	}
	if len(ids) == 0 {
		return 0, 0, nil
	}
	raws, err := d.rdb.HMGet(ctx, d.keys.assignmentData, ids...).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("reap orphaned lane markers: read assignment payloads: %w", err)
	}
	orphans := make([]string, 0, len(ids))
	for i, assignmentID := range ids {
		raw, _ := raws[i].(string)
		if raw != "" {
			continue
		}
		orphans = append(orphans, assignmentID)
	}
	// Sorted for the same reason the other reap laps sort: a bounded pass
	// should pick the same entries every call rather than depend on hash
	// iteration order, so a backlog drains deterministically instead of
	// starving whichever entry keeps losing the race to the limit.
	sort.Strings(orphans)
	inspected = len(orphans)
	if len(orphans) > limit {
		orphans = orphans[:limit]
	}
	reclaimed, err = d.reconcileOrphanedLaneMarkers(ctx, orphans)
	return reclaimed, inspected, err
}

// reconcileOrphanedLaneMarkers issues the guard transitions for one batch of
// orphaned markers. Like the other reaper batches, the transitions travel
// pipelined while each one keeps its own fence, and a command error is
// attributed to its candidate rather than to the batch.
func (d *RedisRunnerDirectory) reconcileOrphanedLaneMarkers(ctx context.Context, assignmentIDs []string) (int, error) {
	if len(assignmentIDs) == 0 {
		return 0, nil
	}
	pipe := d.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(assignmentIDs))
	for _, assignmentID := range assignmentIDs {
		cmds = append(cmds, pipe.Eval(ctx, redisReconcileLaneMarkerLua, []string{
			d.keys.assignmentLane,
			d.keys.assignmentData,
			d.keys.queue,
			d.keys.seen,
		}, assignmentID))
	}
	// Every command has settled by the time Exec returns, each carrying its own
	// error, so the results are read per command below rather than from the
	// pipeline-level error.
	_, _ = pipe.Exec(ctx)
	reconciled := 0
	for i, cmd := range cmds {
		status, err := cmd.Text()
		if err != nil {
			return reconciled, fmt.Errorf("reconcile lane marker %q: %w", assignmentIDs[i], err)
		}
		switch status {
		case "reconciled":
			reconciled++
		case "skipped":
		default:
			return reconciled, fmt.Errorf("reconcile lane marker %q: unexpected result %q", assignmentIDs[i], status)
		}
	}
	return reconciled, nil
}

// reapDeadQueuedAssignmentPage removes the dead queued assignments of one scan
// page and reports how many candidates of the shape that page carried. It is the
// whole per-candidate pipeline: filter by recorded state, read the payloads in
// one call, ask the engine whether each execution is still leaseable, and remove
// only the ones it is not.
func (d *RedisRunnerDirectory) reapDeadQueuedAssignmentPage(ctx context.Context, pairs []string, limit int) (reclaimed, inspected int, err error) {
	if limit <= 0 {
		return 0, 0, nil
	}
	candidates := make([]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == redisAssignmentQueued {
			candidates = append(candidates, pairs[i])
		}
	}
	if len(candidates) == 0 {
		return 0, 0, nil
	}
	// Counted on discovery, before the payload read and before the activeness
	// probe: these entries are the shape this pass drains, and the ones it then
	// decides not to remove — because their execution is still leaseable, or
	// because their payload is missing — are exactly the candidates the ratio
	// against the removal count is there to show.
	inspected = len(candidates)
	// Sorted for the same reason the stranded reaper sorts its candidates: a
	// bounded pass should pick the same entries every call rather than depend on
	// hash iteration order, so a backlog drains deterministically instead of
	// starving whichever entry keeps losing the race to the limit.
	sort.Strings(candidates)

	raws, err := d.rdb.HMGet(ctx, d.keys.assignmentData, candidates...).Result()
	if err != nil {
		return 0, inspected, fmt.Errorf("reap dead queued assignments: read assignment payloads: %w", err)
	}
	// Decode the page and answer the liveness question once for all of it. The
	// per-candidate probe this replaces issued one state-store read per candidate,
	// so a 512-candidate pass cost 512 round trips — which, on a link whose round
	// trip is tens of milliseconds, is the whole cost of the pass and is spent
	// almost entirely on entries that are dead.
	type reaperCandidate struct {
		assignmentID string
		assignment   Assignment
	}
	parsed := make([]reaperCandidate, 0, len(candidates))
	page := make([]Assignment, 0, len(candidates))
	for i, assignmentID := range candidates {
		raw, _ := raws[i].(string)
		if raw == "" {
			// A queued record with no payload cannot be claimed either, but it
			// names no execution, so this reaper cannot prove it dead. Leaving
			// it is the safe direction: a wrong removal here loses work, while
			// leaving it loses nothing that was not already lost.
			continue
		}
		assignment, err := unmarshalRedisAssignment(raw)
		if err != nil {
			// Same reasoning: an undecodable payload is a record this reaper
			// cannot classify. One bad entry must not cost the pass its other
			// candidates, and it must not become a removal.
			continue
		}
		parsed = append(parsed, reaperCandidate{assignmentID: assignmentID, assignment: assignment})
		page = append(page, assignment)
	}
	leaseable, err := d.leaseableExecutions(ctx, page)
	if err != nil {
		return reclaimed, inspected, err
	}

	// The dead candidates are collected first, then removed in pipelined
	// batches. Every removal is still exactly one atomic transition — the
	// safety argument above is untouched — but issuing them one round trip at a
	// time made the pass cost thousands of link latencies with the server idle
	// between them; on a batch-sized pass that was the whole wall time. The
	// limit caps the transitions one pass attempts, taken in the page's sorted
	// order so a bounded pass still picks the same entries every call.
	dead := make([]string, 0, len(parsed))
	for _, c := range parsed {
		if leaseable[c.assignment.Task.ExecutionID] {
			continue
		}
		if len(dead) >= limit {
			break
		}
		dead = append(dead, c.assignmentID)
	}
	reclaimed = 0
	for start := 0; start < len(dead); start += redisReapPipelineBatch {
		end := start + redisReapPipelineBatch
		if end > len(dead) {
			end = len(dead)
		}
		reaped, err := d.reapQueuedAssignments(ctx, dead[start:end])
		reclaimed += reaped
		if err != nil {
			return reclaimed, inspected, err
		}
	}
	return reclaimed, inspected, nil
}

// leaseableExecutions answers, for a whole page of assignments, which of their
// executions can still be run.
//
// The predicate is the engine's own activeness one — present and non-terminal — and
// the namespace comes from each assignment, the same value the poll path resolves
// the lease through, so both paths read the same execution key.
//
// It answers the question for the page at once because both callers need it that
// way: the claim path and the dead-queued reaper each hold a page of candidates of
// which all but a few are expected to be dead. Asked one at a time, a 64-entry page cost 64 sequential
// round trips and a 512-candidate reaper pass cost 512, which is the whole cost of
// either operation and is spent almost entirely on entries that are already gone.
//
// Grouped by namespace because the status key is namespaced, and the batch
// capability is optional: a reader that does not implement it is probed one
// execution at a time, which is what every caller did before this existed.
func (d *RedisRunnerDirectory) leaseableExecutions(ctx context.Context, assignments []Assignment) (map[types.ExecutionID]bool, error) {
	live := make(map[types.ExecutionID]bool, len(assignments))
	if d.executions == nil {
		// Without a status reader there is nothing to prove an assignment dead
		// with, so every one of them stays a candidate. Reporting them all live is
		// the safe direction on both paths: the claim path simply behaves as it did
		// before this probe existed, and the reaper — which is the only caller that
		// removes anything — already declines to run at all in this configuration.
		for _, a := range assignments {
			live[a.Task.ExecutionID] = true
		}
		return live, nil
	}
	byNamespace := make(map[namespace.Namespace][]types.ExecutionID, 1)
	for _, a := range assignments {
		byNamespace[a.Namespace] = append(byNamespace[a.Namespace], a.Task.ExecutionID)
	}
	batch, batched := d.executions.(engine.ExecutionStatusBatchReader)
	for ns, ids := range byNamespace {
		nsCtx := namespace.WithNamespace(ctx, ns)
		if batched {
			statuses, err := batch.GetExecutionStatuses(nsCtx, ids)
			if err != nil {
				return nil, fmt.Errorf("read execution statuses: %w", err)
			}
			for i, id := range ids {
				if i >= len(statuses) {
					live[id] = false
					continue
				}
				live[id] = statuses[i] != "" && !types.IsTerminalExecutionStatus(statuses[i])
			}
			continue
		}
		for _, id := range ids {
			status, found, err := d.executions.GetExecutionStatus(nsCtx, id)
			if err != nil {
				return nil, fmt.Errorf("read execution status %q: %w", id, err)
			}
			live[id] = found && !types.IsTerminalExecutionStatus(status)
		}
	}
	return live, nil
}

// reapQueuedAssignments performs the atomic removals for one pipeline batch.
// Each transition keeps its own fence — the script decides, per assignment,
// whether the record is still 'queued' — so batching changes how the
// transitions travel, not what any of them does, and a concurrent claim races
// each one exactly as it did when they went one at a time. A declined
// transition ('skipped') is the normal outcome of such a race and not an
// error; a command error is attributed to its candidate, which a
// pipeline-level error alone cannot do, and reported once the batch settled.
func (d *RedisRunnerDirectory) reapQueuedAssignments(ctx context.Context, assignmentIDs []string) (int, error) {
	if len(assignmentIDs) == 0 {
		return 0, nil
	}
	pipe := d.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(assignmentIDs))
	for _, assignmentID := range assignmentIDs {
		cmds = append(cmds, pipe.Eval(ctx, redisReapDeadQueuedAssignmentLua, d.appendLaneRequeueKeys([]string{
			d.keys.queue,
			d.keys.seen,
			d.keys.assignmentData,
			d.keys.assignmentState,
			d.keys.assignmentClaim,
			d.keys.assignmentRunner,
			d.keys.assignmentSession,
			d.keys.assignmentLeaseID,
			d.keys.assignmentLeaseToken,
			d.keys.assignmentLeaseMetaKey(assignmentID),
			d.keys.assignmentLeaseMetaLegacy,
		}), assignmentID, d.laneRequeueCandidateCount(), d.laneRequeueModeArg()))
	}
	// Every command has settled by the time Exec returns, each carrying its own
	// error, so the results are read per command below rather than from the
	// pipeline-level error.
	_, _ = pipe.Exec(ctx)
	reclaimed := 0
	for i, cmd := range cmds {
		status, err := cmd.Text()
		if err != nil {
			return reclaimed, fmt.Errorf("reap dead queued assignment %q: %w", assignmentIDs[i], err)
		}
		switch status {
		case "reaped":
			reclaimed++
		case "skipped":
		default:
			return reclaimed, fmt.Errorf("reap dead queued assignment %q: unexpected result %q", assignmentIDs[i], status)
		}
	}
	return reclaimed, nil
}

// redisReapDeadQueuedAssignmentLua removes one assignment record whose
// execution is gone.
//
// KEYS: 1=queue 2=seen 3=assignment:data 4=assignment:state 5=assignment:claim
//
//	6=assignment:runner 7=assignment:session 8=assignment:lease-id
//	9=assignment:lease-token 10=this assignment's lease-metadata key
//	11=pre-U-7 shared lease-metadata hash
//	then, appended by appendLaneRequeueKeys: every candidate queue key (lanes,
//	then the legacy queue) and last the lane marker hash.
//
// ARGV: 1=assignmentID, then the lane candidate count and the lane write mode
// (read by redisLaneRequeueLua; the mode is unused on this path).
//
// The lane-aware tail is what makes this removal a *drop*: the record is gone,
// so every queue copy of it must go with it — including the one on a lane,
// which is a key the record's state never named. The marker first identified
// that lane at enqueue; dropping it here keeps a re-enqueue of the same ID from
// inheriting a placement for a record that no longer exists.
//
// The state fence is the whole safety argument, and it is sufficient on its
// own: redisClaimAssignmentLua both requires state=='queued' and writes
// 'claimed' in the same atomic step, so this script either runs first — leaving
// that claim to fail its own state and payload checks and return 'retry',
// having removed nothing it needed — or runs second, in which case the check
// below skips. A 'leased' assignment has already left 'queued', so it can never
// be a subject here. A repeated call finds no state field and reports
// 'skipped'.
//
// The liveness decision is deliberately NOT taken here. The execution's keys
// live in the engine's namespace-scoped key space, a different key set (and, on
// Redis Cluster, a different slot) from the directory's, so it cannot join this
// transition at all. The caller re-validates the engine's own predicate
// immediately before the call, and this fence is what keeps a concurrent claim
// from being destroyed in the gap between the two.
//
// The field removals beyond queue/data/state mirror ClearAssignment: they are
// no-ops for a well-formed 'queued' record (the enqueue transition already
// cleared the claim and lease fields) and they stop a partial or
// previous-version record from leaving behind a field some other reader would
// interpret as live state.
const redisReapDeadQueuedAssignmentLua = redisLaneRequeueLua + `
local assignmentID = ARGV[1]
if redis.call('HGET', KEYS[4], assignmentID) ~= 'queued' then return 'skipped' end
laneDrop(assignmentID)
redis.call('SREM', KEYS[2], assignmentID)
redis.call('HDEL', KEYS[3], assignmentID)
redis.call('HDEL', KEYS[4], assignmentID)
redis.call('HDEL', KEYS[5], assignmentID)
redis.call('HDEL', KEYS[6], assignmentID)
redis.call('HDEL', KEYS[7], assignmentID)
redis.call('HDEL', KEYS[8], assignmentID)
redis.call('HDEL', KEYS[9], assignmentID)
redis.call('DEL', KEYS[10])
redis.call('HDEL', KEYS[11], assignmentID)
return 'reaped'
`

// redisReconcileLaneMarkerLua removes one orphaned lane marker and the queue
// copies it names.
//
// KEYS: 1=lane marker hash 2=assignment:data 3=legacy queue 4=seen
//
// ARGV: 1=assignmentID
//
// The payload fence is the whole safety argument. assignment:data holds a
// record's payload for exactly as long as the record exists: it is written by
// the enqueue transition in the same atomic step that writes the state field
// and pushes the queue entry, and deleted — in the same step as the marker and
// every queue copy — by each of the four transitions that end a record (the
// reap above, the clear transition, and the two terminal release paths). So a
// missing payload is not a heuristic about age or liveness: it is the record's
// absence, and it is a fact this script re-checks itself rather than trusting
// the caller's read of it.
//
// That re-check is what closes the race with a concurrent re-enqueue. Enqueue
// runs first: the payload is back, and this script skips. This script runs
// first: it clears a record that was already gone, and the enqueue that follows
// rebuilds the payload, the marker, and the queue entries in its own atomic
// step — including the LREM that clears any copy this script's own LREM missed.
// Either order leaves a coherent record.
//
// The queue copies are removed by name rather than from a fixed key list: in
// dual and lane-only mode the marker's value is the lane key the record was
// written to, which is a configuration-derived key that no static key list can
// name — and during a rollback the marker is exactly what still knows a lane
// that is no longer configured. A marker that never named a lane (the legacy
// fallback the enqueue transition records for a node type no lane owns) is
// equal to the legacy key, so the LREM is issued once. The value is always a
// directory key: the only writers of this hash are the enqueue transition and
// the requeue helper, and both store a key out of the candidate list they were
// given. That invariant is still checked before the LREM, because one field
// written by something else must not turn into a WRONGTYPE abort that blocks
// the whole reconciliation pass: a value that is neither the legacy key nor
// lane-prefixed is cleaned out (the HDEL and SREM below) without an LREM at it.
const redisReconcileLaneMarkerLua = `
local assignmentID = ARGV[1]
if redis.call('HEXISTS', KEYS[2], assignmentID) == 1 then return 'skipped' end
local marked = redis.call('HGET', KEYS[1], assignmentID)
if not marked then return 'skipped' end
local lanePrefix = KEYS[3] .. ':lane:'
if marked ~= '' and marked ~= KEYS[3] and string.sub(marked, 1, #lanePrefix) == lanePrefix then
  redis.call('LREM', marked, 0, assignmentID)
end
redis.call('LREM', KEYS[3], 0, assignmentID)
redis.call('SREM', KEYS[4], assignmentID)
redis.call('HDEL', KEYS[1], assignmentID)
return 'reconciled'
`
