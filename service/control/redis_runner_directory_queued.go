package control

import (
	"context"
	"fmt"
	"sort"
	"sync"

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
	// Inspected counts the assignment-state entries this pass read whose
	// recorded state is 'queued'. That is the whole shape it drains, and it is
	// deliberately narrower than the hash it scans: an entry in any other state
	// is not a candidate of this shape, and counting it would make the ratio
	// against the removed count a measure of how full the directory is rather
	// than of how much of its candidate set the pass releases. Most inspected
	// entries are expected NOT to be removed on a healthy fleet — the pass walks
	// the head of a live queue and correctly leaves claimable work alone — so a
	// high inspected/released ratio is only a defect signal when the queue is
	// not converging.
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
func (d *RedisRunnerDirectory) ReapDeadQueuedAssignments(ctx context.Context, limit int) (ReapResult, error) {
	if limit <= 0 || d.executions == nil {
		// Without a status reader the directory cannot tell a live assignment
		// from a dead one, so it reclaims nothing rather than guessing from key
		// names or from age.
		return ReapResult{}, nil
	}
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

	reclaimed = 0
	for _, c := range parsed {
		if reclaimed >= limit {
			break
		}
		if leaseable[c.assignment.Task.ExecutionID] {
			continue
		}
		didReap, err := d.reapQueuedAssignment(ctx, AssignmentID(c.assignmentID))
		if err != nil {
			return reclaimed, inspected, err
		}
		if didReap {
			reclaimed++
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

// reapQueuedAssignment performs the atomic removal. ok is false when the
// transition declined — the assignment was claimed, leased or already removed
// between the scan and the call — which is not an error: every one of those
// outcomes means the entry is no longer this reaper's to remove.
func (d *RedisRunnerDirectory) reapQueuedAssignment(ctx context.Context, assignmentID AssignmentID) (bool, error) {
	status, err := d.evalStatus(ctx, redisReapDeadQueuedAssignmentLua, []string{
		d.keys.queue,
		d.keys.seen,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
		d.keys.assignmentLeaseMetaKey(string(assignmentID)),
		d.keys.assignmentLeaseMetaLegacy,
	}, string(assignmentID))
	if err != nil {
		return false, fmt.Errorf("reap dead queued assignment %q: %w", assignmentID, err)
	}
	switch status {
	case "reaped":
		return true, nil
	case "skipped":
		return false, nil
	default:
		return false, fmt.Errorf("reap dead queued assignment %q: unexpected result %q", assignmentID, status)
	}
}

// redisReapDeadQueuedAssignmentLua removes one assignment record whose
// execution is gone.
//
// KEYS: 1=queue 2=seen 3=assignment:data 4=assignment:state 5=assignment:claim
//
//	6=assignment:runner 7=assignment:session 8=assignment:lease-id
//	9=assignment:lease-token 10=this assignment's lease-metadata key
//	11=pre-U-7 shared lease-metadata hash
//
// ARGV: 1=assignmentID
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
const redisReapDeadQueuedAssignmentLua = `
local assignmentID = ARGV[1]
if redis.call('HGET', KEYS[4], assignmentID) ~= 'queued' then return 'skipped' end
redis.call('LREM', KEYS[1], 0, assignmentID)
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
