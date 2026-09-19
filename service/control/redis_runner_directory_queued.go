package control

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// defaultDeadQueuedAssignmentReapBatch bounds one reaper pass. It caps both how
// many assignments are removed and, through the scan and inspect caps below,
// how much of the shared Redis one call reads.
//
// Twice the stranded-lease reaper's batch on purpose: that one drains a finite
// residue left by crashed runners, while this one drains a continuous arrival —
// every execution that reaches its transient TTL with one of its tasks still
// queued leaves another entry behind. 512 per pass at the sweeper's cadence is
// ~102/min, which has to exceed that arrival rate for the queue to converge
// rather than keep growing: the deployment this exists for measured a queue
// growing by ~42 assignments per minute.
const defaultDeadQueuedAssignmentReapBatch = 512

// DeadQueuedAssignmentReaper is the durable-directory capability that reclaims
// assignments left in 'queued' state after the execution they belong to has
// gone away. Like ClaimReclaimer, StrandedLeaseReaper and the other optional
// capabilities it is discovered by type assertion, so the in-memory directory
// and any other RunnerDirectory implementation are unaffected.
type DeadQueuedAssignmentReaper interface {
	// ReapDeadQueuedAssignments removes up to limit assignments that can never
	// be claimed again, and returns how many it removed. It is idempotent, safe
	// to call concurrently from several replicas, and safe to run against a
	// live directory: it never touches an assignment that is claimable or
	// currently leased.
	ReapDeadQueuedAssignments(ctx context.Context, limit int) (reclaimed int, err error)
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
func (d *RedisRunnerDirectory) ReapDeadQueuedAssignments(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || d.executions == nil {
		// Without a status reader the directory cannot tell a live assignment
		// from a dead one, so it reclaims nothing rather than guessing from key
		// names or from age.
		return 0, nil
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
	// turning without ever adding to the inspected count.
	inspectCap := 4 * limit
	maxPages := inspectCap/batch + 1

	cursor := d.queuedReap.load()
	inspected := 0
	reclaimed := 0
	for pages := 0; ; pages++ {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.assignmentState, cursor, "", int64(batch)).Result()
		if err != nil {
			d.queuedReap.store(cursor)
			return reclaimed, fmt.Errorf("reap dead queued assignments: scan assignment states: %w", err)
		}
		pageReclaimed, err := d.reapDeadQueuedAssignmentPage(ctx, pairs, limit-reclaimed)
		reclaimed += pageReclaimed
		inspected += len(pairs) / 2
		if err != nil {
			// Resume at this page: re-inspecting it is harmless because every
			// step of the pass is idempotent, and it is the only way a page that
			// failed part-way still gets fully covered.
			d.queuedReap.store(cursor)
			return reclaimed, err
		}
		if reclaimed >= limit {
			// Stopped part-way through the page by the removal limit. Hold the
			// page head so the candidates this call did not reach are
			// re-inspected rather than skipped. Each call still removes up to
			// limit entries, so a page cannot pin the cursor forever.
			d.queuedReap.store(cursor)
			return reclaimed, nil
		}
		cursor = next
		if cursor == 0 || inspected >= inspectCap || pages+1 >= maxPages {
			// A completed lap restarts at the head. The hash has no meaningful
			// order to preserve the way the queue list does, so a lap boundary
			// is simply where coverage is known to have wrapped.
			d.queuedReap.store(cursor)
			return reclaimed, nil
		}
	}
}

// reapDeadQueuedAssignmentPage removes the dead queued assignments of one scan
// page. It is the whole per-candidate pipeline: filter by recorded state, read
// the payloads in one call, ask the engine whether each execution is still
// leaseable, and remove only the ones it is not.
func (d *RedisRunnerDirectory) reapDeadQueuedAssignmentPage(ctx context.Context, pairs []string, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	candidates := make([]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == redisAssignmentQueued {
			candidates = append(candidates, pairs[i])
		}
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	// Sorted for the same reason the stranded reaper sorts its candidates: a
	// bounded pass should pick the same entries every call rather than depend on
	// hash iteration order, so a backlog drains deterministically instead of
	// starving whichever entry keeps losing the race to the limit.
	sort.Strings(candidates)

	raws, err := d.rdb.HMGet(ctx, d.keys.assignmentData, candidates...).Result()
	if err != nil {
		return 0, fmt.Errorf("reap dead queued assignments: read assignment payloads: %w", err)
	}
	reclaimed := 0
	for i, assignmentID := range candidates {
		if reclaimed >= limit {
			break
		}
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
		active, err := d.queuedAssignmentExecutionActive(ctx, assignment)
		if err != nil {
			return reclaimed, err
		}
		if active {
			continue
		}
		didReap, err := d.reapQueuedAssignment(ctx, AssignmentID(assignmentID))
		if err != nil {
			return reclaimed, err
		}
		if didReap {
			reclaimed++
		}
	}
	return reclaimed, nil
}

// queuedAssignmentExecutionActive reports whether the assignment's execution can
// still be leased. It is the engine's own activeness predicate: present and
// non-terminal. Namespace comes from the assignment, the same value the poll
// path resolves the lease through, so both paths read the same execution key.
func (d *RedisRunnerDirectory) queuedAssignmentExecutionActive(ctx context.Context, assignment Assignment) (bool, error) {
	status, found, err := d.executions.GetExecutionStatus(
		namespace.WithNamespace(ctx, assignment.Namespace), assignment.Task.ExecutionID)
	if err != nil {
		return false, fmt.Errorf("reap dead queued assignments: read execution status %q: %w",
			assignment.Task.ExecutionID, err)
	}
	return found && !types.IsTerminalExecutionStatus(status), nil
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
