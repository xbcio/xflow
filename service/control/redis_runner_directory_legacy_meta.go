package control

import (
	"context"
	"fmt"
)

// defaultLegacyLeaseMetaReapBatch bounds how many legacy fields one HSCAN can
// propose for deletion. HSCAN's COUNT is a hint, so this is a target rather
// than a guarantee; the caller's own limit is what actually bounds one call.
const defaultLegacyLeaseMetaReapBatch = 128

// LegacyLeaseMetaReaper is the durable-directory capability that drains the
// pre-U-7 assignment lease-metadata hash. It is an optional capability, like
// ClaimReclaimer and LeaseIndexRepairer, so the in-memory directory and any
// other RunnerDirectory implementation are unaffected by it.
type LegacyLeaseMetaReaper interface {
	// ReapOrphanedLegacyAssignmentLeaseMeta removes up to limit legacy-hash
	// fields whose assignment can no longer be read by any version of this
	// system, and returns how many it removed. It is idempotent, safe to call
	// concurrently from several replicas, and safe to run against a live
	// directory: it never touches a field that is still reachable.
	ReapOrphanedLegacyAssignmentLeaseMeta(ctx context.Context, limit int) (int, error)
}

var _ LegacyLeaseMetaReaper = (*RedisRunnerDirectory)(nil)

// ReapOrphanedLegacyAssignmentLeaseMeta drains the pre-U-7 shared
// lease-metadata hash, which U-7 left unreferenced and without any TTL.
//
// # Why the legacy hash cannot simply be deleted
//
// The hash is written by the *previous* binary, not by this one. A server
// instance on the old version that is still running (or that is started again
// by a rollback or an autoscaler) may hold a live lease whose only replay
// record is a field in that hash. Deleting the key — or a field — while such
// an instance is mid-flight destroys metadata the old binary still needs, so
// an unconditional delete is only correct after an operator has proven no old
// binary can return, which is what the runbook's coordinated switch is for.
// That is a poor fit for the actual residue: fields abandoned by processes
// that died long ago, which no operator action will ever reach, and which
// hold task input (potentially secrets for HTTP nodes) with no expiry.
//
// # Why a field can be removed safely without any version negotiation
//
// Both versions keep the per-assignment ownership/identity records in the
// *same* shared hashes — assignmentData, assignmentState, assignmentClaim,
// assignmentRunner, assignmentSession, assignmentLeaseID, assignmentLeaseToken
// are byte-identical between the shipped v0.0.6 and this version; U-7 moved
// only the metadata key. The previous binary reaches its
// HGET(legacyHash, assignmentID) in every path only after it has already read
// and matched those same records for that assignment ID. Therefore a legacy
// field whose assignment has no record left in any of them is unreachable by
// the old binary as well as by this one: it is residue, not live state. That
// predicate is what makes this reaper safe to run online, during a mixed
// version window, and against a rollback — it removes only what nothing can
// read, and it never consults a version marker that could itself be stale.
//
// # Why HSCAN on the single key rather than a prefix scan
//
// A prefix SCAN is answered per node and would miss fields on other Redis
// Cluster nodes; the codebase uses it nowhere in production for that reason.
// HSCAN is a single-key, cursor-advancing operation, so every call is one
// slot, incremental, and bounded — it costs the shared Redis one bucket, not
// one keyspace walk.
//
// # Sensitivity
//
// The fields are assignment IDs, which the runbook already treats as
// controlled-environment data, and the values are the complete runner-facing
// lease including the task input. HSCAN on Redis < 7.4 cannot suppress values,
// so this function receives them and drops them immediately: they are never
// retained, returned, or logged, and the Lua script deletes by field name
// alone.
func (d *RedisRunnerDirectory) ReapOrphanedLegacyAssignmentLeaseMeta(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	batch := defaultLegacyLeaseMetaReapBatch
	if limit < batch {
		batch = limit
	}

	cursor := uint64(0)
	inspected := 0
	reaped := 0
	// inspectCap keeps one call bounded when the hash is mostly live fields. The
	// limit alone bounds deletions but says nothing about how many fields a
	// caller must look at to find that many dead ones, and the directory runs
	// against a Redis shared with everything else.
	inspectCap := 4 * limit
	for {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.assignmentLeaseMetaLegacy, cursor, "", int64(batch)).Result()
		if err != nil {
			return reaped, fmt.Errorf("scan legacy assignment lease metadata: %w", err)
		}
		// HSCAN returns field, value, field, value… The value is the lease
		// payload; only the field name is carried forward.
		candidates := make([]string, 0, len(pairs)/2)
		for i := 0; i+1 < len(pairs); i += 2 {
			candidates = append(candidates, pairs[i])
		}
		// HSCAN's COUNT is a hint, so one page can hold far more fields than the
		// remaining budget. Truncating stops a single call from deleting past
		// its limit; the cursor has already moved past the rest, and the next
		// call re-scans from the start and reaches them once these are gone.
		if remaining := limit - reaped; len(candidates) > remaining {
			candidates = candidates[:remaining]
		}
		if len(candidates) > 0 {
			removed, err := d.reapLegacyAssignmentLeaseMetaFields(ctx, candidates)
			if err != nil {
				return reaped, err
			}
			reaped += removed
		}
		inspected += len(pairs) / 2
		cursor = next
		if cursor == 0 || reaped >= limit || inspected >= inspectCap {
			return reaped, nil
		}
	}
}

// reapLegacyAssignmentLeaseMetaFields deletes the candidate fields that have
// no surviving per-assignment record, in one atomic step. Batching the check
// and the delete into a single script is what keeps the predicate honest: a
// field cannot be re-checked as dead and then deleted after a concurrent
// EnqueueAssignment has already made it live again.
func (d *RedisRunnerDirectory) reapLegacyAssignmentLeaseMetaFields(ctx context.Context, assignmentIDs []string) (int, error) {
	keys := []string{
		d.keys.assignmentLeaseMetaLegacy,
		d.keys.assignmentData,
		d.keys.assignmentState,
		d.keys.assignmentClaim,
		d.keys.assignmentRunner,
		d.keys.assignmentSession,
		d.keys.assignmentLeaseID,
		d.keys.assignmentLeaseToken,
	}
	args := make([]interface{}, 0, len(assignmentIDs))
	for _, assignmentID := range assignmentIDs {
		args = append(args, assignmentID)
	}
	value, err := d.rdb.Eval(ctx, redisReapLegacyAssignmentLeaseMetaLua, keys, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("reap legacy assignment lease metadata: %w", err)
	}
	reaped, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected Redis Lua result type %T", value)
	}
	return int(reaped), nil
}

// redisReapLegacyAssignmentLeaseMetaLua implements the liveness predicate
// documented on ReapOrphanedLegacyAssignmentLeaseMeta. KEYS[1] is the legacy
// hash; KEYS[2..8] are the per-assignment records that both versions read
// before they can reach the legacy field. A field is deleted only when all of
// them are absent. The loop deliberately does not short-circuit: Lua 5.1
// restricts break to the end of a block, and seven HEXISTS against keys that
// are already in the same slot cost less than the correctness argument for
// restructuring it.
const redisReapLegacyAssignmentLeaseMetaLua = `
local reaped = 0
for i = 1, #ARGV do
  local assignmentID = ARGV[i]
  local live = false
  for k = 2, 8 do
    if redis.call('HEXISTS', KEYS[k], assignmentID) == 1 then
      live = true
    end
  end
  if not live then
    redis.call('HDEL', KEYS[1], assignmentID)
    reaped = reaped + 1
  end
end
return reaped
`
