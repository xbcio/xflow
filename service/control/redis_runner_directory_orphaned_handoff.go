package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultOrphanedHandoffReapBatch bounds one reaper pass. It caps both how many
// candidates the pass takes a resolver token for and, through the scan below,
// how much of the shared Redis one call reads.
const defaultOrphanedHandoffReapBatch = 256

// DefaultOrphanedHandoffOwnerStale is how long a handoff's owning runner may go
// without a heartbeat before the control plane treats it as gone for reaping
// purposes.
//
// It is deliberately ten times DefaultRunnerLiveTTL rather than equal to it.
// That TTL decides who may be scheduled work, where a false "live" costs one
// dispatch that fails and is retried; this threshold decides whether the
// control plane may take a resolver token away from a runner that may still be
// polling, where a false "gone" takes debt out from under a runner that is
// merely slow. The two errors are not symmetric, so the preponderance of
// doubt goes to the owner.
const DefaultOrphanedHandoffOwnerStale = 5 * time.Minute

// OrphanedHandoffReaper is the durable-directory capability that lists handoff
// debt no runner can resolve any more. Like ClaimReclaimer, LeaseIndexRepairer,
// StrandedLeaseReaper and DeadQueuedAssignmentReaper it is an optional
// capability, so the in-memory directory and any other RunnerDirectory
// implementation are unaffected.
type OrphanedHandoffReaper interface {
	// ListOrphanedHandoffs takes the resolver token for up to limit handoffs
	// whose owning runner is no longer live, and returns each as the Claim a
	// resolver needs to settle it against the engine.
	//
	// Taking the token is part of listing, which is why this is not a read: the
	// ledger's recovery-ready flag is the single permission to resolve, and a
	// caller that read it and then acted would race its own replicas. A caller
	// that takes it and then decides not to act MUST return the token
	// (MakeClaimHandoffRecoverable), or the debt is stranded behind a deadline it
	// cannot shorten.
	//
	// The returned claims are the candidates this pass is taking responsibility
	// for; it is idempotent and safe to call concurrently from several replicas.
	ListOrphanedHandoffs(ctx context.Context, limit int) ([]Claim, error)
}

var _ OrphanedHandoffReaper = (*RedisRunnerDirectory)(nil)

// ListOrphanedHandoffs reclaims the one handoff shape no channel can reach once
// its owner is gone: handoff state 'lease_may_exist' or 'lease_created' whose
// owning runner has stopped polling.
//
// # Why the existing paths cannot see it
//
//   - ReclaimExpiredClaims enumerates the claim index and is the transition that
//     makes this debt recoverable at all — for these two states its Lua only
//     sets recovery-ready and returns, deliberately never requeueing, because an
//     engine lease may exist and only the engine can say. It is therefore the
//     producer of this shape, not a consumer of it.
//   - The owner's own poll is the only consumer: recoverableHandoff hands the
//     debt to Core, which resolves it against the engine. It runs on the runner
//     that owns the debt, and requires that runner's session. When that runner
//     is gone — a crash, a drain, an evicted pod — nothing runs the check again.
//   - The other maintenance passes are all blind to it by construction:
//     ReapStrandedLeases only drains assignment:state == 'leased',
//     ReapDeadQueuedAssignments only 'queued', ReapLegacyLeaseMeta only the
//     pre-U-7 metadata hash.
//   - Register rebinds a new session to a returning runnerID, and a
//     lease_may_exist handoff is deliberately never requeued there. That is a
//     recovery path for a runner that comes back, not for one that does not.
//
// Nothing is reused here that a returning runner needs, so the shape becomes
// permanently unreachable: the assignment stays 'claimed' for the life of the
// directory and every task behind it is wedged. This reaper is the same
// resolution the poll performs, driven by the control plane so it does not
// depend on the runner that died.
//
// # Cost
//
// The handoff ledger cannot be enumerated per runner — the debt's owner is a
// runner that is gone, so there is no index to walk and no key to guess — so
// this reads the fleet-wide ledger with HSCAN, exactly as
// reapStrandedLedgerHandoffs does and for the same reason. It is bounded three
// ways (page size, release limit, inspect cap) and runs on the slowest cadence
// the sweeper has.
//
// Within that bound the per-candidate cost is deliberately staged, because the
// expensive steps are also the ones a healthy fleet never reaches: two single
// field reads reject most of the ledger (a claim that has not yet expired is not
// this pass's shape), only then does a candidate pay for a registration
// pipeline to establish that its owner is really gone, and only then for the
// take. So the scan stays proportional to the ledger while the pipelines stay
// proportional to the eligible backlog, which is the backlog this pass exists to
// drain.
func (d *RedisRunnerDirectory) ListOrphanedHandoffs(ctx context.Context, limit int) ([]Claim, error) {
	if limit <= 0 {
		return nil, nil
	}
	batch := defaultOrphanedHandoffReapBatch
	if limit < batch {
		batch = limit
	}

	cursor := uint64(0)
	scanned := 0
	claims := make([]Claim, 0, limit)
	inspectCap := 4 * limit
	for {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.handoffState, cursor, "", int64(batch)).Result()
		if err != nil {
			return claims, fmt.Errorf("list orphaned handoffs: scan handoff ledger: %w", err)
		}
		claimIDs := make([]string, 0, len(pairs)/2)
		for i := 0; i+1 < len(pairs); i += 2 {
			switch HandoffDebtState(pairs[i+1]) {
			case HandoffDebtLeaseMayExist, HandoffDebtLeaseCreated:
				claimIDs = append(claimIDs, pairs[i])
			}
		}
		// Sorted for the same reason every other bounded pass sorts: a bounded
		// walk should take the same candidates every call rather than depending on
		// hash iteration order, so a backlog drains deterministically instead of
		// starving whoever keeps losing the race to the limit.
		sort.Strings(claimIDs)
		for _, claimID := range claimIDs {
			if len(claims) >= limit {
				return claims, nil
			}
			claim, ok, err := d.takeOrphanedHandoff(ctx, claimID)
			if err != nil {
				return claims, err
			}
			if ok {
				claims = append(claims, claim)
			}
		}
		scanned += len(pairs) / 2
		cursor = next
		if cursor == 0 || len(claims) >= limit || scanned >= inspectCap {
			return claims, nil
		}
	}
}

// takeOrphanedHandoff returns one ledger record's Claim, but only when it is
// eligible AND this process won the resolver token for it.
//
// The gate order is deliberate: the state and readiness reads are one bounded
// field each, while the liveness check costs a registration pipeline and the
// take costs a write. Testing the cheap predicates first means the common case —
// a fleet whose handoff debt is mostly not yet recoverable, or is owned by a
// runner that is fine — never pays for the expensive ones.
func (d *RedisRunnerDirectory) takeOrphanedHandoff(ctx context.Context, claimID string) (Claim, bool, error) {
	// The state is re-read rather than trusted from the page. A page is a snapshot
	// of a ledger the poll path is mutating concurrently, so a record can have
	// moved on since the scan; takeHandoffRecovery re-checks it too, but the
	// caller needs the current value to decode the record correctly (a
	// 'lease_created' record carries lease metadata a 'lease_may_exist' one does
	// not).
	stateRaw, err := d.rdb.HGet(ctx, d.keys.handoffState, claimID).Result()
	if errors.Is(err, redis.Nil) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, fmt.Errorf("list orphaned handoffs: read handoff state %q: %w", claimID, err)
	}
	state := HandoffDebtState(stateRaw)
	if state != HandoffDebtLeaseMayExist && state != HandoffDebtLeaseCreated {
		return Claim{}, false, nil
	}
	ready, err := d.rdb.HGet(ctx, d.keys.handoffRecoveryReady, claimID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Claim{}, false, fmt.Errorf("list orphaned handoffs: read handoff recovery readiness %q: %w", claimID, err)
	}
	// recovery-ready is the directory's own statement that a resolver may take
	// this debt, and it is only ever set for these states once the claim has
	// expired. Without it the take below cannot succeed, so this is a filter on
	// the only records that were ever going to be candidates.
	if ready != "1" {
		return Claim{}, false, nil
	}
	owner, err := d.rdb.HGet(ctx, d.keys.handoffRunner, claimID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Claim{}, false, fmt.Errorf("list orphaned handoffs: read handoff owner %q: %w", claimID, err)
	}
	if d.handoffOwnerLive(ctx, owner) {
		return Claim{}, false, nil
	}

	status, err := d.takeHandoffRecovery(ctx, claimID)
	if err != nil {
		return Claim{}, false, fmt.Errorf("list orphaned handoffs: take handoff recovery %q: %w", claimID, err)
	}
	if status != "taken" {
		// Lost the token to a concurrent pass, or the debt moved on between the
		// reads above. Both mean the caller must not act on it.
		return Claim{}, false, nil
	}
	claim, ok, err := d.buildHandoffClaim(ctx, claimID, state)
	if err != nil {
		// The token is held, so a failure here must not leave the debt behind a
		// deadline this pass cannot shorten. best effort: the ledger's own
		// transitions are the backstop, and returning the token is what a
		// returning owner's poll needs.
		d.restoreHandoffRecovery(ctx, ClaimID(claimID))
		return Claim{}, false, err
	}
	if !ok {
		return Claim{}, false, nil
	}
	return claim, true, nil
}

// handoffOwnerLive reports whether runnerID is still polling this directory.
//
// A runner that is not registered at all, or that has never heartbeat, is not
// live: an empty runnerID reaches the same answer, which is what the ledger's
// rolling-upgrade entries can carry.
func (d *RedisRunnerDirectory) handoffOwnerLive(ctx context.Context, runnerID string) bool {
	if runnerID == "" {
		return false
	}
	snapshot, ok := d.Runner(ctx, runnerID)
	if !ok {
		return false
	}
	if snapshot.LastHeartbeat.IsZero() {
		return false
	}
	return d.clockNow().Sub(snapshot.LastHeartbeat) <= DefaultOrphanedHandoffOwnerStale
}
