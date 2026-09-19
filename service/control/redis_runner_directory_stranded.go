package control

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
)

// defaultStrandedLeaseReapBatch bounds one reaper pass. It caps both how many
// assignments are released and, through the candidate walks below, how much of
// the shared Redis one call reads.
const defaultStrandedLeaseReapBatch = 256

// StrandedLeaseReaper is the durable-directory capability that reclaims
// assignments left in 'leased' state after their lease metadata expired out of
// the directory. Like ClaimReclaimer, LeaseIndexRepairer and
// LegacyLeaseMetaReaper it is an optional capability, so the in-memory
// directory and any other RunnerDirectory implementation are unaffected.
type StrandedLeaseReaper interface {
	// ReapStrandedLeases releases up to limit assignments whose directory lease
	// record is unrecoverable, and reports how many candidates it inspected and
	// how many of them it released. It is idempotent, safe to call concurrently
	// from several replicas, and safe to run against a live directory: it never
	// touches an assignment whose lease metadata is still present.
	//
	// Inspected counts the candidates the pass found in the shape it drains: the
	// 'leased' assignments its per-runner walks yielded, plus the records the
	// handoff ledger still marks finalized. A candidate it then did not release —
	// a racing renew re-armed its metadata, a racing report released it first —
	// is counted as inspected and not as released, which is what lets the pair
	// report a pass whose scope is wider than the work it finds.
	ReapStrandedLeases(ctx context.Context, limit int) (ReapResult, error)
}

var _ StrandedLeaseReaper = (*RedisRunnerDirectory)(nil)

// ReapStrandedLeases reclaims the one shape both existing reclaim paths are
// structurally blind to: assignment:state == 'leased' while the assignment's
// lease-metadata key has expired out of the directory.
//
// # Why the existing paths cannot see it
//
//   - ReclaimExpiredClaims enumerates the claim index, and the claim keys are
//     deleted in the same atomic step that promotes the assignment to 'leased'
//     (redisFinalizeClaimLua). A leased assignment has left that enumeration
//     behind; widening that Lua's state predicate changes nothing.
//   - The LeaseSweeper enumerates the execution's own lease index, which shares
//     this metadata's transient TTL: it expires with it, so the sweeper stops
//     seeing the lease at exactly the moment it becomes unrecoverable.
//   - replayLease does recognise the shape and releases it, but only while the
//     OWNING runner is polling. Once that runner is gone — a crash, a drain, an
//     evicted pod — nothing runs the check again and the assignment holds its
//     runner's capacity for the life of the directory.
//
// That last point is what this reaper adds: the same release the poll performs,
// driven by the control plane so it does not depend on the runner that died.
//
// # Cost
//
// Pass A walks the per-runner leased index and only reads the whole
// assignment-state hash for a runner whose index is provably short (see
// strandedLeaseCandidates). Pass B reads the fleet-wide handoff ledger, which
// the poll path deliberately avoids because its cost scales with fleet debt
// rather than with one runner's — acceptable here, on a slow maintenance cadence
// with a hard limit, and it is the only enumeration that reaches an assignment
// whose per-runner index entry is gone.
func (d *RedisRunnerDirectory) ReapStrandedLeases(ctx context.Context, limit int) (ReapResult, error) {
	if limit <= 0 {
		return ReapResult{}, nil
	}
	result, err := d.reapStrandedIndexedLeases(ctx, limit)
	if err != nil {
		return result, err
	}
	if result.Released >= limit {
		return result, nil
	}
	ledger, err := d.reapStrandedLedgerHandoffs(ctx, limit-result.Released)
	result.Inspected += ledger.Inspected
	result.Released += ledger.Released
	if err != nil {
		return result, err
	}
	return result, nil
}

// reapStrandedIndexedLeases is pass A: every leased assignment the per-runner
// index accounts for, and, for a runner whose index is short, every leased
// assignment in the directory that belongs to it.
func (d *RedisRunnerDirectory) reapStrandedIndexedLeases(ctx context.Context, limit int) (ReapResult, error) {
	runnerIDs, err := d.ListRunners(ctx)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap stranded leases: list runners: %w", err)
	}
	// Sorted for the same reason replayLease sorts: a bounded pass should pick
	// the same candidates every call rather than depending on hash iteration
	// order, so a backlog drains deterministically instead of starving whoever
	// keeps losing the race to the limit.
	sort.Strings(runnerIDs)
	var result ReapResult
	for _, runnerID := range runnerIDs {
		if result.Released >= limit {
			break
		}
		candidates, err := d.strandedLeaseCandidates(ctx, runnerID)
		if err != nil {
			return result, err
		}
		// Counted on discovery rather than on release, and before the batch can
		// stop the loop: a candidate this call never reached because the limit
		// was already met is still a record in this shape, and leaving it out
		// would make a backlog larger than the batch read as a pass that
		// inspected exactly its batch.
		result.Inspected += len(candidates)
		for _, assignmentID := range candidates {
			if result.Released >= limit {
				break
			}
			didRelease, err := d.reapStrandedAssignment(ctx, runnerID, assignmentID)
			if err != nil {
				return result, err
			}
			if didRelease {
				result.Released++
			}
		}
	}
	return result, nil
}

// reapStrandedLedgerHandoffs is pass B: assignments the handoff ledger still
// records as finalized debt. It reaches the assignment whose per-runner index
// entry is missing — the index can only be written by a control plane that has
// it, and its count check is what heals that, but the ledger is the independent
// record and costs one bounded read to consult.
//
// HSCAN rather than HGETALL for the same reason the legacy reaper uses it: the
// call must stay bounded against a Redis the whole control plane shares. The
// inspect cap is what bounds it when the ledger is mostly records that settle
// concurrently, since the release limit alone says nothing about how many
// entries a caller must look at to find that many stranded ones.
func (d *RedisRunnerDirectory) reapStrandedLedgerHandoffs(ctx context.Context, limit int) (ReapResult, error) {
	if limit <= 0 {
		return ReapResult{}, nil
	}
	batch := defaultStrandedLeaseReapBatch
	if limit < batch {
		batch = limit
	}

	cursor := uint64(0)
	scanned := 0
	var result ReapResult
	inspectCap := 4 * limit
	for {
		pairs, next, err := d.rdb.HScan(ctx, d.keys.handoffState, cursor, "", int64(batch)).Result()
		if err != nil {
			return result, fmt.Errorf("reap stranded leases: scan handoff ledger: %w", err)
		}
		claimIDs := make([]string, 0, len(pairs)/2)
		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i+1] == string(HandoffDebtFinalized) {
				claimIDs = append(claimIDs, pairs[i])
			}
		}
		sort.Strings(claimIDs)
		// Every finalized record this page yielded is a candidate in the shape,
		// counted even if the release limit stops the loop before all of them
		// are resolved. The scan cap below counts differently on purpose: it
		// bounds how much of the ledger one call reads, not how much of the
		// shape it found.
		result.Inspected += len(claimIDs)
		for _, claimID := range claimIDs {
			if result.Released >= limit {
				return result, nil
			}
			didRelease, err := d.reapStrandedHandoff(ctx, claimID)
			if err != nil {
				return result, err
			}
			if didRelease {
				result.Released++
			}
		}
		scanned += len(pairs) / 2
		cursor = next
		if cursor == 0 || result.Released >= limit || scanned >= inspectCap {
			return result, nil
		}
	}
}

// reapStrandedHandoff resolves one finalized ledger record to its assignment
// and runner and hands it to the shared release. The owning runner is only
// needed to prune the per-runner index, and is legitimately absent for a record
// written before that index existed or by a registry that removed the runner;
// the release itself is fenced on the lease identity, not on the runner.
func (d *RedisRunnerDirectory) reapStrandedHandoff(ctx context.Context, claimID string) (bool, error) {
	assignmentID, err := d.rdb.HGet(ctx, d.keys.handoffClaim, claimID).Result()
	if errors.Is(err, redis.Nil) || assignmentID == "" {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reap stranded leases: read handoff claim %q: %w", claimID, err)
	}
	runnerID, err := d.rdb.HGet(ctx, d.keys.handoffRunner, claimID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("reap stranded leases: read handoff runner %q: %w", claimID, err)
	}
	return d.reapStrandedAssignment(ctx, runnerID, assignmentID)
}

// strandedLeaseCandidates returns every assignment runnerID holds in 'leased'
// state, verified against the authoritative state and owner hashes rather than
// trusted from the index.
//
// It resolves candidates exactly the way replayLease does — the per-runner
// index first, then the whole assignment-state hash once the live count proves
// the index short — and it is deliberately a separate implementation rather
// than a shared helper. replayLease returns as soon as it finds one replayable
// lease and requires the lease to belong to the polling session; this must
// visit every candidate and ignores the session, so a shared loop would have to
// grow mode flags that obscure both callers.
func (d *RedisRunnerDirectory) strandedLeaseCandidates(ctx context.Context, runnerID string) ([]string, error) {
	visited := make(map[string]struct{})
	var owned []string
	for pass := 0; ; pass++ {
		candidates, err := d.leasedAssignmentCandidates(ctx, runnerID, pass > 0)
		if err != nil {
			return nil, err
		}
		sort.Strings(candidates)
		live := 0
		for _, assignmentID := range candidates {
			if _, done := visited[assignmentID]; done {
				continue
			}
			visited[assignmentID] = struct{}{}
			state, err := d.rdb.HGet(ctx, d.keys.assignmentState, assignmentID).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return nil, fmt.Errorf("read lease state %q: %w", assignmentID, err)
			}
			if state != redisAssignmentLeased {
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			owner, err := d.rdb.HGet(ctx, d.keys.assignmentRunner, assignmentID).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return nil, fmt.Errorf("read lease owner %q: %w", assignmentID, err)
			}
			if owner != runnerID {
				d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
				continue
			}
			live++
			owned = append(owned, assignmentID)
			if pass > 0 {
				// Confirmed live and owned, and the index did not know about it.
				// Recording it is the safe direction for the index to be wrong
				// in, exactly as in replayLease.
				d.indexLeasedAssignment(ctx, runnerID, assignmentID)
			}
		}
		if pass > 0 {
			return owned, nil
		}
		count, err := d.rdb.HGet(ctx, d.keys.runnerLeaseCount, runnerID).Int()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read runner lease count %q: %w", runnerID, err)
		}
		if live >= count {
			return owned, nil
		}
	}
}

// strandedLeaseIdentity reads the lease a still-'leased' assignment carries,
// but only when that record is unrecoverable: its metadata has expired out of
// the directory, so LookupLease can no longer find it and no renew or report
// can ever succeed. ok is false for every other shape, which is what makes the
// reaper safe against a live lease.
func (d *RedisRunnerDirectory) strandedLeaseIdentity(ctx context.Context, assignmentID string) (engine.LeaseID, engine.LeaseToken, bool, error) {
	state, err := d.rdb.HGet(ctx, d.keys.assignmentState, assignmentID).Result()
	if errors.Is(err, redis.Nil) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("read lease state %q: %w", assignmentID, err)
	}
	if state != redisAssignmentLeased {
		return "", "", false, nil
	}
	exists, err := d.rdb.Exists(ctx, d.keys.assignmentLeaseMetaKey(assignmentID)).Result()
	if err != nil {
		return "", "", false, fmt.Errorf("read lease metadata %q: %w", assignmentID, err)
	}
	if exists != 0 {
		return "", "", false, nil
	}
	leaseID, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseID, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", "", false, fmt.Errorf("read lease id %q: %w", assignmentID, err)
	}
	leaseToken, err := d.rdb.HGet(ctx, d.keys.assignmentLeaseToken, assignmentID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", "", false, fmt.Errorf("read lease token %q: %w", assignmentID, err)
	}
	return engine.LeaseID(leaseID), engine.LeaseToken(leaseToken), true, nil
}

// reapStrandedAssignment releases one candidate if it is stranded, then clears
// the two records the release deliberately leaves behind.
func (d *RedisRunnerDirectory) reapStrandedAssignment(ctx context.Context, runnerID, assignmentID string) (bool, error) {
	leaseID, leaseToken, stranded, err := d.strandedLeaseIdentity(ctx, assignmentID)
	if err != nil {
		return false, err
	}
	if !stranded {
		return false, nil
	}
	outcome, err := d.ReleaseExpiredLease(ctx, ExpiredDirectoryLeaseRequest{
		AssignmentID: AssignmentID(assignmentID),
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
	})
	if err != nil {
		return false, fmt.Errorf("release stranded lease %q: %w", assignmentID, err)
	}
	if outcome != ExpiredDirectoryLeaseReleased {
		// Nothing to do and nothing wrong: a racing report or renew either
		// released the lease first or re-armed its metadata. Both are the
		// outcome this reaper wants, and the token fence is what kept this
		// attempt from disturbing them.
		return false, nil
	}
	if runnerID != "" {
		d.pruneLeasedAssignmentIndex(ctx, runnerID, assignmentID)
	}
	// The release Lua deliberately keeps the finalized handoff record, because
	// a capacity release alone does not prove the lease token is dead — the
	// sweeper settles it after the engine has reclaimed. This reaper is that
	// reclaim for a lease whose directory metadata is gone, so it settles here.
	// Best effort: SettleFinalizedHandoff is itself token-fenced and idempotent,
	// and a failure leaves a record that indexes nothing live.
	_ = d.SettleFinalizedHandoff(ctx, AssignmentID(assignmentID), leaseID, leaseToken)
	return true, nil
}
