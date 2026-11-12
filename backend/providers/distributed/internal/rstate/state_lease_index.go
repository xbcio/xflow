package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file holds the lease-expiry namespace index: the accelerator that lets
// ListExpiredLeases find executions with an expired lease without walking the
// keyspace. It copies the outbox readiness-index protocol (state_outbox_index.go
// — read its file note first); only what differs is written down here.
//
// ---------------------------------------------------------------------------
// The defect it addresses
// ---------------------------------------------------------------------------
//
// The sweep used to call ScanAll(execScanPattern(t, "leases")) on every pass.
// SCAN cost tracks the keyspace, not the match count, so at the measured 2.35M
// keys one pass was ~18k SCAN calls and 9% of all Redis command time — on the
// leader alone, every 10s, to find a handful of lease ZSETs.
//
// ---------------------------------------------------------------------------
// Layout and scoring
// ---------------------------------------------------------------------------
//
//	leaseExpiryExecIndexKey(t) = xflow:ns:<t>:lease-index
//	    ZSET, member = execution ID, score = a LOWER BOUND (unix ms, the same
//	    unit as the per-execution lease ZSET) on that execution's earliest
//	    lease deadline.
//
// The per-execution ZSET (leaseExpiryZSetKey) stays the single source of truth;
// the index only decides which of them the sweep reads. It has no `exec:{` and
// so never matches an execScanPattern glob, and it carries no hash tag, so it is
// only ever written with plain single-key commands from Go — never inside a
// per-execution script (CROSSSLOT).
//
// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------
//
// markLeaseExpiryIndex runs AFTER every transition that can add a NEW member to
// a per-execution lease ZSET: a task lease acquire, a group lease acquire, a
// lease-carrying UpsertNode, and a RepairLeaseIndex reconciliation that indexed
// a lease. The score is min(now, deadline). It must not be the bare deadline:
// ZADD overwrites, and the execution may already hold an earlier, expired lease
// whose score would be raised out of the read window. "now" can still raise
// such a score, but only up to now, so that lease is read at most one grace
// period plus one sweep late — never lost.
//
// Not registered, deliberately:
//
//   - Renewals (renewTaskLeaseLua, renewGroupLeaseLua). They move an existing
//     member later, so the index's lower bound stays a lower bound. A renewal
//     can re-add a member that had gone missing from the per-execution ZSET,
//     but that member was already unindexed before the renewal; recovering it
//     is the walk's and RepairLeaseIndex's job, and paying one extra ZADD on
//     the renewal hot path to cover it is not worth it.
//   - repairLeaseIndexLua. It only rescores a member the sweep has just read
//     from this execution, and the sweep re-arms the index right after.
//   - Every other script that receives the lease ZSET key only ZREMs from it
//     (claim, revoke, commit, retry reset, suspend, cancel, expire, cleanup).
//
// ---------------------------------------------------------------------------
// Read, then re-arm or prune with verify
// ---------------------------------------------------------------------------
//
// The sweep reads `ZRANGEBYSCORE index -inf <before> LIMIT 0 <budget>`, runs the
// unchanged per-execution processing, and then calls rearmLeaseExpiryIndex:
// exact minimum → ZADD, re-read, ZADD again if the minimum dropped (closes the
// clobber race with a producer that wrote before the re-read); empty → ZREM,
// re-read, ZADD if a member appeared (the outbox prune-verify-repair). A
// producer that writes after the re-read also registers after it, so its ZADD
// outlives ours.
//
// ---------------------------------------------------------------------------
// Safety net: the incremental walk, and its staleness bound
// ---------------------------------------------------------------------------
//
// A registration is lost when the process dies between the write and the ZADD,
// when the ZADD fails, and for every lease written before this index existed.
// walkLeaseExpiryKeys recovers those with redisx.ScanPage over the old pattern,
// on a persistent per-namespace cursor, registering each found execution at its
// exact minimum (via the same re-arm).
//
//   - Only one Store per namespace walks at a time (the redisx walk-lock for
//     kind "leases"); every other call issues no SCAN and no DBSIZE.
//   - A holder that finds no recent round-done marker runs a whole round inside
//     the call (backfill). That is one old-style pass, once per cluster. With a
//     marker (handover between replicas) it goes straight to paced steps.
//   - Paced, it takes one step every leaseWalkStepInterval, sized from DBSIZE
//     so that a full round spans about leaseWalkRound. Calls between steps
//     issue no SCAN at all.
//
// staleness bound for a lost registration ≈ leaseWalkRound + leaseWalkStepInterval
//
// (≈1 hour), plus the sweeper's own grace. It bounds DELAY of reclamation,
// never correctness: the lease is still in its per-execution ZSET.
//
// ---------------------------------------------------------------------------
// Orphan members, and the prune step
// ---------------------------------------------------------------------------
//
// The per-execution ZSET is deleted when the execution ends (TTL or terminal
// cleanup), but its index member is not: the index has no per-execution TTL and
// no commit path removes from it. Until a sweep drops it, the member is a pure
// orphan — it is not due unless its score has passed, and when it is, the
// reclaim path still pays a serial per-member read plus re-arm and consumes one
// unit of leaseIndexBatchLimit. At production scale those orphans dominated the
// index (millions of members), so the bounded reclaim path spent its whole
// budget and most of its Redis round trips on executions that no longer exist,
// and the index kept growing faster than the sweep drained it.
//
// pruneOrphanLeaseIndex is therefore run before the reclaim read: one ZRANGEBYSCORE
// page and one non-transactional pipeline of per-key EXISTS per round, then one
// variadic ZREM for the whole round's orphans. It is an accelerator, not part of
// the reclaim contract — the walk recovers anything a failed prune drops (see
// the verify note above the function).
const (
	leaseExpiryExecIndexSuffix = "lease-index"

	// leaseWalkPageCount is the SCAN COUNT hint of one walk page.
	leaseWalkPageCount = redisx.WalkScanCount
	// leaseWalkStepInterval is the minimum time between two throttled walk
	// steps in one namespace.
	leaseWalkStepInterval = 30 * time.Second
	// leaseWalkRound is the target duration of one throttled full round.
	leaseWalkRound = redisx.WalkRound
	// leaseWalkMaxPagesPerStep caps one throttled step so a huge keyspace
	// lengthens the round instead of producing a burst.
	leaseWalkMaxPagesPerStep = 256

	// leaseWalkKind names the walk in the holder-lock and round-done keys.
	leaseWalkKind = "leases"

	// leaseIndexPruneChunk is the number of index members one prune round
	// examines, and leaseIndexPruneRounds caps the rounds in one sweep. Together
	// they clear at most 8192 members per 10s sweep (≈800/s), far above the
	// measured ≈30/s of new registrations, without lengthening the sweep. The
	// members of one round travel in one pipeline, so the round-trip count does
	// not scale with the chunk; the rounds only bound how many chunks one call
	// examines. The scan offset stays at most chunk*rounds: ZRANGEBYSCORE's LIMIT
	// offset is O(offset) and the production index holds millions of members.
	leaseIndexPruneChunk  = 2048
	leaseIndexPruneRounds = 4
)

// leaseExpiryExecIndexKey returns the lease-expiry index for one namespace.
func leaseExpiryExecIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:%s", t, leaseExpiryExecIndexSuffix)
}

// leaseWalkState is one namespace's safety-net walk position.
type leaseWalkState struct {
	cursors    redisx.Cursors
	backfilled bool
	lastStep   time.Time
	holding    bool
}

// markLeaseExpiryIndex registers an execution after a transition that added a
// lease member scored deadlineMs. Best-effort: the transition is applied, a
// failure is only logged, and the walk recovers a lost registration.
func (s *Store) markLeaseExpiryIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID, deadlineMs int64) {
	score := time.Now().UTC().UnixMilli()
	if deadlineMs > 0 && deadlineMs < score {
		score = deadlineMs
	}
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	if err := s.rdb.ZAdd(ctx, leaseExpiryExecIndexKey(t), redis.Z{Score: float64(score), Member: string(id)}).Err(); err != nil {
		s.noteLeaseIndexFailure("mark", err)
	}
}

// rearmLeaseExpiryIndex makes the index member for id agree with the execution's
// lease ZSET: its exact minimum, or no member when it is empty. See the file
// note for the verify step on both branches.
func (s *Store) rearmLeaseExpiryIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	indexKey := leaseExpiryExecIndexKey(t)
	score, ok, err := s.earliestLeaseScore(ctx, t, id)
	if err != nil {
		s.noteLeaseIndexFailure("read", err)
		return
	}
	if ok {
		if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: score, Member: string(id)}).Err(); err != nil {
			s.noteLeaseIndexFailure("add", err)
			return
		}
	} else if err := s.rdb.ZRem(ctx, indexKey, string(id)).Err(); err != nil {
		s.noteLeaseIndexFailure("remove", err)
		return
	}
	verified, stillOK, err := s.earliestLeaseScore(ctx, t, id)
	if err != nil {
		s.noteLeaseIndexFailure("verify", err)
		return
	}
	if !stillOK || (ok && verified >= score) {
		return
	}
	if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: verified, Member: string(id)}).Err(); err != nil {
		s.noteLeaseIndexFailure("repair", err)
	}
}

// earliestLeaseScore reports the smallest score in one execution's lease ZSET.
func (s *Store) earliestLeaseScore(ctx context.Context, t namespace.Namespace, id types.ExecutionID) (float64, bool, error) {
	scored, err := s.rdb.ZRangeWithScores(ctx, leaseExpiryZSetKey(t, id), 0, 0).Result()
	if err != nil {
		return 0, false, fmt.Errorf("read lease index for %q: %w", id, err)
	}
	if len(scored) == 0 {
		return 0, false, nil
	}
	return scored[0].Score, true, nil
}

// dueLeaseExecutions returns up to limit executions whose index score is at or
// before maxMs. ZRangeByScore, never ZRangeArgs — Redis 5.0, see the sweep.
func (s *Store) dueLeaseExecutions(ctx context.Context, t namespace.Namespace, maxMs string, limit int) ([]string, error) {
	return s.rdb.ZRangeByScore(ctx, leaseExpiryExecIndexKey(t), &redis.ZRangeBy{
		Min: "-inf", Max: maxMs, Offset: 0, Count: int64(limit),
	}).Result()
}

// pruneOrphanLeaseIndex drops index members whose per-execution lease ZSET no
// longer exists. It is a best-effort accelerator ahead of the reclaim read: a
// failure is logged and the sweep continues, exactly like the walk. It runs
// detached from the caller's cancellation because the sweep may be abandoned
// while the prune is still useful work.
//
// Old members are examined in rounds. kept counts the members this call found
// alive, and is the next round's offset: executions with a real expired lease
// whose reclaim has not completed stay at the head of the index (their members
// are never removed unless the lease goes away), so an offset of 0 would read
// the same head members every round and never reach the orphans behind them.
// The offset only ever grows to chunk*rounds, never to the size of the index.
//
// Each round reads the members with one ZRANGEBYSCORE, checks them with one
// pipeline of single-key EXISTS, and removes every orphan with one variadic
// ZREM. The pipeline is deliberately the non-transactional one, and each EXISTS
// names exactly one key: the per-execution keys carry {exec-id} hash tags and
// sit on different slots, and MULTI is unavailable on the cluster proxy, so no
// command may span several of them.
func (s *Store) pruneOrphanLeaseIndex(ctx context.Context, t namespace.Namespace, maxMs string) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	indexKey := leaseExpiryExecIndexKey(t)
	kept := 0
	for round := 0; round < leaseIndexPruneRounds; round++ {
		members, err := s.rdb.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
			Min: "-inf", Max: maxMs, Offset: int64(kept), Count: int64(leaseIndexPruneChunk),
		}).Result()
		if err != nil {
			s.noteLeaseIndexFailure("prune", err)
			return
		}
		if len(members) == 0 {
			return
		}
		pipe := s.rdb.Pipeline()
		exists := make([]*redis.IntCmd, len(members))
		for i, member := range members {
			exists[i] = pipe.Exists(ctx, leaseExpiryZSetKey(t, types.ExecutionID(member)))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			s.noteLeaseIndexFailure("prune", err)
			return
		}
		orphans := make([]string, 0, len(members))
		for i, cmd := range exists {
			if cmd.Val() == 0 {
				orphans = append(orphans, members[i])
				continue
			}
			kept++
		}
		if len(orphans) == 0 {
			return
		}
		removals := make([]any, len(orphans))
		for i, orphan := range orphans {
			removals[i] = orphan
		}
		if err := s.rdb.ZRem(ctx, indexKey, removals...).Err(); err != nil {
			s.noteLeaseIndexFailure("prune", err)
			return
		}
		// Verify, the same closure the re-arm uses. A producer writes the
		// per-execution ZSET before it registers in the index. One whose
		// registration landed before this ZREM had it removed with the orphans,
		// but its ZSET is visible here and it is re-armed to its exact minimum.
		// One whose ZSET appeared after the EXISTS above but whose ZADD lands
		// after this ZREM simply survives; the re-arm is then only a duplicate
		// ZADD. Neither ordering can lose a live lease.
		verify := s.rdb.Pipeline()
		alive := make([]*redis.IntCmd, len(orphans))
		for i, orphan := range orphans {
			alive[i] = verify.Exists(ctx, leaseExpiryZSetKey(t, types.ExecutionID(orphan)))
		}
		if _, err := verify.Exec(ctx); err != nil {
			s.noteLeaseIndexFailure("prune", err)
			return
		}
		for i, cmd := range alive {
			if cmd.Val() != 0 {
				s.rearmLeaseExpiryIndex(ctx, t, types.ExecutionID(orphans[i]))
			}
		}
		if len(members) < leaseIndexPruneChunk {
			return
		}
	}
}

// walkLeaseExpiryKeys advances the safety-net walk for one namespace, if it is
// due, and registers every execution whose lease ZSET it finds. See the file
// note for pacing and the staleness bound.
func (s *Store) walkLeaseExpiryKeys(ctx context.Context, t namespace.Namespace) error {
	s.leaseWalkMu.Lock()
	defer s.leaseWalkMu.Unlock()
	if s.leaseWalks == nil {
		s.leaseWalks = make(map[namespace.Namespace]*leaseWalkState)
	}
	st := s.leaseWalks[t]
	if st == nil {
		st = &leaseWalkState{}
		s.leaseWalks[t] = st
	}
	holding, recent, err := redisx.BeginWalk(ctx, s.rdb, string(t), leaseWalkKind, s.walkOwner, st.holding)
	if err != nil {
		return fmt.Errorf("acquire lease walk lock: %w", err)
	}
	if !holding {
		*st = leaseWalkState{}
		return nil
	}
	if !st.holding {
		*st = leaseWalkState{holding: true, backfilled: recent}
	}
	now := time.Now()
	pages := 0
	if st.backfilled {
		if now.Sub(st.lastStep) < leaseWalkStepInterval {
			return nil
		}
		size, err := s.rdb.DBSize(ctx).Result()
		if err != nil {
			return fmt.Errorf("size keyspace for lease walk: %w", err)
		}
		pages = int(max(1, min(redisx.WalkPagesPerStep(size, leaseWalkPageCount, leaseWalkRound, leaseWalkStepInterval), leaseWalkMaxPagesPerStep)))
	}
	for done := 0; !st.backfilled || done < pages; done++ {
		if done > 0 && done%redisx.WalkRenewEveryPages == 0 {
			if ok, err := redisx.RenewWalkLock(ctx, s.rdb, redisx.WalkLockKey(string(t), leaseWalkKind), s.walkOwner, redisx.WalkLockTTL); err != nil {
				return fmt.Errorf("renew lease walk lock: %w", err)
			} else if !ok {
				*st = leaseWalkState{}
				return nil
			}
		}
		keys, next, err := redisx.ScanPage(ctx, s.rdb, st.cursors, execScanPattern(t, "leases"), leaseWalkPageCount)
		if err != nil {
			return fmt.Errorf("walk lease indexes: %w", err)
		}
		st.cursors = next
		for _, key := range keys {
			keyNamespace, id, ok := parseNamespaceExecKey(key)
			if !ok || keyNamespace != t {
				continue
			}
			s.rearmLeaseExpiryIndex(ctx, t, id)
		}
		if cursorsAtStart(next) {
			st.backfilled = true
			if err := redisx.MarkWalkRoundDone(ctx, s.rdb, redisx.WalkDoneKey(string(t), leaseWalkKind), now, leaseWalkRound); err != nil {
				s.noteLeaseIndexFailure("walk-done", err)
			}
			break
		}
	}
	st.lastStep = now
	return nil
}

// cursorsAtStart reports whether every master's cursor is back at zero, i.e. a
// full round has completed.
func cursorsAtStart(c redisx.Cursors) bool {
	for _, v := range c {
		if v != 0 {
			return false
		}
	}
	return true
}

// noteLeaseIndexFailure logs a best-effort index failure, rate-limited like the
// outbox index's.
func (s *Store) noteLeaseIndexFailure(op string, err error) {
	if s.logger == nil {
		return
	}
	s.outboxIndexLogMu.Lock()
	recent := time.Since(s.leaseIndexLastLog) < outboxIndexLogInterval
	if !recent {
		s.leaseIndexLastLog = time.Now()
	}
	s.outboxIndexLogMu.Unlock()
	if recent {
		return
	}
	s.logger.Error("lease expiry index write failed; the lease walk still discovers this lease",
		"op", op, "err", err)
}
