package rstate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/redisx"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file lets OutboxMetrics read namespace indexes instead of walking the
// keyspace. It follows the protocol in state_outbox_index.go: the per-execution
// ZSETs stay the source of truth and the indexes are best-effort accelerators.
//
// READY SIDE. Metrics iterate outboxReadyIndexKey(t). That index can stand in
// for the old `exec:{*}:outbox:ready` scan, including Pending and the oldest
// age, because its membership is "the ready ZSET is non-empty", not "something
// is due":
//
//   - every append path calls markOutboxReadyIndex (see that file's list);
//   - a lease or retry backoff moves a member's score forward but keeps it in
//     the ready ZSET, and the claim script then reports the head score, leased
//     members included, so applyOutboxReadyIndex re-arms rather than prunes;
//   - the only removals (applyOutboxReadyIndex with ready=false and
//     repairStaleOutboxIndexMembers) happen when the ready ZSET is empty or
//     gone, and both verify after removing.
//
// A pending-but-not-due execution is therefore in the index, scored in the
// future. The remaining gap is a lost registration, which the walk below covers.
//
// DEAD SIDE. outboxDeadIndexKey(t) is a SET of executions that may hold dead
// letters. The only ZADD into outbox:dead is recordOutboxFailureLua, so
// RecordOutboxFailure registers when it reports DeadLettered. Replay, delete,
// TTL and cancellation only shrink the dead ZSET; metrics prune the member once
// it is empty, with the same remove-then-verify step the ready index uses, so a
// dead letter written between the check and the SREM is put back.
//
// SAFETY NET. A registration can still be lost (process death between the
// script and the index write, a Redis error, data written before this index
// existed). walkOutboxMetricsKeyspace advances one ScanPage cursor per
// namespace over `exec:{*}:outbox:*` and registers every ready or dead key it
// finds. Until the first full round after process start it walks the whole
// keyspace in one call (backfill); after that it is throttled to roughly one
// round per outboxMetricsWalkRound. So a lost registration is recovered within
// outboxMetricsWalkRound plus one metrics interval.
//
// These are gauges. During backfill, and for up to that bound after a lost
// registration, an execution can be missing from the reading; the next reading
// corrects it. Rank-paged reads of the ready index can likewise skip a member
// that moves while the read runs. Neither is worse than the old ScanAll, which
// was not a snapshot either.
const (
	// outboxDeadIndexSuffix must not match execScanPattern globs (no `exec:{`).
	outboxDeadIndexSuffix = "outbox:dead-index"

	// outboxMetricsIndexPage is how many index members one read fetches.
	outboxMetricsIndexPage = 512

	// outboxMetricsWalkPage is the COUNT hint of one safety-net SCAN page.
	outboxMetricsWalkPage = 1000

	// outboxMetricsWalkRound is the target time for one throttled keyspace
	// round. At 2.35M keys and a 30s metrics interval this is ~118 pages of
	// 1000 per call, against two full COUNT-128 walks per call before.
	outboxMetricsWalkRound = 10 * time.Minute
)

// outboxMetricsWalkState is one namespace's safety-net walk.
type outboxMetricsWalkState struct {
	cursors    redisx.Cursors
	backfilled bool
	last       time.Time
	// credit is how many keys the walk may examine, accrued from elapsed time.
	credit float64
}

func outboxDeadIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:%s", t, outboxDeadIndexSuffix)
}

// markOutboxDeadIndex registers an execution as possibly holding dead letters.
// It runs after the dead letter is written and never fails its caller.
func (s *Store) markOutboxDeadIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	if err := s.rdb.SAdd(ctx, outboxDeadIndexKey(t), string(id)).Err(); err != nil {
		s.noteOutboxMetricsIndexFailure("dead-mark", err)
	}
}

// readyOutboxMetricsFromIndex folds every execution in the ready index into
// the snapshot, reading members by rank.
func (s *Store) readyOutboxMetricsFromIndex(ctx context.Context, t namespace.Namespace, cutoff string, snapshot *engine.OutboxMetricsSnapshot) error {
	indexKey := outboxReadyIndexKey(t)
	seen := make(map[string]struct{})
	for start := int64(0); ; start += outboxMetricsIndexPage {
		members, err := s.rdb.ZRange(ctx, indexKey, start, start+outboxMetricsIndexPage-1).Result()
		if err != nil {
			return fmt.Errorf("read outbox readiness index for namespace %q: %w", t, err)
		}
		for _, member := range members {
			if _, dup := seen[member]; dup {
				continue
			}
			seen[member] = struct{}{}
			if err := s.addReadyOutboxMetrics(ctx, outboxReadyKey(t, types.ExecutionID(member)), cutoff, snapshot); err != nil {
				return err
			}
		}
		if len(members) < outboxMetricsIndexPage {
			return nil
		}
	}
}

// deadOutboxMetricsFromIndex counts dead letters across the dead index and
// prunes members whose dead ZSET is empty.
func (s *Store) deadOutboxMetricsFromIndex(ctx context.Context, t namespace.Namespace, snapshot *engine.OutboxMetricsSnapshot) error {
	indexKey := outboxDeadIndexKey(t)
	var cursor uint64
	for {
		members, next, err := s.rdb.SScan(ctx, indexKey, cursor, "", outboxMetricsIndexPage).Result()
		if err != nil {
			return fmt.Errorf("read outbox dead-letter index for namespace %q: %w", t, err)
		}
		for _, member := range members {
			count, err := s.rdb.ZCard(ctx, outboxDeadKey(t, types.ExecutionID(member))).Result()
			if err != nil {
				return fmt.Errorf("count dead-letter outbox %q: %w", member, err)
			}
			if count > 0 {
				snapshot.DeadLettered += int(count)
				continue
			}
			s.pruneOutboxDeadIndex(ctx, t, member)
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// pruneOutboxDeadIndex removes a member whose dead ZSET was empty, then checks
// again and puts it back if a dead letter arrived meanwhile. The check is sent
// after the SREM reply, so it observes any dead letter written before the SREM;
// one written after it registers after it too.
func (s *Store) pruneOutboxDeadIndex(ctx context.Context, t namespace.Namespace, member string) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	indexKey := outboxDeadIndexKey(t)
	if err := s.rdb.SRem(ctx, indexKey, member).Err(); err != nil {
		s.noteOutboxMetricsIndexFailure("dead-remove", err)
		return
	}
	exists, err := s.rdb.Exists(ctx, outboxDeadKey(t, types.ExecutionID(member))).Result()
	if err != nil {
		s.noteOutboxMetricsIndexFailure("dead-verify", err)
		return
	}
	if exists == 0 {
		return
	}
	if err := s.rdb.SAdd(ctx, indexKey, member).Err(); err != nil {
		s.noteOutboxMetricsIndexFailure("dead-repair", err)
	}
}

// walkOutboxMetricsKeyspace advances the safety-net walk for one namespace and
// registers what it finds. Failures are logged; the indexes are still read.
func (s *Store) walkOutboxMetricsKeyspace(ctx context.Context, t namespace.Namespace) {
	s.outboxMetricsWalkMu.Lock()
	defer s.outboxMetricsWalkMu.Unlock()
	if s.outboxMetricsWalks == nil {
		s.outboxMetricsWalks = make(map[namespace.Namespace]*outboxMetricsWalkState)
	}
	w := s.outboxMetricsWalks[t]
	if w == nil {
		w = &outboxMetricsWalkState{}
		s.outboxMetricsWalks[t] = w
	}
	now := time.Now()
	pages := -1 // backfill: until the round completes
	if w.backfilled {
		size, err := s.rdb.DBSize(ctx).Result()
		if err != nil {
			s.noteOutboxMetricsIndexFailure("walk-size", err)
			return
		}
		elapsed := now.Sub(w.last)
		if elapsed > outboxMetricsWalkRound {
			elapsed = outboxMetricsWalkRound
		}
		w.credit += float64(size) * float64(elapsed) / float64(outboxMetricsWalkRound)
		w.last = now
		// A keyspace smaller than one page still gets one page per round.
		threshold := float64(min(int64(outboxMetricsWalkPage), max(size, 1)))
		pages = 0
		for w.credit >= threshold {
			w.credit -= float64(outboxMetricsWalkPage)
			pages++
		}
		if w.credit < 0 {
			w.credit = 0
		}
		if pages == 0 {
			return
		}
	}
	pattern := execScanPattern(t, "outbox:*")
	for i := 0; pages < 0 || i < pages; i++ {
		keys, next, err := redisx.ScanPage(ctx, s.rdb, w.cursors, pattern, outboxMetricsWalkPage)
		if err != nil {
			s.noteOutboxMetricsIndexFailure("walk", err)
			return
		}
		w.cursors = next
		s.registerOutboxMetricsKeys(ctx, t, keys)
		if cursorsDone(next) {
			if !w.backfilled {
				w.backfilled = true
				w.last = now
			}
			if pages < 0 {
				return
			}
		}
	}
}

// cursorsDone reports whether every master's cursor came back to zero.
func cursorsDone(c redisx.Cursors) bool {
	for _, v := range c {
		if v != 0 {
			return false
		}
	}
	return true
}

// registerOutboxMetricsKeys registers walk-found ready and dead keys in one
// pipeline. Ready members are added with NX so an exact score written by a
// claim is kept; a new one is scored at now, the usual lower bound.
func (s *Store) registerOutboxMetricsKeys(ctx context.Context, t namespace.Namespace, keys []string) {
	ctx, cancel := detachedIndexContext(ctx)
	defer cancel()
	pipe := s.rdb.Pipeline()
	queued := 0
	nowMs := float64(time.Now().UTC().UnixMilli())
	for _, key := range keys {
		id, ok := executionIDFromKey(key)
		if !ok {
			continue
		}
		switch {
		case strings.HasSuffix(key, ":outbox:ready"):
			if !s.outboxIndexEnabled() {
				continue
			}
			pipe.ZAddNX(ctx, outboxReadyIndexKey(t), redis.Z{Score: nowMs, Member: string(id)})
		case strings.HasSuffix(key, ":outbox:dead"):
			pipe.SAdd(ctx, outboxDeadIndexKey(t), string(id))
		default:
			continue
		}
		queued++
	}
	if queued == 0 {
		return
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.noteOutboxMetricsIndexFailure("walk-register", err)
	}
}

// noteOutboxMetricsIndexFailure logs a best-effort metrics-index failure, rate
// limited with the readiness index's log. It does not touch outboxIndexProven:
// these writes are not discovery evidence.
func (s *Store) noteOutboxMetricsIndexFailure(op string, err error) {
	if s.logger == nil {
		return
	}
	s.outboxIndexLogMu.Lock()
	recent := time.Since(s.outboxIndexLastLog) < outboxIndexLogInterval
	if !recent {
		s.outboxIndexLastLog = time.Now()
	}
	s.outboxIndexLogMu.Unlock()
	if recent {
		return
	}
	s.logger.Error("outbox metrics index maintenance failed; the keyspace walk still recovers it",
		"op", op, "err", err)
}
