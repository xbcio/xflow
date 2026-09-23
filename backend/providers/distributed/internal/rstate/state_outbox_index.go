package rstate

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file holds the outbox readiness index: the accelerator that lets a drain
// find ready work in time proportional to the READY BACKLOG instead of to the
// size of the keyspace.
//
// ---------------------------------------------------------------------------
// The defect it addresses
// ---------------------------------------------------------------------------
//
// ListOutboxExecutions discovers ready work by walking the keyspace with SCAN
// and filtering for `...:outbox:ready`. SCAN's COUNT counts keys EXAMINED, not
// keys matched, so at a keyspace of N keys one page reaches about page/N of the
// ready backlog and a cursor needs N/page drains to come all the way around.
// Discovery throughput therefore tracks the keyspace, not the backlog: at the
// reported 13k-67k keys and ~850 ready executions a page-256 drain surfaced
// roughly ten executions, and a full cursor round took some eighty drains. The
// deployment dispatched 68 executions/min against 364 created/min and the
// backlog grew while delivery itself was healthy.
//
// Raising the page (engine.DefaultOutboxDiscoveryPage) only moves the constant:
// costs stay O(keyspace) and no fixed page can track an unbounded keyspace.
//
// ---------------------------------------------------------------------------
// Why the index cannot be atomic, and why that is acceptable
// ---------------------------------------------------------------------------
//
// Every outbox mutation is a Lua script over keys that share the execution's
// `{id}` hash tag. keys.go documents that this is deliberate: the namespace
// prefix is brace-less precisely so the first `{` still opens the execution ID,
// keeping one execution's keys co-located (Lua CROSSSLOT never triggers) while
// different executions spread across slots. Redis Cluster refuses a script that
// touches a key in another slot, so a namespace-global ZSET cannot be written
// in the same atomic transition as the outbox body it indexes. Attempting it
// fails with "Lua script attempted to access a non local key in a cluster node".
//
// The resolution is to stop treating the index as part of the state machine.
// The outbox body and its per-execution ready ZSET remain the single source of
// truth, written atomically exactly as before. The index is a best-effort,
// eventually-consistent ACCELERATOR layered on top, and everything below
// follows from that one reframing:
//
//   - A missed registration is not data loss. The keyspace sweep (the same
//     mechanism that used to be the only path) still runs and still finds it.
//     An index write failure degrades throughput, never correctness.
//   - A stale entry is not a false execution, but it is not work either, and it
//     must never be counted as work. A member whose execution still has a ready
//     set costs one no-op claim that re-arms it; a member whose execution has no
//     outbox left is an ORPHAN, and it is removed by the read that found it,
//     because leaving it to a drain does not repair it when the drain is what
//     the orphan is starving. See readOutboxReadyIndex.
//   - An absent, empty, mistyped or disabled index changes nothing. The
//     dispatcher falls back to sweeping, which is what it did before.
//
// The precedent for a best-effort namespace-global registry in this store is
// namespace_registry.go (`xflow:namespaces`). That one is safe because it is
// append-only and never removes. This index does remove entries, which is where
// the care below goes.
//
// ---------------------------------------------------------------------------
// Layout and scoring
// ---------------------------------------------------------------------------
//
//	outboxReadyIndexKey(t) = xflow:ns:<t>:outbox:ready-index
//	    ZSET, member = execution ID, score = a LOWER BOUND on the instant that
//	    execution's next outbox entry becomes deliverable.
//
// The score is a lower bound rather than the exact minimum, and that is what
// makes registration cheap. Appending an entry registers the execution at
// "now" with a single ZADD and no read of the ready set; a drain that then
// finds nothing due re-arms the member to the exact minimum (or removes it).
// Because the score only ever understates readiness, an imprecise score can
// surface work EARLY, never late: it costs one no-op drain that repairs the
// entry, and it can never hide work behind the read cutoff.
//
// "Due" therefore means the same thing on the index as it does inside one
// execution: score <= now. One member per execution, so the index is bounded by
// the number of executions with work rather than by the number of entries.
//
// The key is deliberately NOT execution-scoped and NOT matching
// execScanPattern(t, "outbox:ready"): the sweep and OutboxMetrics must not see
// it, or the index would be scanned as if it were an execution's ready set.
// It carries no hash tag, so it lands in an arbitrary slot — which is fine
// because it is only ever accessed with ordinary commands (ZADD, ZREM,
// ZRANGEBYSCORE), never inside a multi-key script.
//
// The key has no expiry of its own. That is what makes an orphan possible: a
// member outlives the ready set it points at when the execution's own TTL takes
// the outbox first, or when a removal is lost — the flush's terminating claim,
// which is where removal used to live, reads before it removes and so removes
// nothing when that read fails (a canceled context is enough), and nothing else
// reaps the index. Discovery must therefore treat a member whose ready set is
// gone as not-work and repair it; the index does not clean up after itself by
// being returned.
//
// ---------------------------------------------------------------------------
// The two transitions that maintain it
// ---------------------------------------------------------------------------
//
// markOutboxReadyIndex is called after every atomic transition that APPENDS an
// outbox entry: execution create, entry-seed admission, commit (advance intent),
// advance (next-hop intents), retry reset, lease revocation, group commit and
// group-lease revocation, task expansion, suspend, signal delivery, and
// dead-letter replay. It is a single ZADD and needs no read.
//
// refreshOutboxReadyIndex is called when a CLAIM FINDS NOTHING. That is the one
// moment an execution's readiness is fully determined after a drain, because
// FlushOutbox loops until a claim comes back empty: every ack, release and
// dead-letter has happened, and every advance or skip intent the flush appended
// is already in the ready set. This is where the exact score is recorded, where
// a finished execution's member is removed, and where a member whose ready set
// was deleted (cancellation, failed create rollback) is pruned.
//
// ---------------------------------------------------------------------------
// (a) No entry can be lost
// ---------------------------------------------------------------------------
//
// Two mechanisms, one for each way a member can go missing.
//
//  1. REGISTRATION IS A SECOND, NON-ATOMIC STEP, so it can be skipped (process
//     death between the atomic transition and the ZADD, or a Redis error). The
//     keyspace sweep is what bounds that: ListOutboxExecutions still scans, on a
//     throttled cadence, and every execution it finds is flushed, and every
//     flush ends in a claim that finds nothing, which re-registers. So a missed
//     registration is recovered within
//
//     staleness bound = outboxIndexSweepEveryCalls drains + one cursor round
//
//     which on the reported deployment (≈0.9s drains, 13k keys, page 2048: a
//     round is ≈7 drains) is ≈17s. It is a bound on DELAY, never on delivery.
//     The bound only applies while the sweep is throttled at all, and it is
//     throttled only once the index has proven it carries work — see (d).
//
//  2. THE PRUNE RACES A RE-REGISTRATION, and that race is closed rather than
//     merely bounded — see the protocol below.
//
// ---------------------------------------------------------------------------
// The prune protocol: prune, verify, repair
// ---------------------------------------------------------------------------
//
// refreshOutboxReadyIndex removes a member when its execution has no ready
// entries left. That removal cannot be atomic with the ready ZSET it reads
// (different slots), so a prune can interleave with a producer that just
// appended an entry and registered it:
//
//	prune reads ready -> empty          (t1)
//	producer appends entry E            (t2)
//	producer registers: ZADD index      (t3)
//	prune removes:      ZREM index      (t4)   <- the fresh registration is gone
//
// Left alone this loses a REGISTRATION, not work: E is still in the ready ZSET
// and still in the body hash, so the sweep finds it. The cost would be the
// staleness bound above.
//
// It is nevertheless closed, by making every prune read the ready set AGAIN
// after its own ZREM and re-register if it is no longer empty:
//
//	prune removes:      ZREM index      (t4)
//	prune verifies: read ready -> E     (t5)   <- arrives after t4, sees t2
//	prune repairs:      ZADD index      (t6)
//
// t5 is ordered after t4 on the same connection, so it observes every append
// that preceded t4 — including t2. The only remaining interleavings are
// harmless: a producer that appends after t5 also registers after t5, so its
// ZADD outlives the ZREM. The argument is inductive over concurrent pruners too,
// because each pruner verifies after its own ZREM and every producer registers
// after its own append, so the last write to the index always reflects a read
// that is no older than itself. A ZADD racing a ZADD is benign by comparison:
// the worst case is a stale-early score, which costs one no-op drain.
//
// The verify read is paid ONLY on the prune branch, which is what keeps the
// common path (registration) at two round trips.
//
// ---------------------------------------------------------------------------
// (b) No entry can be duplicated
// ---------------------------------------------------------------------------
//
// The index is a DISCOVERY hint, never a delivery permission. Delivery is still
// gated by the per-entry lease (LeaseOutbox / engine.OutboxDeliveryLeaseTTL),
// which is unchanged: finding an execution twice — from the index and from the
// sweep, or from two drains — can at most cause a second FlushOutbox that finds
// every entry already leased. TestOutboxRediscoveryCannotRedeemALeasedEntry
// pins that property for the sweep; the index adds no new way to bypass it.
//
// ---------------------------------------------------------------------------
// (c) Cluster correctness
// ---------------------------------------------------------------------------
//
// No key layout changes. Every existing atomic transition still touches only
// keys under the execution's `{id}` hash tag, and the index is written by
// ordinary single-key commands issued from Go, outside any script. No new bare
// SCAN is introduced (the AST guard in redisx pins the audited call sites), and
// the index key does not match the sweep or metrics scan patterns.
//
// ---------------------------------------------------------------------------
// (d) Operation without the index
// ---------------------------------------------------------------------------
//
// Three independent ways the store runs without it, all of them ending at the
// keyspace sweep:
//
//   - ConfigureOutboxReadyIndex(false) disables reading and writing it.
//   - A backend with no index at all (the local provider's memoryState) is
//     unaffected: the engine only ever calls ListOutboxExecutions.
//   - A Redis error on the index read abandons the index for that call and
//     sweeps immediately.
//
// The sweep is throttled only once the index has PROVEN it carries work in this
// process (outboxIndexProven, set by a discovery read that returned at least
// one execution — the only direct evidence that the index is delivering). Until
// then — a cold store, a deployment whose registrations are failing, an index
// that is simply never written — every discovery call sweeps, which is exactly
// the pre-index behaviour. The signal is deliberately the READ side: a
// registration succeeding proves only that ZADD works, while a read returning
// work proves the loop the dispatcher actually depends on is closed.
//
// The residual this leaves: an index that carried work and then stopped
// registering keeps the sweep at one call in ten. That is the state
// xflow_outbox_ready rising against a flat xflow_outbox_drain_discovered
// reports, and ConfigureOutboxReadyIndex(false) is the operator's way back.
//
// ---------------------------------------------------------------------------
// What is verified, and what is not
// ---------------------------------------------------------------------------
//
// Verified by construction: cluster-safety (single-key commands only), the
// no-duplication property (the lease is untouched), and the fallback paths.
// Verified by test: registration on a due transition, removal once drained, the
// stale-entry repair, the prune race, the sweep fallback with a stated bound,
// and the discovery cost claim. NOT verified: behaviour against a real Redis
// Cluster. miniredis is single-slot and single-node, so it cannot exercise
// CROSSSLOT, slot migration, or a multi-master SCAN. The design is what keeps
// that gap from being a correctness gap: the index is written with plain
// commands, so cluster rules cannot make it fail in a way the fallback does not
// already cover.
const (
	// outboxReadyIndexSuffix is the index key's suffix within a namespace. It
	// is not one of the execution-scoped suffixes in keys.go and must stay
	// that way: the sweep and metrics scans match `exec:{*}:outbox:ready` and
	// must never treat the index as an execution's ready set.
	outboxReadyIndexSuffix = "outbox:ready-index"

	// outboxIndexSweepEveryCalls is how often the keyspace sweep still runs
	// while the index is proven to be carrying work. It is the staleness bound
	// on a missed registration, in drains: a registration that never happened
	// is found by the sweep, and a full cursor round has to complete on top of
	// that. Ten keeps the sweep's cost amortized while leaving the bound in the
	// tens of seconds at the drain cadence a real deployment runs at.
	//
	// It does not apply until the index has proven itself; see the file note.
	outboxIndexSweepEveryCalls = 10

	// outboxIndexLogInterval bounds how often a best-effort index failure is
	// logged. It is a floor between lines, not a sample rate, so the first
	// failure of an outage is always visible and the rest of it is not.
	outboxIndexLogInterval = time.Minute
)

// outboxReadyIndexKey returns the readiness index for one namespace.
func outboxReadyIndexKey(t namespace.Namespace) string {
	return fmt.Sprintf("xflow:ns:%s:%s", t, outboxReadyIndexSuffix)
}

// outboxIndexEnabled reports whether the readiness index is in use.
func (s *Store) outboxIndexEnabled() bool { return s.outboxIndexOn.Load() }

// markOutboxReadyIndex registers an execution as having ready work, scored at
// now — a lower bound, deliberately, so registration needs one command and no
// read of the ready set.
//
// It is called after an atomic transition that appended an outbox entry, so a
// newly ready execution becomes discoverable without waiting for a sweep. The
// transition has already been applied by Redis, so this is best-effort by
// contract: it reports nothing and never fails its caller. A registration that
// is skipped is recovered by the keyspace sweep within the bound the file note
// states.
func (s *Store) markOutboxReadyIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	if !s.outboxIndexEnabled() {
		return
	}
	if err := s.rdb.ZAdd(ctx, outboxReadyIndexKey(t), redis.Z{
		Score:  float64(time.Now().UTC().UnixMilli()),
		Member: string(id),
	}).Err(); err != nil {
		s.noteOutboxIndexFailure(ctx, "mark", err)
	}
}

// refreshOutboxReadyIndex makes the index agree with the execution's ready ZSET
// for this instant: the exact earliest ready score, or no member at all when
// the execution has nothing ready.
//
// It is called after a claim that found nothing, which is the one moment an
// execution's readiness is fully determined after a drain: FlushOutbox loops
// until a claim comes back empty, so that call happens after every ack, release
// and dead-letter the flush performed, and after any advance or skip intent it
// appended. It is what re-arms a member that was registered optimistically at
// "now" to the instant its next entry actually becomes deliverable, what
// removes a member whose work is gone, and what keeps a drained execution from
// being rediscovered on every tick forever.
func (s *Store) refreshOutboxReadyIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID) {
	if !s.outboxIndexEnabled() {
		return
	}
	score, ready, err := s.earliestReadyScore(ctx, t, id)
	if err != nil {
		s.noteOutboxIndexFailure(ctx, "read", err)
		return
	}
	s.applyOutboxReadyIndex(ctx, t, id, score, ready)
}

// applyOutboxReadyIndex is refreshOutboxReadyIndex given the ready set's state
// instead of reading it.
//
// A caller that has just read that state atomically, on the same key and in the
// same command as the transition it is reporting on, has a strictly better
// answer than a fresh ZRANGE: the claim script sees the ready set with the lease
// already applied, so no append can slip between its read and the lease. Passing
// it in is what lets a claim-finds-nothing flush re-arm the index without paying
// a second round trip for a read it has already done — the difference between
// three round trips per execution and two, which on a link whose round trip is
// ~80ms is the difference between a dispatcher that keeps up and one that does
// not.
//
// readScore is only meaningful when ready; a caller that knows the ready set is
// gone passes ready=false.
func (s *Store) applyOutboxReadyIndex(ctx context.Context, t namespace.Namespace, id types.ExecutionID, readScore float64, ready bool) {
	if !s.outboxIndexEnabled() {
		return
	}
	indexKey := outboxReadyIndexKey(t)
	if ready {
		if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: readScore, Member: string(id)}).Err(); err != nil {
			s.noteOutboxIndexFailure(ctx, "add", err)
		}
		return
	}
	if err := s.rdb.ZRem(ctx, indexKey, string(id)).Err(); err != nil {
		s.noteOutboxIndexFailure(ctx, "remove", err)
		return
	}
	// Prune, then verify, then repair — see the file note for why the verify
	// read is what makes this prune incapable of dropping a registration a
	// concurrent producer just wrote.
	score, ready, err := s.earliestReadyScore(ctx, t, id)
	if err != nil {
		s.noteOutboxIndexFailure(ctx, "verify", err)
		return
	}
	if !ready {
		return
	}
	if err := s.rdb.ZAdd(ctx, indexKey, redis.Z{Score: score, Member: string(id)}).Err(); err != nil {
		s.noteOutboxIndexFailure(ctx, "repair", err)
	}
}

// earliestReadyScore reports the smallest score in one execution's ready ZSET,
// and whether the execution has any ready member at all.
//
// ZRANGE ... 0 0 WITHSCORES ascends by score, so the first element is the
// earliest instant the execution has deliverable work. The key is
// execution-scoped, so this command is slot-local — the read never fights the
// atomic transitions it is reading about.
func (s *Store) earliestReadyScore(ctx context.Context, t namespace.Namespace, id types.ExecutionID) (float64, bool, error) {
	scored, err := s.rdb.ZRangeWithScores(ctx, outboxReadyKey(t, id), 0, 0).Result()
	if err != nil {
		return 0, false, fmt.Errorf("read ready outbox for %q: %w", id, err)
	}
	if len(scored) == 0 {
		return 0, false, nil
	}
	return scored[0].Score, true, nil
}

// readOutboxReadyIndex adds this namespace's due executions to ids, up to
// limit, and reports how many it added.
//
// "Due" is the same predicate the per-execution ready ZSET uses: score <= now.
// A member scored in the future is either leased by a live deliverer or waiting
// out a retry backoff, and neither is deliverable in this tick — exactly the
// distinction OutboxMetrics draws between Pending and Ready.
//
// THE SCORE IS NOT THE WHOLE PREDICATE: a member is work only if the execution
// it names still has a ready set at all. The index key outlives the keys it
// points at — it has no TTL of its own (see the file note) while the ready set
// expires with the execution — so a member whose ready set is gone survives as
// an orphan and is, by its score, indistinguishable from work that is ready. An
// orphan is not work, and returning one as if it were is why this read must not
// stop at the member list:
//
//   - It spends the page budget the index exists to spend on work, so a head of
//     orphans HIDES the live backlog behind it. Discovery returns nothing but
//     dead ids for as long as the head is dead.
//   - `added` is what proves the index to ListOutboxExecutions, so a page of
//     orphans latched the index as carrying work and throttled away the keyspace
//     sweep — the one mechanism that would have found the work they hid. That
//     latch is per-process and the orphans are in Redis, which is why a restart
//     restored service while the orphans stayed to block again.
//
// So a member whose outbox is gone is skipped, counted as nothing, and repaired
// here — this read is the cheapest place to notice it, because discovery has
// already paid for the member list. See repairStaleOutboxIndexMembers.
//
// The members are read in batches and the loop continues past a batch that was
// all orphans, so one call reaches the work behind a dead head instead of
// returning it a page at a time. It terminates because every batch either adds
// at least one live id, removes at least one stale member, or ends the read.
func (s *Store) readOutboxReadyIndex(ctx context.Context, t namespace.Namespace, now time.Time, limit int, ids map[types.ExecutionID]struct{}) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	indexKey := outboxReadyIndexKey(t)
	cutoff := strconv.FormatInt(now.UTC().UnixMilli(), 10)
	added := 0
	for {
		members, err := s.rdb.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    cutoff,
			Offset: 0,
			Count:  int64(limit),
		}).Result()
		if err != nil {
			return added, fmt.Errorf("read outbox readiness index for namespace %q: %w", t, err)
		}
		if len(members) == 0 {
			return added, nil
		}
		live, err := s.outboxMembersLive(ctx, t, members)
		if err != nil {
			return added, err
		}
		var stale []string
		for _, member := range members {
			if !live[member] {
				stale = append(stale, member)
				continue
			}
			id := types.ExecutionID(member)
			if _, seen := ids[id]; seen {
				continue
			}
			ids[id] = struct{}{}
			added++
			if added >= limit {
				break
			}
		}
		if len(stale) > 0 {
			if err := s.repairStaleOutboxIndexMembers(ctx, indexKey, t, now, stale); err != nil {
				return added, err
			}
		}
		if added >= limit {
			return added, nil
		}
		if len(stale) == 0 {
			// Every due member at the head is live and already collected. A live
			// member is deliberately not removed, so re-reading would return the
			// same head forever; whatever is behind it is not this call's budget.
			return added, nil
		}
		// The head advanced by the removals. Read again to reach the work behind
		// the orphans rather than reporting an empty page over a live backlog.
	}
}

// outboxMembersLive reports, per index member, whether the execution it names
// still has a ready ZSET to lease from — which is the index's own membership
// contract, "a member exists exactly when there is work behind it", the same
// predicate refreshOutboxReadyIndex and cleanupCreatedExecution decide removal
// by. Redis drops an empty ZSET, so the key being present means the execution
// has ready entries.
//
// The BODY hash is deliberately not consulted. A member whose ready set survives
// without its body is not an orphan: its first claim pops the ready member,
// finds no body, and the terminating empty claim removes the index member with
// it — the documented one-no-op-claim path, which is already tested. Judging it
// stale here instead would remove a member the ready set still justifies, which
// is the lost-registration direction this file is careful to avoid.
//
// The whole page is probed in ONE pipeline rather than one round trip per
// member, which is the package's idiom for a bounded batch of per-execution
// reads.
func (s *Store) outboxMembersLive(ctx context.Context, t namespace.Namespace, members []string) (map[string]bool, error) {
	pipe := s.rdb.Pipeline()
	exists := make([]*redis.IntCmd, len(members))
	for i, member := range members {
		exists[i] = pipe.Exists(ctx, outboxReadyKey(t, types.ExecutionID(member)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		// Report rather than answer: a probe that failed is not evidence that a
		// member is stale, and the caller's fallback (sweep) is strictly safer
		// than removing a member on an error.
		return nil, fmt.Errorf("probe ready sets for %d index members in namespace %q: %w", len(members), t, err)
	}
	live := make(map[string]bool, len(members))
	for i, cmd := range exists {
		live[members[i]] = cmd.Val() > 0
	}
	return live, nil
}

// repairStaleOutboxIndexMembers removes index members whose execution has no
// outbox left, and puts back any that reappears while it does.
//
// The removal cannot be atomic with the keys it judges: the index is
// namespace-global and those keys are execution-scoped, so a Redis Cluster
// script cannot touch both (see the file note) — the same slot boundary that
// keeps the index out of the atomic transitions. A check-then-remove is
// therefore the strongest form available, and it leaves one interleaving: an
// execution that drained to nothing appends its next entry and re-registers
// between this removal's probe and its ZREM. That is not exotic — Redis drops an
// empty ZSET and an empty HASH, so a fully drained execution has no outbox keys
// at all while it is still alive and about to append.
//
// It is closed the way refreshOutboxReadyIndex closes its own prune: probe,
// remove, then probe AGAIN and re-register what is there. The second probe is
// sent only after the ZREM's reply has been read, which is what orders it after
// the removal on any topology — including a real cluster, where the two
// commands reach different nodes — because any append it must observe was
// executed before the removal it is ordered after.
//
// Recovery does not depend on that: a re-registration this still misses is
// found by the keyspace sweep within the bound the file note states, exactly as
// any other missed registration always was. The removal is cheap where the
// removal this replaces was not: it no longer needs a flush to run, to find
// work, or to have a live context.
func (s *Store) repairStaleOutboxIndexMembers(ctx context.Context, indexKey string, t namespace.Namespace, now time.Time, stale []string) error {
	zrem := make([]any, len(stale))
	for i, member := range stale {
		zrem[i] = member
	}
	pipe := s.rdb.Pipeline()
	pipe.ZRem(ctx, indexKey, zrem...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove %d stale outbox readiness index members for namespace %q: %w", len(stale), t, err)
	}
	live, err := s.outboxMembersLive(ctx, t, stale)
	if err != nil {
		return err
	}
	pipe = s.rdb.Pipeline()
	repaired := 0
	for _, member := range stale {
		if !live[member] {
			continue
		}
		// Scored at the read's own cutoff, the lower bound markOutboxReadyIndex
		// writes, so a member removed a moment before it reappeared is due on the
		// next call instead of waiting out a sweep interval.
		pipe.ZAdd(ctx, indexKey, redis.Z{
			Score:  float64(now.UTC().UnixMilli()),
			Member: member,
		})
		repaired++
	}
	if repaired == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("re-register %d outbox readiness index members for namespace %q: %w", repaired, t, err)
	}
	return nil
}

// noteOutboxIndexFailure records a best-effort index failure. It is deliberately
// a log line and not an error: the caller has already applied its transition,
// and the sweep is the recovery path, so there is nothing to propagate to.
//
// The log is rate-limited because this runs on the outbox hot path: an outage
// that fails every refresh would otherwise produce one line per transition.
func (s *Store) noteOutboxIndexFailure(ctx context.Context, op string, err error) {
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
	s.logger.Error("outbox readiness index refresh failed; the keyspace sweep still discovers this work",
		"op", op, "err", err)
}
